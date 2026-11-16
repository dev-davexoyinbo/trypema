//! Stale-state cleanup loop of a limiter.

use std::{
    sync::{
        Arc, Mutex, PoisonError,
        mpsc::{self, RecvTimeoutError},
    },
    thread,
};

use crate::builder::CleanupConfig;

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
use futures::FutureExt;
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
use tokio::sync::oneshot;

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
use crate::{
    TrypemaError,
    runtime::{new_interval, spawn_task, tick},
};

/// A cleanup pass that a thread loop runs.
pub(crate) trait Cleanup: Send + Sync + 'static {
    /// Remove the state that has had no activity for `stale_after_ms`.
    fn cleanup(&self, stale_after_ms: u64);
}

/// A cleanup pass that a task loop runs on the async runtime.
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
pub(crate) trait AsyncCleanup: Send + Sync + 'static {
    /// The log text of a failed pass.
    const FAILURE_MESSAGE: &'static str;

    /// Remove the state that has had no activity for `stale_after_ms`.
    fn cleanup(&self, stale_after_ms: u64)
    -> impl Future<Output = Result<(), TrypemaError>> + Send;
}

/// The sending half of the stop channel of a running loop.
#[derive(Debug)]
enum StopSender {
    Thread(mpsc::Sender<()>),
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    Task(oneshot::Sender<()>),
}

/// Start and stop state of the stale-state cleanup loop of one owner.
///
/// A loop holds a `Weak` reference to its owner and the receiver of a stop channel. A stop
/// signal, or the drop of the sender when the owner drops, ends the loop at once.
#[derive(Debug)]
pub(crate) struct CleanupLoop {
    config: CleanupConfig,
    stop_sender: Mutex<Option<StopSender>>,
}

impl CleanupLoop {
    pub(crate) fn new(config: CleanupConfig) -> Self {
        Self {
            config,
            stop_sender: Mutex::new(None),
        }
    } // end constructor

    /// Start a loop on its own thread that runs one pass of `owner` per interval.
    ///
    /// The thread holds a strong reference to `owner` only during a pass. Calling while a loop
    /// runs is no-op.
    pub(crate) fn start_thread<T: Cleanup>(&self, owner: &Arc<T>) {
        let mut stop_sender = self
            .stop_sender
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if stop_sender.is_some() {
            return;
        }

        let (sender, receiver) = mpsc::channel();
        let cleanup_interval = self.config.interval;
        let stale_after_ms = self.config.stale_after_ms();
        let weak_owner = Arc::downgrade(owner);

        thread::spawn(move || {
            while let Err(RecvTimeoutError::Timeout) = receiver.recv_timeout(cleanup_interval) {
                let Some(owner) = weak_owner.upgrade() else {
                    break;
                };

                owner.cleanup(stale_after_ms);
            }
        });

        *stop_sender = Some(StopSender::Thread(sender));
    } // end method start_thread

    /// Start a loop on the async runtime that awaits one pass of `owner` per interval.
    ///
    /// A failed pass logs the failure message of `T`, and the loop tries again at the next
    /// interval. The task holds a strong reference to `owner` only during a pass. Calling while
    /// a loop runs is no-op.
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    pub(crate) fn start_task<T: AsyncCleanup>(&self, owner: &Arc<T>) {
        let mut stop_sender = self
            .stop_sender
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if stop_sender.is_some() {
            return;
        }

        let (sender, receiver) = oneshot::channel();
        let cleanup_interval = self.config.interval;
        let stale_after_ms = self.config.stale_after_ms();
        let weak_owner = Arc::downgrade(owner);

        spawn_task(async move {
            let mut stop = receiver.fuse();
            let mut interval = new_interval(cleanup_interval);

            // Tokio intervals tick immediately; Smol intervals wait for the configured duration.
            #[cfg(feature = "redis-tokio")]
            tick(&mut interval).await;

            loop {
                // A stop signal that is ready together with a tick ends the loop without a pass.
                futures::select_biased! {
                    _ = stop => break,
                    () = tick(&mut interval).fuse() => {}
                }

                let Some(owner) = weak_owner.upgrade() else {
                    break;
                };

                if let Err(error) = owner.cleanup(stale_after_ms).await {
                    tracing::warn!(?error, "{}", T::FAILURE_MESSAGE);
                }
            }
        });

        *stop_sender = Some(StopSender::Task(sender));
    } // end method start_task

    /// Send the stop signal to the running loop. Calling while stopped is no-op.
    pub(crate) fn stop(&self) {
        let Some(stop_sender) = self
            .stop_sender
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        else {
            return;
        };

        // A loop that already ended has dropped its receiver, so a failed send needs no action.
        match stop_sender {
            StopSender::Thread(sender) => {
                let _ = sender.send(());
            }
            #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
            StopSender::Task(sender) => {
                let _ = sender.send(());
            }
        }
    } // end method stop
} // end impl

/// A clone has the same configuration and starts stopped, because a running loop belongs to the
/// `Arc` it was started from.
impl Clone for CleanupLoop {
    fn clone(&self) -> Self {
        Self::new(self.config)
    }
} // end impl
