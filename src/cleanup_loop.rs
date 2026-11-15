//! Stale-state cleanup loop shared by providers and directly built limiters.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
};

use crate::builder::CleanupConfig;

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
use crate::{
    TrypemaError,
    runtime::{new_interval, spawn_task, tick},
};

/// Start and stop state of the stale-state cleanup loop of one owner.
///
/// A loop holds only a `Weak` reference to its owner, so it ends at its next wake after the last
/// `Arc` drops. Each start takes a new generation and each stop advances it, so a loop from before
/// a stop ends even when a later start runs before it wakes.
#[derive(Debug)]
pub(crate) struct CleanupLoop {
    config: CleanupConfig,
    is_running: AtomicBool,
    generation: AtomicU64,
}

impl CleanupLoop {
    pub(crate) fn new(config: CleanupConfig) -> Self {
        Self {
            config,
            is_running: AtomicBool::new(false),
            generation: AtomicU64::new(0),
        }
    } // end constructor

    /// Return the generation of a new loop, or `None` when a loop already runs.
    fn begin(&self) -> Option<u64> {
        if self.is_running.swap(true, Ordering::AcqRel) {
            return None;
        }

        Some(
            self.generation
                .fetch_add(1, Ordering::AcqRel)
                .wrapping_add(1),
        )
    } // end fn begin

    fn is_current(&self, generation: u64) -> bool {
        self.is_running.load(Ordering::Acquire)
            && self.generation.load(Ordering::Acquire) == generation
    } // end fn is_current

    pub(crate) fn stop(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.is_running.store(false, Ordering::Release);
    } // end method stop

    /// Start a loop on its own thread that calls `cleanup` once per interval.
    ///
    /// The thread keeps no strong reference to `owner` while it sleeps. It owns `cleanup` and
    /// drops it when the loop ends: at the first wake after the last `Arc` of `owner` drops or
    /// after [`stop`](Self::stop). It is dropped at once when a loop already runs.
    pub(crate) fn start_thread<T, F>(
        owner: &Arc<T>,
        cleanup_loop: fn(&T) -> &CleanupLoop,
        cleanup: F,
    ) where
        T: Send + Sync + 'static,
        F: Fn(&T, u64) + Send + 'static,
    {
        let Some(generation) = cleanup_loop(owner).begin() else {
            return;
        };

        let weak_owner = Arc::downgrade(owner);

        thread::spawn(move || {
            loop {
                let Some(owner) = weak_owner.upgrade() else {
                    break;
                };
                let interval = cleanup_loop(&owner).config.interval;
                drop(owner);
                thread::sleep(interval);

                let Some(owner) = weak_owner.upgrade() else {
                    break;
                };
                let state = cleanup_loop(&owner);
                if !state.is_current(generation) {
                    break;
                }
                cleanup(&owner, state.config.stale_after_ms());
            }
        });
    } // end method start_thread

    /// Start a loop on the async runtime that awaits `cleanup` once per interval.
    ///
    /// A failed pass logs `failure_message` and the loop tries again at the next interval. The
    /// task owns `cleanup` and drops it when the loop ends, as for
    /// [`start_thread`](Self::start_thread).
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    pub(crate) fn start_task<T, F, Fut>(
        owner: &Arc<T>,
        cleanup_loop: fn(&T) -> &CleanupLoop,
        cleanup: F,
        failure_message: &'static str,
    ) where
        T: Send + Sync + 'static,
        F: Fn(Arc<T>, u64) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), TrypemaError>> + Send,
    {
        let state = cleanup_loop(owner);
        let Some(generation) = state.begin() else {
            return;
        };

        let cleanup_interval = state.config.interval;
        let weak_owner = Arc::downgrade(owner);

        spawn_task(async move {
            let mut interval = new_interval(cleanup_interval);

            // Tokio intervals tick immediately; Smol intervals wait for the configured duration.
            #[cfg(feature = "redis-tokio")]
            tick(&mut interval).await;

            loop {
                tick(&mut interval).await;

                let Some(owner) = weak_owner.upgrade() else {
                    break;
                };

                let state = cleanup_loop(&owner);
                if !state.is_current(generation) {
                    break;
                }

                let stale_after_ms = state.config.stale_after_ms();
                if let Err(error) = cleanup(owner, stale_after_ms).await {
                    tracing::warn!(?error, "{failure_message}");
                }
            }
        });
    } // end method start_task
} // end impl

/// A clone has the same configuration and starts stopped, because a running loop belongs to the
/// `Arc` it was started from.
impl Clone for CleanupLoop {
    fn clone(&self) -> Self {
        Self::new(self.config)
    }
} // end impl
