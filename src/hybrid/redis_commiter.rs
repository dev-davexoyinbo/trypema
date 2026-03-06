use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::FutureExt;
use tokio::sync::{Notify, mpsc};

use crate::redis::tick;
use crate::runtime::{TaskHandle, sleep, spawn_task_handle};
use crate::{
    TrypemaError,
    hybrid::common::RedisRateLimiterSignal,
    redis::{new_interval, spawn_task},
};

pub(crate) struct RedisCommitterOptions<T> {
    pub sync_interval: Duration,
    pub channel_capacity: usize,
    pub max_batch_size: usize,
    pub limiter_sender: mpsc::Sender<RedisRateLimiterSignal>,
    pub is_active_notify: Arc<Notify>,
    pub redis_proxy: Box<dyn RedisProxyCommitter<T> + Send + Sync>,
}

pub(crate) enum AbsoluteHybridCommitterSignal<T> {
    Commit(T),
}

impl<T> From<T> for AbsoluteHybridCommitterSignal<T> {
    fn from(commit: T) -> Self {
        Self::Commit(commit)
    }
}

#[async_trait::async_trait]
pub(crate) trait RedisProxyCommitter<CommitType>
where
    CommitType: Send + Sync,
{
    async fn batch_commit_state(&self, commits: &[CommitType]) -> Result<(), TrypemaError>;
}

pub(crate) struct RedisCommitter;

impl RedisCommitter {
    pub fn run<T>(
        options: RedisCommitterOptions<T>,
    ) -> mpsc::Sender<AbsoluteHybridCommitterSignal<T>>
    where
        T: Send + Sync + 'static,
    {
        let RedisCommitterOptions {
            sync_interval,
            channel_capacity,
            max_batch_size,
            limiter_sender,
            redis_proxy,
            is_active_notify,
        } = options;

        let (tx, mut rx) = mpsc::channel::<AbsoluteHybridCommitterSignal<T>>(channel_capacity);

        spawn_task(async move {
            let mut flush_interval = new_interval(sync_interval);
            let mut batch: Vec<T> = Vec::new();
            let is_active = Arc::new(AtomicBool::new(false));

            let is_active_cancel_task: TaskHandle = spawn_task_handle({
                let is_active = is_active.clone();
                let is_active_notify = is_active_notify.clone();
                let sleep_interval =
                    Duration::from_millis(30_000.max(sync_interval.as_millis() as u64 * 10)) / 2;

                async move {
                    loop {
                        sleep(sleep_interval).await;

                        futures::select! {
                            _ = is_active_notify.notified().fuse() => {
                                is_active.store(true, Ordering::Release);
                            }
                            _ = sleep(sleep_interval).fuse() => {
                                is_active.store(false, Ordering::Release);
                            }
                        }
                    }
                }
            });

            // Tokio's interval ticks immediately on first await; discard that so we flush
            // after `sync_interval`. Smol's interval already waits for the duration.
            #[cfg(feature = "redis-tokio")]
            tick(&mut flush_interval).await;

            loop {
                if !is_active.load(Ordering::Acquire) {
                    is_active_notify.notified().await;
                    is_active.store(true, Ordering::Release);
                }

                {
                    futures::select! {
                        _ = tick(&mut flush_interval).fuse() => {
                            if let Err(err) =
                                Self::flush_to_redis(&redis_proxy, &mut batch, max_batch_size).await
                            {
                                tracing::error!(error = ?err, "Failed to flush to Redis");
                                continue;
                            };

                            if let Err(err) = limiter_sender.try_send(RedisRateLimiterSignal::Flush)
                            {
                                tracing::trace!(error = ?err, "Failed to send flush signal to Redis rate limiter");
                            }
                        }
                        commit = rx.recv().fuse() => {
                            let Some(AbsoluteHybridCommitterSignal::Commit(commit)) = commit else {
                                break;
                            };

                            batch.push(commit);
                        }

                    }
                }

                while let Ok(AbsoluteHybridCommitterSignal::Commit(commit)) = rx.try_recv() {
                    batch.push(commit);
                }
            }

            #[cfg(feature = "redis-tokio")]
            is_active_cancel_task.abort();

            drop(is_active_cancel_task);

            if let Err(err) = Self::flush_to_redis(&redis_proxy, &mut batch, max_batch_size).await {
                tracing::error!(error = ?err, "Failed to flush to Redis");
            };
        });

        tx
    } // end method run

    async fn flush_to_redis<T>(
        redis_proxy: &Box<dyn RedisProxyCommitter<T> + Send + Sync>,
        batch: &mut Vec<T>,
        max_batch_size: usize,
    ) -> Result<(), TrypemaError>
    where
        T: Send + Sync,
    {
        if batch.is_empty() {
            return Ok(());
        }

        let mut processed = 0;

        while processed < batch.len() {
            let end = (processed + max_batch_size).min(batch.len());
            let chunk = &batch[processed..end];

            redis_proxy.batch_commit_state(chunk).await.map_err(|err| {
                TrypemaError::CustomError(format!("Failed to commit state: {err:?}"))
            })?;

            processed = end;
        }

        batch.drain(..processed);

        Ok(())
    } // end method flush_to_redis
} // end impl AbsoluteRedisCommitter
