use std::time::Duration;

use tokio::sync::mpsc;

use crate::redis::{RedisProxyCommitter, tick};
use crate::{
    TrypemaError,
    hybrid::common::RedisRateLimiterSignal,
    redis::{RedisKey, new_interval, spawn_task},
};

#[derive(Debug)]
pub(crate) struct AbsoluteHybridCommit {
    pub key: RedisKey,
    pub window_size_seconds: u64,
    pub window_limit: u64,
    pub rate_group_size_ms: u64,
    pub count: u64,
}

#[derive(Debug)]
pub(crate) struct SuppressedHybridCommit {
    pub key: RedisKey,
    pub window_size_seconds: u64,
    pub window_limit: u64,
    pub rate_group_size_ms: u64,
    pub count: u64,
}

pub(crate) struct AbsoluteHybridCommitterOptions<T> {
    pub sync_interval: Duration,
    pub channel_capacity: usize,
    pub max_batch_size: usize,
    pub limiter_sender: mpsc::Sender<RedisRateLimiterSignal>,
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

pub(crate) struct AbsoluteHybridCommitter;

impl AbsoluteHybridCommitter {
    pub fn run<T>(
        options: AbsoluteHybridCommitterOptions<T>,
    ) -> mpsc::Sender<AbsoluteHybridCommitterSignal<T>>
    where
        T: Send + Sync + 'static,
    {
        let AbsoluteHybridCommitterOptions {
            sync_interval,
            channel_capacity,
            max_batch_size,
            limiter_sender,
            redis_proxy,
        } = options;

        let (tx, mut rx) = mpsc::channel::<AbsoluteHybridCommitterSignal<T>>(channel_capacity);

        spawn_task(async move {
            let mut flush_interval = new_interval(sync_interval);
            let mut batch: Vec<T> = Vec::new();

            // Tokio's interval ticks immediately on first await; discard that so we flush
            // after `sync_interval`. Smol's interval already waits for the duration.
            #[cfg(feature = "redis-tokio")]
            tick(&mut flush_interval).await;

            loop {
                {
                    let tick_fut = tick(&mut flush_interval);
                    let recv_fut = rx.recv();

                    futures::pin_mut!(tick_fut);
                    futures::pin_mut!(recv_fut);

                    match futures::future::select(tick_fut, recv_fut).await {
                        futures::future::Either::Left((_tick, _recv_fut)) => {
                            // Make sure to only flush to redis on tick. This is because flushes
                            // are time consuming and if we flush on every commit in recv_fut, it
                            // would hinder the read commands from being able to complete since the
                            // commits take time to complete.
                            if let Err(err) =
                                Self::flush_to_redis(&redis_proxy, &mut batch, max_batch_size).await
                            {
                                tracing::error!(error = ?err, "Failed to flush to Redis");
                                continue;
                            };

                            if let Err(err) = limiter_sender.try_send(RedisRateLimiterSignal::Flush)
                            {
                                tracing::debug!(error = ?err, "Failed to send flush signal to Redis rate limiter");
                                continue;
                            }
                        }
                        futures::future::Either::Right((commit, _tick_fut)) => {
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
