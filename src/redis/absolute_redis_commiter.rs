use std::time::Duration;

use tokio::sync::mpsc;

use crate::{
    TrypemaError,
    redis::{RedisKey, RedisRateLimiterSignal, absolute_redis_proxy::AbsoluteRedisProxy},
};

#[derive(Debug)]
pub(crate) struct AbsoluteRedisCommit {
    pub key: RedisKey,
    pub window_size_seconds: u64,
    pub window_limit: u64,
    pub rate_group_size_ms: u64,
    pub count: u64,
}

pub(crate) struct AbsoluteRedisCommitterOptions {
    pub local_cache_duration: Duration,
    pub channel_capacity: usize,
    pub max_batch_size: usize,
    pub limiter_sender: mpsc::Sender<RedisRateLimiterSignal>,
    pub redis_proxy: AbsoluteRedisProxy,
}

pub(crate) enum AbsoluteRedisCommitterSignal {
    Commit(AbsoluteRedisCommit),
}

impl From<AbsoluteRedisCommit> for AbsoluteRedisCommitterSignal {
    fn from(commit: AbsoluteRedisCommit) -> Self {
        Self::Commit(commit)
    }
}

pub(crate) struct AbsoluteRedisCommitter; // end struct AbsoluteRedisCommitter

impl AbsoluteRedisCommitter {
    pub fn run(
        options: AbsoluteRedisCommitterOptions,
    ) -> mpsc::Sender<AbsoluteRedisCommitterSignal> {
        let AbsoluteRedisCommitterOptions {
            local_cache_duration,
            channel_capacity,
            max_batch_size,
            limiter_sender,
            redis_proxy,
        } = options;

        let (tx, mut rx) = mpsc::channel::<AbsoluteRedisCommitterSignal>(channel_capacity);

        tokio::spawn(async move {
            let mut flush_interval = tokio::time::interval(local_cache_duration);
            let mut batch: Vec<AbsoluteRedisCommit> = Vec::new();

            // discard the first tick
            flush_interval.tick().await;

            loop {
                tokio::select! {
                    _ = flush_interval.tick() => {
                        if let Err(err) = Self::flush_to_redis(&redis_proxy, &mut batch, max_batch_size).await {
                            tracing::error!(error = ?err, "Failed to flush to Redis");
                            continue;
                        };

                        if let Err(err) = limiter_sender.try_send(RedisRateLimiterSignal::Flush) {
                            tracing::debug!(error = ?err, "Failed to send flush signal to Redis rate limiter");
                            continue;
                        }
                    },
                    commit = rx.recv() => {
                        let Some(AbsoluteRedisCommitterSignal::Commit(commit)) = commit else {
                            break;
                        };

                        batch.push(commit);
                    }
                }

                while let Ok(AbsoluteRedisCommitterSignal::Commit(commit)) = rx.try_recv() {
                    batch.push(commit);
                }
            }
        });

        tx
    } // end method run

    async fn flush_to_redis(
        redis_proxy: &AbsoluteRedisProxy,
        batch: &mut Vec<AbsoluteRedisCommit>,
        max_batch_size: usize,
    ) -> Result<(), TrypemaError> {
        if batch.is_empty() {
            return Ok(());
        }

        let mut processed = 0;

        while processed < batch.len() {
            let end = (processed + max_batch_size).min(batch.len());
            let chunk = &batch[processed..end];

            redis_proxy.batch_commit_state(chunk).await?;

            processed = end;
        }

        batch.drain(..processed);

        Ok(())
    } // end method flush_to_redis
} // end impl AbsoluteRedisCommitter
