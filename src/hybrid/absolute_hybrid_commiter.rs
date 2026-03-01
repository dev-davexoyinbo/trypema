use std::time::Duration;

use tokio::sync::mpsc;

use crate::{
    TrypemaError,
    hybrid::{
        absolute_hybrid_redis_proxy::AbsoluteHybridRedisProxy, common::RedisRateLimiterSignal,
    },
    redis::RedisKey,
};

#[derive(Debug)]
pub(crate) struct AbsoluteHybridCommit {
    pub key: RedisKey,
    pub window_size_seconds: u64,
    pub window_limit: u64,
    pub rate_group_size_ms: u64,
    pub count: u64,
}

pub(crate) struct AbsoluteHybridCommitterOptions {
    pub sync_interval: Duration,
    pub channel_capacity: usize,
    pub max_batch_size: usize,
    pub limiter_sender: mpsc::Sender<RedisRateLimiterSignal>,
    pub redis_proxy: AbsoluteHybridRedisProxy,
}

pub(crate) enum AbsoluteHybridCommitterSignal {
    Commit(AbsoluteHybridCommit),
}

impl From<AbsoluteHybridCommit> for AbsoluteHybridCommitterSignal {
    fn from(commit: AbsoluteHybridCommit) -> Self {
        Self::Commit(commit)
    }
}

pub(crate) struct AbsoluteHybridCommitter; // end struct AbsoluteRedisCommitter

impl AbsoluteHybridCommitter {
    pub fn run(
        options: AbsoluteHybridCommitterOptions,
    ) -> mpsc::Sender<AbsoluteHybridCommitterSignal> {
        let AbsoluteHybridCommitterOptions {
            sync_interval,
            channel_capacity,
            max_batch_size,
            limiter_sender,
            redis_proxy,
        } = options;

        let (tx, mut rx) = mpsc::channel::<AbsoluteHybridCommitterSignal>(channel_capacity);

        tokio::spawn(async move {
            let mut flush_interval = tokio::time::interval(sync_interval);
            let mut batch: Vec<AbsoluteHybridCommit> = Vec::new();

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
                        let Some(AbsoluteHybridCommitterSignal::Commit(commit)) = commit else {
                            break;
                        };

                        batch.push(commit);
                    }
                }

                while let Ok(AbsoluteHybridCommitterSignal::Commit(commit)) = rx.try_recv() {
                    batch.push(commit);
                }
            }
        });

        tx
    } // end method run

    async fn flush_to_redis(
        redis_proxy: &AbsoluteHybridRedisProxy,
        batch: &mut Vec<AbsoluteHybridCommit>,
        max_batch_size: usize,
    ) -> Result<(), TrypemaError> {
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
