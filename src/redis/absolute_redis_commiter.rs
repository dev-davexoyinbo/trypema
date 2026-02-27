use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use tokio::sync::mpsc;

use crate::redis::{AbsoluteRedisRateLimiter, RedisRateLimiterSignal};

pub struct AbsoluteRedisCommit {}

pub struct AbsoluteRedisCommitterOptions {
    pub local_cache_duration: Duration,
    pub channel_capacity: usize,
    pub max_batch_size: usize,
    pub limiter_sender: mpsc::Sender<RedisRateLimiterSignal>,
}

pub struct AbsoluteRedisCommitter; // end struct AbsoluteRedisCommitter

impl AbsoluteRedisCommitter {
    pub fn run(options: AbsoluteRedisCommitterOptions) -> mpsc::Sender<AbsoluteRedisCommit> {
        let AbsoluteRedisCommitterOptions {
            local_cache_duration,
            channel_capacity,
            max_batch_size,
            limiter_sender,
        } = options;

        let (tx, mut rx) = mpsc::channel::<AbsoluteRedisCommit>(channel_capacity);

        tokio::spawn(async move {
            let mut flush_interval = tokio::time::interval(local_cache_duration);
            let is_idle = AtomicBool::new(true);
            let mut batch: Vec<AbsoluteRedisCommit> = Vec::with_capacity(max_batch_size);

            // discard the first tick
            flush_interval.tick().await;

            loop {
                if is_idle.load(Ordering::Relaxed) {
                    tokio::select! {
                        _ = flush_interval.tick() => {
                            if let Err(err) = limiter_sender.send(RedisRateLimiterSignal::Flush).await {
                                tracing::error!(error = ?err, "Failed to send flush signal to Redis rate limiter");
                                break;
                            }
                        },
                        commit = rx.recv() => {
                            let Some(commit) = commit else {
                                break;
                            };

                            batch.push(commit);
                        }
                    }
                } else {
                    let Some(commit) = rx.recv().await else {
                        is_idle.store(true, Ordering::Relaxed);
                        break;
                    };

                    batch.push(commit);
                }

                while batch.len() < max_batch_size
                    && let Ok(commit) = rx.try_recv()
                {
                    batch.push(commit);
                }

                Self::flush_to_redis(&batch).await;

                batch.clear();
            }
        });

        tx
    } // end method run

    async fn flush_to_redis(batch: &Vec<AbsoluteRedisCommit>) {
        todo!()
    } // end method flush_to_redis
} // end impl AbsoluteRedisCommitter
