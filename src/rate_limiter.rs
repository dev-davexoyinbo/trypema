//! Top-level entrypoint that wires provider implementations.
//!
//! Today the crate ships a single provider (`local`), exposed via
//! [`RateLimiter::local`]. Additional providers (e.g. shared/distributed state) can be
//! added behind this facade.

use std::{sync::Arc, time::Duration};

use crate::{LocalRateLimiterOptions, LocalRateLimiterProvider};

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
use crate::redis::{RedisRateLimiterOptions, RedisRateLimiterProvider};

/// Top-level configuration for [`RateLimiter`].
#[derive(Clone, Debug)]
pub struct RateLimiterOptions {
    /// Options for the local provider.
    pub local: LocalRateLimiterOptions,
    /// Options for the Redis provider.
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    pub redis: RedisRateLimiterOptions,
}

/// Rate limiter entrypoint.
///
/// This type wires together one or more providers (currently `local`).
pub struct RateLimiter {
    local: LocalRateLimiterProvider,
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
    redis: RedisRateLimiterProvider,
}

impl RateLimiter {
    /// Create a new [`RateLimiter`].
    pub fn new(options: RateLimiterOptions) -> Self {
        Self {
            local: LocalRateLimiterProvider::new(options.local),
            #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
            #[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
            redis: RedisRateLimiterProvider::new(options.redis),
        }
    }

    /// Run a cleanup loop that evicts expired buckets.
    ///
    /// This spawns background tasks to periodically clean up stale state:
    /// - Local provider: always spawns a thread for synchronous cleanup
    /// - Redis provider: spawns an async task if the appropriate runtime is available
    ///
    /// Configuration:
    /// - `stale_after_ms`: keys inactive for this duration are removed (default: 10 minutes)
    /// - `cleanup_interval_ms`: how often to run cleanup (default: 30 seconds)
    ///
    /// # Memory management
    ///
    /// The cleanup loop holds only a `Weak` reference to the `RateLimiter`, not a strong
    /// `Arc` reference. This means:
    /// - The cleanup loop will not prevent the `RateLimiter` from being dropped
    /// - When all `Arc<RateLimiter>` references are dropped, the cleanup loop automatically exits
    /// - You can safely drop your `Arc<RateLimiter>` references without worrying about
    ///   background tasks keeping the limiter alive indefinitely
    ///
    /// # Runtime requirements
    ///
    /// With `redis-tokio` feature:
    /// - Attempts to spawn on the current Tokio runtime via `Handle::try_current()`
    /// - If no runtime is detected, logs a warning and skips Redis cleanup
    ///
    /// With `redis-smol` feature:
    /// - Spawns a detached Smol task
    /// - Only makes progress if your application drives a Smol executor
    ///
    /// # Panics
    ///
    /// Does not panic. Redis cleanup errors are logged but do not stop the loop.
    pub fn run_cleanup_loop(self: &Arc<Self>) {
        self.run_cleanup_loop_with_config(10 * 60 * 1000, 30 * 1000);
    } // end method run_cleanup_loop

    /// Run a cleanup loop with custom timing configuration.
    ///
    /// See [`RateLimiter::run_cleanup_loop`] for details on runtime requirements and memory management.
    ///
    /// Like `run_cleanup_loop`, this method uses `Weak` references internally, so dropping all
    /// `Arc<RateLimiter>` references will cause the cleanup loop to exit gracefully.
    ///
    /// # Arguments
    ///
    /// * `stale_after_ms` - keys inactive for this duration are removed
    /// * `cleanup_interval_ms` - how often to run cleanup
    pub fn run_cleanup_loop_with_config(
        self: &Arc<Self>,
        stale_after_ms: u64,
        cleanup_interval_ms: u64,
    ) {
        // Always spawn local cleanup (sync, no runtime needed)
        {
            let rl = Arc::downgrade(self);
            std::thread::spawn(move || {
                let interval = Duration::from_millis(cleanup_interval_ms);
                loop {
                    let Some(rl) = rl.upgrade() else { break };
                    rl.local.cleanup(stale_after_ms);
                    std::thread::sleep(interval);
                }
            });
        }

        // Redis cleanup depends on feature and runtime availability
        #[cfg(feature = "redis-tokio")]
        {
            let rl = Arc::downgrade(self);

            tokio::spawn(async move {
                use std::time::Duration;

                use tokio::time::interval;

                let mut interval = interval(Duration::from_millis(cleanup_interval_ms));
                interval.tick().await;

                loop {
                    interval.tick().await;
                    let Some(rl) = rl.upgrade() else { break };
                    if let Err(e) = rl.redis.cleanup(stale_after_ms).await {
                        tracing::warn!(
                            error = ?e,
                            "Redis cleanup failed, will retry"
                        );
                    }
                }
            });
        }

        #[cfg(feature = "redis-smol")]
        {
            use smol::Timer;
            use smol::stream::StreamExt;
            use std::time::Duration;

            let rl = Arc::downgrade(self);
            smol::spawn(async move {
                let mut interval = Timer::interval(Duration::from_millis(cleanup_interval_ms));
                loop {
                    interval.next().await;

                    let Some(rl) = rl.upgrade() else { break };
                    if let Err(e) = rl.redis.cleanup(stale_after_ms).await {
                        tracing::warn!(
                            error = ?e,
                            "Redis cleanup failed, will retry"
                        );
                    }
                }
            })
            .detach();
        }
    } // end method run_cleanup_loop_with_config

    /// Access the Redis provider.
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
    pub fn redis(&self) -> &RedisRateLimiterProvider {
        &self.redis
    }

    /// Access the local provider.
    pub fn local(&self) -> &LocalRateLimiterProvider {
        &self.local
    }
}
