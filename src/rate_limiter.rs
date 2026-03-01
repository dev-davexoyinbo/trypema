//! Top-level rate limiter facade.
//!
//! This module provides [`RateLimiter`], the main entry point for rate limiting.
//! It coordinates multiple providers:
//! - [`LocalRateLimiterProvider`]: In-process rate limiting
//! - [`RedisRateLimiterProvider`]: Distributed rate limiting (requires Redis 6.2+)
//!
//! # Examples
//!
//! ```no_run
//! use std::sync::Arc;
//! use trypema::{HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimiter, RateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds};
//! use trypema::local::LocalRateLimiterOptions;
//! # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
//! # use trypema::redis::RedisRateLimiterOptions;
//! #
//! # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
//! # fn options() -> RateLimiterOptions {
//! #     RateLimiterOptions {
//! #         local: LocalRateLimiterOptions {
//! #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
//! #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
//! #             hard_limit_factor: HardLimitFactor::default(),
//! #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
//! #         },
//! #         redis: RedisRateLimiterOptions {
//! #             connection_manager: todo!(),
//! #             prefix: None,
//! #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
//! #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
//! #             hard_limit_factor: HardLimitFactor::default(),
//! #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
//! #         },
//! #     }
//! # }
//! #
//! # #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
//! # fn options() -> RateLimiterOptions {
//! #     RateLimiterOptions {
//! #         local: LocalRateLimiterOptions {
//! #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
//! #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
//! #             hard_limit_factor: HardLimitFactor::default(),
//! #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
//! #         },
//! #     }
//! # }
//!
//! let rl = Arc::new(RateLimiter::new(options()));
//!
//! // Start background cleanup (optional but recommended)
//! rl.run_cleanup_loop();
//!
//! let rate = RateLimit::try_from(10.0).unwrap();
//! let decision = rl.local().absolute().inc("user_123", &rate, 1);
//! ```

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
use crate::hybrid::HybridRateLimiterProvider;
use crate::{LocalRateLimiterOptions, LocalRateLimiterProvider};

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
use crate::redis::{RedisRateLimiterOptions, RedisRateLimiterProvider};

/// Configuration for [`RateLimiter`].
///
/// Configures both local and Redis providers. If Redis features are disabled,
/// only `local` is required.
///
/// # Examples
///
/// Local-only configuration:
/// ```no_run
/// use trypema::{HardLimitFactor, RateGroupSizeMs, RateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds};
/// use trypema::local::LocalRateLimiterOptions;
/// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
/// # use trypema::redis::RedisRateLimiterOptions;
/// #
/// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
/// # fn options() -> RateLimiterOptions {
/// #     RateLimiterOptions {
/// #         local: LocalRateLimiterOptions {
/// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
/// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
/// #             hard_limit_factor: HardLimitFactor::default(),
/// #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
/// #         },
/// #         redis: RedisRateLimiterOptions {
/// #             connection_manager: todo!(),
/// #             prefix: None,
/// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
/// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
/// #             hard_limit_factor: HardLimitFactor::default(),
/// #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
/// #         },
/// #     }
/// # }
/// #
/// # #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
/// # fn options() -> RateLimiterOptions {
/// #     RateLimiterOptions {
/// #         local: LocalRateLimiterOptions {
/// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
/// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
/// #             hard_limit_factor: HardLimitFactor::default(),
/// #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
/// #         },
/// #     }
/// # }
///
/// let options = options();
/// ```
#[derive(Clone, Debug)]
pub struct RateLimiterOptions {
    /// Configuration for the local (in-process) provider.
    pub local: LocalRateLimiterOptions,

    /// Configuration for the Redis (distributed) provider.
    ///
    /// Only available with `redis-tokio` or `redis-smol` features.
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    pub redis: RedisRateLimiterOptions,
}

/// Primary rate limiter facade.
///
/// Provides access to multiple rate limiting providers and strategies:
/// - **Local provider:** In-process rate limiting with per-key state
/// - **Redis provider:** Distributed rate limiting across processes/servers
///
/// Each provider supports multiple strategies (absolute, suppressed).
///
/// # Thread Safety
///
/// `RateLimiter` is thread-safe and designed for use in `Arc<RateLimiter>`.
/// The cleanup loop holds only a `Weak` reference, so dropping all `Arc` references
/// automatically stops background tasks.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use trypema::{RateLimit, RateLimiter, RateLimiterOptions};
/// use trypema::{HardLimitFactor, RateGroupSizeMs, SuppressionFactorCacheMs, WindowSizeSeconds};
/// use trypema::local::LocalRateLimiterOptions;
/// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
/// # use trypema::redis::RedisRateLimiterOptions;
/// #
/// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
/// # fn options() -> RateLimiterOptions {
/// #     RateLimiterOptions {
/// #         local: LocalRateLimiterOptions {
/// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
/// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
/// #             hard_limit_factor: HardLimitFactor::default(),
/// #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
/// #         },
/// #         redis: RedisRateLimiterOptions {
/// #             connection_manager: todo!(),
/// #             prefix: None,
/// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
/// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
/// #             hard_limit_factor: HardLimitFactor::default(),
/// #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
/// #         },
/// #     }
/// # }
/// #
/// # #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
/// # fn options() -> RateLimiterOptions {
/// #     RateLimiterOptions {
/// #         local: LocalRateLimiterOptions {
/// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
/// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
/// #             hard_limit_factor: HardLimitFactor::default(),
/// #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
/// #         },
/// #     }
/// # }
///
/// let rl = Arc::new(RateLimiter::new(options()));
/// rl.run_cleanup_loop();
///
/// let rate = RateLimit::try_from(5.0).unwrap();
/// let decision = rl.local().absolute().inc("user_123", &rate, 1);
/// ```
pub struct RateLimiter {
    local: LocalRateLimiterProvider,
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
    redis: RedisRateLimiterProvider,
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
    hybrid: HybridRateLimiterProvider,
    is_loop_running: AtomicBool,
}

impl RateLimiter {
    /// Create a new rate limiter with the given configuration.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use trypema::{HardLimitFactor, RateGroupSizeMs, RateLimiter, RateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds};
    /// use trypema::local::LocalRateLimiterOptions;
    /// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    /// # use trypema::redis::RedisRateLimiterOptions;
    /// #
    /// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    /// # fn options() -> RateLimiterOptions {
    /// #     RateLimiterOptions {
    /// #         local: LocalRateLimiterOptions {
    /// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
    /// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
    /// #             hard_limit_factor: HardLimitFactor::default(),
    /// #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
    /// #         },
    /// #         redis: RedisRateLimiterOptions {
    /// #             connection_manager: todo!(),
    /// #             prefix: None,
    /// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
    /// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
    /// #             hard_limit_factor: HardLimitFactor::default(),
    /// #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
    /// #         },
    /// #     }
    /// # }
    /// #
    /// # #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
    /// # fn options() -> RateLimiterOptions {
    /// #     RateLimiterOptions {
    /// #         local: LocalRateLimiterOptions {
    /// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
    /// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
    /// #             hard_limit_factor: HardLimitFactor::default(),
    /// #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
    /// #         },
    /// #     }
    /// # }
    ///
    /// let rl = RateLimiter::new(options());
    /// ```
    pub fn new(options: RateLimiterOptions) -> Self {
        Self {
            local: LocalRateLimiterProvider::new(options.local),
            #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
            #[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
            redis: RedisRateLimiterProvider::new(options.redis.clone()),
            #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
            #[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
            hybrid: HybridRateLimiterProvider::new(options.redis),
            is_loop_running: AtomicBool::new(false),
        }
    }

    /// Run a cleanup loop that evicts expired buckets.
    ///
    /// This spawns background tasks to periodically clean up stale state:
    /// - Local provider: always spawns a thread for synchronous cleanup
    /// - Redis provider: spawns an async task if the appropriate runtime is available
    ///
    /// This method is idempotent: calling it multiple times while the loop is already running
    /// is a no-op.
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
    /// This method is idempotent: calling it multiple times while the loop is already running
    /// is a no-op.
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
        if self.is_loop_running.swap(true, Ordering::SeqCst) {
            return;
        }

        #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
        {
            let rl = Arc::downgrade(self);
            std::thread::spawn(move || {
                let interval = Duration::from_millis(cleanup_interval_ms);

                // Run after first interval tick.
                std::thread::sleep(interval);

                loop {
                    let Some(rl) = rl.upgrade() else {
                        break;
                    };

                    if !rl.is_loop_running.load(Ordering::SeqCst) {
                        break;
                    }

                    rl.local.cleanup(stale_after_ms);
                    std::thread::sleep(interval);
                }
            });
        }

        #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
        {
            let rl = Arc::downgrade(self);
            crate::runtime::spawn_task(async move {
                let interval = Duration::from_millis(cleanup_interval_ms);
                let mut interval = crate::runtime::new_interval(interval);

                // Run after first interval tick.
                crate::runtime::tick(&mut interval).await;

                loop {
                    crate::runtime::tick(&mut interval).await;

                    let Some(rl) = rl.upgrade() else {
                        break;
                    };

                    if !rl.is_loop_running.load(Ordering::SeqCst) {
                        break;
                    }

                    rl.local.cleanup(stale_after_ms);

                    if let Err(e) = rl.redis.cleanup(stale_after_ms).await {
                        tracing::warn!(error = ?e, "Redis cleanup failed, will retry");
                    }

                    if let Err(e) = rl.hybrid.cleanup(stale_after_ms).await {
                        tracing::warn!(error = ?e, "Hybrid cleanup failed, will retry");
                    }
                }
            });
        }
    } // end method run_cleanup_loop_with_config

    /// Stop the cleanup loop.
    ///
    /// This method is idempotent and safe to call multiple times.
    ///
    /// Stopping is best-effort and asynchronous: background tasks will exit on their next check/tick.
    pub fn stop_cleanup_loop(self: &Arc<Self>) {
        self.is_loop_running.store(false, Ordering::SeqCst);
    } // end method stop_cleanup_loop

    /// Access the Redis provider for distributed rate limiting.
    ///
    /// Requires Redis 6.2+ and one of the Redis features (`redis-tokio` or `redis-smol`).
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # async fn example() -> Result<(), trypema::TrypemaError> {
    /// use trypema::{
    ///     HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimiter, RateLimiterOptions,
    ///     SuppressionFactorCacheMs, WindowSizeSeconds,
    /// };
    /// use trypema::local::LocalRateLimiterOptions;
    /// use trypema::redis::{RedisKey, RedisRateLimiterOptions};
    ///
    /// let rl = RateLimiter::new(RateLimiterOptions {
    ///     local: LocalRateLimiterOptions {
    ///         window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
    ///         rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
    ///         hard_limit_factor: HardLimitFactor::default(),
    ///         suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
    ///     },
    ///     redis: RedisRateLimiterOptions {
    ///         connection_manager: todo!("create redis::aio::ConnectionManager"),
    ///         prefix: None,
    ///         window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
    ///         rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
    ///         hard_limit_factor: HardLimitFactor::default(),
    ///         suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
    ///     },
    /// });
    ///
    /// let key = RedisKey::try_from("user_123".to_string())?;
    /// let rate = RateLimit::try_from(10.0)?;
    ///
    /// let _decision = rl.redis().absolute().inc(&key, &rate, 1).await?;
    /// # Ok(()) }
    /// ```
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
    pub fn redis(&self) -> &RedisRateLimiterProvider {
        &self.redis
    }

    ///...
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
    pub fn hybrid(&self) -> &HybridRateLimiterProvider {
        &self.hybrid
    }

    /// Access the local provider for in-process rate limiting.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use trypema::{HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimiter, RateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds};
    /// # use trypema::local::LocalRateLimiterOptions;
    /// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    /// # use trypema::redis::RedisRateLimiterOptions;
    /// #
    /// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    /// # fn options() -> RateLimiterOptions {
    /// #     RateLimiterOptions {
    /// #         local: LocalRateLimiterOptions {
    /// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
    /// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
    /// #             hard_limit_factor: HardLimitFactor::default(),
    /// #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
    /// #         },
    /// #         redis: RedisRateLimiterOptions {
    /// #             connection_manager: todo!(),
    /// #             prefix: None,
    /// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
    /// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
    /// #             hard_limit_factor: HardLimitFactor::default(),
    /// #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
    /// #         },
    /// #     }
    /// # }
    /// #
    /// # #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
    /// # fn options() -> RateLimiterOptions {
    /// #     RateLimiterOptions {
    /// #         local: LocalRateLimiterOptions {
    /// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
    /// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
    /// #             hard_limit_factor: HardLimitFactor::default(),
    /// #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
    /// #         },
    /// #     }
    /// # }
    /// # let rl = RateLimiter::new(options());
    /// let rate = RateLimit::try_from(10.0).unwrap();
    /// let decision = rl.local().absolute().inc("user_123", &rate, 1);
    /// ```
    pub fn local(&self) -> &LocalRateLimiterProvider {
        &self.local
    }
}
