//! Top-level rate limiter facade.
//!
//! This module provides [`RateLimiter`], the main entry point for the Trypema library.
//! A single `RateLimiter` instance gives access to all three providers:
//!
//! - [`LocalRateLimiterProvider`] via [`RateLimiter::local()`] — in-process rate limiting
//! - [`RedisRateLimiterProvider`] via [`RateLimiter::redis()`] — distributed rate limiting (Redis 6.2+)
//! - [`HybridRateLimiterProvider`] via [`RateLimiter::hybrid()`] — local fast-path with periodic Redis sync
//!
//! `RateLimiter` is thread-safe and designed to be wrapped in `Arc<RateLimiter>`. The
//! optional cleanup loop uses `Weak` references internally, so dropping all `Arc` references
//! automatically stops background tasks without risk of keeping the limiter alive.
//!
//! # Examples
//!
//! ```no_run
//! use std::sync::Arc;
//! use trypema::{HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimiter, RateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds};
//! use trypema::local::LocalRateLimiterOptions;
//! # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
//! # use trypema::redis::RedisRateLimiterOptions;
//! # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
//! # use trypema::hybrid::SyncIntervalMs;
//! #
//! # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
//! # fn options() -> RateLimiterOptions {
//! #     let window_size_seconds = WindowSizeSeconds::try_from(60).unwrap();
//! #     let rate_group_size_ms = RateGroupSizeMs::try_from(10).unwrap();
//! #     let hard_limit_factor = HardLimitFactor::default();
//! #     let suppression_factor_cache_ms = SuppressionFactorCacheMs::default();
//! #     let sync_interval_ms = SyncIntervalMs::default();
//! #
//! #     RateLimiterOptions {
//! #         local: LocalRateLimiterOptions {
//! #             window_size_seconds,
//! #             rate_group_size_ms,
//! #             hard_limit_factor,
//! #             suppression_factor_cache_ms,
//! #         },
//! #         redis: RedisRateLimiterOptions {
//! #             connection_manager: todo!(),
//! #             prefix: None,
//! #             window_size_seconds,
//! #             rate_group_size_ms,
//! #             hard_limit_factor,
//! #             suppression_factor_cache_ms,
//! #             sync_interval_ms,
//! #         },
//! #     }
//! # }
//! #
//! # #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
//! # fn options() -> RateLimiterOptions {
//! #     let window_size_seconds = WindowSizeSeconds::try_from(60).unwrap();
//! #     let rate_group_size_ms = RateGroupSizeMs::try_from(10).unwrap();
//! #     let hard_limit_factor = HardLimitFactor::default();
//! #     let suppression_factor_cache_ms = SuppressionFactorCacheMs::default();
//! #
//! #     RateLimiterOptions {
//! #         local: LocalRateLimiterOptions {
//! #             window_size_seconds,
//! #             rate_group_size_ms,
//! #             hard_limit_factor,
//! #             suppression_factor_cache_ms,
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
use crate::{
    HardLimitFactor, LocalRateLimiterOptions, LocalRateLimiterProvider, RateGroupSizeMs,
    SuppressionFactorCacheMs, TrypemaError, WindowSizeSeconds,
};

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
use crate::redis::{RedisKey, RedisRateLimiterOptions, RedisRateLimiterProvider};

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
use crate::hybrid::SyncIntervalMs;

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
use redis::aio::ConnectionManager;

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
/// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
/// # use trypema::hybrid::SyncIntervalMs;
/// #
/// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
/// # fn options() -> RateLimiterOptions {
/// #     let window_size_seconds = WindowSizeSeconds::try_from(60).unwrap();
/// #     let rate_group_size_ms = RateGroupSizeMs::try_from(10).unwrap();
/// #     let hard_limit_factor = HardLimitFactor::default();
/// #     let suppression_factor_cache_ms = SuppressionFactorCacheMs::default();
/// #     let sync_interval_ms = SyncIntervalMs::default();
/// #
/// #     RateLimiterOptions {
/// #         local: LocalRateLimiterOptions {
/// #             window_size_seconds,
/// #             rate_group_size_ms,
/// #             hard_limit_factor,
/// #             suppression_factor_cache_ms,
/// #         },
/// #         redis: RedisRateLimiterOptions {
/// #             connection_manager: todo!(),
/// #             prefix: None,
/// #             window_size_seconds,
/// #             rate_group_size_ms,
/// #             hard_limit_factor,
/// #             suppression_factor_cache_ms,
/// #             sync_interval_ms,
/// #         },
/// #     }
/// # }
/// #
/// # #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
/// # fn options() -> RateLimiterOptions {
/// #     let window_size_seconds = WindowSizeSeconds::try_from(60).unwrap();
/// #     let rate_group_size_ms = RateGroupSizeMs::try_from(10).unwrap();
/// #     let hard_limit_factor = HardLimitFactor::default();
/// #     let suppression_factor_cache_ms = SuppressionFactorCacheMs::default();
/// #
/// #     RateLimiterOptions {
/// #         local: LocalRateLimiterOptions {
/// #             window_size_seconds,
/// #             rate_group_size_ms,
/// #             hard_limit_factor,
/// #             suppression_factor_cache_ms,
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

/// Default configuration for [`RateLimiterOptions`].
///
/// Only available when Redis features are disabled. When Redis features are enabled,
/// `RateLimiterOptions` requires a `ConnectionManager` which has no meaningful default.
#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
impl Default for RateLimiterOptions {
    fn default() -> Self {
        Self {
            local: LocalRateLimiterOptions::default(),
        }
    }
}

/// Primary rate limiter facade.
///
/// Provides access to all rate limiting providers and strategies through a single instance:
///
/// - **Local provider** (`rl.local()`) — in-process, sub-microsecond latency
/// - **Redis provider** (`rl.redis()`) — distributed via atomic Lua scripts
/// - **Hybrid provider** (`rl.hybrid()`) — local fast-path with periodic Redis sync
///
/// Each provider exposes two strategies: **absolute** (deterministic sliding-window) and
/// **suppressed** (probabilistic degradation).
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
/// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
/// # use trypema::hybrid::SyncIntervalMs;
/// #
/// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
/// # fn options() -> RateLimiterOptions {
/// #     let window_size_seconds = WindowSizeSeconds::try_from(60).unwrap();
/// #     let rate_group_size_ms = RateGroupSizeMs::try_from(10).unwrap();
/// #     let hard_limit_factor = HardLimitFactor::default();
/// #     let suppression_factor_cache_ms = SuppressionFactorCacheMs::default();
/// #     let sync_interval_ms = SyncIntervalMs::default();
/// #
/// #     RateLimiterOptions {
/// #         local: LocalRateLimiterOptions {
/// #             window_size_seconds,
/// #             rate_group_size_ms,
/// #             hard_limit_factor,
/// #             suppression_factor_cache_ms,
/// #         },
/// #         redis: RedisRateLimiterOptions {
/// #             connection_manager: todo!(),
/// #             prefix: None,
/// #             window_size_seconds,
/// #             rate_group_size_ms,
/// #             hard_limit_factor,
/// #             suppression_factor_cache_ms,
/// #             sync_interval_ms,
/// #         },
/// #     }
/// # }
/// #
/// # #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
/// # fn options() -> RateLimiterOptions {
/// #     let window_size_seconds = WindowSizeSeconds::try_from(60).unwrap();
/// #     let rate_group_size_ms = RateGroupSizeMs::try_from(10).unwrap();
/// #     let hard_limit_factor = HardLimitFactor::default();
/// #     let suppression_factor_cache_ms = SuppressionFactorCacheMs::default();
/// #
/// #     RateLimiterOptions {
/// #         local: LocalRateLimiterOptions {
/// #             window_size_seconds,
/// #             rate_group_size_ms,
/// #             hard_limit_factor,
/// #             suppression_factor_cache_ms,
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

impl Drop for RateLimiter {
    fn drop(&mut self) {
        self.stop_cleanup_loop();
    }
}

impl RateLimiter {
    /// Create a new rate limiter with the given configuration.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use trypema::{HardLimitFactor, RateGroupSizeMs, RateLimiter, RateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds};
    /// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    /// # use trypema::hybrid::SyncIntervalMs;
    /// use trypema::local::LocalRateLimiterOptions;
    /// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    /// # use trypema::redis::RedisRateLimiterOptions;
    /// #
    /// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    /// # fn options() -> RateLimiterOptions {
    /// #     let window_size_seconds = WindowSizeSeconds::try_from(60).unwrap();
    /// #     let rate_group_size_ms = RateGroupSizeMs::try_from(10).unwrap();
    /// #     let hard_limit_factor = HardLimitFactor::default();
    /// #     let suppression_factor_cache_ms = SuppressionFactorCacheMs::default();
    /// #     let sync_interval_ms = SyncIntervalMs::default();
    /// #
    /// #     RateLimiterOptions {
    /// #         local: LocalRateLimiterOptions {
    /// #             window_size_seconds,
    /// #             rate_group_size_ms,
    /// #             hard_limit_factor,
    /// #             suppression_factor_cache_ms,
    /// #         },
    /// #         redis: RedisRateLimiterOptions {
    /// #             connection_manager: todo!(),
    /// #             prefix: None,
    /// #             window_size_seconds,
    /// #             rate_group_size_ms,
    /// #             hard_limit_factor,
    /// #             suppression_factor_cache_ms,
    /// #             sync_interval_ms,
    /// #         },
    /// #     }
    /// # }
    /// #
    /// # #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
    /// # fn options() -> RateLimiterOptions {
    /// #     let window_size_seconds = WindowSizeSeconds::try_from(60).unwrap();
    /// #     let rate_group_size_ms = RateGroupSizeMs::try_from(10).unwrap();
    /// #     let hard_limit_factor = HardLimitFactor::default();
    /// #     let suppression_factor_cache_ms = SuppressionFactorCacheMs::default();
    /// #
    /// #     RateLimiterOptions {
    /// #         local: LocalRateLimiterOptions {
    /// #             window_size_seconds,
    /// #             rate_group_size_ms,
    /// #             hard_limit_factor,
    /// #             suppression_factor_cache_ms,
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
    pub fn stop_cleanup_loop(&self) {
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
    /// use trypema::hybrid::SyncIntervalMs;
    /// use trypema::local::LocalRateLimiterOptions;
    /// use trypema::redis::{RedisKey, RedisRateLimiterOptions};
    ///
    /// let window_size_seconds = WindowSizeSeconds::try_from(60)?;
    /// let rate_group_size_ms = RateGroupSizeMs::try_from(10)?;
    /// let hard_limit_factor = HardLimitFactor::default();
    /// let suppression_factor_cache_ms = SuppressionFactorCacheMs::default();
    /// let sync_interval_ms = SyncIntervalMs::default();
    ///
    /// let rl = RateLimiter::new(RateLimiterOptions {
    ///     local: LocalRateLimiterOptions {
    ///         window_size_seconds,
    ///         rate_group_size_ms,
    ///         hard_limit_factor,
    ///         suppression_factor_cache_ms,
    ///     },
    ///     redis: RedisRateLimiterOptions {
    ///         connection_manager: todo!("create redis::aio::ConnectionManager"),
    ///         prefix: None,
    ///         window_size_seconds,
    ///         rate_group_size_ms,
    ///         hard_limit_factor,
    ///         suppression_factor_cache_ms,
    ///         sync_interval_ms,
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

    /// Access the hybrid provider for Redis-backed limiting with a local fast-path.
    ///
    /// The hybrid provider keeps a local in-memory fast-path for low-latency admission checks and
    /// periodically flushes local increments to Redis in batches. This can reduce Redis round trips
    /// compared to using [`RateLimiter::redis`], at the cost of some additional approximation due to
    /// sync lag.
    ///
    /// Requires Redis 6.2+ and one of the Redis features (`redis-tokio` or `redis-smol`).
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # async fn example() -> Result<(), trypema::TrypemaError> {
    /// use trypema::{RateLimit, RateLimiter};
    /// use trypema::redis::RedisKey;
    ///
    /// let rl: RateLimiter = todo!("construct RateLimiter with RedisRateLimiterOptions");
    /// let key = RedisKey::try_from("user_123".to_string())?;
    /// let rate = RateLimit::try_from(10.0)?;
    ///
    /// let _ = rl.hybrid().absolute().inc(&key, &rate, 1).await?;
    /// let _ = rl.hybrid().suppressed().inc(&key, &rate, 1).await?;
    /// # Ok(()) }
    /// ```
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
    /// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    /// # use trypema::hybrid::SyncIntervalMs;
    /// # use trypema::local::LocalRateLimiterOptions;
    /// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    /// # use trypema::redis::RedisRateLimiterOptions;
    /// #
    /// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    /// # fn options() -> RateLimiterOptions {
    /// #     let window_size_seconds = WindowSizeSeconds::try_from(60).unwrap();
    /// #     let rate_group_size_ms = RateGroupSizeMs::try_from(10).unwrap();
    /// #     let hard_limit_factor = HardLimitFactor::default();
    /// #     let suppression_factor_cache_ms = SuppressionFactorCacheMs::default();
    /// #     let sync_interval_ms = SyncIntervalMs::default();
    /// #
    /// #     RateLimiterOptions {
    /// #         local: LocalRateLimiterOptions {
    /// #             window_size_seconds,
    /// #             rate_group_size_ms,
    /// #             hard_limit_factor,
    /// #             suppression_factor_cache_ms,
    /// #         },
    /// #         redis: RedisRateLimiterOptions {
    /// #             connection_manager: todo!(),
    /// #             prefix: None,
    /// #             window_size_seconds,
    /// #             rate_group_size_ms,
    /// #             hard_limit_factor,
    /// #             suppression_factor_cache_ms,
    /// #             sync_interval_ms,
    /// #         },
    /// #     }
    /// # }
    /// #
    /// # #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
    /// # fn options() -> RateLimiterOptions {
    /// #     let window_size_seconds = WindowSizeSeconds::try_from(60).unwrap();
    /// #     let rate_group_size_ms = RateGroupSizeMs::try_from(10).unwrap();
    /// #     let hard_limit_factor = HardLimitFactor::default();
    /// #     let suppression_factor_cache_ms = SuppressionFactorCacheMs::default();
    /// #
    /// #     RateLimiterOptions {
    /// #         local: LocalRateLimiterOptions {
    /// #             window_size_seconds,
    /// #             rate_group_size_ms,
    /// #             hard_limit_factor,
    /// #             suppression_factor_cache_ms,
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

/// Builder for [`RateLimiter`].
///
/// Construct via [`RateLimiter::builder()`]. All fields have sensible defaults derived
/// from each type's own [`Default`] implementation, so changing a type's default
/// automatically propagates here without code changes.
///
/// # Without Redis features
///
/// ```no_run
/// use trypema::RateLimiter;
///
/// let rl = RateLimiter::builder()
///     .window_size_seconds(60)
///     .build()
///     .unwrap();
/// ```
///
/// # With Redis features
///
/// ```no_run
/// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
/// # async fn example() {
/// # let connection_manager: redis::aio::ConnectionManager = todo!();
/// use trypema::RateLimiter;
///
/// let rl = RateLimiter::builder(connection_manager)
///     .window_size_seconds(60)
///     .build()
///     .unwrap();
/// # }
/// ```
pub struct RateLimiterBuilder {
    window_size_seconds: u64,
    rate_group_size_ms: u64,
    hard_limit_factor: f64,
    suppression_factor_cache_ms: u64,
    stale_after_ms: u64,
    cleanup_interval_ms: u64,
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    connection_manager: ConnectionManager,
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    redis_prefix: Option<RedisKey>,
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    sync_interval_ms: u64,
}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
impl Default for RateLimiterBuilder {
    fn default() -> Self {
        Self {
            window_size_seconds: *WindowSizeSeconds::default(),
            rate_group_size_ms: *RateGroupSizeMs::default(),
            hard_limit_factor: *HardLimitFactor::default(),
            suppression_factor_cache_ms: *SuppressionFactorCacheMs::default(),
            stale_after_ms: 10 * 60 * 1000,
            cleanup_interval_ms: 30 * 1000,
        }
    }
}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
impl RateLimiter {
    /// Create a builder for [`RateLimiter`] with sensible defaults.
    pub fn builder() -> RateLimiterBuilder {
        RateLimiterBuilder::default()
    }
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
impl RateLimiterBuilder {
    fn new_with_connection_manager(connection_manager: ConnectionManager) -> Self {
        Self {
            window_size_seconds: *WindowSizeSeconds::default(),
            rate_group_size_ms: *RateGroupSizeMs::default(),
            hard_limit_factor: *HardLimitFactor::default(),
            suppression_factor_cache_ms: *SuppressionFactorCacheMs::default(),
            stale_after_ms: 10 * 60 * 1000,
            cleanup_interval_ms: 30 * 1000,
            connection_manager,
            redis_prefix: None,
            sync_interval_ms: *SyncIntervalMs::default(),
        }
    }
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
impl RateLimiter {
    /// Create a builder for [`RateLimiter`].
    ///
    /// The `connection_manager` is required because it has no default.
    /// All other options default to sensible values.
    pub fn builder(connection_manager: ConnectionManager) -> RateLimiterBuilder {
        RateLimiterBuilder::new_with_connection_manager(connection_manager)
    }
}

impl RateLimiterBuilder {
    /// Set the sliding window duration in seconds. Default: [`WindowSizeSeconds::default()`].
    pub fn window_size_seconds(mut self, v: u64) -> Self {
        self.window_size_seconds = v;
        self
    }

    /// Set the bucket coalescing interval in milliseconds. Default: [`RateGroupSizeMs::default()`].
    pub fn rate_group_size_ms(mut self, v: u64) -> Self {
        self.rate_group_size_ms = v;
        self
    }

    /// Set the hard cutoff multiplier for the suppressed strategy. Default: [`HardLimitFactor::default()`].
    pub fn hard_limit_factor(mut self, v: f64) -> Self {
        self.hard_limit_factor = v;
        self
    }

    /// Set the suppression factor cache duration in milliseconds. Default: [`SuppressionFactorCacheMs::default()`].
    pub fn suppression_factor_cache_ms(mut self, v: u64) -> Self {
        self.suppression_factor_cache_ms = v;
        self
    }

    /// Set how long (ms) a key must be inactive before it is considered stale. Default: 600,000 (10 min).
    pub fn stale_after_ms(mut self, v: u64) -> Self {
        self.stale_after_ms = v;
        self
    }

    /// Set how often (ms) the cleanup loop runs. Default: 30,000 (30 sec).
    pub fn cleanup_interval_ms(mut self, v: u64) -> Self {
        self.cleanup_interval_ms = v;
        self
    }

    /// Set the Redis key prefix. Default: `None` (uses `"trypema"`).
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
    pub fn redis_prefix(mut self, v: RedisKey) -> Self {
        self.redis_prefix = Some(v);
        self
    }

    /// Set the hybrid provider sync interval in milliseconds. Default: [`SyncIntervalMs::default()`].
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
    pub fn sync_interval_ms(mut self, v: u64) -> Self {
        self.sync_interval_ms = v;
        self
    }

    /// Build the [`RateLimiter`], wrapped in [`Arc`], with the cleanup loop already running.
    ///
    /// Returns `Err` if any option value fails validation.
    pub fn build(self) -> Result<Arc<RateLimiter>, TrypemaError> {
        let options = RateLimiterOptions {
            local: LocalRateLimiterOptions {
                window_size_seconds: WindowSizeSeconds::try_from(self.window_size_seconds)?,
                rate_group_size_ms: RateGroupSizeMs::try_from(self.rate_group_size_ms)?,
                hard_limit_factor: HardLimitFactor::try_from(self.hard_limit_factor)?,
                suppression_factor_cache_ms: SuppressionFactorCacheMs::try_from(
                    self.suppression_factor_cache_ms,
                )?,
            },
            #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
            redis: RedisRateLimiterOptions {
                connection_manager: self.connection_manager,
                prefix: self.redis_prefix,
                window_size_seconds: WindowSizeSeconds::try_from(self.window_size_seconds)?,
                rate_group_size_ms: RateGroupSizeMs::try_from(self.rate_group_size_ms)?,
                hard_limit_factor: HardLimitFactor::try_from(self.hard_limit_factor)?,
                suppression_factor_cache_ms: SuppressionFactorCacheMs::try_from(
                    self.suppression_factor_cache_ms,
                )?,
                sync_interval_ms: SyncIntervalMs::try_from(self.sync_interval_ms)?,
            },
        };
        let rl = Arc::new(RateLimiter::new(options));
        rl.run_cleanup_loop_with_config(self.stale_after_ms, self.cleanup_interval_ms);
        Ok(rl)
    }
}
