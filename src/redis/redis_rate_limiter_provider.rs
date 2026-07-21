use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use redis::aio::ConnectionManager;

use crate::{
    BucketSize, HardLimitFactor, RateLimiterBuilder, SuppressionFactorCachePeriod, TrypemaError,
    WindowSize,
    builder::{CleanupConfig, ProviderConfig},
    runtime::{new_interval, spawn_task, tick},
};

use super::{AbsoluteRedisRateLimiter, RedisKey, SuppressedRedisRateLimiter};

/// Internal Redis provider configuration assembled by the public builder.
#[derive(Clone, Debug)]
pub(crate) struct RedisRateLimiterConfig {
    pub connection_manager: ConnectionManager,
    pub prefix: Option<RedisKey>,
    pub provider: ProviderConfig,
}

impl RedisRateLimiterConfig {
    pub(crate) fn new(connection_manager: ConnectionManager, provider: ProviderConfig) -> Self {
        Self {
            connection_manager,
            prefix: None,
            provider,
        }
    }
}

/// Builder for [`RedisRateLimiterProvider`].
#[derive(Clone, Debug)]
pub struct RedisRateLimiterBuilder {
    connection_manager: ConnectionManager,
    prefix: Option<RedisKey>,
    provider: ProviderConfig,
    cleanup: CleanupConfig,
}

impl RedisRateLimiterBuilder {
    fn new(connection_manager: ConnectionManager) -> Self {
        Self {
            connection_manager,
            prefix: None,
            provider: ProviderConfig::default(),
            cleanup: CleanupConfig::default(),
        }
    }

    /// Set prefix used for Redis keys.
    pub fn prefix(mut self, value: RedisKey) -> Self {
        self.prefix = Some(value);
        self
    }
}

impl RateLimiterBuilder for RedisRateLimiterBuilder {
    type Provider = RedisRateLimiterProvider;

    fn window_size(mut self, value: WindowSize) -> Self {
        self.provider.window_size = value;
        self
    }

    fn bucket_size(mut self, value: BucketSize) -> Self {
        self.provider.bucket_size = value;
        self
    }

    fn hard_limit_factor(mut self, value: HardLimitFactor) -> Self {
        self.provider.hard_limit_factor = value;
        self
    }

    fn suppression_factor_cache_period(mut self, value: SuppressionFactorCachePeriod) -> Self {
        self.provider.suppression_factor_cache_period = value;
        self
    }

    fn stale_after(mut self, value: Duration) -> Self {
        self.cleanup.stale_after = value;
        self
    }

    fn cleanup_interval(mut self, value: Duration) -> Self {
        self.cleanup.interval = value;
        self
    }

    fn cleanup_enabled(mut self, enabled: bool) -> Self {
        self.cleanup.enabled = enabled;
        self
    }

    fn build(self) -> Result<Arc<Self::Provider>, TrypemaError> {
        let provider_config = self.provider.validate()?;
        let cleanup = self.cleanup.validate()?;

        let mut config = RedisRateLimiterConfig::new(self.connection_manager, provider_config);

        config.prefix = self.prefix;

        let provider = Arc::new(RedisRateLimiterProvider::new(config, cleanup));

        if cleanup.enabled {
            provider.start_cleanup_loop();
        }

        Ok(provider)
    }
}

/// Provider for Redis-backed distributed rate limiting.
///
/// Enables rate limiting across multiple processes or servers using Redis as a
/// shared backend. All operations are implemented as atomic Lua scripts, so each individual Redis
/// call is atomic while overall admission remains best-effort under concurrency.
///
/// # Requirements
///
/// - **Redis:** >= 7.2.0
/// - **Runtime:** Tokio or Smol
///
/// # Consistency Semantics
///
/// - **Atomic operations:** Each Lua script execution is atomic within Redis
/// - **Best-effort limiting:** Overall rate limiting is approximate (not linearizable)
/// - **Concurrent overshoot:** Multiple clients can exceed limits simultaneously
///
/// See crate-level documentation for data model, cleanup, and operational considerations.
///
/// # Examples
///
/// ```no_run
/// # use trypema::{RateLimiterBuilder, redis::RedisRateLimiterProvider};
/// # async fn example(connection_manager: trypema::redis::ConnectionManager) {
/// # let rl = RedisRateLimiterProvider::builder(connection_manager).build().unwrap();
/// use trypema::{RateLimit, RateLimitDecision};
/// use trypema::redis::RedisKey;
///
/// let key = RedisKey::try_from("user_123").unwrap();
/// let rate = RateLimit::per_second(10.0).unwrap();
///
/// assert!(matches!(
///     rl.absolute().inc(&key, &rate, 1).await.unwrap(),
///     RateLimitDecision::Allowed
/// ));
/// # }
/// ```
#[derive(Debug)]
pub struct RedisRateLimiterProvider {
    absolute: AbsoluteRedisRateLimiter,
    suppressed: SuppressedRedisRateLimiter,
    cleanup: CleanupConfig,
    is_cleanup_loop_running: AtomicBool,
    cleanup_generation: AtomicU64,
}

impl RedisRateLimiterProvider {
    /// Create builder using connection manager and default validated configuration.
    pub fn builder(connection_manager: ConnectionManager) -> RedisRateLimiterBuilder {
        RedisRateLimiterBuilder::new(connection_manager)
    }

    pub(crate) fn new(options: RedisRateLimiterConfig, cleanup: CleanupConfig) -> Self {
        Self {
            absolute: AbsoluteRedisRateLimiter::new(options.clone()),
            suppressed: SuppressedRedisRateLimiter::new(options),
            cleanup,
            is_cleanup_loop_running: AtomicBool::new(false),
            cleanup_generation: AtomicU64::new(0),
        }
    }

    /// Start stale-state cleanup. Calling while already running is no-op.
    pub fn start_cleanup_loop(self: &Arc<Self>) {
        if self.is_cleanup_loop_running.swap(true, Ordering::AcqRel) {
            return;
        }

        let generation = self
            .cleanup_generation
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1);
        let provider = Arc::downgrade(self);
        let cleanup_interval = self.cleanup.interval;

        spawn_task(async move {
            let mut interval = new_interval(cleanup_interval);

            // Tokio intervals tick immediately; Smol intervals wait for the configured duration.
            #[cfg(feature = "redis-tokio")]
            tick(&mut interval).await;

            loop {
                tick(&mut interval).await;

                let Some(provider) = provider.upgrade() else {
                    break;
                };

                if !provider.is_cleanup_loop_running.load(Ordering::Acquire)
                    || provider.cleanup_generation.load(Ordering::Acquire) != generation
                {
                    break;
                }

                if let Err(error) = provider.cleanup(provider.cleanup.stale_after_ms()).await {
                    tracing::warn!(?error, "Redis cleanup failed, will retry");
                }
            }
        });
    }

    /// Stop stale-state cleanup. Calling while stopped is no-op.
    pub fn stop_cleanup_loop(&self) {
        self.cleanup_generation.fetch_add(1, Ordering::AcqRel);
        self.is_cleanup_loop_running.store(false, Ordering::Release);
    }

    /// Access the absolute allow/reject strategy.
    ///
    /// Returns a reference to the Redis absolute rate limiter, which provides
    /// distributed sliding-window enforcement via atomic Lua scripts.
    ///
    /// See [`AbsoluteRedisRateLimiter`] for full documentation.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use trypema::{RateLimiterBuilder, redis::RedisRateLimiterProvider};
    /// # async fn example(connection_manager: trypema::redis::ConnectionManager) {
    /// let rl = RedisRateLimiterProvider::builder(connection_manager).build().unwrap();
    /// use trypema::{RateLimit, RateLimitDecision};
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from("user_123").unwrap();
    /// let rate = RateLimit::per_second(10.0).unwrap();
    /// assert!(matches!(
    ///     rl.absolute().inc(&key, &rate, 1).await.unwrap(),
    ///     RateLimitDecision::Allowed
    /// ));
    /// # }
    /// ```
    pub fn absolute(&self) -> &AbsoluteRedisRateLimiter {
        &self.absolute
    }

    /// Access the suppressed strategy for probabilistic suppression.
    ///
    /// Returns a reference to the Redis suppressed rate limiter, which provides
    /// distributed probabilistic suppression via atomic Lua scripts.
    ///
    /// See [`SuppressedRedisRateLimiter`] for full documentation.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use trypema::{RateLimiterBuilder, redis::RedisRateLimiterProvider};
    /// # async fn example(connection_manager: trypema::redis::ConnectionManager) {
    /// let rl = RedisRateLimiterProvider::builder(connection_manager).build().unwrap();
    /// use trypema::{RateLimit, RateLimitDecision};
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from("user_123").unwrap();
    /// let rate = RateLimit::per_second(10.0).unwrap();
    ///
    /// match rl.suppressed().inc(&key, &rate, 1).await.unwrap() {
    ///     RateLimitDecision::Allowed => {}
    ///     RateLimitDecision::Suppressed {
    ///         is_allowed,
    ///         suppression_factor,
    ///         ..
    ///     } => {
    ///         let _ = (is_allowed, suppression_factor);
    ///     }
    ///     RateLimitDecision::Rejected { .. } => unreachable!(),
    /// }
    /// # }
    /// ```
    pub fn suppressed(&self) -> &SuppressedRedisRateLimiter {
        &self.suppressed
    }

    pub(crate) async fn cleanup(&self, stale_after_ms: u64) -> Result<(), TrypemaError> {
        self.absolute.cleanup(stale_after_ms).await?;
        self.suppressed.cleanup(stale_after_ms).await
    }
}
