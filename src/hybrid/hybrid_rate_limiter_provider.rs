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
    hybrid::{AbsoluteHybridRateLimiter, AllowedLag, SuppressedHybridRateLimiter, SyncInterval},
    redis::RedisKey,
    runtime::{new_interval, spawn_task, tick},
};

#[derive(Clone, Debug)]
pub(crate) struct HybridRateLimiterConfig {
    pub connection_manager: ConnectionManager,
    pub prefix: Option<RedisKey>,
    pub provider: ProviderConfig,
    pub sync_interval: SyncInterval,
    pub allowed_lag: AllowedLag,
    pub allowed_lag_applies_to_rejections: bool,
}

/// Builder for [`HybridRateLimiterProvider`].
#[derive(Clone, Debug)]
pub struct HybridRateLimiterBuilder {
    connection_manager: ConnectionManager,
    prefix: Option<RedisKey>,
    provider: ProviderConfig,
    sync_interval: SyncInterval,
    allowed_lag: AllowedLag,
    allowed_lag_applies_to_rejections: bool,
    cleanup: CleanupConfig,
}

impl HybridRateLimiterBuilder {
    fn new(connection_manager: ConnectionManager) -> Self {
        Self {
            connection_manager,
            prefix: None,
            provider: ProviderConfig::default(),
            sync_interval: SyncInterval::default(),
            allowed_lag: AllowedLag::default(),
            allowed_lag_applies_to_rejections: false,
            cleanup: CleanupConfig::default(),
        }
    }

    /// Set prefix used for Redis keys.
    pub fn prefix(mut self, value: RedisKey) -> Self {
        self.prefix = Some(value);
        self
    }

    /// Set background Redis synchronization interval.
    pub fn sync_interval(mut self, value: SyncInterval) -> Self {
        self.sync_interval = value;
        self
    }

    /// Set the maximum age of local state that answers a read-only call.
    ///
    /// A read-only call on local state older than this lag reads Redis again. See
    /// [`AllowedLag`] for the calls it applies to.
    pub fn allowed_lag(mut self, value: AllowedLag) -> Self {
        self.allowed_lag = value;
        self
    }

    /// Apply the allowed lag to the rejection cache of the absolute strategy.
    ///
    /// Default `false`: a rejected key answers `is_allowed` and `get_estimate` from its local
    /// rejection cache until the retry TTL ends, which can be up to one window. A limit that
    /// another instance deletes or raises in Redis stays invisible for that time.
    ///
    /// With `true`, `is_allowed` and `get_estimate` on a rejected key read Redis again once the
    /// Redis read behind the rejection is older than the allowed lag. Concurrent calls share one
    /// Redis read, and a refresh that still rejects answers later calls from the cache until the
    /// lag passes again. `inc` and `try_inc` keep the retry TTL rule. A checked rejected key is
    /// refreshed more often, and each refresh that still rejects starts a new cache time, so
    /// cleanup keeps it while it is checked, as with `false`.
    ///
    /// The suppressed strategy does not use this setting. Its suppression cache is bounded by
    /// the suppression-factor cache period.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use trypema::{RateLimiterBuilder, hybrid::{AllowedLag, HybridRateLimiterProvider}};
    /// # async fn example(connection_manager: trypema::redis::ConnectionManager) {
    /// let provider = HybridRateLimiterProvider::builder(connection_manager)
    ///     // Optional: only available with 'redis-tokio' or 'redis-smol'
    ///     .allowed_lag(AllowedLag::seconds_or_panic(1))
    ///     // Optional: only available with 'redis-tokio' or 'redis-smol'
    ///     .allowed_lag_applies_to_rejections(true)
    ///     .build()
    ///     .unwrap();
    /// # let _ = provider;
    /// # }
    /// ```
    pub fn allowed_lag_applies_to_rejections(mut self, value: bool) -> Self {
        self.allowed_lag_applies_to_rejections = value;
        self
    }
}

impl RateLimiterBuilder for HybridRateLimiterBuilder {
    type Provider = HybridRateLimiterProvider;

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

        let config = HybridRateLimiterConfig {
            connection_manager: self.connection_manager,
            prefix: self.prefix,
            provider: provider_config,
            sync_interval: self.sync_interval,
            allowed_lag: self.allowed_lag,
            allowed_lag_applies_to_rejections: self.allowed_lag_applies_to_rejections,
        };

        let provider = Arc::new(HybridRateLimiterProvider::new(config, cleanup));

        if cleanup.enabled {
            provider.start_cleanup_loop();
        }

        Ok(provider)
    }
}

/// Provider for hybrid rate limiting (local fast-path + Redis sync).
///
/// This provider is backed by Redis, but keeps per-key in-memory state so common-case admission
/// checks can avoid per-request Redis I/O. Local increments are flushed to Redis in batches.
///
/// It is constructed from a public Redis connection manager and exposes
/// [`AbsoluteHybridRateLimiter`] and [`SuppressedHybridRateLimiter`] as strategies.
///
/// Compared with the Redis provider, hybrid admission avoids steady-state per-request Redis I/O
/// and batches commits. Increments from this instance reach Redis within the synchronization
/// interval. A read-only call answers from `Accepting` or absent-key state only while that state
/// is younger than the allowed lag. A cached rejection answers until its retry TTL ends (up to
/// one window), or with `allowed_lag_applies_to_rejections(true)` also only within the allowed
/// lag for read-only calls. A cached suppression state answers until its cache period ends.
///
/// # Strategies
///
/// - [`HybridRateLimiterProvider::absolute`]: deterministic sliding-window enforcement
/// - [`HybridRateLimiterProvider::suppressed`]: probabilistic suppression near/over the limit
///
/// # Requirements
///
/// - Redis 7.2+
/// - One of: `redis-tokio` or `redis-smol` features
#[derive(Debug)]
pub struct HybridRateLimiterProvider {
    absolute: Arc<AbsoluteHybridRateLimiter>,
    suppressed: Arc<SuppressedHybridRateLimiter>,
    cleanup: CleanupConfig,
    is_cleanup_loop_running: AtomicBool,
    cleanup_generation: AtomicU64,
}

impl HybridRateLimiterProvider {
    /// Create builder using connection manager and default validated configuration.
    pub fn builder(connection_manager: ConnectionManager) -> HybridRateLimiterBuilder {
        HybridRateLimiterBuilder::new(connection_manager)
    }

    pub(crate) fn new(options: HybridRateLimiterConfig, cleanup: CleanupConfig) -> Self {
        Self {
            absolute: AbsoluteHybridRateLimiter::new(options.clone()),
            suppressed: SuppressedHybridRateLimiter::new(options),
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
                    tracing::warn!(?error, "Hybrid cleanup failed, will retry");
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
    /// See [`AbsoluteHybridRateLimiter`] for full documentation.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use trypema::{RateLimiterBuilder, hybrid::HybridRateLimiterProvider};
    /// # async fn example(connection_manager: trypema::redis::ConnectionManager) {
    /// let rl = HybridRateLimiterProvider::builder(connection_manager).build().unwrap();
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
    pub fn absolute(&self) -> &AbsoluteHybridRateLimiter {
        &self.absolute
    }

    /// Access the suppressed strategy for probabilistic suppression.
    ///
    /// See [`SuppressedHybridRateLimiter`] for full documentation.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use trypema::{RateLimiterBuilder, hybrid::HybridRateLimiterProvider};
    /// # async fn example(connection_manager: trypema::redis::ConnectionManager) {
    /// let rl = HybridRateLimiterProvider::builder(connection_manager).build().unwrap();
    /// use trypema::{RateLimit, RateLimitDecision};
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from("user_123").unwrap();
    /// let rate = RateLimit::per_second(10.0).unwrap();
    /// assert!(matches!(
    ///     rl.suppressed().inc(&key, &rate, 1).await.unwrap(),
    ///     RateLimitDecision::Allowed
    /// ));
    /// # }
    /// ```
    pub fn suppressed(&self) -> &SuppressedHybridRateLimiter {
        &self.suppressed
    }

    pub(crate) async fn cleanup(&self, stale_after_ms: u64) -> Result<(), TrypemaError> {
        // The two strategies are independent. A failure of one must not stop the other.
        let absolute = self.absolute.cleanup(stale_after_ms).await;
        let suppressed = self.suppressed.cleanup(stale_after_ms).await;

        if let (Err(_), Err(error)) = (&absolute, &suppressed) {
            tracing::warn!(?error, "Suppressed cleanup also failed");
        }

        absolute.and(suppressed)
    }
}
