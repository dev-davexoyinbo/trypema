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
    hybrid::{AbsoluteHybridRateLimiter, SuppressedHybridRateLimiter, SyncInterval},
    redis::RedisKey,
    runtime::{new_interval, spawn_task, tick},
};

#[derive(Clone, Debug)]
pub(crate) struct HybridRateLimiterConfig {
    pub connection_manager: ConnectionManager,
    pub prefix: Option<RedisKey>,
    pub provider: ProviderConfig,
    pub sync_interval: SyncInterval,
}

/// Builder for [`HybridRateLimiterProvider`].
#[derive(Clone, Debug)]
pub struct HybridRateLimiterBuilder {
    connection_manager: ConnectionManager,
    prefix: Option<RedisKey>,
    provider: ProviderConfig,
    sync_interval: SyncInterval,
    cleanup: CleanupConfig,
}

impl HybridRateLimiterBuilder {
    fn new(connection_manager: ConnectionManager) -> Self {
        Self {
            connection_manager,
            prefix: None,
            provider: ProviderConfig::default(),
            sync_interval: SyncInterval::default(),
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
/// Compared to the pure Redis provider:
/// - ✅ Lower steady-state latency for admission checks (no per-request Redis I/O)
/// - ✅ Reduced Redis load via batched commits
/// - ❌ More approximation: admission decisions reflect Redis state with up to `sync_interval`
///   of lag
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

    /// Access the absolute strategy for strict sliding-window enforcement.
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
        self.absolute.cleanup(stale_after_ms).await?;
        self.suppressed.cleanup(stale_after_ms).await
    }
}
