use std::sync::Arc;

use redis::aio::ConnectionManager;

use crate::{
    RateLimiterBuilder, RateLimiterTarget, TrypemaError,
    builder::{CleanupConfig, ProviderConfig, sealed},
    cleanup_loop::CleanupLoop,
    hybrid::{AbsoluteHybridRateLimiter, AllowedLag, SuppressedHybridRateLimiter, SyncInterval},
    redis::RedisKey,
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

/// Backend of a [`RateLimiterBuilder`] that builds local state that synchronizes with Redis.
///
/// Holds the connection manager, the key prefix, the synchronization interval, and the allowed
/// lag settings. Create the builder with [`RateLimiterBuilder::hybrid`].
#[derive(Clone, Debug)]
pub struct HybridBackend {
    connection_manager: ConnectionManager,
    prefix: Option<RedisKey>,
    sync_interval: SyncInterval,
    allowed_lag: AllowedLag,
    allowed_lag_applies_to_rejections: bool,
}

impl HybridBackend {
    pub(crate) fn into_config(self, provider: ProviderConfig) -> HybridRateLimiterConfig {
        HybridRateLimiterConfig {
            connection_manager: self.connection_manager,
            prefix: self.prefix,
            provider,
            sync_interval: self.sync_interval,
            allowed_lag: self.allowed_lag,
            allowed_lag_applies_to_rejections: self.allowed_lag_applies_to_rejections,
        }
    } // end fn into_config
} // end impl

#[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
impl RateLimiterBuilder<HybridBackend> {
    /// Create a builder for the hybrid provider or one of its two limiters.
    ///
    /// The type that the caller expects from `build()` selects the target:
    /// [`HybridRateLimiterProvider`], [`AbsoluteHybridRateLimiter`], or
    /// [`SuppressedHybridRateLimiter`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example(connection_manager: trypema::redis::ConnectionManager) -> Result<(), trypema::TrypemaError> {
    /// use std::sync::Arc;
    /// use trypema::{
    ///     RateLimiterBuilder, WindowSize,
    ///     hybrid::{AbsoluteHybridRateLimiter, HybridRateLimiterProvider, SyncInterval},
    ///     redis::RedisKey,
    /// };
    ///
    /// let provider: Arc<HybridRateLimiterProvider> =
    ///     RateLimiterBuilder::hybrid(connection_manager.clone()).build()?;
    ///
    /// let limiter: Arc<AbsoluteHybridRateLimiter> = RateLimiterBuilder::hybrid(connection_manager)
    ///     // Optional: only available with 'redis-tokio' or 'redis-smol'
    ///     .prefix(RedisKey::try_from("my-service")?)
    ///     // Optional: only available with 'redis-tokio' or 'redis-smol'
    ///     .sync_interval(SyncInterval::milliseconds_or_panic(10))
    ///     // Optional: only available with 'redis-tokio' or 'redis-smol'
    ///     .allowed_lag_applies_to_rejections(true)
    ///     // Optional: sliding-window duration
    ///     .window_size(WindowSize::seconds_or_panic(60))
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A target of another backend does not compile:
    ///
    /// ```compile_fail
    /// use std::sync::Arc;
    /// use trypema::{RateLimiterBuilder, hybrid::AbsoluteHybridRateLimiter};
    ///
    /// let limiter: Arc<AbsoluteHybridRateLimiter> = RateLimiterBuilder::local().build()?;
    /// # Ok::<(), trypema::TrypemaError>(())
    /// ```
    pub fn hybrid(connection_manager: ConnectionManager) -> Self {
        Self::new(HybridBackend {
            connection_manager,
            prefix: None,
            sync_interval: SyncInterval::default(),
            allowed_lag: AllowedLag::default(),
            allowed_lag_applies_to_rejections: false,
        })
    } // end constructor
} // end impl

#[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
impl<T> RateLimiterBuilder<HybridBackend, T> {
    /// Set prefix used for Redis keys.
    pub fn prefix(mut self, value: RedisKey) -> Self {
        self.backend.prefix = Some(value);
        self
    } // end method prefix

    /// Set background Redis synchronization interval.
    pub fn sync_interval(mut self, value: SyncInterval) -> Self {
        self.backend.sync_interval = value;
        self
    } // end method sync_interval

    /// Set the maximum age of local state that answers a read-only call.
    ///
    /// A read-only call on local state older than this lag reads Redis again. See
    /// [`AllowedLag`] for the calls it applies to.
    pub fn allowed_lag(mut self, value: AllowedLag) -> Self {
        self.backend.allowed_lag = value;
        self
    } // end method allowed_lag

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
    /// The suppressed strategy, of a provider or a [`SuppressedHybridRateLimiter`], does not use
    /// this setting. Its suppression cache is bounded by the suppression-factor cache period.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use trypema::hybrid::{AllowedLag, HybridRateLimiterProvider};
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
        self.backend.allowed_lag_applies_to_rejections = value;
        self
    } // end method allowed_lag_applies_to_rejections
} // end impl

impl sealed::Sealed for HybridRateLimiterProvider {}

impl RateLimiterTarget for HybridRateLimiterProvider {
    type Backend = HybridBackend;

    fn build_from(builder: RateLimiterBuilder<HybridBackend>) -> Result<Arc<Self>, TrypemaError> {
        let (backend, config, cleanup) = builder.validate()?;

        let provider = Arc::new(Self::new(backend.into_config(config), cleanup));

        if cleanup.enabled {
            provider.start_cleanup_loop();
        }

        Ok(provider)
    }
} // end impl

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
    cleanup_loop: CleanupLoop,
}

impl HybridRateLimiterProvider {
    /// Create a builder whose `build()` returns this provider.
    ///
    /// Same as [`RateLimiterBuilder::hybrid`] with the target fixed to this type.
    pub fn builder(
        connection_manager: ConnectionManager,
    ) -> RateLimiterBuilder<HybridBackend, Self> {
        RateLimiterBuilder::hybrid(connection_manager).with_target()
    }

    pub(crate) fn new(options: HybridRateLimiterConfig, cleanup: CleanupConfig) -> Self {
        Self {
            absolute: AbsoluteHybridRateLimiter::new(options.clone(), cleanup),
            suppressed: SuppressedHybridRateLimiter::new(options, cleanup),
            cleanup_loop: CleanupLoop::new(cleanup),
        }
    }

    /// Start stale-state cleanup. Calling while already running is no-op.
    pub fn start_cleanup_loop(self: &Arc<Self>) {
        CleanupLoop::start_task(
            self,
            |provider| &provider.cleanup_loop,
            |provider, stale_after_ms| async move { provider.cleanup(stale_after_ms).await },
            "Hybrid cleanup failed, will retry",
        );
    }

    /// Stop stale-state cleanup. Calling while stopped is no-op.
    pub fn stop_cleanup_loop(&self) {
        self.cleanup_loop.stop();
    }

    /// Access the absolute allow/reject strategy.
    ///
    /// See [`AbsoluteHybridRateLimiter`] for full documentation.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use trypema::hybrid::HybridRateLimiterProvider;
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
    /// # use trypema::hybrid::HybridRateLimiterProvider;
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
