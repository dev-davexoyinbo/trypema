use redis::aio::ConnectionManager;

use crate::{
    AbsoluteRedisRateLimiter, HardLimitFactor, RateGroupSizeMs, RedisKey,
    SuppressedRedisRateLimiter, TrypemaError, WindowSizeSeconds, common::SuppressionFactorCacheMs,
    hybrid::SyncIntervalMs,
};

/// Configuration for Redis-backed rate limiters.
///
/// Shared configuration for [`RedisRateLimiterProvider`] and
/// [`crate::hybrid::HybridRateLimiterProvider`].
///
/// The same settings define the Redis connection, key prefix, windowing behavior, and hybrid
/// sync interval.
///
/// # Requirements
///
/// - **Redis version:** >= 7.2.0
/// - **Runtime:** Tokio or Smol (via `redis-tokio` or `redis-smol` features)
///
/// # Examples
///
/// ```
/// # trypema::__doctest_helpers::with_redis_rate_limiter(|rl| async move {
/// use trypema::redis::RedisRateLimiterOptions;
///
/// let url = std::env::var("REDIS_URL")
///     .unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
/// let conn = redis::Client::open(url)
///     .unwrap()
///     .get_connection_manager()
///     .await
///     .unwrap();
///
/// let _options = RedisRateLimiterOptions::new(conn);
/// let _ = rl;
/// # });
/// ```
#[derive(Clone, Debug)]
pub struct RedisRateLimiterOptions {
    /// Redis connection manager from the `redis` crate.
    ///
    /// Use `ConnectionManager` for automatic connection pooling and reconnection.
    ///
    pub connection_manager: ConnectionManager,

    /// Optional prefix for all Redis keys.
    ///
    /// If provided, all keys will be prefixed with `<prefix>:<user_key>:...`
    /// If `None`, defaults to `"trypema"`.
    ///
    /// # Validation
    ///
    /// Must satisfy [`RedisKey`] constraints:
    /// - Not empty
    /// - ≤ 255 bytes
    /// - No `:` character
    ///
    /// # Examples
    ///
    /// ```
    /// use trypema::redis::RedisKey;
    ///
    /// // With prefix: myapp:user_123:absolute:h
    /// let _prefix = Some(RedisKey::try_from("myapp".to_string()).unwrap());
    ///
    /// // Default prefix: trypema:user_123:absolute:h
    /// let _prefix: Option<RedisKey> = None;
    /// ```
    pub prefix: Option<RedisKey>,

    /// Sliding window duration for admission decisions.
    ///
    /// Same semantics as [`crate::local::LocalRateLimiterOptions::window_size_seconds`]. See
    /// [`WindowSizeSeconds`] for details.
    pub window_size_seconds: WindowSizeSeconds,

    /// Bucket coalescing interval in milliseconds.
    ///
    /// Same semantics as [`crate::local::LocalRateLimiterOptions::rate_group_size_ms`]. See
    /// [`RateGroupSizeMs`] for details.
    pub rate_group_size_ms: RateGroupSizeMs,

    /// Hard cutoff multiplier for the suppressed strategy.
    ///
    /// Same semantics as [`crate::local::LocalRateLimiterOptions::hard_limit_factor`]. See
    /// [`HardLimitFactor`] for details.
    pub hard_limit_factor: HardLimitFactor,

    /// Cache duration (milliseconds) for suppression factor recomputation.
    ///
    /// Same semantics as
    /// [`crate::local::LocalRateLimiterOptions::suppression_factor_cache_ms`].
    pub suppression_factor_cache_ms: SuppressionFactorCacheMs,

    /// Sync interval (milliseconds) for the hybrid provider's background flush.
    ///
    /// This option is used by the hybrid provider (`rl.hybrid()`) to determine how often local
    /// increments are committed to Redis.
    ///
    /// The pure Redis provider (`rl.redis()`) does not maintain a local fast-path and therefore
    /// does not use this value.
    pub sync_interval_ms: SyncIntervalMs,
}

impl RedisRateLimiterOptions {
    /// Create a new [`RedisRateLimiterOptions`] with the given `connection_manager`.
    ///
    /// All other fields default to their type's [`Default`] value:
    /// - `prefix`: `None` (uses `"trypema"`)
    /// - `window_size_seconds`: [`WindowSizeSeconds::default()`]
    /// - `rate_group_size_ms`: [`RateGroupSizeMs::default()`]
    /// - `hard_limit_factor`: [`HardLimitFactor::default()`]
    /// - `suppression_factor_cache_ms`: [`SuppressionFactorCacheMs::default()`]
    /// - `sync_interval_ms`: [`SyncIntervalMs::default()`]
    ///
    /// Override specific fields with struct update syntax:
    ///
    /// ```
    /// # trypema::__doctest_helpers::with_redis_rate_limiter(|rl| async move {
    /// use trypema::WindowSizeSeconds;
    /// use trypema::redis::RedisRateLimiterOptions;
    ///
    /// let url = std::env::var("REDIS_URL")
    ///     .unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    /// let conn = redis::Client::open(url)
    ///     .unwrap()
    ///     .get_connection_manager()
    ///     .await
    ///     .unwrap();
    ///
    /// let _options = RedisRateLimiterOptions {
    ///     window_size_seconds: WindowSizeSeconds::new_or_panic(60),
    ///     ..RedisRateLimiterOptions::new(conn)
    /// };
    /// let _ = rl;
    /// # });
    /// ```
    pub fn new(connection_manager: ConnectionManager) -> Self {
        Self {
            connection_manager,
            prefix: None,
            window_size_seconds: WindowSizeSeconds::default(),
            rate_group_size_ms: RateGroupSizeMs::default(),
            hard_limit_factor: HardLimitFactor::default(),
            suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
            sync_interval_ms: SyncIntervalMs::default(),
        }
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
/// # Strategies
///
/// - **Absolute:** ✅ Implemented (Lua scripts)
/// - **Suppressed:** ✅ Implemented (Lua scripts)
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
/// ```
/// # trypema::__doctest_helpers::with_redis_rate_limiter(|rl| async move {
/// use trypema::{RateLimit, RateLimitDecision};
/// use trypema::redis::RedisKey;
///
/// let key = RedisKey::try_from(trypema::__doctest_helpers::unique_key()).unwrap();
/// let rate = RateLimit::try_from(10.0).unwrap();
///
/// assert!(matches!(
///     rl.redis().absolute().inc(&key, &rate, 1).await.unwrap(),
///     RateLimitDecision::Allowed
/// ));
/// # });
/// ```
#[derive(Clone, Debug)]
pub struct RedisRateLimiterProvider {
    absolute: AbsoluteRedisRateLimiter,
    suppressed: SuppressedRedisRateLimiter,
}

impl RedisRateLimiterProvider {
    pub(crate) fn new(options: RedisRateLimiterOptions) -> Self {
        Self {
            absolute: AbsoluteRedisRateLimiter::new(options.clone()),
            suppressed: SuppressedRedisRateLimiter::new(options),
        }
    }

    /// Access the absolute strategy for strict enforcement.
    ///
    /// Returns a reference to the Redis absolute rate limiter, which provides
    /// distributed sliding-window enforcement via atomic Lua scripts.
    ///
    /// **Status:** ✅ Implemented
    ///
    /// See [`AbsoluteRedisRateLimiter`] for full documentation.
    ///
    /// # Examples
    ///
    /// ```
    /// # trypema::__doctest_helpers::with_redis_rate_limiter(|rl| async move {
    /// use trypema::{RateLimit, RateLimitDecision};
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from(trypema::__doctest_helpers::unique_key()).unwrap();
    /// let rate = RateLimit::try_from(10.0).unwrap();
    /// assert!(matches!(
    ///     rl.redis().absolute().inc(&key, &rate, 1).await.unwrap(),
    ///     RateLimitDecision::Allowed
    /// ));
    /// # });
    /// ```
    pub fn absolute(&self) -> &AbsoluteRedisRateLimiter {
        &self.absolute
    }

    /// Access the suppressed strategy for probabilistic suppression.
    ///
    /// **Status:** ✅ Implemented
    ///
    /// Returns a reference to the Redis suppressed rate limiter, which provides
    /// distributed probabilistic suppression via atomic Lua scripts.
    ///
    /// See [`SuppressedRedisRateLimiter`] for full documentation.
    ///
    /// # Examples
    ///
    /// ```
    /// # trypema::__doctest_helpers::with_redis_rate_limiter(|rl| async move {
    /// use trypema::{RateLimit, RateLimitDecision};
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from(trypema::__doctest_helpers::unique_key()).unwrap();
    /// let rate = RateLimit::try_from(10.0).unwrap();
    ///
    /// match rl.redis().suppressed().inc(&key, &rate, 1).await.unwrap() {
    ///     RateLimitDecision::Allowed => {}
    ///     RateLimitDecision::Suppressed {
    ///         is_allowed,
    ///         suppression_factor,
    ///     } => {
    ///         let _ = (is_allowed, suppression_factor);
    ///     }
    ///     RateLimitDecision::Rejected { .. } => unreachable!(),
    /// }
    /// # });
    /// ```
    pub fn suppressed(&self) -> &SuppressedRedisRateLimiter {
        &self.suppressed
    }

    pub(crate) async fn cleanup(&self, stale_after_ms: u64) -> Result<(), TrypemaError> {
        self.absolute.cleanup(stale_after_ms).await?;
        self.suppressed.cleanup(stale_after_ms).await
    }
}
