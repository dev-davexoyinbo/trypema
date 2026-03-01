use redis::aio::ConnectionManager;

use crate::{
    AbsoluteRedisRateLimiter, HardLimitFactor, RateGroupSizeMs, RedisKey,
    SuppressedRedisRateLimiter, TrypemaError, WindowSizeSeconds, common::SuppressionFactorCacheMs,
    hybrid::SyncIntervalMs,
};

/// Configuration for Redis-backed rate limiters.
///
/// Configures connection, key prefix, and rate limiting parameters for the Redis provider.
///
/// # Requirements
///
/// - **Redis version:** >= 6.2.0
/// - **Runtime:** Tokio or Smol (via `redis-tokio` or `redis-smol` features)
///
/// # Examples
///
/// ```rust,no_run
/// # async fn example() -> Result<(), trypema::TrypemaError> {
/// use trypema::{HardLimitFactor, RateGroupSizeMs, WindowSizeSeconds};
/// use trypema::SuppressionFactorCacheMs;
/// use trypema::hybrid::SyncIntervalMs;
/// use trypema::redis::{RedisKey, RedisRateLimiterOptions};
///
/// let sync_interval_ms = SyncIntervalMs::default();
/// let options = RedisRateLimiterOptions {
///     connection_manager: todo!("create redis::aio::ConnectionManager"),
///     prefix: Some(RedisKey::try_from("myapp".to_string())?),
///     window_size_seconds: WindowSizeSeconds::try_from(60)?,
///     rate_group_size_ms: RateGroupSizeMs::try_from(10)?,
///     hard_limit_factor: HardLimitFactor::try_from(1.5)?,
///     suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
///     sync_interval_ms,
/// };
/// let _ = options;
/// # Ok(()) }
/// ```
#[derive(Clone, Debug)]
pub struct RedisRateLimiterOptions {
    /// Redis connection manager from the `redis` crate.
    ///
    /// Use `ConnectionManager` for automatic connection pooling and reconnection.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # async fn example() -> Result<(), trypema::TrypemaError> {
    /// let _connection_manager: redis::aio::ConnectionManager =
    ///     todo!("create redis::aio::ConnectionManager");
    /// # Ok(()) }
    /// ```
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
    /// ```rust,no_run
    /// use trypema::redis::RedisKey;
    ///
    /// // With prefix: myapp:user_123:absolute:h
    /// let _prefix = Some(RedisKey::try_from("myapp".to_string())?);
    ///
    /// // Default prefix: trypema:user_123:absolute:h
    /// let _prefix: Option<RedisKey> = None;
    /// # Ok::<(), trypema::TrypemaError>(())
    /// ```
    pub prefix: Option<RedisKey>,

    /// Sliding window duration for admission decisions.
    ///
    /// Same semantics as local provider. See [`WindowSizeSeconds`] for details.
    pub window_size_seconds: WindowSizeSeconds,

    /// Bucket coalescing interval in milliseconds.
    ///
    /// Same semantics as local provider. See [`RateGroupSizeMs`] for details.
    pub rate_group_size_ms: RateGroupSizeMs,

    /// Hard cutoff multiplier for the suppressed strategy.
    ///
    /// Same semantics as local provider. See [`HardLimitFactor`] for details.
    pub hard_limit_factor: HardLimitFactor,

    /// Cache duration (milliseconds) for suppression factor recomputation.
    ///
    /// Same semantics as local provider.
    pub suppression_factor_cache_ms: SuppressionFactorCacheMs,

    /// Sync interval (milliseconds) for flushing local cache.
    ///
    /// Same semantics as local provider.
    pub sync_interval_ms: SyncIntervalMs,
}

/// Provider for Redis-backed distributed rate limiting.
///
/// Enables rate limiting across multiple processes or servers using Redis as a
/// shared backend. All operations are implemented as atomic Lua scripts.
///
/// # Requirements
///
/// - **Redis:** >= 6.2.0
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
/// ```rust,no_run
/// # async fn example() -> Result<(), trypema::TrypemaError> {
/// use trypema::{
///     HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision, RateLimiter,
///     RateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds,
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
/// match rl.redis().absolute().inc(&key, &rate, 1).await? {
///     RateLimitDecision::Allowed => {}
///     RateLimitDecision::Rejected { retry_after_ms, .. } => {
///         let _ = retry_after_ms;
///     }
///     _ => unreachable!(),
/// }
/// # Ok(()) }
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
    /// ```rust,no_run
    /// # async fn example(rl: &trypema::RateLimiter, key: &trypema::redis::RedisKey, rate: &trypema::RateLimit) -> Result<(), trypema::TrypemaError> {
    /// let _decision = rl.redis().absolute().inc(key, rate, 1).await?;
    /// # Ok(()) }
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
    pub fn suppressed(&self) -> &SuppressedRedisRateLimiter {
        &self.suppressed
    }

    pub(crate) async fn cleanup(&self, stale_after_ms: u64) -> Result<(), TrypemaError> {
        self.absolute.cleanup(stale_after_ms).await?;
        self.suppressed.cleanup(stale_after_ms).await
    }
}
