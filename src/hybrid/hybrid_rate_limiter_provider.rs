use std::sync::Arc;

use crate::{
    TrypemaError,
    hybrid::{
        absolute_hybrid_rate_limiter::AbsoluteHybridRateLimiter,
        suppressed_hybrid_rate_limiter::SuppressedHybridRateLimiter,
    },
    redis::RedisRateLimiterOptions,
};

/// Provider for hybrid rate limiting (local fast-path + Redis sync).
///
/// This provider is backed by Redis, but keeps per-key in-memory state so common-case admission
/// checks can avoid per-request Redis I/O. Local increments are flushed to Redis in batches.
///
/// It uses [`RedisRateLimiterOptions`] for the shared Redis connection and timing configuration,
/// and exposes [`AbsoluteHybridRateLimiter`] and [`SuppressedHybridRateLimiter`] as strategies.
///
/// Compared to the pure Redis provider (`rl.redis()`):
/// - ✅ Lower steady-state latency for admission checks (no per-request Redis I/O)
/// - ✅ Reduced Redis load via batched commits
/// - ❌ More approximation: admission decisions reflect Redis state with up to `sync_interval_ms`
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
#[derive(Clone, Debug)]
pub struct HybridRateLimiterProvider {
    absolute: Arc<AbsoluteHybridRateLimiter>,
    suppressed: Arc<SuppressedHybridRateLimiter>,
}

impl HybridRateLimiterProvider {
    pub(crate) fn new(options: RedisRateLimiterOptions) -> Self {
        Self {
            absolute: AbsoluteHybridRateLimiter::new(options.clone()),
            suppressed: SuppressedHybridRateLimiter::new(options),
        }
    }

    /// Access the absolute strategy for strict sliding-window enforcement.
    ///
    /// See [`AbsoluteHybridRateLimiter`] for full documentation.
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
    ///     rl.hybrid().absolute().inc(&key, &rate, 1).await.unwrap(),
    ///     RateLimitDecision::Allowed
    /// ));
    /// # });
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
    /// ```
    /// # trypema::__doctest_helpers::with_redis_rate_limiter(|rl| async move {
    /// use trypema::{RateLimit, RateLimitDecision};
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from(trypema::__doctest_helpers::unique_key()).unwrap();
    /// let rate = RateLimit::try_from(10.0).unwrap();
    /// assert!(matches!(
    ///     rl.hybrid().suppressed().inc(&key, &rate, 1).await.unwrap(),
    ///     RateLimitDecision::Allowed
    /// ));
    /// # });
    /// ```
    pub fn suppressed(&self) -> &SuppressedHybridRateLimiter {
        &self.suppressed
    }

    pub(crate) async fn cleanup(&self, stale_after_ms: u64) -> Result<(), TrypemaError> {
        self.absolute.cleanup(stale_after_ms).await?;
        self.suppressed.cleanup(stale_after_ms).await
    }
}
