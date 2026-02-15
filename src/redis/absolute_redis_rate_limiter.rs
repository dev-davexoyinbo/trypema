use crate::{RateLimitDecision, RedisRateLimiterOptions, TrypemaError};

/// A rate limiter backed by Redis.
pub struct AbsoluteRedisRateLimiter {
    redis_client: redis::Client,
}

impl AbsoluteRedisRateLimiter {
    pub(crate) fn new(options: RedisRateLimiterOptions) -> Self {
        Self {
            redis_client: options.redis_client,
        }
    }

    /// Check admission and, if allowed, increment the observed count for `key`.
    pub fn inc(
        &self,
        _key: &str,
        _rate_limit: &crate::RateLimit,
        _count: u64,
    ) -> Result<RateLimitDecision, TrypemaError> {
        todo!("AbsoluteRedisRateLimiter::inc");
    }

    /// Determine whether `key` is currently allowed.
    ///
    /// Returns [`RateLimitDecision::Allowed`] if the current sliding window total
    /// is below the window limit, otherwise returns [`RateLimitDecision::Rejected`]
    /// with a best-effort `retry_after_ms`.
    ///
    /// This method performs lazy eviction of expired buckets for the key.
    pub fn is_allowed(&self, _key: &str) -> crate::RateLimitDecision {
        todo!("AbsoluteRedisRateLimiter::is_allowed");
    }
}
