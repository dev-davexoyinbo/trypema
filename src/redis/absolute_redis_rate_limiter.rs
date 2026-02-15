use crate::RedisRateLimiterOptions;

/// A rate limiter backed by Redis.
pub struct AbsoluteRedisRateLimiter {}

impl AbsoluteRedisRateLimiter {
    pub(crate) fn new(_options: RedisRateLimiterOptions) -> Self {
        Self {}
    }
}
