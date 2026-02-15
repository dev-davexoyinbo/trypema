use crate::RedisRateLimiterOptions;

/// A rate limiter backed by Redis.
pub struct SuppressedRedisRateLimiter {}

impl SuppressedRedisRateLimiter {
    pub(crate) fn new(_options: RedisRateLimiterOptions) -> Self {
        Self {}
    }
}
