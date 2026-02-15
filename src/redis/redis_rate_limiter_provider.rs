/// Configuration for Redis rate limiter implementations.
#[derive(Clone, Debug)]
pub struct RedisRateLimiterOptions {}

/// A rate limiter backed by Redis.
pub struct RedisRateLimiterProvider {
    options: RedisRateLimiterOptions,
}

impl RedisRateLimiterProvider {
    pub(crate) fn new(options: RedisRateLimiterOptions) -> Self {
        Self { options }
    }
}
