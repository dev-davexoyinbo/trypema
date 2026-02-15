use crate::{AbsoluteRedisRateLimiter, SuppressedRedisRateLimiter};

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

    /// Absolute Redis rate limiter implementation.
    pub fn absolute(&self) -> AbsoluteRedisRateLimiter {
        AbsoluteRedisRateLimiter::new(self.options.clone())
    }

    /// Suppressed Redis rate limiter implementation.
    pub fn suppressed(&self) -> SuppressedRedisRateLimiter {
        SuppressedRedisRateLimiter::new(self.options.clone())
    }
}
