use crate::{AbsoluteRedisRateLimiter, RedisKey, SuppressedRedisRateLimiter};

/// Configuration for Redis rate limiter implementations.
#[derive(Clone, Debug)]
pub struct RedisRateLimiterOptions {
    /// The Redis client to use.
    pub redis_client: redis::Client,
    /// The prefix to use for keys.
    pub prefix: Option<RedisKey>,
}

/// A rate limiter backed by Redis.
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

    /// Absolute Redis rate limiter implementation.
    pub fn absolute(&self) -> &AbsoluteRedisRateLimiter {
        &self.absolute
    }

    /// Suppressed Redis rate limiter implementation.
    pub fn suppressed(&self) -> &SuppressedRedisRateLimiter {
        &self.suppressed
    }
}
