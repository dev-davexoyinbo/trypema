use redis::aio::ConnectionManager;

use crate::{
    AbsoluteRedisRateLimiter, HardLimitFactor, RateGroupSizeMs, RedisKey,
    SuppressedRedisRateLimiter, WindowSizeSeconds,
};

/// Configuration for Redis rate limiter implementations.
#[derive(Clone, Debug)]
pub struct RedisRateLimiterOptions {
    /// The Redis client to use.
    pub connection_manager: ConnectionManager,
    /// The prefix to use for keys.
    pub prefix: Option<RedisKey>,
    /// Sliding window size in seconds.
    pub window_size_seconds: WindowSizeSeconds,
    /// Coalescing interval (in milliseconds) for grouping increments close in time.
    pub rate_group_size_ms: RateGroupSizeMs,

    /// Multiplier used by [`SuppressedRedisRateLimiter`] as a hard cutoff.
    pub hard_limit_factor: HardLimitFactor,
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
