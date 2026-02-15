//! Top-level entrypoint that wires provider implementations.
//!
//! Today the crate ships a single provider (`local`), exposed via
//! [`RateLimiter::local`]. Additional providers (e.g. shared/distributed state) can be
//! added behind this facade.

use crate::{
    LocalRateLimiterOptions, LocalRateLimiterProvider, RedisRateLimiterOptions,
    RedisRateLimiterProvider,
};

/// Top-level configuration for [`RateLimiter`].
#[derive(Clone, Debug)]
pub struct RateLimiterOptions {
    /// Options for the local provider.
    pub local: LocalRateLimiterOptions,
    /// Options for the Redis provider.
    pub redis: RedisRateLimiterOptions,
}

/// Rate limiter entrypoint.
///
/// This type wires together one or more providers (currently `local`).
pub struct RateLimiter {
    local: LocalRateLimiterProvider,
    redis: RedisRateLimiterProvider,
}

impl RateLimiter {
    /// Create a new [`RateLimiter`].
    pub fn new(options: RateLimiterOptions) -> Self {
        Self {
            local: LocalRateLimiterProvider::new(options.local),
            redis: RedisRateLimiterProvider::new(options.redis),
        }
    }

    /// Access the Redis provider.
    pub fn redis(&self) -> &RedisRateLimiterProvider {
        &self.redis
    }

    /// Access the local provider.
    pub fn local(&self) -> &LocalRateLimiterProvider {
        &self.local
    }
}
