//! Top-level entrypoint that wires provider implementations.
//!
//! Today the crate ships a single provider (`local`), exposed via
//! [`RateLimiter::local`]. Additional providers (e.g. shared/distributed state) can be
//! added behind this facade.

use crate::{LocalRateLimiterOptions, LocalRateLimiterProvider};

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
use crate::{RedisRateLimiterOptions, RedisRateLimiterProvider};

/// Top-level configuration for [`RateLimiter`].
#[derive(Clone, Debug)]
pub struct RateLimiterOptions {
    /// Options for the local provider.
    pub local: LocalRateLimiterOptions,
    /// Options for the Redis provider.
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    pub redis: RedisRateLimiterOptions,
}

/// Rate limiter entrypoint.
///
/// This type wires together one or more providers (currently `local`).
pub struct RateLimiter {
    local: LocalRateLimiterProvider,
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
    redis: RedisRateLimiterProvider,
}

impl RateLimiter {
    /// Create a new [`RateLimiter`].
    pub fn new(options: RateLimiterOptions) -> Self {
        Self {
            local: LocalRateLimiterProvider::new(options.local),
            #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
            #[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
            redis: RedisRateLimiterProvider::new(options.redis),
        }
    }

    /// Access the Redis provider.
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
    pub fn redis(&self) -> &RedisRateLimiterProvider {
        &self.redis
    }

    /// Access the local provider.
    pub fn local(&self) -> &LocalRateLimiterProvider {
        &self.local
    }
}
