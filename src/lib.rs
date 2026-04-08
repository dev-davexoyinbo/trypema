#![doc = include_str!("../docs/lib.md")]
#![cfg_attr(docsrs, feature(doc_cfg), warn(rustdoc::broken_intra_doc_links))]
#![deny(missing_docs)]
#![forbid(unsafe_code)]

#[cfg(all(feature = "redis-tokio", feature = "redis-smol"))]
compile_error!("Features `redis-tokio` and `redis-smol` are mutually exclusive");

mod rate_limiter;
pub use rate_limiter::*;

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
pub(crate) mod runtime;

pub mod local;
use local::*;

/// Redis-backed distributed rate limiter implementations.
///
/// Provides [`RedisRateLimiterProvider`], which executes
/// all operations as atomic Lua scripts against Redis 7.2+. Each `inc()` or `is_allowed()`
/// call results in one Redis round-trip.
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
pub mod redis;

/// Hybrid rate limiter implementations (local fast-path + periodic Redis sync).
///
/// Provides [`HybridRateLimiterProvider`](hybrid::HybridRateLimiterProvider), which maintains
/// local in-memory state for low-latency admission checks and periodically flushes accumulated
/// increments to Redis in batches. This reduces Redis round-trips compared to the pure Redis
/// provider, at the cost of some additional approximation due to sync lag.
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
pub mod hybrid;

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
use redis::*;

mod error;
pub use error::*;

mod common;
pub use common::{
    HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision, SuppressionFactorCacheMs,
    WindowSizeSeconds,
};

#[cfg(test)]
mod tests;

#[doc(hidden)]
pub mod __doctest_helpers;
