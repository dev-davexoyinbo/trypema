#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg), warn(rustdoc::broken_intra_doc_links))]
#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod rate_limiter;
pub use rate_limiter::*;

pub mod local;
use local::*;

/// Redis-specific rate limiter implementations.
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
pub mod redis;
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
use redis::*;

mod error;
pub use error::*;

mod common;
pub use common::{
    HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision, WindowSizeSeconds,
};

#[cfg(test)]
mod tests;
