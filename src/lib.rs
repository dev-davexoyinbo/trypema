#![doc = include_str!("../README.md")]
#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod rate_limiter;
pub use rate_limiter::*;

mod local;
pub use local::*;

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
mod redis;
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
pub use redis::*;

mod error;
pub use error::*;

mod common;
pub use common::{
    HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision, WindowSizeSeconds,
};

#[cfg(test)]
mod tests;
