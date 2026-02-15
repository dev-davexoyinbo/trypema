#![doc = include_str!("../README.md")]
#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod rate_limiter;
pub use rate_limiter::*;

mod local;
pub use local::*;

mod common;
pub use common::RateLimitDecision;

#[cfg(test)]
mod tests;
