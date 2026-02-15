//! Trypema is a set of high-performance rate limiting primitives.
//!
//! The crate focuses on:
//! - concurrency safety (multi-threaded access)
//! - low per-request overhead
//! - predictable latency under load
//!
//! This crate is still in development (pre-release); APIs and behavior may change.
//!
//! # Quick start
//!
//! ```rust
//! use trypema::{LocalRateLimiterOptions, RateLimiter, RateLimiterOptions, RateLimitDecision};
//!
//! let rl = RateLimiter::new(RateLimiterOptions {
//!     local: LocalRateLimiterOptions {
//!         window_size_seconds: 60,
//!         rate_group_size_ms: 10,
//!     },
//! });
//!
//! let key = "user:123";
//! rl.local().absolute().inc(key, /*rate_limit=*/ 5, /*count=*/ 1);
//!
//! match rl.local().absolute().is_allowed(key) {
//!     RateLimitDecision::Allowed => {
//!         // proceed
//!     }
//!     RateLimitDecision::Rejected { retry_after_ms, .. } => {
//!         let _ = retry_after_ms;
//!         // reject / retry later
//!     }
//! }
//! ```

mod rate_limiter;
pub use rate_limiter::*;

mod local;
pub use local::*;

mod common;
pub use common::RateLimitDecision;

#[cfg(test)]
mod tests;
