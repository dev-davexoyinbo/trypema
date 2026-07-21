//! In-process rate limiting provider.
//!
//! The local provider maintains rate limiting state within the current process using
//! thread-safe data structures ([`DashMap`](dashmap::DashMap) and atomics).
//!
//! It is synchronous, requires no external service, and keeps state within one process. Use
//! [`AbsoluteLocalRateLimiter`] for allow/reject admission or [`SuppressedLocalRateLimiter`] for
//! probabilistic load shedding. Absolute admission is best-effort under concurrency.
//!
//! # Examples
//!
//! ```
//! use trypema::{RateLimit, RateLimiterBuilder, local::LocalRateLimiterProvider};
//!
//! let rl = LocalRateLimiterProvider::builder()
//!     .disable_cleanup()
//!     .build()
//!     .unwrap();
//! let rate = RateLimit::per_second(10.0).unwrap();
//!
//! // Absolute strategy: allow/reject admission
//! let decision = rl.absolute().inc("user_123", &rate, 1);
//!
//! // Suppressed strategy: probabilistic suppression
//! let decision = rl.suppressed().inc("api_endpoint", &rate, 1);
//! ```

mod absolute_local_rate_limiter;
pub use absolute_local_rate_limiter::AbsoluteLocalRateLimiter;

mod local_rate_limiter_provider;
pub use local_rate_limiter_provider::{LocalRateLimiterBuilder, LocalRateLimiterProvider};

pub(crate) mod suppressed_local_rate_limiter;
pub use suppressed_local_rate_limiter::SuppressedLocalRateLimiter;
