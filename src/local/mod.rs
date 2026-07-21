//! In-process rate limiting provider.
//!
//! The local provider maintains rate limiting state within the current process using
//! thread-safe data structures ([`DashMap`](dashmap::DashMap) and atomics).
//!
//! # Key Characteristics
//!
//! - **Thread-safe:** Safe for concurrent use across multiple threads
//! - **Zero external dependencies:** No network or database required
//! - **Low latency:** Sub-microsecond admission checks (no I/O)
//! - **Process-scoped:** State is not shared across processes
//!
//! # Strategies
//!
//! - [`AbsoluteLocalRateLimiter`]: Strict sliding-window enforcement
//! - [`SuppressedLocalRateLimiter`]: Probabilistic suppression for graceful degradation
//!
//! # When to Use
//!
//! ✅ **Use local provider when:**
//! - Single-process application
//! - Low-latency requirements
//! - No need for distributed coordination
//! - Simple deployment (no Redis/external dependencies)
//!
//! ❌ **Don't use local provider when:**
//! - Multiple application instances need shared limits
//! - Horizontal scaling requires coordinated rate limiting
//! - Rate limits must survive process restarts
//!
//! # Examples
//!
//! ```
//! use trypema::{RateLimit, RateLimiterBuilder, local::LocalRateLimiterProvider};
//!
//! let rl = LocalRateLimiterProvider::builder()
//!     .cleanup_enabled(false)
//!     .build()
//!     .unwrap();
//! let rate = RateLimit::per_second(10.0).unwrap();
//!
//! // Absolute strategy: strict enforcement
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
