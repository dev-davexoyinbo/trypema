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
//! - [`SuppressedLocalRateLimiter`]: Probabilistic suppression with dual tracking
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
//! ```no_run
//! use trypema::{HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision, RateLimiter, RateLimiterOptions, WindowSizeSeconds};
//! use trypema::local::LocalRateLimiterOptions;
//! # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
//! # use trypema::redis::RedisRateLimiterOptions;
//! #
//! # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
//! # fn options() -> RateLimiterOptions {
//! #     RateLimiterOptions {
//! #         local: LocalRateLimiterOptions {
//! #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
//! #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
//! #             hard_limit_factor: HardLimitFactor::default(),
//! #         },
//! #         redis: RedisRateLimiterOptions {
//! #             connection_manager: todo!(),
//! #             prefix: None,
//! #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
//! #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
//! #             hard_limit_factor: HardLimitFactor::default(),
//! #         },
//! #     }
//! # }
//! #
//! # #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
//! # fn options() -> RateLimiterOptions {
//! #     RateLimiterOptions {
//! #         local: LocalRateLimiterOptions {
//! #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
//! #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
//! #             hard_limit_factor: HardLimitFactor::default(),
//! #         },
//! #     }
//! # }
//!
//! let rl = RateLimiter::new(options());
//!
//! let rate = RateLimit::try_from(10.0).unwrap();
//!
//! // Absolute strategy: strict enforcement
//! let decision = rl.local().absolute().inc("user_123", &rate, 1);
//!
//! // Suppressed strategy: probabilistic suppression
//! let decision = rl.local().suppressed().inc("api_endpoint", &rate, 1);
//! ```

mod absolute_local_rate_limiter;
pub use absolute_local_rate_limiter::*;

mod local_rate_limiter_provider;
pub use local_rate_limiter_provider::*;

mod suppressed_local_rate_limiter;
pub use suppressed_local_rate_limiter::*;
