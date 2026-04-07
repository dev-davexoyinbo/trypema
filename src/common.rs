//! Common types shared across all rate limiting providers.
//!
//! This module defines the core types used throughout the crate. All public types are
//! re-exported at the crate root for convenience.
//!
//! # Types
//!
//! | Type | Purpose |
//! |------|---------|
//! | [`RateLimitDecision`] | The admission decision returned by every `inc()` and `is_allowed()` call |
//! | [`RateLimit`] | Per-second rate limit (positive `f64`, supports non-integer rates) |
//! | [`WindowSizeSeconds`] | Sliding window duration in seconds (≥ 1) |
//! | [`RateGroupSizeMs`] | Bucket coalescing interval in milliseconds (≥ 1, default 100ms) |
//! | [`HardLimitFactor`] | Hard cutoff multiplier for the suppressed strategy (≥ 1.0, default 1.0) |
//! | [`SuppressionFactorCacheMs`] | Cache duration for suppression factor recomputation (≥ 1, default 100ms) |

use std::{
    ops::{Deref, DerefMut},
    sync::atomic::AtomicU64,
    time::Instant,
};

use crate::TrypemaError;

#[derive(Debug)]
pub(crate) struct InstantRate {
    pub count: AtomicU64,
    pub declined: AtomicU64,
    pub timestamp: Instant,
}

/// Result of a rate limit admission check.
///
///
/// Returned by all rate limiting strategies to indicate whether a request should proceed.
///
/// # Variants
///
/// - [`Allowed`](RateLimitDecision::Allowed): Request admitted, proceed normally
/// - [`Rejected`](RateLimitDecision::Rejected): Request denied with backoff hints
/// - [`Suppressed`](RateLimitDecision::Suppressed): Probabilistic suppression (suppressed strategy only)
///
/// # Important Notes
///
/// - Rejection metadata (`retry_after_ms`, `remaining_after_waiting`) is **best-effort**
/// - Metadata accuracy degrades with bucket coalescing and concurrent access
/// - Use for backoff hints, not strict guarantees
///
/// # Examples
///
/// ```no_run
/// use trypema::{RateLimitDecision, RateLimit};
/// # use trypema::{HardLimitFactor, RateGroupSizeMs, RateLimiter, RateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds};
/// # use trypema::local::LocalRateLimiterOptions;
/// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
/// # use trypema::redis::RedisRateLimiterOptions;
/// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
/// # use trypema::hybrid::SyncIntervalMs;
/// #
/// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
/// # fn options() -> RateLimiterOptions {
/// #     let window_size_seconds = WindowSizeSeconds::try_from(60).unwrap();
/// #     let rate_group_size_ms = RateGroupSizeMs::try_from(10).unwrap();
/// #     let hard_limit_factor = HardLimitFactor::default();
/// #     let suppression_factor_cache_ms = SuppressionFactorCacheMs::default();
/// #     let sync_interval_ms = SyncIntervalMs::default();
/// #
/// #     RateLimiterOptions {
/// #         local: LocalRateLimiterOptions {
/// #             window_size_seconds,
/// #             rate_group_size_ms,
/// #             hard_limit_factor,
/// #             suppression_factor_cache_ms,
/// #         },
/// #         redis: RedisRateLimiterOptions {
/// #             connection_manager: todo!(),
/// #             prefix: None,
/// #             window_size_seconds,
/// #             rate_group_size_ms,
/// #             hard_limit_factor,
/// #             suppression_factor_cache_ms,
/// #             sync_interval_ms,
/// #         },
/// #     }
/// # }
/// #
/// # #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
/// # fn options() -> RateLimiterOptions {
/// #     let window_size_seconds = WindowSizeSeconds::try_from(60).unwrap();
/// #     let rate_group_size_ms = RateGroupSizeMs::try_from(10).unwrap();
/// #     let hard_limit_factor = HardLimitFactor::default();
/// #     let suppression_factor_cache_ms = SuppressionFactorCacheMs::default();
/// #
/// #     RateLimiterOptions {
/// #         local: LocalRateLimiterOptions {
/// #             window_size_seconds,
/// #             rate_group_size_ms,
/// #             hard_limit_factor,
/// #             suppression_factor_cache_ms,
/// #         },
/// #     }
/// # }
/// #
/// # let rl = RateLimiter::new(options());
/// # let rate = RateLimit::try_from(10.0).unwrap();
///
/// match rl.local().absolute().inc("user_123", &rate, 1) {
///     RateLimitDecision::Allowed => {
///         // Process request
///         println!("Request allowed");
///     }
///     RateLimitDecision::Rejected { retry_after_ms, remaining_after_waiting, .. } => {
///         // Send 429 with Retry-After header
///         println!("Rate limited, retry in {}ms", retry_after_ms);
///         println!("Estimated remaining: {}", remaining_after_waiting);
///     }
///     RateLimitDecision::Suppressed { is_allowed, suppression_factor } => {
///         // Only from suppressed strategy
///         if is_allowed {
///             println!("Allowed with suppression factor {}", suppression_factor);
///         } else {
///             println!("Suppressed");
///         }
///     }
/// }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, strum_macros::Display)]
pub enum RateLimitDecision {
    /// Request is allowed to proceed.
    ///
    /// The increment has been recorded in the limiter's state.
    Allowed,

    /// Request exceeds the rate limit and is rejected.
    ///
    /// The increment was **not** recorded. Includes best-effort metadata for backoff.
    Rejected {
        /// Sliding window size used for this decision (in seconds).
        window_size_seconds: u64,

        /// **Best-effort** estimate of milliseconds until capacity becomes available.
        ///
        /// Computed from the oldest active bucket's remaining TTL. May be inaccurate due to:
        /// - Bucket coalescing (merges nearby increments)
        /// - Concurrent requests changing bucket ages
        /// - New increments extending bucket lifetimes
        ///
        /// Use as a backoff hint, not a guarantee.
        retry_after_ms: u128,

        /// **Best-effort** estimate of how many requests will still be counted in the
        /// window after `retry_after_ms` elapses.
        ///
        /// Computed as `total_count - oldest_bucket_count`. This represents the number of
        /// requests that will remain in the window once the oldest bucket expires. May be:
        /// - `0` if all activity is heavily coalesced into the oldest bucket
        /// - Inaccurate if concurrent activity modifies buckets
        ///
        /// Use for rough capacity indication only.
        remaining_after_waiting: u64,
    },

    /// Request handled by probabilistic suppression (suppressed strategy only).
    ///
    /// The suppressed strategy tracks the total observed rate (all calls) and the number of
    /// calls declined by suppression. From these you can derive accepted usage as:
    /// `accepted = observed - declined`.
    ///
    /// When observed rate exceeds target, it probabilistically denies some requests to keep
    /// accepted rate near the limit.
    ///
    /// **Always check `is_allowed`** to determine if this specific call was admitted.
    Suppressed {
        /// Current suppression rate (0.0 = no suppression, 1.0 = full suppression).
        ///
        /// Computed as: `1.0 - (rate_limit / perceived_rate)`
        suppression_factor: f64,

        /// Whether this specific call was admitted.
        ///
        /// - `true`: Request allowed
        /// - `false`: Request suppressed (do not proceed)
        ///
        /// **Use this as the admission decision.**
        is_allowed: bool,
    },
}

/// Per-second rate limit for a key.
///
/// Wraps a positive `f64` to support non-integer rates (e.g., `5.5` requests/second).
///
/// Window capacity is computed as: `window_size_seconds × rate_limit`
///
/// # Implementation Notes
///
/// - Stored as `f64` for flexible rate specifications
/// - Enforced against `u64` counters internally
/// - Implementations may round when computing integer window capacities
///
/// # Examples
///
/// ```
/// use trypema::RateLimit;
///
/// // Integer rate
/// let rate = RateLimit::try_from(10.0).unwrap();
/// assert_eq!(*rate, 10.0);
///
/// // Non-integer rate
/// let rate = RateLimit::try_from(5.5).unwrap();
/// assert_eq!(*rate, 5.5);
///
/// // Invalid: must be positive
/// assert!(RateLimit::try_from(0.0).is_err());
/// assert!(RateLimit::try_from(-1.0).is_err());
///
/// // Unbounded rate (for testing)
/// let unlimited = RateLimit::max();
/// ```
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct RateLimit(f64);

impl RateLimit {
    /// Create a practically-unbounded rate limit.
    ///
    /// Returns `RateLimit(f64::MAX)`. Useful for testing or when you want to track
    /// usage without enforcing limits.
    ///
    /// # Examples
    ///
    /// ```
    /// use trypema::RateLimit;
    ///
    /// let unlimited = RateLimit::max();
    /// assert_eq!(*unlimited, f64::MAX);
    /// ```
    pub fn max() -> Self {
        Self(f64::MAX)
    }

    /// Fallible constructor. Equivalent to `TryFrom` but more ergonomic as a direct call.
    pub fn new(value: f64) -> Result<Self, TrypemaError> {
        Self::try_from(value)
    }

    /// Panicking constructor. The `_or_panic` suffix signals that this call can panic.
    pub fn new_or_panic(value: f64) -> Self {
        Self::try_from(value).expect("RateLimit value must be greater than 0")
    }
}

impl Deref for RateLimit {
    type Target = f64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<f64> for RateLimit {
    type Error = TrypemaError;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if value <= 0f64 {
            Err(TrypemaError::InvalidRateLimit(
                "rate limit must be greater than 0".to_string(),
            ))
        } else {
            Ok(Self(value))
        }
    }
}

impl DerefMut for RateLimit {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Sliding window size in seconds.
///
/// Validated newtype ensuring window size is at least 1 second.
///
/// The window size determines how far back in time the limiter looks when making
/// admission decisions. Larger windows smooth out bursts but delay recovery.
///
/// # Validation
///
/// - Must be >= 1
/// - Typical values: 10-300 seconds
///
/// # Examples
///
/// ```
/// use trypema::WindowSizeSeconds;
///
/// // Valid
/// let window = WindowSizeSeconds::try_from(60).unwrap();
/// assert_eq!(*window, 60);
///
/// // Invalid: too small
/// assert!(WindowSizeSeconds::try_from(0).is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct WindowSizeSeconds(u64);

impl Default for WindowSizeSeconds {
    /// Returns a window size of 10 seconds.
    fn default() -> Self {
        Self(10)
    }
}

impl WindowSizeSeconds {
    /// Fallible constructor. Equivalent to `TryFrom` but more ergonomic as a direct call.
    pub fn new(value: u64) -> Result<Self, TrypemaError> {
        Self::try_from(value)
    }

    /// Panicking constructor. The `_or_panic` suffix signals that this call can panic.
    pub fn new_or_panic(value: u64) -> Self {
        Self::try_from(value).expect("WindowSizeSeconds must be at least 1")
    }
}

impl Deref for WindowSizeSeconds {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for WindowSizeSeconds {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl TryFrom<u64> for WindowSizeSeconds {
    type Error = TrypemaError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if value < 1 {
            Err(TrypemaError::InvalidWindowSizeSeconds(
                "Window size must be at least 1".to_string(),
            ))
        } else {
            Ok(Self(value))
        }
    }
}

/// Bucket coalescing interval in milliseconds.
///
/// Validated newtype controlling how aggressively increments are merged into buckets.
///
/// Increments occurring within this interval of each other are coalesced into the same
/// bucket, reducing memory usage and improving performance at the cost of timing granularity.
///
/// # Validation
///
/// - Must be >= 1
/// - Typical values: 10-100 milliseconds
///
/// # Trade-offs
///
/// **Larger values (50-100ms):**
/// - ✅ Lower memory usage (fewer buckets)
/// - ✅ Better performance
/// - ❌ Coarser rejection metadata
///
/// **Smaller values (1-20ms):**
/// - ✅ More accurate timing
/// - ✅ Better rejection metadata
/// - ❌ Higher memory usage
///
/// # Examples
///
/// ```
/// use trypema::RateGroupSizeMs;
///
/// // Recommended starting point
/// let coalescing = RateGroupSizeMs::try_from(10).unwrap();
///
/// // Higher performance, coarser timing
/// let aggressive = RateGroupSizeMs::try_from(100).unwrap();
///
/// // Invalid
/// assert!(RateGroupSizeMs::try_from(0).is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct RateGroupSizeMs(u64);

impl Default for RateGroupSizeMs {
    /// Returns a rate group size of 100 ms.
    fn default() -> Self {
        Self(100)
    }
}

impl RateGroupSizeMs {
    /// Fallible constructor. Equivalent to `TryFrom` but more ergonomic as a direct call.
    pub fn new(value: u64) -> Result<Self, TrypemaError> {
        Self::try_from(value)
    }

    /// Panicking constructor. The `_or_panic` suffix signals that this call can panic.
    pub fn new_or_panic(value: u64) -> Self {
        Self::try_from(value).expect("RateGroupSizeMs must be greater than 0")
    }
}

impl Deref for RateGroupSizeMs {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RateGroupSizeMs {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl TryFrom<u64> for RateGroupSizeMs {
    type Error = TrypemaError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if value == 0 {
            Err(TrypemaError::InvalidRateGroupSizeMs(
                "Rate group size must be greater than 0".to_string(),
            ))
        } else {
            Ok(Self(value))
        }
    }
}

/// Hard cutoff multiplier for the suppressed strategy.
///
/// Defines the absolute maximum rate as: `rate_limit × hard_limit_factor`
///
/// The suppressed strategy uses probabilistic suppression to keep the accepted rate
/// near the target limit. This factor sets a hard ceiling beyond which all requests
/// are unconditionally denied (suppression factor forced to `1.0`).
///
/// # Validation
///
/// Must be ≥ 1.0. A value below 1.0 would set the hard limit *below* the base rate
/// limit, which makes no sense — suppression would be permanently active even under
/// normal load.
///
/// # Values
///
/// - `1.0` (default): Hard limit equals base limit (suppression ramps from 0 to 1 with no headroom)
/// - `1.5–2.0` (**recommended**): Allows 50–100% burst headroom before full suppression
/// - `> 2.0`: Very permissive; large gap between target and hard limit
///
/// # Only Relevant For
///
/// The suppressed strategy. Ignored by the absolute strategy.
///
/// # Examples
///
/// ```
/// use trypema::{HardLimitFactor, RateLimit};
///
/// // Default: no headroom
/// let factor = HardLimitFactor::default();
/// assert_eq!(*factor, 1.0);
///
/// // Recommended: 50% burst headroom
/// let factor = HardLimitFactor::try_from(1.5).unwrap();
/// let rate = RateLimit::try_from(10.0).unwrap();
/// // Hard limit = 10.0 × 1.5 = 15.0 req/s
///
/// // Invalid: must be at least 1.0
/// assert!(HardLimitFactor::try_from(0.0).is_err());
/// assert!(HardLimitFactor::try_from(0.99).is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct HardLimitFactor(f64);

impl Default for HardLimitFactor {
    /// Returns a hard limit factor of `1.0` (no headroom).
    ///
    /// This means the hard cutoff equals the base rate limit.
    /// Consider using `1.5-2.0` for the suppressed strategy.
    fn default() -> Self {
        Self(1f64)
    }
}

impl HardLimitFactor {
    /// Fallible constructor. Equivalent to `TryFrom` but more ergonomic as a direct call.
    pub fn new(value: f64) -> Result<Self, TrypemaError> {
        Self::try_from(value)
    }

    /// Panicking constructor. The `_or_panic` suffix signals that this call can panic.
    pub fn new_or_panic(value: f64) -> Self {
        Self::try_from(value).expect("HardLimitFactor must be >= 1.0")
    }
}

impl Deref for HardLimitFactor {
    type Target = f64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<f64> for HardLimitFactor {
    type Error = TrypemaError;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if value < 1f64 {
            Err(TrypemaError::InvalidHardLimitFactor(
                "Hard limit factor must be greater than or equal to 1".to_string(),
            ))
        } else {
            Ok(Self(value))
        }
    }
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, strum_macros::Display)]
pub(crate) enum RateType {
    #[strum(to_string = "absolute")]
    Absolute,
    #[strum(to_string = "suppressed")]
    Suppressed,
    #[strum(to_string = "hybrid_absolute")]
    HybridAbsolute,
    #[strum(to_string = "hybrid_suppressed")]
    HybridSuppressed,
}

/// Cache duration (milliseconds) for suppression factor recomputation.
///
/// The suppressed strategy computes a suppression factor based on the current perceived rate
/// relative to the rate limit. This computation involves iterating over recent buckets and
/// can be non-trivial under high throughput. To amortise this cost, the computed factor is
/// cached per key for up to `SuppressionFactorCacheMs`.
///
/// # Trade-offs
///
/// - **Shorter cache** (10–50ms): suppression reacts faster to traffic changes, but
///   recomputation happens more frequently.
/// - **Longer cache** (100–1000ms): less CPU overhead, but suppression may lag behind
///   sudden traffic changes.
///
/// # Validation
///
/// Must be ≥ 1. A value of `0` returns [`TrypemaError::InvalidSuppressionFactorCacheMs`].
///
/// # Examples
///
/// ```
/// use trypema::SuppressionFactorCacheMs;
///
/// // Default: 100ms
/// let cache = SuppressionFactorCacheMs::default();
/// assert_eq!(*cache, 100);
///
/// // Custom: 50ms for faster reaction
/// let cache = SuppressionFactorCacheMs::try_from(50).unwrap();
///
/// // Invalid: 0ms
/// assert!(SuppressionFactorCacheMs::try_from(0).is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct SuppressionFactorCacheMs(u64);

impl Default for SuppressionFactorCacheMs {
    /// Returns a suppression factor cache duration of 100 ms.
    fn default() -> Self {
        Self(100)
    }
}

impl SuppressionFactorCacheMs {
    /// Fallible constructor. Equivalent to `TryFrom` but more ergonomic as a direct call.
    pub fn new(value: u64) -> Result<Self, TrypemaError> {
        Self::try_from(value)
    }

    /// Panicking constructor. The `_or_panic` suffix signals that this call can panic.
    pub fn new_or_panic(value: u64) -> Self {
        Self::try_from(value).expect("SuppressionFactorCacheMs must be greater than 0")
    }
}

impl Deref for SuppressionFactorCacheMs {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<u64> for SuppressionFactorCacheMs {
    type Error = TrypemaError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if value == 0 {
            Err(TrypemaError::InvalidSuppressionFactorCacheMs(
                "Suppression factor cache must be greater than 0".to_string(),
            ))
        } else {
            Ok(Self(value))
        }
    }
}
