//! Common types shared across all rate limiting providers.
//!
//! This module defines core types used throughout the crate:
//! - [`RateLimitDecision`]: Admission decision result
//! - [`RateLimit`]: Per-second rate limit (positive `f64`)
//! - [`WindowSizeSeconds`]: Sliding window duration
//! - [`RateGroupSizeMs`]: Bucket coalescing interval
//! - [`HardLimitFactor`]: Hard cutoff multiplier for suppressed strategy
//!
//! These types are re-exported at the crate root for convenience.

use std::{
    collections::VecDeque,
    ops::{Deref, DerefMut},
    sync::atomic::AtomicU64,
    time::Instant,
};

use crate::TrypemaError;

pub(crate) struct InstantRate {
    pub count: AtomicU64,
    pub timestamp: Instant,
}

pub(crate) struct RateLimitSeries {
    pub limit: RateLimit,
    pub series: VecDeque<InstantRate>,
    pub total: AtomicU64,
}

impl RateLimitSeries {
    pub fn new(limit: RateLimit) -> Self {
        Self {
            limit,
            series: VecDeque::new(),
            total: AtomicU64::new(0),
        }
    }
}

/// Result of a rate limit admission check.
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
/// ```
/// use trypema::{RateLimitDecision, RateLimit};
/// # use trypema::{RateLimiter, RateLimiterOptions, LocalRateLimiterOptions};
/// # use trypema::{WindowSizeSeconds, RateGroupSizeMs, HardLimitFactor};
/// # let rl = RateLimiter::new(RateLimiterOptions {
/// #     local: LocalRateLimiterOptions {
/// #         window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
/// #         rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
/// #         hard_limit_factor: HardLimitFactor::default(),
/// #     },
/// # });
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
        retry_after_ms: u64,

        /// **Best-effort** estimate of window usage after waiting `retry_after_ms`.
        ///
        /// Represents the count in the oldest bucket that will expire. May be:
        /// - `0` if heavily coalesced into one bucket
        /// - Inaccurate if concurrent activity modifies buckets
        ///
        /// Use for rough capacity indication only.
        remaining_after_waiting: u64,
    },

    /// Request handled by probabilistic suppression (suppressed strategy only).
    ///
    /// The suppressed strategy tracks both observed (all calls) and accepted (admitted calls)
    /// rates. When observed rate exceeds target, it probabilistically denies some requests
    /// to keep accepted rate near the limit.
    ///
    /// **Always check `is_allowed`** to determine if this specific call was admitted.
    Suppressed {
        /// Current suppression rate (0.0 = no suppression, 1.0 = full suppression).
        ///
        /// Computed as: `1.0 - (perceived_rate / rate_limit)`
        suppression_factor: f64,

        /// Whether this specific call was admitted.
        ///
        /// - `true`: Request allowed, increment recorded in accepted series
        /// - `false`: Request suppressed, increment **not** recorded in accepted series
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
/// are unconditionally rejected (not suppressed).
///
/// # Values
///
/// - `1.0` (default): Hard limit equals base limit (suppression less useful)
/// - `1.5-2.0`: Recommended; allows 50-100% burst headroom before hard rejection
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
/// // Invalid: must be positive
/// assert!(HardLimitFactor::try_from(0.0).is_err());
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

impl Deref for HardLimitFactor {
    type Target = f64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<f64> for HardLimitFactor {
    type Error = TrypemaError;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if value <= 0f64 {
            Err(TrypemaError::InvalidHardLimitFactor(
                "Hard limit factor must be greater than 0".to_string(),
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
    #[strum(to_string = "suppressed_observed")]
    SuppressedObserved,
}
