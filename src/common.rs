//! Common types shared across all rate limiting providers.
//!
//! These types define Trypema's shared configuration and result model. They are all re-exported
//! at the crate root for convenience.
//!
//! # Types
//!
//! | Type | Purpose |
//! |------|---------|
//! | [`RateLimitDecision`] | The admission decision returned by every `inc()` and `is_allowed()` call |
//! | [`SuppressedRateLimitSnapshot`] | Live counters and suppression factor returned by suppressed `get()` calls |
//! | [`RateLimit`] | Per-second rate limit (positive `f64`, supports non-integer rates) |
//! | [`WindowSize`] | Sliding window duration in seconds (≥ 1) |
//! | [`BucketSize`] | Bucket coalescing interval in milliseconds (≥ 1, default 100ms) |
//! | [`HardLimitFactor`] | Hard cutoff multiplier for the suppressed strategy (≥ 1.0, default 1.0) |
//! | [`SuppressionFactorCachePeriod`] | Cache duration for suppression factor recomputation (≥ 1, default 100ms) |
//! | [`RateLimitComparator`] | Guard condition for conditional writes (`set_if`) against the current window total |

use std::{
    sync::atomic::AtomicU64,
    time::{Duration, Instant},
};

use crate::TrypemaError;

pub(crate) const MILLISECONDS_PER_SECOND: u64 = 1_000;
pub(crate) const SECONDS_PER_MINUTE: u64 = 60;
const MINUTES_PER_HOUR: u64 = 60;
const HOURS_PER_DAY: u64 = 24;
const DAYS_PER_WEEK: u64 = 7;
const DAYS_PER_MONTH: u64 = 30;
pub(crate) const SECONDS_PER_HOUR: u64 = SECONDS_PER_MINUTE * MINUTES_PER_HOUR;
const SECONDS_PER_DAY: u64 = SECONDS_PER_HOUR * HOURS_PER_DAY;
const SECONDS_PER_WEEK: u64 = SECONDS_PER_DAY * DAYS_PER_WEEK;
const SECONDS_PER_MONTH: u64 = SECONDS_PER_DAY * DAYS_PER_MONTH;

pub(crate) fn checked_duration(
    value: u64,
    multiplier: u64,
    name: &str,
    error: fn(String) -> TrypemaError,
) -> Result<u64, TrypemaError> {
    if value == 0 {
        return Err(error(format!("{name} must be greater than 0")));
    }

    value
        .checked_mul(multiplier)
        .ok_or_else(|| error(format!("{name} is too large")))
}

pub(crate) fn duration_from_milliseconds(milliseconds: u128) -> Duration {
    let seconds = milliseconds / u128::from(MILLISECONDS_PER_SECOND);
    if seconds > u128::from(u64::MAX) {
        return Duration::MAX;
    }

    let subsec_milliseconds = milliseconds % u128::from(MILLISECONDS_PER_SECOND);
    Duration::new(seconds as u64, (subsec_milliseconds as u32) * 1_000_000)
}

/// Hash builder used by Trypema's internal concurrent maps.
///
/// AHash is the default because these maps are private, hot-path data structures. Alternatives
/// include the standard library's `std::collections::hash_map::RandomState` when stronger
/// adversarial collision resistance is preferred, or another `BuildHasher` selected after
/// representative performance and security evaluation.
pub(crate) type RandomState = ahash::RandomState;
// pub(crate) type RandomState = std::collections::hash_map::RandomState;

#[derive(Debug)]
pub(crate) struct Bucket {
    pub count: AtomicU64,
    pub declined_count: AtomicU64,
    pub timestamp: Instant,
}

/// Result of a rate limit admission check.
///
/// Returned by every strategy to indicate whether a request should proceed.
///
/// # Variants
///
/// - [`Allowed`](RateLimitDecision::Allowed): Request admitted, proceed normally
/// - [`Rejected`](RateLimitDecision::Rejected): Request denied with backoff hints
/// - [`Suppressed`](RateLimitDecision::Suppressed): Probabilistic suppression (suppressed strategy only)
///
/// # Important Notes
///
/// - Rejection metadata (`retry_after`, `remaining_after_waiting`) is **best-effort**
/// - Metadata accuracy degrades with bucket coalescing and concurrent access
/// - Use for backoff hints, not strict guarantees
///
/// # Examples
///
/// ```
/// # use trypema::{RateLimiterBuilder, local::LocalRateLimiterProvider};
/// # let rl = LocalRateLimiterProvider::builder().cleanup_enabled(false).build().unwrap();
/// use trypema::{RateLimit, RateLimitDecision};
///
/// let rate = RateLimit::per_second(10.0).unwrap();
///
/// match rl.absolute().inc("user_123", &rate, 1) {
///     RateLimitDecision::Allowed => {
///         println!("Request allowed");
///     }
///     RateLimitDecision::Rejected { retry_after, remaining_after_waiting, .. } => {
///         println!("Rate limited, retry in {retry_after:?}");
///         println!("Capacity after waiting: {}", remaining_after_waiting);
///     }
///     RateLimitDecision::Suppressed { is_allowed, suppression_factor, .. } => {
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
    #[non_exhaustive]
    Rejected {
        /// Sliding window duration used for this decision.
        window_size: WindowSize,

        /// **Best-effort** duration until capacity becomes available.
        ///
        /// Computed from the oldest active bucket's remaining TTL. May be inaccurate due to:
        /// - Bucket coalescing (merges nearby increments)
        /// - Concurrent requests changing bucket ages
        /// - New increments extending bucket lifetimes
        ///
        /// Use as a backoff hint, not a guarantee.
        retry_after: Duration,

        /// **Best-effort** estimate of how much capacity becomes available after
        /// `retry_after` elapses.
        ///
        /// This is the count released when the oldest live bucket expires. For example, if a
        /// full window contains buckets with counts `3` and `7`, this value is `3`: the caller
        /// can send three count units after waiting. If grouping coalesced all usage into the
        /// oldest bucket, this is the full grouped count. It may be inaccurate if concurrent
        /// activity modifies the buckets.
        ///
        /// Use as a capacity hint, not a guarantee.
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
    #[non_exhaustive]
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

/// A best-effort snapshot of a suppressed rate limiter's live state.
///
/// Returned by the suppressed strategy's `get()` method for every provider. Counts include only
/// buckets that are live at the time of the read. Under concurrent use, individual fields may be
/// observed while another request is updating the limiter.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
#[non_exhaustive]
pub struct SuppressedRateLimitSnapshot {
    /// Total observed usage in the live window, including accepted and declined usage.
    pub total: u64,

    /// Total declined usage in the live window.
    pub total_declined: u64,

    /// Current suppression factor in the inclusive range `0.0..=1.0`.
    pub suppression_factor: f64,
}

/// Result of a conditional total update.
///
/// Distinguishes comparator misses from successful updates that leave total unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConditionalSetOutcome {
    /// Whether comparator matched and update semantics were applied.
    pub matched: bool,
    /// Live total observed before conditional update.
    pub previous_total: u64,
    /// Live total after operation, or unchanged total when comparator missed.
    pub current_total: u64,
}

#[cfg(test)]
impl PartialEq<(u64, u64)> for ConditionalSetOutcome {
    fn eq(&self, other: &(u64, u64)) -> bool {
        (self.current_total, self.previous_total) == *other
    }
}

/// Per-second rate limit for a key.
///
/// Wraps a positive `f64` so Trypema can express non-integer limits such as `0.5` or `5.5`
/// requests per second.
///
/// Window capacity is computed as `window_size in seconds × per-second rate limit`.
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
/// let rate = RateLimit::per_second(10.0).unwrap();
/// assert_eq!(rate.as_per_second(), 10.0);
/// assert_eq!(rate.as_per_minute(), 600.0);
/// assert_eq!(rate.as_per_hour(), 36_000.0);
/// assert_eq!(rate.as_per_day(), 864_000.0);
/// assert_eq!(rate.as_per_week(), 6_048_000.0);
/// assert_eq!(rate.as_per_month(), 25_920_000.0);
///
/// let rate = RateLimit::per_second(5.5).unwrap();
/// assert_eq!(rate.as_per_second(), 5.5);
///
/// let rate = RateLimit::per_second_or_panic(2.5);
/// assert_eq!(rate.as_per_second(), 2.5);
///
/// // Invalid: must be positive
/// assert!(RateLimit::per_second(0.0).is_err());
/// assert!(RateLimit::per_second(-1.0).is_err());
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
    /// assert_eq!(unlimited.as_per_second(), f64::MAX);
    /// ```
    pub fn max() -> Self {
        Self(f64::MAX)
    }

    /// Return rate expressed as count per second.
    pub fn as_per_second(self) -> f64 {
        self.0
    }

    /// Return rate expressed as count per minute.
    pub fn as_per_minute(self) -> f64 {
        self.as_per_period(SECONDS_PER_MINUTE)
    }

    /// Return rate expressed as count per hour.
    pub fn as_per_hour(self) -> f64 {
        self.as_per_period(SECONDS_PER_HOUR)
    }

    /// Return rate expressed as count per day.
    pub fn as_per_day(self) -> f64 {
        self.as_per_period(SECONDS_PER_DAY)
    }

    /// Return rate expressed as count per week.
    pub fn as_per_week(self) -> f64 {
        self.as_per_period(SECONDS_PER_WEEK)
    }

    /// Return rate expressed as count per 30-day month.
    pub fn as_per_month(self) -> f64 {
        self.as_per_period(SECONDS_PER_MONTH)
    }

    /// Create a rate limit expressed as count per second.
    pub fn per_second(value: f64) -> Result<Self, TrypemaError> {
        Self::from_per_second(value)
    }

    /// Create a rate limit expressed as count per second.
    ///
    /// # Panics
    ///
    /// Panics when the value does not produce a greater than 0 per-second rate.
    pub fn per_second_or_panic(value: f64) -> Self {
        Self::per_second(value).unwrap()
    }

    /// Create a rate limit expressed as count per minute.
    pub fn per_minute(value: f64) -> Result<Self, TrypemaError> {
        Self::from_period(value, SECONDS_PER_MINUTE)
    }

    /// Create a rate limit expressed as count per minute.
    ///
    /// # Panics
    ///
    /// Panics when the value does not produce a greater than 0 per-second rate.
    pub fn per_minute_or_panic(value: f64) -> Self {
        Self::per_minute(value).unwrap()
    }

    /// Create a rate limit expressed as count per hour.
    pub fn per_hour(value: f64) -> Result<Self, TrypemaError> {
        Self::from_period(value, SECONDS_PER_HOUR)
    }

    /// Create a rate limit expressed as count per hour.
    ///
    /// # Panics
    ///
    /// Panics when the value does not produce a greater than 0 per-second rate.
    pub fn per_hour_or_panic(value: f64) -> Self {
        Self::per_hour(value).unwrap()
    }

    /// Create a rate limit expressed as count per day.
    pub fn per_day(value: f64) -> Result<Self, TrypemaError> {
        Self::from_period(value, SECONDS_PER_DAY)
    }

    /// Create a rate limit expressed as count per day.
    ///
    /// # Panics
    ///
    /// Panics when the value does not produce a greater than 0 per-second rate.
    pub fn per_day_or_panic(value: f64) -> Self {
        Self::per_day(value).unwrap()
    }

    /// Create a rate limit expressed as count per week.
    pub fn per_week(value: f64) -> Result<Self, TrypemaError> {
        Self::from_period(value, SECONDS_PER_WEEK)
    }

    /// Create a rate limit expressed as count per week.
    ///
    /// # Panics
    ///
    /// Panics when the value does not produce a greater than 0 per-second rate.
    pub fn per_week_or_panic(value: f64) -> Self {
        Self::per_week(value).unwrap()
    }

    /// Create a rate limit expressed as count per 30-day month.
    pub fn per_month(value: f64) -> Result<Self, TrypemaError> {
        Self::from_period(value, SECONDS_PER_MONTH)
    }

    /// Create a rate limit expressed as count per 30-day month.
    ///
    /// # Panics
    ///
    /// Panics when the value does not produce a greater than 0 per-second rate.
    pub fn per_month_or_panic(value: f64) -> Self {
        Self::per_month(value).unwrap()
    }

    fn from_period(value: f64, period_seconds: u64) -> Result<Self, TrypemaError> {
        Self::from_per_second(value / (period_seconds as f64))
    }

    fn as_per_period(self, period_seconds: u64) -> f64 {
        self.0 * (period_seconds as f64)
    }

    fn from_per_second(value: f64) -> Result<Self, TrypemaError> {
        if !value.is_finite() || value <= 0f64 {
            Err(TrypemaError::InvalidRateLimit(
                "rate limit must be greater than 0".to_string(),
            ))
        } else {
            Ok(Self(value))
        }
    }
}

/// Sliding window size stored in seconds.
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
/// use trypema::WindowSize;
///
/// let window = WindowSize::seconds(60).unwrap();
/// assert_eq!(window.as_seconds(), 60);
/// assert_eq!(window.as_milliseconds(), 60_000);
/// assert_eq!(window.as_minutes(), 1.0);
/// assert_eq!(window.as_hours(), 1.0 / 60.0);
///
/// let window = WindowSize::seconds_or_panic(30);
/// assert_eq!(window.as_seconds(), 30);
///
/// // Invalid: too small
/// assert!(WindowSize::seconds(0).is_err());
/// ```
///
/// Validated values do not expose mutable dereferencing:
///
/// ```compile_fail
/// use trypema::WindowSize;
/// let mut window = WindowSize::seconds_or_panic(10);
/// *window = 0;
/// ```
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct WindowSize(u64);

impl Default for WindowSize {
    /// Returns a window size of 10 seconds.
    fn default() -> Self {
        Self(10)
    }
}

impl WindowSize {
    /// Return window duration in seconds.
    pub fn as_seconds(self) -> u64 {
        self.0
    }

    /// Return window duration in milliseconds.
    pub fn as_milliseconds(self) -> u128 {
        u128::from(self.0) * u128::from(MILLISECONDS_PER_SECOND)
    }

    /// Return the window duration in minutes.
    pub fn as_minutes(self) -> f64 {
        self.as_period(SECONDS_PER_MINUTE)
    }

    /// Return the window duration in hours.
    pub fn as_hours(self) -> f64 {
        self.as_period(SECONDS_PER_HOUR)
    }

    /// Return the window duration in days.
    pub fn as_days(self) -> f64 {
        self.as_period(SECONDS_PER_DAY)
    }

    /// Return the window duration in weeks.
    pub fn as_weeks(self) -> f64 {
        self.as_period(SECONDS_PER_WEEK)
    }

    /// Return the window duration in 30-day months.
    pub fn as_months(self) -> f64 {
        self.as_period(SECONDS_PER_MONTH)
    }

    /// Create a window size expressed in seconds.
    pub fn seconds(value: u64) -> Result<Self, TrypemaError> {
        Self::from_seconds(value, 1)
    }

    /// Create a window size expressed in seconds.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero.
    pub fn seconds_or_panic(value: u64) -> Self {
        Self::seconds(value).unwrap()
    }

    /// Create a window size expressed in minutes.
    pub fn minutes(value: u64) -> Result<Self, TrypemaError> {
        Self::from_seconds(value, SECONDS_PER_MINUTE)
    }

    /// Create a window size expressed in minutes.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero or conversion overflows.
    pub fn minutes_or_panic(value: u64) -> Self {
        Self::minutes(value).unwrap()
    }

    /// Create a window size expressed in hours.
    pub fn hours(value: u64) -> Result<Self, TrypemaError> {
        Self::from_seconds(value, SECONDS_PER_HOUR)
    }

    /// Create a window size expressed in hours.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero or conversion overflows.
    pub fn hours_or_panic(value: u64) -> Self {
        Self::hours(value).unwrap()
    }

    /// Create a window size expressed in days.
    pub fn days(value: u64) -> Result<Self, TrypemaError> {
        Self::from_seconds(value, SECONDS_PER_DAY)
    }

    /// Create a window size expressed in days.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero or conversion overflows.
    pub fn days_or_panic(value: u64) -> Self {
        Self::days(value).unwrap()
    }

    /// Create a window size expressed in weeks.
    pub fn weeks(value: u64) -> Result<Self, TrypemaError> {
        Self::from_seconds(value, SECONDS_PER_WEEK)
    }

    /// Create a window size expressed in weeks.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero or conversion overflows.
    pub fn weeks_or_panic(value: u64) -> Self {
        Self::weeks(value).unwrap()
    }

    /// Create a window size expressed in 30-day months.
    pub fn months(value: u64) -> Result<Self, TrypemaError> {
        Self::from_seconds(value, SECONDS_PER_MONTH)
    }

    /// Create a window size expressed in 30-day months.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero or conversion overflows.
    pub fn months_or_panic(value: u64) -> Self {
        Self::months(value).unwrap()
    }

    fn from_seconds(value: u64, multiplier: u64) -> Result<Self, TrypemaError> {
        checked_duration(
            value,
            multiplier,
            "window size",
            TrypemaError::InvalidWindowSize,
        )
        .map(Self)
    }

    fn as_period(self, period_seconds: u64) -> f64 {
        (self.0 as f64) / (period_seconds as f64)
    }
}

/// Bucket coalescing interval stored in milliseconds.
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
/// use trypema::BucketSize;
///
/// let coalescing = BucketSize::milliseconds(10).unwrap();
/// assert_eq!(coalescing.as_milliseconds(), 10);
///
/// let aggressive = BucketSize::milliseconds(100).unwrap();
/// let precise = BucketSize::milliseconds_or_panic(1);
/// let _ = (aggressive, precise);
///
/// // Invalid
/// assert!(BucketSize::milliseconds(0).is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct BucketSize(u64);

impl Default for BucketSize {
    /// Returns a rate group size of 100 ms.
    fn default() -> Self {
        Self(100)
    }
}

impl BucketSize {
    /// Return bucket-coalescing interval in milliseconds.
    pub fn as_milliseconds(self) -> u64 {
        self.0
    }

    /// Create a bucket size expressed in milliseconds.
    pub fn milliseconds(value: u64) -> Result<Self, TrypemaError> {
        Self::from_milliseconds(value, 1)
    }

    /// Create a bucket size expressed in milliseconds.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero.
    pub fn milliseconds_or_panic(value: u64) -> Self {
        Self::milliseconds(value).unwrap()
    }

    /// Create a bucket size expressed in seconds.
    pub fn seconds(value: u64) -> Result<Self, TrypemaError> {
        Self::from_milliseconds(value, MILLISECONDS_PER_SECOND)
    }

    /// Create a bucket size expressed in seconds.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero or conversion overflows.
    pub fn seconds_or_panic(value: u64) -> Self {
        Self::seconds(value).unwrap()
    }

    /// Create a bucket size expressed in minutes.
    pub fn minutes(value: u64) -> Result<Self, TrypemaError> {
        Self::from_milliseconds(value, MILLISECONDS_PER_SECOND * SECONDS_PER_MINUTE)
    }

    /// Create a bucket size expressed in minutes.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero or conversion overflows.
    pub fn minutes_or_panic(value: u64) -> Self {
        Self::minutes(value).unwrap()
    }

    /// Create a bucket size expressed in hours.
    pub fn hours(value: u64) -> Result<Self, TrypemaError> {
        Self::from_milliseconds(value, MILLISECONDS_PER_SECOND * SECONDS_PER_HOUR)
    }

    /// Create a bucket size expressed in hours.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero or conversion overflows.
    pub fn hours_or_panic(value: u64) -> Self {
        Self::hours(value).unwrap()
    }

    /// Create a bucket size expressed in days.
    pub fn days(value: u64) -> Result<Self, TrypemaError> {
        Self::from_milliseconds(value, MILLISECONDS_PER_SECOND * SECONDS_PER_DAY)
    }

    /// Create a bucket size expressed in days.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero or conversion overflows.
    pub fn days_or_panic(value: u64) -> Self {
        Self::days(value).unwrap()
    }

    /// Create a bucket size expressed in weeks.
    pub fn weeks(value: u64) -> Result<Self, TrypemaError> {
        Self::from_milliseconds(value, MILLISECONDS_PER_SECOND * SECONDS_PER_WEEK)
    }

    /// Create a bucket size expressed in weeks.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero or conversion overflows.
    pub fn weeks_or_panic(value: u64) -> Self {
        Self::weeks(value).unwrap()
    }

    /// Create a bucket size expressed in 30-day months.
    pub fn months(value: u64) -> Result<Self, TrypemaError> {
        Self::from_milliseconds(value, MILLISECONDS_PER_SECOND * SECONDS_PER_MONTH)
    }

    /// Create a bucket size expressed in 30-day months.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero or conversion overflows.
    pub fn months_or_panic(value: u64) -> Self {
        Self::months(value).unwrap()
    }

    fn from_milliseconds(value: u64, multiplier: u64) -> Result<Self, TrypemaError> {
        checked_duration(
            value,
            multiplier,
            "bucket size",
            TrypemaError::InvalidBucketSize,
        )
        .map(Self)
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
/// let factor = HardLimitFactor::default();
/// assert_eq!(factor.as_multiplier(), 1.0);
///
/// let factor = HardLimitFactor::new(1.5).unwrap();
/// let rate = RateLimit::per_second(10.0).unwrap();
/// let bursty = HardLimitFactor::new_or_panic(2.0);
/// let _ = (rate, bursty);
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
    /// Return hard-limit multiplier.
    pub fn as_multiplier(self) -> f64 {
        self.0
    }

    /// Fallible constructor. Equivalent to `TryFrom` but more ergonomic as a direct call.
    pub fn new(value: f64) -> Result<Self, TrypemaError> {
        Self::try_from(value)
    }

    /// Panicking constructor. The `_or_panic` suffix signals that this call can panic.
    pub fn new_or_panic(value: f64) -> Self {
        Self::try_from(value).unwrap()
    }
}

impl TryFrom<f64> for HardLimitFactor {
    type Error = TrypemaError;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if !value.is_finite() || value < 1f64 {
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

/// Suppression-factor cache period stored in milliseconds.
///
/// The suppressed strategy computes a suppression factor based on the current perceived rate
/// relative to the rate limit. This computation involves iterating over recent buckets and
/// can be non-trivial under high throughput. To amortise this cost, the computed factor is
/// cached per key for up to `SuppressionFactorCachePeriod`.
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
/// Must be ≥ 1. A value of `0` returns [`TrypemaError::InvalidSuppressionFactorCachePeriod`].
///
/// # Examples
///
/// ```
/// use trypema::SuppressionFactorCachePeriod;
///
/// let cache = SuppressionFactorCachePeriod::default();
/// assert_eq!(cache.as_milliseconds(), 100);
///
/// let cache = SuppressionFactorCachePeriod::milliseconds(50).unwrap();
/// let fast = SuppressionFactorCachePeriod::milliseconds_or_panic(10);
/// let _ = (cache, fast);
///
/// // Invalid: 0ms
/// assert!(SuppressionFactorCachePeriod::milliseconds(0).is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct SuppressionFactorCachePeriod(u64);

impl Default for SuppressionFactorCachePeriod {
    /// Returns a suppression factor cache duration of 100 ms.
    fn default() -> Self {
        Self(100)
    }
}

impl SuppressionFactorCachePeriod {
    /// Return cache period in milliseconds.
    pub fn as_milliseconds(self) -> u64 {
        self.0
    }

    /// Create a suppression-factor cache period expressed in milliseconds.
    pub fn milliseconds(value: u64) -> Result<Self, TrypemaError> {
        Self::from_milliseconds(value, 1)
    }

    /// Create a suppression-factor cache period expressed in milliseconds.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero.
    pub fn milliseconds_or_panic(value: u64) -> Self {
        Self::milliseconds(value).unwrap()
    }

    /// Create a suppression-factor cache period expressed in seconds.
    pub fn seconds(value: u64) -> Result<Self, TrypemaError> {
        Self::from_milliseconds(value, MILLISECONDS_PER_SECOND)
    }

    /// Create a suppression-factor cache period expressed in seconds.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero or conversion overflows.
    pub fn seconds_or_panic(value: u64) -> Self {
        Self::seconds(value).unwrap()
    }

    /// Create a suppression-factor cache period expressed in minutes.
    pub fn minutes(value: u64) -> Result<Self, TrypemaError> {
        Self::from_milliseconds(value, MILLISECONDS_PER_SECOND * SECONDS_PER_MINUTE)
    }

    /// Create a suppression-factor cache period expressed in minutes.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero or conversion overflows.
    pub fn minutes_or_panic(value: u64) -> Self {
        Self::minutes(value).unwrap()
    }

    /// Create a suppression-factor cache period expressed in hours.
    pub fn hours(value: u64) -> Result<Self, TrypemaError> {
        Self::from_milliseconds(value, MILLISECONDS_PER_SECOND * SECONDS_PER_HOUR)
    }

    /// Create a suppression-factor cache period expressed in hours.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero or conversion overflows.
    pub fn hours_or_panic(value: u64) -> Self {
        Self::hours(value).unwrap()
    }

    /// Create a suppression-factor cache period expressed in days.
    pub fn days(value: u64) -> Result<Self, TrypemaError> {
        Self::from_milliseconds(value, MILLISECONDS_PER_SECOND * SECONDS_PER_DAY)
    }

    /// Create a suppression-factor cache period expressed in days.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero or conversion overflows.
    pub fn days_or_panic(value: u64) -> Self {
        Self::days(value).unwrap()
    }

    fn from_milliseconds(value: u64, multiplier: u64) -> Result<Self, TrypemaError> {
        checked_duration(
            value,
            multiplier,
            "suppression-factor cache period",
            TrypemaError::InvalidSuppressionFactorCachePeriod,
        )
        .map(Self)
    }
}

/// Guard condition for conditional writes against a key's current window total.
///
/// Used by `set_if`-style operations: the write is applied only when the key's
/// current (post-eviction) window total satisfies the comparator. The embedded
/// operand is the value the current total is compared **against**.
///
/// # Variants
///
/// - [`Eq`](RateLimitComparator::Eq): matches when the current total **equals** the operand
/// - [`Lt`](RateLimitComparator::Lt): matches when the current total is **less than** the operand
/// - [`Gt`](RateLimitComparator::Gt): matches when the current total is **greater than** the operand
/// - [`Ne`](RateLimitComparator::Ne): matches when the current total is **not equal to** the operand
/// - [`Always`](RateLimitComparator::Always): **always** matches (unconditional write through the guarded path)
///
/// # Common Idioms
///
/// - `Lt(count)` with `set_if(key, rate, Lt(count), count)` — *raise-only* write: lifts the
///   window total to at least `count` and never lowers it. Idempotent, safe to retry — the
///   canonical priming pattern.
/// - `Eq(0)` — initialize only when the window is empty.
/// - `Always` — unconditional overwrite.
///
/// # Examples
///
/// ```
/// use trypema::RateLimitComparator;
///
/// assert!(RateLimitComparator::Eq(5).matches(5));
/// assert!(!RateLimitComparator::Eq(5).matches(4));
///
/// assert!(RateLimitComparator::Lt(5).matches(4));
/// assert!(!RateLimitComparator::Lt(5).matches(5));
///
/// assert!(RateLimitComparator::Gt(5).matches(6));
/// assert!(!RateLimitComparator::Gt(5).matches(5));
///
/// assert!(RateLimitComparator::Ne(5).matches(6));
/// assert!(!RateLimitComparator::Ne(5).matches(5));
///
/// assert!(RateLimitComparator::Always.matches(0));
/// assert!(RateLimitComparator::Always.matches(u64::MAX));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitComparator {
    /// Matches when the current window total is equal to the embedded operand.
    Eq(u64),
    /// Matches when the current window total is less than the embedded operand.
    Lt(u64),
    /// Matches when the current window total is greater than the embedded operand.
    Gt(u64),
    /// Matches when the current window total is not equal to the embedded operand.
    Ne(u64),
    /// Always matches.
    ///
    /// This is primarily useful for delegating unconditional writes through the
    /// conditional write path.
    Always,
}

impl RateLimitComparator {
    /// Returns whether `current` satisfies this comparator.
    pub fn matches(self, current: u64) -> bool {
        match self {
            Self::Eq(operand) => current == operand,
            Self::Lt(operand) => current < operand,
            Self::Gt(operand) => current > operand,
            Self::Ne(operand) => current != operand,
            Self::Always => true,
        }
    }

    /// Wire encoding used by the Redis Lua scripts: `(op, operand)`.
    ///
    /// `Always` carries no operand; `0` is sent as a placeholder and ignored by the script.
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    pub(crate) fn redis_args(self) -> (&'static str, u64) {
        match self {
            Self::Eq(operand) => ("eq", operand),
            Self::Lt(operand) => ("lt", operand),
            Self::Gt(operand) => ("gt", operand),
            Self::Ne(operand) => ("ne", operand),
            Self::Always => ("nil", 0),
        }
    }
}

/// Selects which side of an existing sliding-window history is retained when
/// [`set_if_preserve_history`](crate::local::AbsoluteLocalRateLimiter::set_if_preserve_history)
/// changes its total.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryPreservation {
    /// Retain the newest buckets. Reductions consume history from oldest to newest,
    /// while increases are added to the newest bucket.
    PreserveNewest,
    /// Retain the oldest buckets. Reductions consume history from newest to oldest,
    /// while increases are added to the oldest bucket.
    PreserveOldest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HistoryUpdateMode {
    Replace,
    Preserve(HistoryPreservation),
}

impl HistoryUpdateMode {
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    pub(crate) fn redis_arg(self) -> &'static str {
        match self {
            Self::Replace => "replace",
            Self::Preserve(HistoryPreservation::PreserveNewest) => "preserve_newest",
            Self::Preserve(HistoryPreservation::PreserveOldest) => "preserve_oldest",
        }
    }
}
