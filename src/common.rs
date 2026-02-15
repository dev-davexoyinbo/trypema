use std::{
    collections::VecDeque,
    ops::{Deref, DerefMut},
    sync::atomic::AtomicU64,
    time::Instant,
};

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
/// Returned by rate limiter implementations to indicate whether work for a key
/// should proceed, and (when rejected) to provide best-effort backoff hints.
pub enum RateLimitDecision {
    /// The request/work is allowed.
    Allowed,
    /// The request/work is rejected.
    ///
    /// Includes best-effort hints for callers that want to communicate backoff.
    Rejected {
        /// Sliding window size used for the decision.
        window_size_seconds: u64,
        /// Milliseconds until the oldest sample exits the window.
        retry_after_ms: u64,
        /// Estimated remaining count after waiting `retry_after_ms`.
        remaining_after_waiting: u64,
    },
    /// The request/work is rejected and should be retried later.
    /// This is a hint for callers that want to communicate backoff.
    Suppressed {
        /// Calculated suppression factor.
        suppression_factor: f64,
        /// Is allowed
        is_allowed: bool,
    },
}

/// Per-second rate limit for a key.
///
/// This is represented as a positive `f64` to allow non-integer limits.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct RateLimit(f64);

impl RateLimit {
    /// A practically-unbounded rate limit.
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
    type Error = String;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if value <= 0f64 {
            Err("Rate limit must be greater than 0".to_string())
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
    type Error = String;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if value < 1 {
            Err("Window size must be at least 1".to_string())
        } else {
            Ok(Self(value))
        }
    }
}

/// Coalescing interval (in milliseconds) for grouping increments close in time.
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
    type Error = String;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if value == 0 {
            Err("Rate group size must be greater than 0".to_string())
        } else {
            Ok(Self(value))
        }
    }
}

/// Multiplier used by strategies that apply a "hard" cutoff beyond the base rate limit.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct HardLimitFactor(f64);

impl Default for HardLimitFactor {
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
    type Error = String;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if value <= 0f64 {
            Err("Rate limit must be greater than 0".to_string())
        } else {
            Ok(Self(value))
        }
    }
}
