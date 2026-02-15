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

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct RateLimit(f64);

impl RateLimit {
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
