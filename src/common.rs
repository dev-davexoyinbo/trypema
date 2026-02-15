use std::{collections::VecDeque, sync::atomic::AtomicU64, time::Instant};

pub(crate) struct InstantRate {
    pub count: AtomicU64,
    pub timestamp: Instant,
}

pub(crate) struct RateLimit {
    pub limit: u64,
    pub series: VecDeque<InstantRate>,
    pub total: AtomicU64,
}

impl RateLimit {
    pub fn new(limit: u64) -> Self {
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
