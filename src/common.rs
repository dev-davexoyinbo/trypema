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

pub enum RateLimitDecision {
    Allowed,
    Rejected {
        window_size_seconds: u64,
        retry_after_ms: u64,
        remaining_after_waiting: u64,
    },
}
