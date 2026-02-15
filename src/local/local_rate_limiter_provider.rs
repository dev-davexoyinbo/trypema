use crate::{AbsoluteLocalRateLimiter, SuppressedLocalRateLimiter};

/// Configuration for local rate limiter implementations.
///
/// These options tune the time window and update coalescing strategy.
#[derive(Clone, Debug)]
pub struct LocalRateLimiterOptions {
    /// Sliding window size used when evaluating admission.
    pub window_size_seconds: u64,
    /// Coalescing interval for increments close in time.
    ///
    /// When multiple `inc` calls happen within this interval, they may be grouped
    /// into a single time bucket to reduce overhead.
    pub rate_group_size_ms: u16,
}

/// A collection of local rate limiter implementations.
///
/// Exposes multiple strategies behind a single provider handle.
pub struct LocalRateLimiterProvider {
    absolute: AbsoluteLocalRateLimiter,
    suppressed: SuppressedLocalRateLimiter,
}

impl LocalRateLimiterProvider {
    pub(crate) fn new(options: LocalRateLimiterOptions) -> Self {
        Self {
            absolute: AbsoluteLocalRateLimiter::new(options.clone()),
            suppressed: SuppressedLocalRateLimiter::new(options.clone()),
        }
    }

    /// Absolute local limiter implementation.
    pub fn absolute(&self) -> &AbsoluteLocalRateLimiter {
        &self.absolute
    }

    /// Suppressed local limiter implementation.
    ///
    /// This is currently a stub while additional strategies are developed.
    pub fn suppressed(&self) -> &SuppressedLocalRateLimiter {
        &self.suppressed
    }
}
