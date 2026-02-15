use crate::{
    common::{HardLimitFactor, RateGroupSizeMs, WindowSizeSeconds},
    AbsoluteLocalRateLimiter, SuppressedLocalRateLimiter,
};

/// Configuration for local rate limiter implementations.
///
/// These options tune the time window and update coalescing strategy.
#[derive(Clone, Debug)]
pub struct LocalRateLimiterOptions {
    /// Sliding window size used when evaluating admission.
    pub window_size_seconds: WindowSizeSeconds,
    /// Coalescing interval for increments close in time.
    ///
    /// When multiple `inc` calls happen within this interval, they may be grouped
    /// into a single time bucket to reduce overhead.
    pub rate_group_size_ms: RateGroupSizeMs,

    /// Multiplier used by [`SuppressedLocalRateLimiter`] as a hard cutoff.
    ///
    /// The suppressed strategy may probabilistically reject work to keep the accepted
    /// rate near the base limit. This factor defines an absolute maximum rate
    /// (`rate_limit * hard_limit_factor`) beyond which work is rejected unconditionally.
    ///
    /// A value of `1.0` means the hard cutoff equals the base limit.
    pub hard_limit_factor: HardLimitFactor,
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
    ///
    /// See [`AbsoluteLocalRateLimiter`] for semantics and trade-offs.
    pub fn absolute(&self) -> &AbsoluteLocalRateLimiter {
        &self.absolute
    }

    /// Suppressed local limiter implementation.
    ///
    /// This strategy exposes suppression metadata via [`crate::RateLimitDecision::Suppressed`].
    pub fn suppressed(&self) -> &SuppressedLocalRateLimiter {
        &self.suppressed
    }
}
