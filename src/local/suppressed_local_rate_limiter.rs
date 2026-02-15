use crate::LocalRateLimiterOptions;

/// Placeholder for an alternative local rate limiter strategy.
///
/// This type exists to reserve the API surface while additional strategies and
/// providers are implemented.
pub struct SuppressedLocalRateLimiter {}

impl SuppressedLocalRateLimiter {
    pub(crate) fn new(_options: LocalRateLimiterOptions) -> Self {
        Self {}
    }
}
