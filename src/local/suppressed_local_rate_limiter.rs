use crate::LocalRateLimiterOptions;

pub struct SuppressedLocalRateLimiter {}

impl SuppressedLocalRateLimiter {
    pub(crate) fn new(_options: LocalRateLimiterOptions) -> Self {
        Self {}
    }
}
