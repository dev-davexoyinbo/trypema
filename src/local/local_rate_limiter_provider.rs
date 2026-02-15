use crate::{AbsoluteLocalRateLimiter, SuppressedLocalRateLimiter};

#[derive(Clone, Debug)]
pub struct LocalRateLimiterOptions {
    pub window_size_seconds: u64,
    pub rate_group_size_ms: u16,
}

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

    pub fn absolute(&self) -> &AbsoluteLocalRateLimiter {
        &self.absolute
    }

    pub fn suppressed(&self) -> &SuppressedLocalRateLimiter {
        &self.suppressed
    }
}
