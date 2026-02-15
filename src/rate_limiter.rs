use crate::{LocalRateLimiterOptions, LocalRateLimiterProvider};

pub struct RateLimiterOptions {
    pub local: LocalRateLimiterOptions,
}

pub struct RateLimiter {
    local: LocalRateLimiterProvider,
}

impl RateLimiter {
    pub fn new(options: RateLimiterOptions) -> Self {
        Self {
            local: LocalRateLimiterProvider::new(options.local),
        }
    }

    pub fn local(&self) -> &LocalRateLimiterProvider {
        &self.local
    }
}
