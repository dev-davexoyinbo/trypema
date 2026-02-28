use crate::{TrypemaError, redis::RedisRateLimiterOptions};

#[derive(Debug, Clone)]
pub struct SuppressedHybridRateLimiter;

impl SuppressedHybridRateLimiter {
    pub(crate) fn new(_options: RedisRateLimiterOptions) -> Self {
        Self
    }

    pub(crate) async fn cleanup(&self, _stale_after_ms: u64) -> Result<(), TrypemaError> {
        Ok(())
    }
}
