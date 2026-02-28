use std::sync::Arc;

use crate::{
    TrypemaError,
    hybrid::{
        absolute_hybrid_rate_limiter::AbsoluteHybridRateLimiter,
        suppressed_hybrid_rate_limiter::SuppressedHybridRateLimiter,
    },
    redis::RedisRateLimiterOptions,
};

///...
#[derive(Clone, Debug)]
pub struct HybridRateLimiterProvider {
    absolute: Arc<AbsoluteHybridRateLimiter>,
    suppressed: SuppressedHybridRateLimiter,
}

impl HybridRateLimiterProvider {
    pub(crate) fn new(options: RedisRateLimiterOptions) -> Self {
        Self {
            absolute: AbsoluteHybridRateLimiter::new(options.clone()),
            suppressed: SuppressedHybridRateLimiter::new(options),
        }
    }
    ///...
    pub fn absolute(&self) -> &AbsoluteHybridRateLimiter {
        &self.absolute
    }

    ///...
    pub fn suppressed(&self) -> &SuppressedHybridRateLimiter {
        &self.suppressed
    }

    pub(crate) async fn cleanup(&self, stale_after_ms: u64) -> Result<(), TrypemaError> {
        self.absolute.cleanup(stale_after_ms).await?;
        self.suppressed.cleanup(stale_after_ms).await
    }
}
