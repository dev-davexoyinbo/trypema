//! Shared provider-builder API and configuration.

use std::{sync::Arc, time::Duration};

use crate::{BucketSize, HardLimitFactor, SuppressionFactorCachePeriod, TrypemaError, WindowSize};

/// Shared configuration API implemented by every rate-limiter provider builder.
///
/// Provider-specific settings, such as Redis prefixes and hybrid synchronization intervals,
/// remain inherent methods on their concrete builders.
pub trait RateLimiterBuilder: Sized {
    /// Provider produced by this builder.
    type Provider;

    /// Set sliding-window duration.
    fn window_size(self, value: WindowSize) -> Self;

    /// Set bucket-coalescing interval.
    ///
    /// The bucket size must be less than or equal to the configured window size when
    /// [`build`](Self::build) is called.
    fn bucket_size(self, value: BucketSize) -> Self;

    /// Set hard cutoff multiplier used by suppressed strategy.
    fn hard_limit_factor(self, value: HardLimitFactor) -> Self;

    /// Set suppression-factor cache period.
    fn suppression_factor_cache_period(self, value: SuppressionFactorCachePeriod) -> Self;

    /// Set inactivity duration after which state becomes eligible for cleanup.
    fn stale_after(self, value: Duration) -> Self;

    /// Set interval between stale-state cleanup passes.
    fn cleanup_interval(self, value: Duration) -> Self;

    /// Enable or disable automatic cleanup startup when provider is built.
    fn cleanup_enabled(self, enabled: bool) -> Self;

    /// Build provider and start cleanup when enabled.
    ///
    /// Returns [`TrypemaError::InvalidBucketSize`] when the configured bucket size exceeds the
    /// configured window size.
    fn build(self) -> Result<Arc<Self::Provider>, TrypemaError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ProviderConfig {
    pub window_size: WindowSize,
    pub bucket_size: BucketSize,
    pub hard_limit_factor: HardLimitFactor,
    pub suppression_factor_cache_period: SuppressionFactorCachePeriod,
}

impl ProviderConfig {
    pub(crate) fn validate(self) -> Result<Self, TrypemaError> {
        if u128::from(self.bucket_size.as_milliseconds()) > self.window_size.as_milliseconds() {
            return Err(TrypemaError::InvalidBucketSize(
                "bucket size must be less than or equal to window size".to_string(),
            ));
        }

        Ok(self)
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CleanupConfig {
    pub enabled: bool,
    pub stale_after: Duration,
    pub interval: Duration,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            stale_after: Duration::from_secs(10 * 60),
            interval: Duration::from_secs(30),
        }
    }
}

impl CleanupConfig {
    pub(crate) fn validate(self) -> Result<Self, TrypemaError> {
        validate_cleanup_duration("stale-after duration", self.stale_after)?;
        validate_cleanup_duration("cleanup interval", self.interval)?;
        Ok(self)
    }

    pub(crate) fn stale_after_ms(self) -> u64 {
        self.stale_after.as_millis() as u64
    }
}

fn validate_cleanup_duration(name: &str, value: Duration) -> Result<(), TrypemaError> {
    if value.as_millis() == 0 {
        return Err(TrypemaError::InvalidCleanupConfiguration(format!(
            "{name} must be at least 1 millisecond"
        )));
    }

    if value.as_millis() > u64::MAX as u128 {
        return Err(TrypemaError::InvalidCleanupConfiguration(format!(
            "{name} must fit in u64 milliseconds"
        )));
    }

    Ok(())
}
