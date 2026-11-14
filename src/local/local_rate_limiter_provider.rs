use std::{sync::Arc, time::Duration};

use crate::{
    BucketSize, HardLimitFactor, RateLimiterBuilder, SuppressionFactorCachePeriod, TrypemaError,
    WindowSize,
    builder::{CleanupConfig, ProviderConfig},
    cleanup_loop::CleanupLoop,
};

use super::{AbsoluteLocalRateLimiter, SuppressedLocalRateLimiter};

/// Builder for [`LocalRateLimiterProvider`].
#[derive(Clone, Copy, Debug, Default)]
pub struct LocalRateLimiterBuilder {
    provider: ProviderConfig,
    cleanup: CleanupConfig,
}

impl LocalRateLimiterBuilder {
    pub(crate) fn validate(self) -> Result<(ProviderConfig, CleanupConfig), TrypemaError> {
        Ok((self.provider.validate()?, self.cleanup.validate()?))
    }
}

impl RateLimiterBuilder for LocalRateLimiterBuilder {
    type Provider = LocalRateLimiterProvider;

    fn window_size(mut self, value: WindowSize) -> Self {
        self.provider.window_size = value;
        self
    }

    fn bucket_size(mut self, value: BucketSize) -> Self {
        self.provider.bucket_size = value;
        self
    }

    fn hard_limit_factor(mut self, value: HardLimitFactor) -> Self {
        self.provider.hard_limit_factor = value;
        self
    }

    fn suppression_factor_cache_period(mut self, value: SuppressionFactorCachePeriod) -> Self {
        self.provider.suppression_factor_cache_period = value;
        self
    }

    fn stale_after(mut self, value: Duration) -> Self {
        self.cleanup.stale_after = value;
        self
    }

    fn cleanup_interval(mut self, value: Duration) -> Self {
        self.cleanup.interval = value;
        self
    }

    fn cleanup_enabled(mut self, enabled: bool) -> Self {
        self.cleanup.enabled = enabled;
        self
    }

    fn build(self) -> Result<Arc<Self::Provider>, TrypemaError> {
        let (config, cleanup) = self.validate()?;

        let provider = Arc::new(LocalRateLimiterProvider::new(config, cleanup));

        if cleanup.enabled {
            provider.start_cleanup_loop();
        }

        Ok(provider)
    }
}

/// Provider for in-process rate limiting strategies.
///
/// Exposes [`AbsoluteLocalRateLimiter`] and [`SuppressedLocalRateLimiter`] under one shared
/// configuration.
///
/// # Strategies
///
/// - **Absolute:** Best-effort sliding-window allow/reject admission
/// - **Suppressed:** Probabilistic suppression for graceful degradation
///
/// # Thread Safety
///
/// All strategies are thread-safe and designed for concurrent use.
///
/// # Examples
///
/// ```
/// # use trypema::{RateLimiterBuilder, local::LocalRateLimiterProvider};
/// # let rl = LocalRateLimiterProvider::builder().disable_cleanup().build().unwrap();
/// use trypema::RateLimit;
///
/// let rate = RateLimit::per_second(10.0).unwrap();
/// let abs_decision = rl.absolute().inc("user_123", &rate, 1);
/// let sup_decision = rl.suppressed().inc("user_456", &rate, 1);
/// ```
#[derive(Debug)]
pub struct LocalRateLimiterProvider {
    absolute: AbsoluteLocalRateLimiter,
    suppressed: SuppressedLocalRateLimiter,
    cleanup_loop: CleanupLoop,
}

impl LocalRateLimiterProvider {
    /// Create builder using default validated configuration.
    pub fn builder() -> LocalRateLimiterBuilder {
        LocalRateLimiterBuilder::default()
    }

    pub(crate) fn new(config: ProviderConfig, cleanup: CleanupConfig) -> Self {
        Self {
            absolute: AbsoluteLocalRateLimiter::new(config, cleanup),
            suppressed: SuppressedLocalRateLimiter::new(config, cleanup),
            cleanup_loop: CleanupLoop::new(cleanup),
        }
    }

    /// Start stale-state cleanup. Calling while already running is no-op.
    pub fn start_cleanup_loop(self: &Arc<Self>) {
        CleanupLoop::start_thread(self, |provider| &provider.cleanup_loop, Self::cleanup);
    }

    /// Stop stale-state cleanup. Calling while stopped is no-op.
    pub fn stop_cleanup_loop(&self) {
        self.cleanup_loop.stop();
    }

    /// Access the absolute strategy.
    ///
    /// Returns the absolute local limiter. Admission is allow/reject and best-effort under
    /// concurrency; concurrent callers may temporarily overshoot.
    ///
    /// See [`AbsoluteLocalRateLimiter`] for full documentation.
    ///
    /// # Examples
    ///
    /// ```
    /// # use trypema::{RateLimiterBuilder, local::LocalRateLimiterProvider};
    /// # let rl = LocalRateLimiterProvider::builder().disable_cleanup().build().unwrap();
    /// use trypema::{RateLimit, RateLimitDecision};
    ///
    /// let rate = RateLimit::per_second(10.0).unwrap();
    /// let decision = rl.absolute().inc("user_123", &rate, 1);
    /// assert!(matches!(decision, RateLimitDecision::Allowed));
    /// ```
    pub fn absolute(&self) -> &AbsoluteLocalRateLimiter {
        &self.absolute
    }

    /// Access the suppressed strategy.
    ///
    /// Returns a reference to the suppressed local rate limiter, which provides
    /// probabilistic suppression for graceful degradation under load.
    ///
    /// Returns [`RateLimitDecision::Suppressed`](crate::RateLimitDecision::Suppressed)
    /// with suppression metadata.
    ///
    /// See [`SuppressedLocalRateLimiter`] for full documentation.
    ///
    /// # Examples
    ///
    /// ```
    /// # use trypema::{RateLimiterBuilder, local::LocalRateLimiterProvider};
    /// # let rl = LocalRateLimiterProvider::builder().disable_cleanup().build().unwrap();
    /// use trypema::{RateLimit, RateLimitDecision};
    ///
    /// let rate = RateLimit::per_second(10.0).unwrap();
    /// match rl.suppressed().inc("user_123", &rate, 1) {
    ///     RateLimitDecision::Suppressed { is_allowed, suppression_factor, .. } => {
    ///         println!("suppression: {suppression_factor}, allowed: {is_allowed}");
    ///     }
    ///     RateLimitDecision::Allowed => {}
    ///     RateLimitDecision::Rejected { .. } => unreachable!(),
    /// }
    /// ```
    pub fn suppressed(&self) -> &SuppressedLocalRateLimiter {
        &self.suppressed
    }

    pub(crate) fn cleanup(&self, stale_after_ms: u64) {
        self.absolute.cleanup(stale_after_ms);
        self.suppressed.cleanup(stale_after_ms);
    }
}
