use std::sync::Arc;

use crate::{
    RateLimiterBuilder, RateLimiterTarget, TrypemaError,
    builder::{CleanupConfig, ProviderConfig, sealed},
    cleanup_loop::CleanupLoop,
};

use super::{AbsoluteLocalRateLimiter, SuppressedLocalRateLimiter};

/// Backend of a [`RateLimiterBuilder`] that builds in-process state.
///
/// It has no settings of its own. Create the builder with [`RateLimiterBuilder::local`].
#[derive(Clone, Copy, Debug)]
pub struct LocalBackend;

impl RateLimiterBuilder<LocalBackend> {
    /// Create a builder for the local provider or one of its two limiters.
    ///
    /// The type that the caller expects from `build()` selects the target:
    /// [`LocalRateLimiterProvider`], [`AbsoluteLocalRateLimiter`], or
    /// [`SuppressedLocalRateLimiter`].
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use trypema::{
    ///     RateLimiterBuilder, WindowSize,
    ///     local::{LocalRateLimiterProvider, SuppressedLocalRateLimiter},
    /// };
    ///
    /// let provider: Arc<LocalRateLimiterProvider> = RateLimiterBuilder::local().build()?;
    ///
    /// let limiter: Arc<SuppressedLocalRateLimiter> = RateLimiterBuilder::local()
    ///     // Optional: sliding-window duration
    ///     .window_size(WindowSize::seconds_or_panic(60))
    ///     .build()?;
    /// # Ok::<(), trypema::TrypemaError>(())
    /// ```
    pub fn local() -> Self {
        Self::new(LocalBackend)
    } // end constructor
} // end impl

impl sealed::Sealed for LocalRateLimiterProvider {}

impl RateLimiterTarget for LocalRateLimiterProvider {
    type Backend = LocalBackend;

    fn build_from(builder: RateLimiterBuilder<LocalBackend>) -> Result<Arc<Self>, TrypemaError> {
        let (_, config, cleanup) = builder.validate()?;

        let provider = Arc::new(Self::new(config, cleanup));

        if cleanup.enabled {
            provider.start_cleanup_loop();
        }

        Ok(provider)
    }
} // end impl

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
/// # use trypema::local::LocalRateLimiterProvider;
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
    /// Create a builder whose `build()` returns this provider.
    ///
    /// Same as [`RateLimiterBuilder::local`] with the target fixed to this type.
    pub fn builder() -> RateLimiterBuilder<LocalBackend, Self> {
        RateLimiterBuilder::local().with_target()
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
    /// # use trypema::local::LocalRateLimiterProvider;
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
    /// # use trypema::local::LocalRateLimiterProvider;
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
