//! Builder for every provider and limiter, and the configuration it validates.

use std::{fmt, marker::PhantomData, sync::Arc, time::Duration};

use crate::{BucketSize, HardLimitFactor, SuppressionFactorCachePeriod, TrypemaError, WindowSize};

/// Builder for every provider and every limiter of one backend.
///
/// Start from the backend:
///
/// - `RateLimiterBuilder::local()` builds in-process state.
/// - `RateLimiterBuilder::redis(connection_manager)` builds Redis state. Only available with
///   `redis-tokio` or `redis-smol`.
/// - `RateLimiterBuilder::hybrid(connection_manager)` builds local state that synchronizes with
///   Redis. Only available with `redis-tokio` or `redis-smol`.
///
/// `B` is the backend and holds its own settings: [`LocalBackend`](crate::local::LocalBackend),
/// `RedisBackend`, or `HybridBackend`. `T` is the target of `build()`. With the default
/// [`AnyTarget`], the type that the caller expects selects the target: the provider of the
/// backend or one of its two limiters. A `builder()` shortcut on a provider or limiter, such as
/// `LocalRateLimiterProvider::builder()`, returns a builder whose `T` is that type, so its
/// `build()` needs no annotation.
///
/// A provider holds both strategies under one configuration. A limiter built directly runs one
/// strategy only. It reads and writes the same Redis keys as that strategy of a provider with the
/// same prefix.
///
/// All settings have defaults. A target ignores the settings that its strategy does not use:
///
/// - The absolute limiters do not use [`hard_limit_factor`](Self::hard_limit_factor) and
///   [`suppression_factor_cache_period`](Self::suppression_factor_cache_period).
/// - The suppressed hybrid limiter does not use `allowed_lag_applies_to_rejections`.
///
/// `build()` validates the settings and starts stale-state cleanup unless cleanup is disabled. A
/// limiter has one cleanup loop, and a provider starts the loops of its two limiters. A loop ends
/// at once after the last `Arc` of the built value drops.
///
/// # Examples
///
/// ```
/// use std::{sync::Arc, time::Duration};
/// use trypema::{
///     RateLimit, RateLimitDecision, RateLimiterBuilder, WindowSize,
///     local::{AbsoluteLocalRateLimiter, LocalRateLimiterProvider},
/// };
///
/// // The annotation selects the target.
/// let limiter: Arc<AbsoluteLocalRateLimiter> = RateLimiterBuilder::local()
///     // Optional: sliding-window duration
///     .window_size(WindowSize::seconds_or_panic(60))
///     // Optional: inactivity duration before cleanup removes a key
///     .stale_after(Duration::from_secs(300))
///     .build()?;
///
/// let rate = RateLimit::per_second_or_panic(10.0);
/// assert!(matches!(limiter.inc("user-123", &rate, 1), RateLimitDecision::Allowed));
///
/// // The shortcut fixes the target.
/// let provider = LocalRateLimiterProvider::builder().build()?;
/// assert!(matches!(provider.absolute().inc("user-123", &rate, 1), RateLimitDecision::Allowed));
/// # Ok::<(), trypema::TrypemaError>(())
/// ```
///
/// A shortcut builder builds only its own type, and a builder builds only the types of its
/// backend:
///
/// ```compile_fail
/// use std::sync::Arc;
/// use trypema::local::{AbsoluteLocalRateLimiter, SuppressedLocalRateLimiter};
///
/// let limiter: Arc<SuppressedLocalRateLimiter> = AbsoluteLocalRateLimiter::builder().build()?;
/// # Ok::<(), trypema::TrypemaError>(())
/// ```
pub struct RateLimiterBuilder<B, T = AnyTarget> {
    pub(crate) backend: B,
    pub(crate) config: ProviderConfig,
    pub(crate) cleanup: CleanupConfig,
    target: PhantomData<fn() -> T>,
}

/// Target of a [`RateLimiterBuilder`] that the type the caller expects from `build()` selects.
#[derive(Clone, Copy, Debug)]
pub struct AnyTarget;

/// Provider or limiter that a [`RateLimiterBuilder`] of backend [`Backend`](Self::Backend) can
/// build.
///
/// Implemented for each provider and its two limiters. The trait is sealed.
pub trait RateLimiterTarget: sealed::Sealed + Sized {
    /// Backend of the builder that builds this type.
    type Backend;

    #[doc(hidden)]
    fn build_from(builder: RateLimiterBuilder<Self::Backend>) -> Result<Arc<Self>, TrypemaError>;
}

pub(crate) mod sealed {
    pub trait Sealed {}
}

impl<B> RateLimiterBuilder<B> {
    pub(crate) fn new(backend: B) -> Self {
        Self {
            backend,
            config: ProviderConfig::default(),
            cleanup: CleanupConfig::default(),
            target: PhantomData,
        }
    } // end constructor
} // end impl

impl<B, T> RateLimiterBuilder<B, T> {
    /// Set sliding-window duration.
    pub fn window_size(mut self, value: WindowSize) -> Self {
        self.config.window_size = value;
        self
    } // end method window_size

    /// Set bucket-coalescing interval.
    ///
    /// The bucket size must be less than or equal to the configured window size when `build()`
    /// is called.
    pub fn bucket_size(mut self, value: BucketSize) -> Self {
        self.config.bucket_size = value;
        self
    } // end method bucket_size

    /// Set hard cutoff multiplier used by suppressed strategy.
    ///
    /// The absolute limiters do not use it.
    pub fn hard_limit_factor(mut self, value: HardLimitFactor) -> Self {
        self.config.hard_limit_factor = value;
        self
    } // end method hard_limit_factor

    /// Set suppression-factor cache period.
    ///
    /// The absolute limiters do not use it.
    pub fn suppression_factor_cache_period(mut self, value: SuppressionFactorCachePeriod) -> Self {
        self.config.suppression_factor_cache_period = value;
        self
    } // end method suppression_factor_cache_period

    /// Set inactivity duration after which state becomes eligible for cleanup.
    pub fn stale_after(mut self, value: Duration) -> Self {
        self.cleanup.stale_after = value;
        self
    } // end method stale_after

    /// Set interval between stale-state cleanup passes.
    pub fn cleanup_interval(mut self, value: Duration) -> Self {
        self.cleanup.interval = value;
        self
    } // end method cleanup_interval

    /// Disable automatic cleanup startup when the provider or limiter is built.
    pub fn disable_cleanup(self) -> Self {
        self.cleanup_enabled(false)
    } // end method disable_cleanup

    /// Enable automatic cleanup startup when the provider or limiter is built.
    pub fn enable_cleanup(self) -> Self {
        self.cleanup_enabled(true)
    } // end method enable_cleanup

    /// Set automatic cleanup startup from a runtime boolean.
    ///
    /// Prefer [`enable_cleanup`](Self::enable_cleanup) or
    /// [`disable_cleanup`](Self::disable_cleanup) when value is known statically.
    pub fn cleanup_enabled(mut self, enabled: bool) -> Self {
        self.cleanup.enabled = enabled;
        self
    } // end method cleanup_enabled

    pub(crate) fn with_target<U>(self) -> RateLimiterBuilder<B, U> {
        RateLimiterBuilder {
            backend: self.backend,
            config: self.config,
            cleanup: self.cleanup,
            target: PhantomData,
        }
    } // end fn with_target

    /// Return the backend settings with the validated shared settings.
    pub(crate) fn validate(self) -> Result<(B, ProviderConfig, CleanupConfig), TrypemaError> {
        Ok((
            self.backend,
            self.config.validate()?,
            self.cleanup.validate()?,
        ))
    } // end fn validate
} // end impl

impl<B> RateLimiterBuilder<B, AnyTarget> {
    /// Build the provider or limiter that the caller expects, and start cleanup when enabled.
    ///
    /// The type `T` comes from the context, such as `let limiter: Arc<T> = builder.build()?;`,
    /// or from a turbofish, `build::<T>()`. It must be the provider of this backend or one of its
    /// two limiters.
    ///
    /// # Errors
    ///
    /// Returns [`TrypemaError::InvalidBucketSize`] when the configured bucket size exceeds the
    /// configured window size. Returns [`TrypemaError::InvalidCleanupConfiguration`] when the
    /// stale-after duration or the cleanup interval is shorter than 1 millisecond or does not
    /// fit in `u64` milliseconds.
    pub fn build<T>(self) -> Result<Arc<T>, TrypemaError>
    where
        T: RateLimiterTarget<Backend = B>,
    {
        T::build_from(self)
    } // end method build
} // end impl

impl<T: RateLimiterTarget> RateLimiterBuilder<T::Backend, T> {
    /// Build the provider or limiter of the `builder()` shortcut that created this builder, and
    /// start cleanup when enabled.
    ///
    /// # Errors
    ///
    /// Returns [`TrypemaError::InvalidBucketSize`] when the configured bucket size exceeds the
    /// configured window size. Returns [`TrypemaError::InvalidCleanupConfiguration`] when the
    /// stale-after duration or the cleanup interval is shorter than 1 millisecond or does not
    /// fit in `u64` milliseconds.
    pub fn build(self) -> Result<Arc<T>, TrypemaError> {
        T::build_from(self.with_target())
    } // end method build
} // end impl

impl<B: Clone, T> Clone for RateLimiterBuilder<B, T> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            config: self.config,
            cleanup: self.cleanup,
            target: PhantomData,
        }
    }
} // end impl

impl<B: Copy, T> Copy for RateLimiterBuilder<B, T> {}

impl<B: fmt::Debug, T> fmt::Debug for RateLimiterBuilder<B, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RateLimiterBuilder")
            .field("backend", &self.backend)
            .field("config", &self.config)
            .field("cleanup", &self.cleanup)
            .finish()
    }
} // end impl

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
