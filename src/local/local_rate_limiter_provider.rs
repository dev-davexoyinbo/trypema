use crate::{
    AbsoluteLocalRateLimiter, SuppressedLocalRateLimiter,
    common::{HardLimitFactor, RateGroupSizeMs, SuppressionFactorCacheMs, WindowSizeSeconds},
};

/// Configuration for local (in-process) rate limiters.
///
/// Configures the sliding window, bucket coalescing, and hard limit behavior
/// for both absolute and suppressed strategies.
///
/// # Field Descriptions
///
/// See individual field documentation for detailed information on each parameter.
///
/// # Examples
///
/// ```
/// use trypema::{HardLimitFactor, RateGroupSizeMs, SuppressionFactorCacheMs, WindowSizeSeconds};
/// use trypema::local::LocalRateLimiterOptions;
///
/// // Recommended defaults for most use cases
/// let options = LocalRateLimiterOptions {
///     window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),   // 60s sliding window
///     rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),      // 10ms coalescing
///     hard_limit_factor: HardLimitFactor::try_from(1.5).unwrap(),      // 50% burst headroom
///     suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
/// };
///
/// // High-precision, low-coalescing
/// let precise = LocalRateLimiterOptions {
///     window_size_seconds: WindowSizeSeconds::try_from(10).unwrap(),
///     rate_group_size_ms: RateGroupSizeMs::try_from(1).unwrap(),
///     hard_limit_factor: HardLimitFactor::default(),
///     suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
/// };
///
/// // High-performance, aggressive coalescing
/// let fast = LocalRateLimiterOptions {
///     window_size_seconds: WindowSizeSeconds::try_from(120).unwrap(),
///     rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
///     hard_limit_factor: HardLimitFactor::try_from(2.0).unwrap(),
///     suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
/// };
/// ```
#[derive(Clone, Debug)]
pub struct LocalRateLimiterOptions {
    /// Sliding window duration for admission decisions.
    ///
    /// Determines how far back in time the limiter looks. Larger windows smooth bursts
    /// but delay recovery after hitting limits.
    ///
    /// **Typical values:** 10-300 seconds  
    /// **Recommended:** 60 seconds
    pub window_size_seconds: WindowSizeSeconds,

    /// Bucket coalescing interval in milliseconds.
    ///
    /// Increments within this interval are merged into the same bucket to reduce overhead.
    /// Larger values improve performance but reduce timing accuracy.
    ///
    /// **Typical values:** 10-100 milliseconds  
    /// **Recommended:** 10 milliseconds
    pub rate_group_size_ms: RateGroupSizeMs,

    /// Hard cutoff multiplier for the suppressed strategy.
    ///
    /// Defines the absolute maximum rate: `rate_limit × hard_limit_factor`
    ///
    /// Beyond this limit, the suppressed strategy transitions to full suppression
    /// (`suppression_factor = 1.0`). Only relevant for [`SuppressedLocalRateLimiter`].
    ///
    /// **Typical values:** 1.0-2.0  
    /// **Recommended:** 1.5 (50% burst headroom)  
    /// **Note:** Ignored by [`AbsoluteLocalRateLimiter`]
    pub hard_limit_factor: HardLimitFactor,

    /// Cache duration (milliseconds) for suppression factor recomputation.
    ///
    /// The suppressed strategy caches the computed suppression factor per key to avoid
    /// recomputing it on every request.
    ///
    /// **Typical values:** 10-1000 ms
    /// **Recommended:** 10-200 ms
    pub suppression_factor_cache_ms: SuppressionFactorCacheMs,
}

/// Provider for in-process rate limiting strategies.
///
/// Provides access to multiple local rate limiting strategies that share the same
/// configuration but implement different enforcement policies.
///
/// # Strategies
///
/// - **Absolute:** Strict sliding-window enforcement
/// - **Suppressed:** Probabilistic suppression for graceful degradation
///
/// # Thread Safety
///
/// All strategies are thread-safe and designed for concurrent use.
///
/// # Examples
///
/// ```no_run
/// use trypema::{HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimiter, RateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds};
/// use trypema::local::LocalRateLimiterOptions;
/// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
/// # use trypema::redis::RedisRateLimiterOptions;
/// #
/// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
/// # fn options() -> RateLimiterOptions {
/// #     RateLimiterOptions {
/// #         local: LocalRateLimiterOptions {
/// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
/// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
/// #             hard_limit_factor: HardLimitFactor::default(),
/// #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
/// #         },
/// #         redis: RedisRateLimiterOptions {
/// #             connection_manager: todo!(),
/// #             prefix: None,
/// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
/// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
/// #             hard_limit_factor: HardLimitFactor::default(),
/// #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
/// #         },
/// #     }
/// # }
/// #
/// # #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
/// # fn options() -> RateLimiterOptions {
/// #     RateLimiterOptions {
/// #         local: LocalRateLimiterOptions {
/// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
/// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
/// #             hard_limit_factor: HardLimitFactor::default(),
/// #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
/// #         },
/// #     }
/// # }
///
/// let rl = RateLimiter::new(options());
///
/// let rate = RateLimit::try_from(10.0).unwrap();
///
/// // Choose strategy based on requirements
/// let abs_decision = rl.local().absolute().inc("user_123", &rate, 1);
/// let sup_decision = rl.local().suppressed().inc("user_456", &rate, 1);
/// ```
#[derive(Debug)]
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

    /// Access the absolute strategy.
    ///
    /// Returns a reference to the absolute local rate limiter, which provides strict
    /// sliding-window enforcement with deterministic behavior.
    ///
    /// See [`AbsoluteLocalRateLimiter`] for full documentation.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use trypema::{HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimiter, RateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds};
    /// # use trypema::local::LocalRateLimiterOptions;
    /// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    /// # use trypema::redis::RedisRateLimiterOptions;
    /// #
    /// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    /// # fn options() -> RateLimiterOptions {
    /// #     RateLimiterOptions {
    /// #         local: LocalRateLimiterOptions {
    /// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
    /// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
    /// #             hard_limit_factor: HardLimitFactor::default(),
    /// #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
    /// #         },
    /// #         redis: RedisRateLimiterOptions {
    /// #             connection_manager: todo!(),
    /// #             prefix: None,
    /// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
    /// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
    /// #             hard_limit_factor: HardLimitFactor::default(),
    /// #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
    /// #         },
    /// #     }
    /// # }
    /// #
    /// # #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
    /// # fn options() -> RateLimiterOptions {
    /// #     RateLimiterOptions {
    /// #         local: LocalRateLimiterOptions {
    /// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
    /// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
    /// #             hard_limit_factor: HardLimitFactor::default(),
    /// #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
    /// #         },
    /// #     }
    /// # }
    /// # let rl = RateLimiter::new(options());
    /// let rate = RateLimit::try_from(10.0).unwrap();
    /// let decision = rl.local().absolute().inc("user_123", &rate, 1);
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
    /// ```no_run
    /// # use trypema::{HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision, RateLimiter, RateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds};
    /// # use trypema::local::LocalRateLimiterOptions;
    /// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    /// # use trypema::redis::RedisRateLimiterOptions;
    /// #
    /// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    /// # fn options() -> RateLimiterOptions {
    /// #     RateLimiterOptions {
    /// #         local: LocalRateLimiterOptions {
    /// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
    /// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
    /// #             hard_limit_factor: HardLimitFactor::default(),
    /// #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
    /// #         },
    /// #         redis: RedisRateLimiterOptions {
    /// #             connection_manager: todo!(),
    /// #             prefix: None,
    /// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
    /// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
    /// #             hard_limit_factor: HardLimitFactor::default(),
    /// #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
    /// #         },
    /// #     }
    /// # }
    /// #
    /// # #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
    /// # fn options() -> RateLimiterOptions {
    /// #     RateLimiterOptions {
    /// #         local: LocalRateLimiterOptions {
    /// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
    /// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
    /// #             hard_limit_factor: HardLimitFactor::default(),
    /// #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
    /// #         },
    /// #     }
    /// # }
    /// # let rl = RateLimiter::new(options());
    /// let rate = RateLimit::try_from(10.0).unwrap();
    /// match rl.local().suppressed().inc("user_123", &rate, 1) {
    ///     RateLimitDecision::Suppressed { is_allowed, suppression_factor } => {
    ///         println!("Suppression: {}, allowed: {}", suppression_factor, is_allowed);
    ///     }
    ///     _ => {}
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
