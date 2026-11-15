use std::sync::Arc;

use redis::aio::ConnectionManager;

use crate::{
    RateLimiterBuilder, RateLimiterTarget, TrypemaError, WindowSize,
    builder::{CleanupConfig, ProviderConfig, sealed},
    cleanup_loop::CleanupLoop,
};

use super::{AbsoluteRedisRateLimiter, RedisKey, SuppressedRedisRateLimiter};

/// Internal Redis provider configuration assembled by the public builder.
#[derive(Clone, Debug)]
pub(crate) struct RedisRateLimiterConfig {
    pub connection_manager: ConnectionManager,
    pub prefix: Option<RedisKey>,
    pub provider: ProviderConfig,
}

/// Backend of a [`RateLimiterBuilder`] that builds Redis state.
///
/// Holds the connection manager and the key prefix. Create the builder with
/// [`RateLimiterBuilder::redis`].
#[derive(Clone, Debug)]
pub struct RedisBackend {
    connection_manager: ConnectionManager,
    prefix: Option<RedisKey>,
}

impl RedisBackend {
    pub(crate) fn into_config(self, provider: ProviderConfig) -> RedisRateLimiterConfig {
        RedisRateLimiterConfig {
            connection_manager: self.connection_manager,
            prefix: self.prefix,
            provider,
        }
    } // end fn into_config
} // end impl

#[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
impl RateLimiterBuilder<RedisBackend> {
    /// Create a builder for the Redis provider or one of its two limiters.
    ///
    /// The type that the caller expects from `build()` selects the target:
    /// [`RedisRateLimiterProvider`], [`AbsoluteRedisRateLimiter`], or
    /// [`SuppressedRedisRateLimiter`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example(connection_manager: trypema::redis::ConnectionManager) -> Result<(), trypema::TrypemaError> {
    /// use std::sync::Arc;
    /// use trypema::{
    ///     RateLimiterBuilder, WindowSize,
    ///     redis::{AbsoluteRedisRateLimiter, RedisKey, RedisRateLimiterProvider},
    /// };
    ///
    /// let provider: Arc<RedisRateLimiterProvider> =
    ///     RateLimiterBuilder::redis(connection_manager.clone()).build()?;
    ///
    /// let limiter: Arc<AbsoluteRedisRateLimiter> = RateLimiterBuilder::redis(connection_manager)
    ///     // Optional: only available with 'redis-tokio' or 'redis-smol'
    ///     .prefix(RedisKey::try_from("my-service")?)
    ///     // Optional: sliding-window duration
    ///     .window_size(WindowSize::seconds_or_panic(60))
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A target of another backend does not compile:
    ///
    /// ```compile_fail
    /// # async fn example(connection_manager: trypema::redis::ConnectionManager) -> Result<(), trypema::TrypemaError> {
    /// use std::sync::Arc;
    /// use trypema::{RateLimiterBuilder, hybrid::AbsoluteHybridRateLimiter};
    ///
    /// let limiter: Arc<AbsoluteHybridRateLimiter> =
    ///     RateLimiterBuilder::redis(connection_manager).build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn redis(connection_manager: ConnectionManager) -> Self {
        Self::new(RedisBackend {
            connection_manager,
            prefix: None,
        })
    } // end constructor
} // end impl

#[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
impl<T> RateLimiterBuilder<RedisBackend, T> {
    /// Set prefix used for Redis keys.
    pub fn prefix(mut self, value: RedisKey) -> Self {
        self.backend.prefix = Some(value);
        self
    } // end method prefix
} // end impl

impl sealed::Sealed for RedisRateLimiterProvider {}

impl RateLimiterTarget for RedisRateLimiterProvider {
    type Backend = RedisBackend;

    fn build_from(builder: RateLimiterBuilder<RedisBackend>) -> Result<Arc<Self>, TrypemaError> {
        let (backend, config, cleanup) = builder.validate()?;

        let provider = Arc::new(Self::new(backend.into_config(config), cleanup));

        if cleanup.enabled {
            provider.start_cleanup_loop();
        }

        Ok(provider)
    }
} // end impl

/// Provider for Redis-backed distributed rate limiting.
///
/// Enables rate limiting across multiple processes or servers using Redis as a
/// shared backend. All operations are implemented as atomic Lua scripts, so each individual Redis
/// call is atomic while overall admission remains best-effort under concurrency.
///
/// # Requirements
///
/// - **Redis:** >= 7.2.0
/// - **Runtime:** Tokio or Smol
///
/// # Consistency Semantics
///
/// - **Atomic operations:** Each Lua script execution is atomic within Redis
/// - **Best-effort limiting:** Overall rate limiting is approximate (not linearizable)
/// - **Concurrent overshoot:** Multiple clients can exceed limits simultaneously
///
/// See crate-level documentation for data model, cleanup, and operational considerations.
///
/// # Examples
///
/// ```no_run
/// # use trypema::redis::RedisRateLimiterProvider;
/// # async fn example(connection_manager: trypema::redis::ConnectionManager) {
/// # let rl = RedisRateLimiterProvider::builder(connection_manager).build().unwrap();
/// use trypema::{RateLimit, RateLimitDecision};
/// use trypema::redis::RedisKey;
///
/// let key = RedisKey::try_from("user_123").unwrap();
/// let rate = RateLimit::per_second(10.0).unwrap();
///
/// assert!(matches!(
///     rl.absolute().inc(&key, &rate, 1).await.unwrap(),
///     RateLimitDecision::Allowed
/// ));
/// # }
/// ```
#[derive(Debug)]
pub struct RedisRateLimiterProvider {
    absolute: AbsoluteRedisRateLimiter,
    suppressed: SuppressedRedisRateLimiter,
    cleanup_loop: CleanupLoop,
}

impl RedisRateLimiterProvider {
    /// Create a builder whose `build()` returns this provider.
    ///
    /// Same as [`RateLimiterBuilder::redis`] with the target fixed to this type.
    pub fn builder(
        connection_manager: ConnectionManager,
    ) -> RateLimiterBuilder<RedisBackend, Self> {
        RateLimiterBuilder::redis(connection_manager).with_target()
    }

    pub(crate) fn new(options: RedisRateLimiterConfig, cleanup: CleanupConfig) -> Self {
        Self {
            absolute: AbsoluteRedisRateLimiter::new(options.clone(), cleanup),
            suppressed: SuppressedRedisRateLimiter::new(options, cleanup),
            cleanup_loop: CleanupLoop::new(cleanup),
        }
    }

    /// Start stale-state cleanup. Calling while already running is no-op.
    pub fn start_cleanup_loop(self: &Arc<Self>) {
        CleanupLoop::start_task(
            self,
            |provider| &provider.cleanup_loop,
            |provider, stale_after_ms| async move { provider.cleanup(stale_after_ms).await },
            "Redis cleanup failed, will retry",
        );
    }

    /// Stop stale-state cleanup. Calling while stopped is no-op.
    pub fn stop_cleanup_loop(&self) {
        self.cleanup_loop.stop();
    }

    /// Return the sliding-window duration that this provider was built with.
    ///
    /// This is the builder's `window_size`, or the default of 10 seconds when the builder did not
    /// set one. The absolute and suppressed strategies of the provider use the same window.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # async fn example(connection_manager: trypema::redis::ConnectionManager) -> Result<(), trypema::TrypemaError> {
    /// use trypema::{WindowSize, redis::RedisRateLimiterProvider};
    ///
    /// let provider = RedisRateLimiterProvider::builder(connection_manager)
    ///     // Optional: sliding-window duration
    ///     .window_size(WindowSize::seconds_or_panic(90))
    ///     .build()?;
    ///
    /// assert_eq!(provider.window_size(), WindowSize::seconds_or_panic(90));
    /// # Ok(())
    /// # }
    /// ```
    pub fn window_size(&self) -> WindowSize {
        self.absolute.window_size()
    } // end method window_size

    /// Access the absolute allow/reject strategy.
    ///
    /// Returns a reference to the Redis absolute rate limiter, which provides
    /// distributed sliding-window enforcement via atomic Lua scripts.
    ///
    /// See [`AbsoluteRedisRateLimiter`] for full documentation.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use trypema::redis::RedisRateLimiterProvider;
    /// # async fn example(connection_manager: trypema::redis::ConnectionManager) {
    /// let rl = RedisRateLimiterProvider::builder(connection_manager).build().unwrap();
    /// use trypema::{RateLimit, RateLimitDecision};
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from("user_123").unwrap();
    /// let rate = RateLimit::per_second(10.0).unwrap();
    /// assert!(matches!(
    ///     rl.absolute().inc(&key, &rate, 1).await.unwrap(),
    ///     RateLimitDecision::Allowed
    /// ));
    /// # }
    /// ```
    pub fn absolute(&self) -> &AbsoluteRedisRateLimiter {
        &self.absolute
    }

    /// Access the suppressed strategy for probabilistic suppression.
    ///
    /// Returns a reference to the Redis suppressed rate limiter, which provides
    /// distributed probabilistic suppression via atomic Lua scripts.
    ///
    /// See [`SuppressedRedisRateLimiter`] for full documentation.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use trypema::redis::RedisRateLimiterProvider;
    /// # async fn example(connection_manager: trypema::redis::ConnectionManager) {
    /// let rl = RedisRateLimiterProvider::builder(connection_manager).build().unwrap();
    /// use trypema::{RateLimit, RateLimitDecision};
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from("user_123").unwrap();
    /// let rate = RateLimit::per_second(10.0).unwrap();
    ///
    /// match rl.suppressed().inc(&key, &rate, 1).await.unwrap() {
    ///     RateLimitDecision::Allowed => {}
    ///     RateLimitDecision::Suppressed {
    ///         is_allowed,
    ///         suppression_factor,
    ///         ..
    ///     } => {
    ///         let _ = (is_allowed, suppression_factor);
    ///     }
    ///     RateLimitDecision::Rejected { .. } => unreachable!(),
    /// }
    /// # }
    /// ```
    pub fn suppressed(&self) -> &SuppressedRedisRateLimiter {
        &self.suppressed
    }

    pub(crate) async fn cleanup(&self, stale_after_ms: u64) -> Result<(), TrypemaError> {
        // The two strategies are independent. A failure of one must not stop the other.
        let absolute = self.absolute.cleanup(stale_after_ms).await;
        let suppressed = self.suppressed.cleanup(stale_after_ms).await;

        if let (Err(_), Err(error)) = (&absolute, &suppressed) {
            tracing::warn!(?error, "Suppressed cleanup also failed");
        }

        absolute.and(suppressed)
    }
}
