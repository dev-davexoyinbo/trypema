/// All errors that can occur when using the Trypema rate limiter.
///
/// Errors fall into two categories:
///
/// - **Validation errors** — Returned when constructing configuration types with invalid values
///   (e.g., a rate limit of `0.0`, an empty Redis key). These are returned as deterministic
///   `Err` values from validated constructors and conversions.
///
/// - **Redis errors** — Returned from async Redis operations when the connection fails, a Lua
///   script encounters an error, or Redis returns an unexpected result. Only available when the
///   `redis-tokio` or `redis-smol` feature is enabled.
///
/// This enum implements [`std::error::Error`] via `thiserror` and can be used with `?` in
/// functions returning `Result<_, TrypemaError>`.
///
/// # Examples
///
/// ```
/// use trypema::{RateLimit, TrypemaError};
///
/// // Validation errors are returned by unit-explicit constructors
/// match RateLimit::per_second(-1.0) {
///     Err(TrypemaError::InvalidRateLimit(msg)) => {
///         println!("Invalid rate: {}", msg);
///     }
///     _ => {}
/// }
/// ```
#[derive(Debug, thiserror::Error, PartialEq)]
#[non_exhaustive]
pub enum TrypemaError {
    /// Redis operation failed.
    ///
    /// Wraps errors from the `redis` crate, including connection failures,
    /// command errors, and protocol errors.
    ///
    /// Only available with `redis-tokio` or `redis-smol` features.
    ///
    /// # Common Causes
    ///
    /// - Connection refused (Redis not running)
    /// - Authentication failure
    /// - Network timeout
    /// - Redis command error
    /// - Lua script execution error
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
    #[error("redis error: {0}")]
    RedisError(#[from] redis::RedisError),

    /// Rate limit value is invalid.
    ///
    /// Rate limits must be positive (> 0.0).
    ///
    /// # Examples
    ///
    /// ```
    /// use trypema::{RateLimit, TrypemaError};
    ///
    /// assert!(matches!(
    ///     RateLimit::per_second(0.0),
    ///     Err(TrypemaError::InvalidRateLimit(_))
    /// ));
    /// assert!(matches!(
    ///     RateLimit::per_second(-5.0),
    ///     Err(TrypemaError::InvalidRateLimit(_))
    /// ));
    /// ```
    #[error("invalid rate limit: {0}")]
    InvalidRateLimit(String),

    /// Window size is invalid.
    ///
    /// Window size must be >= 1 second.
    ///
    /// # Examples
    ///
    /// ```
    /// use trypema::{WindowSize, TrypemaError};
    ///
    /// assert!(matches!(
    ///     WindowSize::seconds(0),
    ///     Err(TrypemaError::InvalidWindowSize(_))
    /// ));
    /// ```
    #[error("invalid window size: {0}")]
    InvalidWindowSize(String),

    /// Bucket size is invalid.
    ///
    /// Bucket size must be >= 1 millisecond.
    ///
    /// # Examples
    ///
    /// ```
    /// use trypema::{BucketSize, TrypemaError};
    ///
    /// assert!(matches!(
    ///     BucketSize::milliseconds(0),
    ///     Err(TrypemaError::InvalidBucketSize(_))
    /// ));
    /// ```
    #[error("invalid bucket size: {0}")]
    InvalidBucketSize(String),

    /// Hard limit factor is invalid.
    ///
    /// Hard limit factor must be ≥ 1.0. A value below 1.0 would set the hard limit
    /// below the base rate limit, causing suppression to be permanently active.
    ///
    /// # Examples
    ///
    /// ```
    /// use trypema::{HardLimitFactor, TrypemaError};
    ///
    /// assert!(matches!(
    ///     HardLimitFactor::try_from(0.0),
    ///     Err(TrypemaError::InvalidHardLimitFactor(_))
    /// ));
    /// assert!(matches!(
    ///     HardLimitFactor::try_from(0.5),
    ///     Err(TrypemaError::InvalidHardLimitFactor(_))
    /// ));
    /// ```
    #[error("invalid hard limit factor: {0}")]
    InvalidHardLimitFactor(String),

    /// Redis key is invalid.
    ///
    /// Redis keys must satisfy:
    /// - Not empty
    /// - ≤ 255 bytes
    /// - No `:` character (used internally as separator)
    ///
    /// Only available with `redis-tokio` or `redis-smol` features.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use trypema::{TrypemaError};
    /// use trypema::redis::RedisKey;
    ///
    /// // Invalid: contains ':'
    /// assert!(matches!(
    ///     RedisKey::try_from("user:123".to_string()),
    ///     Err(TrypemaError::InvalidRedisKey(_))
    /// ));
    ///
    /// // Invalid: empty
    /// assert!(matches!(
    ///     RedisKey::try_from("".to_string()),
    ///     Err(TrypemaError::InvalidRedisKey(_))
    /// ));
    /// ```
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
    #[error("invalid Redis key: {0}")]
    InvalidRedisKey(String),

    /// Redis Lua script returned an unexpected result.
    ///
    /// This indicates a bug or unexpected state in the Lua script execution.
    /// Please file an issue if you encounter this error.
    ///
    /// Only available with `redis-tokio` or `redis-smol` features.
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    #[cfg_attr(docsrs, doc(cfg(any(feature = "redis-tokio", feature = "redis-smol"))))]
    #[error("unexpected Redis script result for {operation} (key={key}): {result}")]
    UnexpectedRedisScriptResult {
        /// Operation name (e.g., `"absolute.inc"`).
        operation: &'static str,
        /// User-provided rate limiting key.
        key: String,
        /// Raw status string returned by the Lua script.
        result: String,
    },

    /// Custom error for internal use or extensions.
    ///
    /// Not typically encountered by end users.
    #[error("custom error: {0}")]
    CustomError(String),

    /// Suppression-factor cache period is invalid.
    ///
    /// The duration must be greater than 0.
    #[error("invalid suppression-factor cache period: {0}")]
    InvalidSuppressionFactorCachePeriod(String),

    /// Hybrid sync interval is invalid.
    ///
    /// The interval must be greater than 0 milliseconds and fit in the internal `u64`
    /// millisecond representation.
    #[error("invalid sync interval: {0}")]
    InvalidSyncInterval(String),

    /// Stale-state cleanup timing is invalid.
    #[error("invalid cleanup configuration: {0}")]
    InvalidCleanupConfiguration(String),
}
