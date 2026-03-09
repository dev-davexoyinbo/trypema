/// All errors that can occur when using the Trypema rate limiter.
///
/// Errors fall into two categories:
///
/// - **Validation errors** — Returned when constructing configuration types with invalid values
///   (e.g., a rate limit of `0.0`, an empty Redis key). These are returned as `Err` from
///   `TryFrom` implementations and are deterministic.
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
/// // Validation errors are returned by TryFrom implementations
/// match RateLimit::try_from(-1.0) {
///     Err(TrypemaError::InvalidRateLimit(msg)) => {
///         println!("Invalid rate: {}", msg);
///     }
///     _ => {}
/// }
/// ```
#[derive(Debug, thiserror::Error, PartialEq, Clone)]
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
    ///     RateLimit::try_from(0.0),
    ///     Err(TrypemaError::InvalidRateLimit(_))
    /// ));
    /// assert!(matches!(
    ///     RateLimit::try_from(-5.0),
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
    /// use trypema::{WindowSizeSeconds, TrypemaError};
    ///
    /// assert!(matches!(
    ///     WindowSizeSeconds::try_from(0),
    ///     Err(TrypemaError::InvalidWindowSizeSeconds(_))
    /// ));
    /// ```
    #[error("invalid window size: {0}")]
    InvalidWindowSizeSeconds(String),

    /// Rate group size is invalid.
    ///
    /// Rate group size must be >= 1 millisecond.
    ///
    /// # Examples
    ///
    /// ```
    /// use trypema::{RateGroupSizeMs, TrypemaError};
    ///
    /// assert!(matches!(
    ///     RateGroupSizeMs::try_from(0),
    ///     Err(TrypemaError::InvalidRateGroupSizeMs(_))
    /// ));
    /// ```
    #[error("invalid rate group size: {0}")]
    InvalidRateGroupSizeMs(String),

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

    /// Invalid suppression factor cache duration.
    ///
    /// The duration must be greater than 0.
    #[error("invalid suppression factor cache duration: {0}")]
    InvalidSuppressionFactorCacheMs(String),
}
