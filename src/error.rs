/// Error type for this crate.
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum TrypemaError {
    /// Redis error.
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    #[error("redis error: {0}")]
    RedisError(#[from] redis::RedisError),
    /// Invalid rate limit.
    #[error("invalid rate limit: {0}")]
    InvalidRateLimit(String),
    /// Invalid window size.
    #[error("invalid window size: {0}")]
    InvalidWindowSizeSeconds(String),
    /// Invalid rate group size.
    #[error("invalid rate group size: {0}")]
    InvalidRateGroupSizeMs(String),
    /// Invalid hard limit factor.
    #[error("invalid hard limit factor: {0}")]
    InvalidHardLimitFactor(String),
    /// Invalid Redis key.
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    #[error("invalid Redis key: {0}")]
    InvalidRedisKey(String),
}
