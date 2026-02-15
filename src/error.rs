/// Error type for this crate.
#[derive(Debug, thiserror::Error)]
pub enum TrypemaError {
    /// Redis error.
    #[error("redis error: {0}")]
    RedisError(#[from] redis::RedisError),
}
