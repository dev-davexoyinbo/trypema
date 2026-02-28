/// A signal to the Redis rate limiter to flush its local cache.
pub enum RedisRateLimiterSignal {
    /// Flush the local cache.
    Flush,
}
