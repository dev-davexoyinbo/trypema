#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
mod runtime;

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
mod test_absolute_hybrid_rate_limiter;
mod test_absolute_local_rate_limiter;
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
mod test_absolute_redis_rate_limiter;
mod test_cleanup_loop;
mod test_common_validation;
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
mod test_suppressed_hybrid_rate_limiter;
mod test_suppressed_local_rate_limiter;
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
mod test_suppressed_redis_rate_limiter;
