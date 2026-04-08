//! Setup helpers shared across all doc-test examples.
//!
//! This module is not part of the public API and will not appear in generated documentation.
//! It is `#[doc(hidden)]` and exists solely to reduce boilerplate in doc-test code blocks.

use std::sync::Arc;

use crate::RateLimiter;

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
use std::future::Future;

/// Returns a unique key string for test isolation.
///
/// Generates a random suffix so parallel doc-tests do not collide on shared Redis state.
pub fn unique_key() -> String {
    let n: u64 = rand::random();
    format!("dt_{n}")
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())
}

/// Creates an [`Arc<RateLimiter>`] ready for use in doc-test examples.
///
/// - **Without Redis features:** constructs a local-only limiter (no network I/O).
/// - **With Redis features:** connects to `$REDIS_URL` (default: `redis://127.0.0.1:6379`)
///   and returns a full limiter with local, Redis, and hybrid providers available.
#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
pub fn rate_limiter() -> Arc<RateLimiter> {
    RateLimiter::builder().build().unwrap()
}

/// Creates an [`Arc<RateLimiter>`] ready for use in doc-test examples.
///
/// Connects to `$REDIS_URL` (default: `redis://127.0.0.1:6379`) using the Tokio runtime.
#[cfg(feature = "redis-tokio")]
pub fn rate_limiter() -> Arc<RateLimiter> {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let conn = redis::Client::open(redis_url())
            .unwrap()
            .get_connection_manager()
            .await
            .unwrap();
        RateLimiter::builder(conn).build().unwrap()
    })
}

/// Build a Redis-enabled [`Arc<RateLimiter>`] and run `f` on the same runtime.
///
/// This keeps connection setup and all Redis-backed operations inside one runtime-owned
/// execution scope, which avoids dropped-runtime issues in doctests.
#[cfg(feature = "redis-tokio")]
pub fn with_redis_rate_limiter<F, Fut, T>(f: F) -> T
where
    F: FnOnce(Arc<RateLimiter>) -> Fut,
    Fut: Future<Output = T>,
{
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(async move {
            let conn = redis::Client::open(redis_url())
                .unwrap()
                .get_connection_manager()
                .await
                .unwrap();
            let rl = RateLimiter::builder(conn).build().unwrap();
            f(rl).await
        })
}

/// Creates an [`Arc<RateLimiter>`] ready for use in doc-test examples.
///
/// Connects to `$REDIS_URL` (default: `redis://127.0.0.1:6379`) using the Smol runtime.
#[cfg(feature = "redis-smol")]
pub fn rate_limiter() -> Arc<RateLimiter> {
    smol::block_on(async {
        let conn = redis::Client::open(redis_url())
            .unwrap()
            .get_connection_manager()
            .await
            .unwrap();
        RateLimiter::builder(conn).build().unwrap()
    })
}

/// Build a Redis-enabled [`Arc<RateLimiter>`] and run `f` on the same runtime.
///
/// This keeps connection setup and all Redis-backed operations inside one runtime-owned
/// execution scope, which avoids dropped-runtime issues in doctests.
#[cfg(feature = "redis-smol")]
pub fn with_redis_rate_limiter<F, Fut, T>(f: F) -> T
where
    F: FnOnce(Arc<RateLimiter>) -> Fut,
    Fut: Future<Output = T>,
{
    smol::block_on(async move {
        let conn = redis::Client::open(redis_url())
            .unwrap()
            .get_connection_manager()
            .await
            .unwrap();
        let rl = RateLimiter::builder(conn).build().unwrap();
        f(rl).await
    })
}

/// Drive an async expression to completion in doc-test examples.
///
/// Used to wrap async Redis/hybrid method calls in doc examples without requiring
/// `#[tokio::main]` (which needs the `tokio/macros` feature).
#[cfg(feature = "redis-tokio")]
pub fn block_on<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    tokio::runtime::Runtime::new().unwrap().block_on(f)
}

/// Drive an async expression to completion in doc-test examples.
///
/// Used to wrap async Redis/hybrid method calls in doc examples without requiring
/// a specific runtime macro.
#[cfg(feature = "redis-smol")]
pub fn block_on<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    smol::block_on(f)
}
