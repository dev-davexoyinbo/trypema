//! Shared test helpers for Redis-backed rate limiter tests.

use std::{env, time::Duration};

use crate::{
    common::RateType,
    redis::{RedisKey, common::RedisKeyGenerator},
};

use super::runtime;

/// Read `REDIS_URL` from the environment, panic if unset.
pub fn redis_url() -> String {
    env::var("REDIS_URL").unwrap_or_else(|_| {
        panic!(
            "REDIS_URL env var must be set for Redis-backed tests \
             (e.g. REDIS_URL=redis://127.0.0.1:16379/)"
        )
    })
}

/// Create a Redis connection manager for provider tests.
pub async fn connection_manager() -> redis::aio::ConnectionManager {
    redis::Client::open(redis_url())
        .unwrap()
        .get_connection_manager()
        .await
        .unwrap()
}

/// Generate a unique, random test prefix so parallel tests don't collide.
pub fn unique_prefix() -> RedisKey {
    let n: u64 = rand::random();
    RedisKey::try_from(format!("trypema_test_{n}")).unwrap()
}

/// Convenience constructor for a `RedisKey` from a string literal.
pub fn key(s: &str) -> RedisKey {
    RedisKey::try_from(s.to_string()).unwrap()
}

/// Return a `RedisKeyGenerator` for the given prefix and rate type.
pub fn key_gen(prefix: &RedisKey, rate_type: RateType) -> RedisKeyGenerator {
    RedisKeyGenerator::new(prefix.clone(), rate_type)
}

/// Wait long enough for the background hybrid committer to flush two ticks.
pub async fn wait_for_hybrid_sync(sync_interval: u64) {
    runtime::async_sleep(Duration::from_millis(sync_interval * 2 + 50)).await;
}

/// Fail with a clear message when the local state may already be older than the allowed lag.
///
/// `synced_before` is taken before the call that read Redis, so the state is younger than this.
pub fn assert_inside_allowed_lag(synced_before: std::time::Instant, allowed_lag_ms: u64) {
    let elapsed = synced_before.elapsed();
    assert!(
        elapsed < Duration::from_millis(allowed_lag_ms),
        "test setup took {elapsed:?}, longer than the {allowed_lag_ms} ms allowed lag; \
         the inside-the-lag assertion cannot be checked"
    );
}

/// Sleep until the local state stored by a call that returned at `synced_after` is older than
/// the allowed lag.
pub async fn sleep_past_allowed_lag(synced_after: std::time::Instant, allowed_lag_ms: u64) {
    let target = Duration::from_millis(allowed_lag_ms + 200);
    let elapsed = synced_after.elapsed();
    if elapsed < target {
        runtime::async_sleep(target - elapsed).await;
    }
}
