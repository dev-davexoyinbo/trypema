//! Shared test helpers for Redis-backed rate limiter tests.

use std::{env, time::Duration};

use crate::{RedisKey, common::RateType, redis::common::RedisKeyGenerator};

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
pub async fn wait_for_hybrid_sync(sync_interval_ms: u64) {
    runtime::async_sleep(Duration::from_millis(sync_interval_ms * 2 + 50)).await;
}
