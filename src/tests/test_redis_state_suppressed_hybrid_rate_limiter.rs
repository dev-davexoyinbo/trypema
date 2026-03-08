//! Redis state inspection tests for the **suppressed hybrid** rate limiter.
//!
//! The hybrid suppressed limiter accumulates counts locally and flushes to Redis only when
//! the local soft-limit budget is exhausted.  These tests verify that after a flush the
//! correct keys are written with the correct values.
//!
//! # Redis data model (hybrid_suppressed, per user-key `K`, prefix `P`)
//!
//! | Redis key                              | Type        | Meaning                                          |
//! |----------------------------------------|-------------|--------------------------------------------------|
//! | `P:K:hybrid_suppressed:h`              | Hash        | `timestamp_ms → count` (total increments)        |
//! | `P:K:hybrid_suppressed:hd`             | Hash        | `timestamp_ms → declined_count`                  |
//! | `P:K:hybrid_suppressed:a`              | Sorted set  | Active bucket timestamps (scores = ts_ms)        |
//! | `P:K:hybrid_suppressed:w`              | String      | Stored hard window limit                         |
//! | `P:K:hybrid_suppressed:t`              | String      | Running total count (allowed + denied)           |
//! | `P:K:hybrid_suppressed:d`              | String      | Running total declined count                     |
//! | `P:K:hybrid_suppressed:sf`             | String      | Cached suppression factor (with PX TTL)          |
//! | `P:active_entities`                    | Sorted set  | All active user-keys (for cleanup)               |
//!
//! **Important:** because the hybrid limiter batches writes, tests must wait for the
//! background committer to flush (`wait_for_hybrid_sync`) before inspecting Redis state.

use std::{collections::HashMap, env, time::Duration};

use redis::AsyncCommands;

use super::runtime;

use crate::hybrid::SyncIntervalMs;
use crate::{
    HardLimitFactor, LocalRateLimiterOptions, RateGroupSizeMs, RateLimit, RateLimitDecision,
    RateLimiter, RateLimiterOptions, RedisKey, RedisRateLimiterOptions, SuppressionFactorCacheMs,
    WindowSizeSeconds,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn redis_url() -> String {
    env::var("REDIS_URL").unwrap_or_else(|_| {
        panic!(
            "REDIS_URL env var must be set for Redis-backed tests (e.g. REDIS_URL=redis://127.0.0.1:16379/)"
        )
    })
}

fn unique_prefix() -> RedisKey {
    let n: u64 = rand::random();
    RedisKey::try_from(format!("trypema_test_{n}")).unwrap()
}

fn key(s: &str) -> RedisKey {
    RedisKey::try_from(s.to_string()).unwrap()
}

async fn build_limiter(
    url: &str,
    window_size_seconds: u64,
    rate_group_size_ms: u64,
    hard_limit_factor: f64,
    suppression_factor_cache_ms: u64,
    sync_interval_ms: u64,
    prefix: RedisKey,
) -> std::sync::Arc<RateLimiter> {
    let client = redis::Client::open(url).unwrap();
    let cm = client.get_connection_manager().await.unwrap();

    let options = RateLimiterOptions {
        local: LocalRateLimiterOptions {
            window_size_seconds: WindowSizeSeconds::try_from(window_size_seconds).unwrap(),
            rate_group_size_ms: RateGroupSizeMs::try_from(rate_group_size_ms).unwrap(),
            hard_limit_factor: HardLimitFactor::try_from(hard_limit_factor).unwrap(),
            suppression_factor_cache_ms: SuppressionFactorCacheMs::try_from(
                suppression_factor_cache_ms,
            )
            .unwrap(),
        },
        redis: RedisRateLimiterOptions {
            connection_manager: cm,
            prefix: Some(prefix),
            window_size_seconds: WindowSizeSeconds::try_from(window_size_seconds).unwrap(),
            rate_group_size_ms: RateGroupSizeMs::try_from(rate_group_size_ms).unwrap(),
            hard_limit_factor: HardLimitFactor::try_from(hard_limit_factor).unwrap(),
            suppression_factor_cache_ms: SuppressionFactorCacheMs::try_from(
                suppression_factor_cache_ms,
            )
            .unwrap(),
            sync_interval_ms: SyncIntervalMs::try_from(sync_interval_ms).unwrap(),
        },
    };

    std::sync::Arc::new(RateLimiter::new(options))
}

/// `{prefix}:{user_key}:hybrid_suppressed:{suffix}`
fn redis_key(prefix: &RedisKey, user_key: &RedisKey, suffix: &str) -> String {
    format!("{}:{}:hybrid_suppressed:{}", **prefix, **user_key, suffix)
}

/// Wait long enough for the background committer to flush two ticks.
async fn wait_for_hybrid_sync(sync_interval_ms: u64) {
    runtime::async_sleep(Duration::from_millis(sync_interval_ms * 2 + 50)).await;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Before the local soft-limit budget is exhausted no hybrid_suppressed Redis keys should
/// exist — the hybrid limiter must not write to Redis while it can still serve locally.
#[test]
fn redis_state_hybrid_suppressed_no_redis_keys_before_soft_limit_overflow() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 1_u64;
        let hard_limit_factor = 2.0_f64;
        let sync_interval_ms = 2000_u64; // very slow tick

        let rl = build_limiter(
            &url,
            window_size_seconds,
            1000,
            hard_limit_factor,
            100,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let soft_cap = (window_size_seconds as f64 * *rate_limit) as u64; // 5

        // Fill exactly the soft limit.
        for _ in 0..soft_cap {
            let d = rl
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            match d {
                RateLimitDecision::Allowed => {}
                RateLimitDecision::Suppressed {
                    is_allowed: true, ..
                } => {}
                other => panic!("unexpected: {other:?}"),
            }
        }

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // No commit should have happened.
        let total: Option<u64> = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        assert!(
            total.is_none(),
            "total count key must not exist before soft-limit overflow"
        );
        let hash_len: u64 = conn.hlen(redis_key(&prefix, &k, "h")).await.unwrap();
        assert_eq!(hash_len, 0, "hash must be empty before soft-limit overflow");
    });
}

/// After the soft limit is overflowed (triggering a commit), the total count key must
/// appear in Redis with a value that reflects the committed increments.
#[test]
fn redis_state_hybrid_suppressed_commit_writes_total_count_after_overflow() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 1_u64;
        let hard_limit_factor = 1.0_f64; // soft == hard
        let sync_interval_ms = 25_u64;
        let cache_ms = 5_u64;

        let rl = build_limiter(
            &url,
            window_size_seconds,
            1000,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let soft_cap = (window_size_seconds as f64 * *rate_limit) as u64; // 5

        // Fill local budget.
        for _ in 0..soft_cap {
            let d = rl
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            match d {
                RateLimitDecision::Allowed => {}
                RateLimitDecision::Suppressed {
                    is_allowed: true, ..
                } => {}
                other => panic!("unexpected: {other:?}"),
            }
        }

        // Overflow — queues commit.
        let _ = rl
            .hybrid()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();

        // Wait for committer flush.
        wait_for_hybrid_sync(sync_interval_ms).await;

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        assert!(
            total > 0,
            "total count should be > 0 after overflow commit, got {total}"
        );
        // The suppressed strategy records every call (allowed + denied) in the total count,
        // so after soft_cap allowed increments plus one overflow increment the committed
        // total can be up to soft_cap + 1.
        assert!(
            total <= soft_cap + 1,
            "total count ({total}) should not exceed soft_cap + 1 ({soft_cap} + 1)"
        );

        let hash: HashMap<String, u64> =
            conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        assert!(!hash.is_empty(), "hash must have at least one bucket after commit");
    });
}

/// When the committed total exceeds the hard limit, Redis should record a suppression factor
/// equal to 1.0 in the `sf` cache key.
#[test]
fn redis_state_hybrid_suppressed_sf_is_one_at_hard_limit_after_commit() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 1_u64;
        let hard_limit_factor = 2.0_f64;
        let sync_interval_ms = 25_u64;
        let cache_ms = 5_u64;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        // soft=5, hard=10
        let hard_cap = (window_size_seconds as f64 * *rate_limit * hard_limit_factor) as u64;

        let rl = build_limiter(
            &url,
            window_size_seconds,
            1000,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;

        // Drive past the hard limit.
        for _ in 0..=hard_cap {
            let _ = rl
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
        }
        wait_for_hybrid_sync(sync_interval_ms).await;
        // Let any sf cache expire.
        runtime::async_sleep(Duration::from_millis(cache_ms + 50)).await;

        // Reading from a fresh instance forces a Redis read_state which writes the sf cache.
        let rl2 = build_limiter(
            &url,
            window_size_seconds,
            1000,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;
        let sf = rl2
            .hybrid()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();
        assert!(
            (sf - 1.0).abs() < 1e-12,
            "suppression factor must be 1.0 at hard limit, got {sf}"
        );

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // The sf cache key must exist and hold "1".
        let sf_raw: Option<String> = conn.get(redis_key(&prefix, &k, "sf")).await.unwrap();
        assert!(
            sf_raw.is_some(),
            "sf cache key must be set after read_state at hard limit"
        );
        let sf_val: f64 = sf_raw.unwrap().parse().expect("sf must be a valid float");
        assert!(
            (sf_val - 1.0).abs() < 1e-12,
            "sf cache must equal 1.0 at hard limit, got {sf_val}"
        );
    });
}

/// The `sf` cache key must have a positive TTL (set via PX in the Lua script).
#[test]
fn redis_state_hybrid_suppressed_sf_cache_key_has_ttl() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 10_u64;
        let hard_limit_factor = 2.0_f64;
        let sync_interval_ms = 25_u64;
        let cache_ms = 500_u64;

        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();
        let soft_cap = (window_size_seconds as f64 * *rate_limit) as u64;

        let rl = build_limiter(
            &url,
            window_size_seconds,
            1000,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;

        // Drive just past the soft limit so the suppression zone is entered and sf is cached.
        for _ in 0..=soft_cap {
            let _ = rl
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
        }
        wait_for_hybrid_sync(sync_interval_ms).await;

        // Force a Redis read so the sf cache key is written.
        let rl2 = build_limiter(
            &url,
            window_size_seconds,
            1000,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;
        let _ = rl2
            .hybrid()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let pttl: i64 = conn
            .pttl(redis_key(&prefix, &k, "sf"))
            .await
            .unwrap();

        assert!(
            pttl > 0,
            "sf cache key must have a positive TTL (PX set), got {pttl}"
        );
        assert!(
            pttl <= cache_ms as i64,
            "TTL must be <= cache_ms={cache_ms}, got {pttl}"
        );
    });
}

/// The hash sum (`h`) must equal the total count key (`t`) and the declined hash sum
/// (`hd`) must equal the declined count key (`d`) after a commit that includes denials.
#[test]
fn redis_state_hybrid_suppressed_hash_sums_match_counters_after_commit_with_denials() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 1_u64;
        let hard_limit_factor = 2.0_f64;
        let sync_interval_ms = 25_u64;
        let cache_ms = 5_u64;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let hard_cap = (window_size_seconds as f64 * *rate_limit * hard_limit_factor) as u64;

        let rl = build_limiter(
            &url,
            window_size_seconds,
            1000,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;

        // Drive past the hard limit to cause denials.
        for _ in 0..=hard_cap {
            let _ = rl
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
        }
        wait_for_hybrid_sync(sync_interval_ms).await;

        // Trigger another commit cycle that includes declined counts.
        let rl2 = build_limiter(
            &url,
            window_size_seconds,
            1000,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;
        for _ in 0..5 {
            let _ = rl2
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
        }
        wait_for_hybrid_sync(sync_interval_ms).await;

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        let declined: u64 = conn
            .get(redis_key(&prefix, &k, "d"))
            .await
            .unwrap_or(0u64);

        let hash: HashMap<String, u64> =
            conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        let hash_d: HashMap<String, u64> =
            conn.hgetall(redis_key(&prefix, &k, "hd")).await.unwrap();

        let hash_sum: u64 = hash.values().sum();
        let hash_d_sum: u64 = hash_d.values().sum();

        assert_eq!(
            hash_sum, total,
            "hash sum ({hash_sum}) must equal total count ({total})"
        );
        assert_eq!(
            hash_d_sum, declined,
            "declined hash sum ({hash_d_sum}) must equal declined count ({declined})"
        );
        assert!(
            total >= declined,
            "total ({total}) must be >= declined ({declined})"
        );
    });
}

/// After the window expires and a new commit is made, stale buckets must be evicted so
/// the total count reflects only the fresh increments.
#[test]
fn redis_state_hybrid_suppressed_evicts_expired_buckets_on_next_read_state() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 1_u64;
        let hard_limit_factor = 1.0_f64; // soft == hard
        let sync_interval_ms = 25_u64;
        let cache_ms = 5_u64;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let cap = (window_size_seconds as f64 * *rate_limit) as u64;

        let rl = build_limiter(
            &url,
            window_size_seconds,
            1000,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;

        // Fill and overflow to commit.
        for _ in 0..=cap {
            let _ = rl
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
        }
        wait_for_hybrid_sync(sync_interval_ms).await;

        // Wait for the window to expire.
        runtime::async_sleep(Duration::from_millis(window_size_seconds * 1000 + 100)).await;
        runtime::async_sleep(Duration::from_millis(cache_ms + 50)).await;

        // A fresh limiter read_state triggers eviction.
        let rl2 = build_limiter(
            &url,
            window_size_seconds,
            1000,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;
        let sf = rl2
            .hybrid()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();
        assert!(
            (sf - 0.0).abs() < 1e-12,
            "suppression factor must be 0.0 after window expiry, got {sf}"
        );

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap_or(0);
        assert_eq!(
            total, 0,
            "total count must be 0 after window eviction, got {total}"
        );
    });
}

/// Two separate prefixes must maintain independent Redis namespaces — usage committed
/// under prefix A must not appear under prefix B.
#[test]
fn redis_state_hybrid_suppressed_different_prefixes_are_isolated() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix_a = unique_prefix();
        let prefix_b = unique_prefix();
        let window_size_seconds = 1_u64;
        let sync_interval_ms = 25_u64;
        let hard_limit_factor = 1.0_f64;
        let cache_ms = 5_u64;

        let rl_a = build_limiter(
            &url,
            window_size_seconds,
            1000,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix_a.clone(),
        )
        .await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let cap = (window_size_seconds as f64 * *rate_limit) as u64;

        // A overflows and commits.
        for _ in 0..=cap {
            let _ = rl_a
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
        }
        wait_for_hybrid_sync(sync_interval_ms).await;

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // Prefix B must be untouched.
        let total_b: Option<u64> = conn.get(redis_key(&prefix_b, &k, "t")).await.unwrap();
        assert!(
            total_b.is_none(),
            "prefix B should not have any state after prefix A's commit"
        );
    });
}

/// The `hybrid_suppressed` keyspace must not be contaminated by writes to the `suppressed`
/// (pure Redis) keyspace — they use different `rate_type` segments in the key.
#[test]
fn redis_state_hybrid_suppressed_does_not_contaminate_redis_suppressed_keyspace() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 1_u64;
        let hard_limit_factor = 2.0_f64;
        let sync_interval_ms = 2000_u64; // slow tick so no background flush
        let cache_ms = 100_u64;

        let rl = build_limiter(
            &url,
            window_size_seconds,
            1000,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let soft_cap = (window_size_seconds as f64 * *rate_limit) as u64;

        // Use only the hybrid suppressed path.
        for _ in 0..soft_cap {
            let d = rl
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            match d {
                RateLimitDecision::Allowed => {}
                RateLimitDecision::Suppressed {
                    is_allowed: true, ..
                } => {}
                other => panic!("unexpected: {other:?}"),
            }
        }

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // The pure `suppressed` (non-hybrid) keyspace must be empty.
        let suppressed_total_key = format!("{}:{}:suppressed:t", prefix.as_str(), k.as_str());
        let suppressed_total: Option<u64> = conn.get(&suppressed_total_key).await.unwrap();
        assert!(
            suppressed_total.is_none(),
            "redis suppressed keyspace must not be contaminated by hybrid suppressed writes"
        );
    });
}

/// The hard window limit stored in the Redis `w` key must equal
/// `floor(window_size_seconds * rate_limit * hard_limit_factor)`.
#[test]
fn redis_state_hybrid_suppressed_window_limit_key_equals_hard_limit_after_commit() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 5_u64;
        let rate_limit_value = 2f64;
        let hard_limit_factor = 3.0_f64;
        // hard_window_limit = floor(5 * 2 * 3) = 30
        let expected_hard_limit = 30_u64;

        let sync_interval_ms = 25_u64;
        let cache_ms = 5_u64;

        let k = key("k");
        let rate_limit = RateLimit::try_from(rate_limit_value).unwrap();
        let soft_cap = (window_size_seconds as f64 * *rate_limit) as u64;

        let rl = build_limiter(
            &url,
            window_size_seconds,
            1000,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;

        // Overflow to trigger commit.
        for _ in 0..=soft_cap {
            let _ = rl
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
        }
        wait_for_hybrid_sync(sync_interval_ms).await;

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let stored_limit: u64 = conn.get(redis_key(&prefix, &k, "w")).await.unwrap();
        assert_eq!(
            stored_limit, expected_hard_limit,
            "stored window limit should equal hard_limit"
        );
    });
}
