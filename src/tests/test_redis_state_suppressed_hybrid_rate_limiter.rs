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
//! | `P:hybrid_suppressed:active_entities`  | Sorted set  | All active user-keys (for cleanup)               |
//!
//! **Important:** because the hybrid limiter batches writes, tests must wait for the
//! background committer to flush (`wait_for_hybrid_sync`) before inspecting Redis state.

use std::{collections::HashMap, time::Duration};

use redis::AsyncCommands;

use super::common::{key, key_gen, redis_url, unique_prefix, wait_for_hybrid_sync};
use super::runtime;

use crate::common::RateType;
use crate::hybrid::SyncIntervalMs;
use crate::{
    HardLimitFactor, HistoryPreservation, LocalRateLimiterOptions, RateGroupSizeMs, RateLimit,
    RateLimitComparator, RateLimitDecision, RateLimiter, RateLimiterOptions, RedisKey,
    RedisRateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

/// Construct the canonical Redis key for a given suffix using the key generator.
fn redis_key(prefix: &RedisKey, user_key: &RedisKey, suffix: &str) -> String {
    let kg = key_gen(prefix, RateType::HybridSuppressed);
    match suffix {
        "h" => kg.get_hash_key(user_key),
        "hd" => kg.get_hash_declined_key(user_key),
        "a" => kg.get_active_keys(user_key),
        "w" => kg.get_window_limit_key(user_key),
        "t" => kg.get_total_count_key(user_key),
        "d" => kg.get_total_declined_key(user_key),
        "sf" => kg.get_suppression_factor_key(user_key),
        _ => panic!("unknown suffix for hybrid_suppressed rate type: {suffix}"),
    }
}

fn active_entities_key(prefix: &RedisKey) -> String {
    key_gen(prefix, RateType::HybridSuppressed).get_active_entities_key()
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
        for accepted_after in 1..=soft_cap {
            let d = rl
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            assert!(
                matches!(d, RateLimitDecision::Allowed),
                "accepted_after={accepted_after}, d={d:?}"
            );
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
        for accepted_after in 1..=soft_cap {
            let d = rl
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            assert!(
                matches!(d, RateLimitDecision::Allowed),
                "accepted_after={accepted_after}, d={d:?}"
            );
        }

        // Overflow is observed and declined, then queued for commit with the admitted history.
        let overflow = rl
            .hybrid()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(
            matches!(
                overflow,
                RateLimitDecision::Suppressed {
                    suppression_factor: 1f64,
                    is_allowed: false,
                }
            ),
            "overflow: {overflow:?}"
        );

        // Wait for committer flush.
        wait_for_hybrid_sync(sync_interval_ms).await;

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        assert_eq!(total, soft_cap + 1);

        let declined: u64 = conn.get(redis_key(&prefix, &k, "d")).await.unwrap();
        assert_eq!(declined, 1);

        let hash: HashMap<String, u64> = conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        assert_eq!(hash.values().sum::<u64>(), soft_cap + 1);
        let declined_hash: HashMap<String, u64> =
            conn.hgetall(redis_key(&prefix, &k, "hd")).await.unwrap();
        assert_eq!(declined_hash.values().sum::<u64>(), 1);
    });
}

/// An exact-hard accepted batch is flushed without declines and causes the commit script to cache
/// factor one. The immediate next local call is declined and its later flush preserves exact
/// observed/declined accounting.
#[test]
fn redis_state_hybrid_suppressed_exact_hard_commit_caches_one_and_next_call_declines() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 10_u64;
        let hard_limit_factor = 2.0_f64;
        let sync_interval_ms = 25_u64;
        let cache_ms = 60_000_u64;

        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();
        // soft=10, hard=20
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

        let reaches_hard = rl
            .hybrid()
            .suppressed()
            .inc(&k, &rate_limit, hard_cap)
            .await
            .unwrap();
        assert!(
            matches!(reaches_hard, RateLimitDecision::Allowed),
            "reaches_hard: {reaches_hard:?}"
        );
        wait_for_hybrid_sync(sync_interval_ms).await;

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        assert_eq!(total, hard_cap);
        let declined: Option<u64> = conn.get(redis_key(&prefix, &k, "d")).await.unwrap();
        assert_eq!(declined, None);

        let history: HashMap<String, u64> =
            conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        assert_eq!(history.values().sum::<u64>(), hard_cap);
        let declined_history: HashMap<String, u64> =
            conn.hgetall(redis_key(&prefix, &k, "hd")).await.unwrap();
        assert!(declined_history.is_empty());

        let ordering_count: u64 = conn.zcard(redis_key(&prefix, &k, "a")).await.unwrap();
        assert_eq!(ordering_count, 1);
        let stored_limit: u64 = conn.get(redis_key(&prefix, &k, "w")).await.unwrap();
        assert_eq!(stored_limit, hard_cap);

        let factor: f64 = conn.get(redis_key(&prefix, &k, "sf")).await.unwrap();
        assert!((factor - 1.0).abs() < 1e-12, "factor: {factor}");
        let factor_ttl: i64 = conn.pttl(redis_key(&prefix, &k, "sf")).await.unwrap();
        assert!(factor_ttl > 0 && factor_ttl <= cache_ms as i64);

        let active_score: Option<f64> = conn
            .zscore(active_entities_key(&prefix), k.to_string())
            .await
            .unwrap();
        assert!(active_score.is_some(), "key should be registered as active");

        let next = rl
            .hybrid()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(
            matches!(
                next,
                RateLimitDecision::Suppressed {
                    suppression_factor: 1f64,
                    is_allowed: false,
                }
            ),
            "next: {next:?}"
        );

        wait_for_hybrid_sync(sync_interval_ms).await;

        let total_after: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        assert_eq!(total_after, hard_cap + 1);
        let declined_after: u64 = conn.get(redis_key(&prefix, &k, "d")).await.unwrap();
        assert_eq!(declined_after, 1);
        let history_after: HashMap<String, u64> =
            conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        assert_eq!(history_after.values().sum::<u64>(), hard_cap + 1);
        let declined_history_after: HashMap<String, u64> =
            conn.hgetall(redis_key(&prefix, &k, "hd")).await.unwrap();
        assert_eq!(declined_history_after.values().sum::<u64>(), 1);
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

        assert_eq!(
            rl.hybrid()
                .suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Nil, soft_cap + 1,)
                .await
                .unwrap(),
            (soft_cap + 1, 0)
        );

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
        let factor = rl2
            .hybrid()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();
        let expected_factor = 1.0_f64 - (1.0_f64 / (soft_cap + 1) as f64);
        assert!((factor - expected_factor).abs() < 1e-12);

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let pttl: i64 = conn.pttl(redis_key(&prefix, &k, "sf")).await.unwrap();

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
        let window_size_seconds = 10_u64;
        let hard_limit_factor = 2.0_f64;
        let sync_interval_ms = 25_u64;
        let cache_ms = 60_000_u64;

        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();
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

        let reaches_hard = rl
            .hybrid()
            .suppressed()
            .inc(&k, &rate_limit, hard_cap)
            .await
            .unwrap();
        assert!(
            matches!(reaches_hard, RateLimitDecision::Allowed),
            "reaches_hard: {reaches_hard:?}"
        );

        for denied_after in 1..=5_u64 {
            let decision = rl
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            assert!(
                matches!(
                    decision,
                    RateLimitDecision::Suppressed {
                        suppression_factor: 1f64,
                        is_allowed: false,
                    }
                ),
                "denied_after={denied_after}, decision={decision:?}"
            );
        }
        wait_for_hybrid_sync(sync_interval_ms).await;

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        let declined: u64 = conn.get(redis_key(&prefix, &k, "d")).await.unwrap_or(0u64);

        let hash: HashMap<String, u64> = conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        let hash_d: HashMap<String, u64> =
            conn.hgetall(redis_key(&prefix, &k, "hd")).await.unwrap();

        let hash_sum: u64 = hash.values().sum();
        let hash_d_sum: u64 = hash_d.values().sum();

        assert_eq!(total, hard_cap + 5);
        assert_eq!(declined, 5);
        assert_eq!(
            hash_sum, total,
            "hash sum ({hash_sum}) must equal total count ({total})"
        );
        assert_eq!(
            hash_d_sum, declined,
            "declined hash sum ({hash_d_sum}) must equal declined count ({declined})"
        );
    });
}

/// A hybrid read must evict expired buckets and invalidate a still-live Redis
/// suppression cache before calculating the new factor.
#[test]
fn redis_state_hybrid_suppressed_evicts_expired_buckets_on_next_read_state() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 1_u64;
        let hard_limit_factor = 1.0_f64; // soft == hard
        let sync_interval_ms = 25_u64;
        let cache_ms = 5_000_u64;

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

        assert_eq!(
            rl.hybrid()
                .suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Nil, cap)
                .await
                .unwrap(),
            (cap, 0)
        );

        // Prime a long-lived Redis suppression cache before the history expires.
        let cache_priming_reader = build_limiter(
            &url,
            window_size_seconds,
            1000,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;
        let sf_before = cache_priming_reader
            .hybrid()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();
        assert_eq!(sf_before, 1.0, "setup must cache full suppression");

        // Wait for the window to expire.
        runtime::async_sleep(Duration::from_millis(window_size_seconds * 1000 + 100)).await;

        // A fresh limiter read_state must invalidate the still-live cache when it
        // evicts the expired buckets.
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

        assert_eq!(
            rl_a.hybrid()
                .suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Nil, cap + 1,)
                .await
                .unwrap(),
            (cap + 1, 0)
        );

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
        for accepted_after in 1..=soft_cap {
            let d = rl
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            assert!(
                matches!(d, RateLimitDecision::Allowed),
                "accepted_after={accepted_after}, d={d:?}"
            );
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

// ---------------------------------------------------------------------------
// Cleanup tests
// ---------------------------------------------------------------------------

/// After cleanup with a stale threshold the entity has exceeded, all per-entity Redis keys
/// (h, hd, a, w, t, d, sf) must be deleted and the entity removed from `active_entities`.
#[test]
fn redis_state_hybrid_suppressed_cleanup_removes_all_redis_keys_for_stale_entity() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 5_u64;
        let hard_limit_factor = 2.0_f64;
        let sync_interval_ms = 25_u64;
        let cache_ms = 60_000_u64; // long TTL so sf doesn't expire naturally
        let stale_after_ms = 150_u64;

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

        let reaches_hard = rl
            .hybrid()
            .suppressed()
            .inc(&k, &rate_limit, hard_cap)
            .await
            .unwrap();
        assert!(
            matches!(reaches_hard, RateLimitDecision::Allowed),
            "reaches_hard: {reaches_hard:?}"
        );

        let declined = rl
            .hybrid()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(
            matches!(
                declined,
                RateLimitDecision::Suppressed {
                    suppression_factor: 1f64,
                    is_allowed: false,
                }
            ),
            "declined: {declined:?}"
        );
        wait_for_hybrid_sync(sync_interval_ms).await;

        // Read from a fresh instance to prove the committed factor is independently visible.
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
        let factor = rl2
            .hybrid()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();
        assert!((factor - 1.0).abs() < 1e-12, "factor: {factor}");

        wait_for_hybrid_sync(sync_interval_ms * 3).await;

        let kg = key_gen(&prefix, RateType::HybridSuppressed);
        let active_entities_key = kg.get_active_entities_key();

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // Verify all expected keys exist before cleanup.
        for entity_key in kg.get_all_entity_keys(&k) {
            let exists: bool = conn.exists(&entity_key).await.unwrap();
            assert!(exists, "key {entity_key} must exist before cleanup");
        }
        let score: Option<f64> = conn.zscore(&active_entities_key, k.as_str()).await.unwrap();
        assert!(
            score.is_some(),
            "entity must be in active_entities before cleanup"
        );

        // Wait until the entity is stale.
        runtime::async_sleep(Duration::from_millis(stale_after_ms + 50)).await;

        rl2.hybrid()
            .suppressed()
            .cleanup(stale_after_ms)
            .await
            .unwrap();

        // All per-entity keys must be deleted.
        for entity_key in kg.get_all_entity_keys(&k) {
            let exists: bool = conn.exists(&entity_key).await.unwrap();
            assert!(!exists, "key {entity_key} must be deleted after cleanup");
        }
        let score_after: Option<f64> = conn.zscore(&active_entities_key, k.as_str()).await.unwrap();
        assert!(
            score_after.is_none(),
            "entity must be removed from active_entities after cleanup"
        );
    });
}

/// An entity whose last-commit timestamp is within `stale_after_ms` must survive cleanup.
#[test]
fn redis_state_hybrid_suppressed_cleanup_does_not_remove_active_entity() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 5_u64;
        let hard_limit_factor = 1.0_f64;
        let sync_interval_ms = 25_u64;
        let cache_ms = 5_u64;
        let stale_after_ms = 5_000_u64; // very long — entity will not be stale yet

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
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

        let reaches_hard = rl
            .hybrid()
            .suppressed()
            .inc(&k, &rate_limit, soft_cap)
            .await
            .unwrap();
        assert!(matches!(reaches_hard, RateLimitDecision::Allowed));
        let overflow = rl
            .hybrid()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(
            matches!(
                overflow,
                RateLimitDecision::Suppressed {
                    suppression_factor: 1f64,
                    is_allowed: false,
                }
            ),
            "overflow: {overflow:?}"
        );
        wait_for_hybrid_sync(sync_interval_ms).await;

        // Immediately cleanup with a long threshold — nothing should be removed.
        rl.hybrid()
            .suppressed()
            .cleanup(stale_after_ms)
            .await
            .unwrap();

        let active_entities_key =
            key_gen(&prefix, RateType::HybridSuppressed).get_active_entities_key();

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let t_exists: bool = conn.exists(redis_key(&prefix, &k, "t")).await.unwrap();
        assert!(
            t_exists,
            "total count key must still exist for active entity"
        );
        let score: Option<f64> = conn.zscore(&active_entities_key, k.as_str()).await.unwrap();
        assert!(
            score.is_some(),
            "active entity must remain in active_entities after cleanup"
        );
    });
}

/// After cleanup removes Redis state, a subsequent `inc` for the same key must be allowed
/// (the in-memory state must also be cleared so the limiter starts fresh from Redis).
#[test]
fn redis_state_hybrid_suppressed_cleanup_allows_fresh_requests_after_cleanup() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 5_u64;
        let hard_limit_factor = 2.0_f64; // soft_cap=10, hard_cap=20 — genuine suppression zone
        let sync_interval_ms = 25_u64;
        let cache_ms = 5_u64;
        let stale_after_ms = 150_u64;

        let k = key("k");
        let rate_limit = RateLimit::try_from(2f64).unwrap();
        let soft_cap = (window_size_seconds as f64 * *rate_limit) as u64; // 10

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

        let reaches_soft = rl
            .hybrid()
            .suppressed()
            .inc(&k, &rate_limit, soft_cap)
            .await
            .unwrap();
        assert!(matches!(reaches_soft, RateLimitDecision::Allowed));
        let above_soft = rl
            .hybrid()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(
            matches!(
                above_soft,
                RateLimitDecision::Suppressed {
                    suppression_factor,
                    is_allowed: true,
                } if suppression_factor.abs() < 1e-12
            ),
            "above_soft: {above_soft:?}"
        );
        wait_for_hybrid_sync(sync_interval_ms).await;

        // Wait until entity is stale, then clean up.
        runtime::async_sleep(Duration::from_millis(stale_after_ms + 50)).await;
        rl.hybrid()
            .suppressed()
            .cleanup(stale_after_ms)
            .await
            .unwrap();

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let t_exists: bool = conn.exists(redis_key(&prefix, &k, "t")).await.unwrap();
        assert!(!t_exists, "total count key must be deleted after cleanup");

        // The next request must be allowed — stale in-memory Suppressing state must be cleared.
        let decision = rl
            .hybrid()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        let is_allowed = match decision {
            RateLimitDecision::Allowed => true,
            RateLimitDecision::Suppressed { is_allowed, .. } => is_allowed,
            _ => false,
        };
        assert!(
            is_allowed,
            "expected Allowed or Suppressed{{is_allowed:true}} after cleanup but got {decision:?}"
        );
    });
}

/// The `sf` cache key with a long TTL must be explicitly deleted by the cleanup Lua script
/// (not merely allowed to expire naturally).
#[test]
fn redis_state_hybrid_suppressed_sf_key_deleted_by_cleanup() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 5_u64;
        let hard_limit_factor = 2.0_f64;
        let sync_interval_ms = 25_u64;
        let cache_ms = 60_000_u64; // 60 s TTL — won't expire naturally during the test
        let stale_after_ms = 150_u64;

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

        let reaches_hard = rl
            .hybrid()
            .suppressed()
            .inc(&k, &rate_limit, hard_cap)
            .await
            .unwrap();
        assert!(matches!(reaches_hard, RateLimitDecision::Allowed));
        let declined = rl
            .hybrid()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(
            matches!(
                declined,
                RateLimitDecision::Suppressed {
                    suppression_factor: 1f64,
                    is_allowed: false,
                }
            ),
            "declined: {declined:?}"
        );
        wait_for_hybrid_sync(sync_interval_ms).await;

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
        let factor = rl2
            .hybrid()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();
        assert!((factor - 1.0).abs() < 1e-12, "factor: {factor}");

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // Confirm sf exists with a long TTL.
        let pttl: i64 = conn.pttl(redis_key(&prefix, &k, "sf")).await.unwrap();
        assert!(
            pttl > 1_000,
            "sf key must have a long TTL before cleanup, got {pttl}ms"
        );

        // Wait for entity to become stale, then cleanup.
        runtime::async_sleep(Duration::from_millis(stale_after_ms + 50)).await;

        rl2.hybrid()
            .suppressed()
            .cleanup(stale_after_ms)
            .await
            .unwrap();

        // sf must be explicitly deleted by the Lua script — not just waiting for TTL.
        let sf_exists: bool = conn.exists(redis_key(&prefix, &k, "sf")).await.unwrap();
        assert!(
            !sf_exists,
            "sf key must be deleted by cleanup, not left to expire via TTL"
        );
    });
}

/// The Redis `w` key preserves the untruncated hard window limit after a hybrid commit.
#[test]
fn redis_state_hybrid_suppressed_window_limit_key_equals_hard_limit_after_commit() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 6_u64;
        let rate_limit_value = 0.5_f64;
        let hard_limit_factor = 1.5_f64;
        // raw hard window = 6 * 0.5 * 1.5 = 4.5; capacities are soft=3 and hard=4.
        let expected_hard_limit = 4.5_f64;
        let expected_hard_capacity = expected_hard_limit as u64;

        let sync_interval_ms = 25_u64;
        let cache_ms = 5_u64;

        let k = key("k");
        let rate_limit = RateLimit::try_from(rate_limit_value).unwrap();
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

        let reaches_hard = rl
            .hybrid()
            .suppressed()
            .inc(&k, &rate_limit, expected_hard_capacity)
            .await
            .unwrap();
        assert!(
            matches!(reaches_hard, RateLimitDecision::Allowed),
            "reaches_hard: {reaches_hard:?}"
        );
        wait_for_hybrid_sync(sync_interval_ms).await;

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let stored_limit: f64 = conn.get(redis_key(&prefix, &k, "w")).await.unwrap();
        assert!(
            (stored_limit - expected_hard_limit).abs() < 1e-12,
            "stored hard window limit should equal the configured hard window limit"
        );
    });
}

/// A matched `set_if` writes into the hybrid_suppressed keyspace: single bucket, total set,
/// declined counters reset, cached suppression factor dropped, hard window limit defined.
#[test]
fn redis_state_hybrid_suppressed_set_if_writes_state_and_drops_factor() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let rl = build_limiter(&url, 6, 1000, 1.0, 60_000, 25, prefix.clone()).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(10f64).unwrap();

        let (new_total, old_total) = rl
            .hybrid()
            .suppressed()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(40), 40)
            .await
            .unwrap();
        assert_eq!((new_total, old_total), (40, 0));

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        assert_eq!(total, 40);

        let buckets: HashMap<String, u64> =
            conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        assert_eq!(buckets.len(), 1, "exactly one bucket expected: {buckets:?}");
        assert_eq!(buckets.values().sum::<u64>(), 40);

        let declined_total: Option<u64> = conn.get(redis_key(&prefix, &k, "d")).await.unwrap();
        assert!(
            declined_total.is_none(),
            "declined total must be reset, got {declined_total:?}"
        );

        let factor: Option<String> = conn.get(redis_key(&prefix, &k, "sf")).await.unwrap();
        assert!(
            factor.is_none(),
            "cached suppression factor must be dropped, got {factor:?}"
        );

        let hard_window_limit: u64 = conn.get(redis_key(&prefix, &k, "w")).await.unwrap();
        assert_eq!(
            hard_window_limit, 60,
            "hard window limit must be window * rate * hard_limit_factor (6 * 10 * 1)"
        );
    });
}

#[test]
fn redis_state_hybrid_suppressed_accepting_baseline_declines_are_not_overlaid_twice() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let rl = build_limiter(&url, 60, 1_000, 2.0, 60_000, 2_000, prefix.clone()).await;
        let k = key("k");
        let low_rate = RateLimit::try_from(1f64).unwrap();
        let high_rate = RateLimit::try_from(100f64).unwrap();

        let reaches_hard = rl
            .hybrid()
            .suppressed()
            .inc(&k, &low_rate, 120)
            .await
            .unwrap();
        assert!(
            matches!(reaches_hard, RateLimitDecision::Allowed),
            "reaches_hard: {reaches_hard:?}"
        );
        assert!(matches!(
            rl.hybrid()
                .suppressed()
                .inc(&k, &low_rate, 1)
                .await
                .unwrap(),
            RateLimitDecision::Suppressed {
                is_allowed: false,
                suppression_factor: 1f64,
            }
        ));

        // Commit the suppressing state's one declined call while raising the
        // limit enough that the next Redis refresh enters Accepting state.
        assert_eq!(
            rl.hybrid()
                .suppressed()
                .set_if_preserve_history(
                    &k,
                    &high_rate,
                    RateLimitComparator::Eq(121),
                    121,
                    HistoryPreservation::PreserveNewest,
                )
                .await
                .unwrap(),
            (121, 121)
        );
        assert_eq!(
            rl.hybrid()
                .suppressed()
                .get_suppression_factor(&k)
                .await
                .unwrap(),
            0.0
        );
        assert!(matches!(
            rl.hybrid()
                .suppressed()
                .inc(&k, &high_rate, 1)
                .await
                .unwrap(),
            RateLimitDecision::Allowed
        ));

        // The Accepting state's declined count is the committed Redis baseline,
        // not a pending local decline overlay. Preserving the pending accepted call
        // must therefore keep the declined total at one instead of doubling it.
        assert_eq!(
            rl.hybrid()
                .suppressed()
                .set_if_preserve_history(
                    &k,
                    &high_rate,
                    RateLimitComparator::Eq(122),
                    122,
                    HistoryPreservation::PreserveNewest,
                )
                .await
                .unwrap(),
            (122, 122)
        );

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        let declined: u64 = conn.get(redis_key(&prefix, &k, "d")).await.unwrap();
        let bucket_total = conn
            .hgetall::<_, HashMap<String, u64>>(redis_key(&prefix, &k, "h"))
            .await
            .unwrap()
            .values()
            .sum::<u64>();
        let bucket_declined = conn
            .hgetall::<_, HashMap<String, u64>>(redis_key(&prefix, &k, "hd"))
            .await
            .unwrap()
            .values()
            .sum::<u64>();

        assert_eq!(total, 122);
        assert_eq!(bucket_total, total);
        assert_eq!(declined, 1);
        assert_eq!(bucket_declined, declined);
    });
}

#[test]
fn redis_state_hybrid_suppressed_set_if_no_match_is_true_noop() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let rl = build_limiter(&url, 6, 1_000, 1.0, 60_000, 25, prefix.clone()).await;
        let k = key("k");
        let seed_rate = RateLimit::try_from(10f64).unwrap();
        let replacement_rate = RateLimit::try_from(20f64).unwrap();
        let key_generator = key_gen(&prefix, RateType::HybridSuppressed);

        assert_eq!(
            rl.hybrid()
                .suppressed()
                .set_if(&k, &seed_rate, RateLimitComparator::Nil, 17)
                .await
                .unwrap(),
            (17, 0)
        );
        assert_eq!(
            rl.hybrid()
                .suppressed()
                .get_suppression_factor(&k)
                .await
                .unwrap(),
            0.0
        );

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let history_key = redis_key(&prefix, &k, "h");
        let declined_history_key = redis_key(&prefix, &k, "hd");
        let ordering_key = redis_key(&prefix, &k, "a");
        let total_key = redis_key(&prefix, &k, "t");
        let declined_total_key = redis_key(&prefix, &k, "d");
        let limit_key = redis_key(&prefix, &k, "w");
        let factor_key = redis_key(&prefix, &k, "sf");

        let history_before: HashMap<String, u64> = conn.hgetall(&history_key).await.unwrap();
        let declined_history_before: HashMap<String, u64> =
            conn.hgetall(&declined_history_key).await.unwrap();
        let ordering_before: Vec<(String, f64)> =
            conn.zrange_withscores(&ordering_key, 0, -1).await.unwrap();
        let total_before: u64 = conn.get(&total_key).await.unwrap();
        let declined_total_before: Option<u64> = conn.get(&declined_total_key).await.unwrap();
        let limit_before: u64 = conn.get(&limit_key).await.unwrap();
        let factor_before: Option<String> = conn.get(&factor_key).await.unwrap();
        let limit_ttl_before: i64 = conn.pttl(&limit_key).await.unwrap();
        let factor_ttl_before: i64 = conn.pttl(&factor_key).await.unwrap();
        let active_score_before: Option<f64> = conn
            .zscore(key_generator.get_active_entities_key(), k.to_string())
            .await
            .unwrap();
        assert!(factor_before.is_some(), "setup must create a factor cache");

        runtime::async_sleep(Duration::from_millis(50)).await;

        assert_eq!(
            rl.hybrid()
                .suppressed()
                .set_if(&k, &replacement_rate, RateLimitComparator::Gt(1_000), 5,)
                .await
                .unwrap(),
            (17, 17)
        );

        let history_after: HashMap<String, u64> = conn.hgetall(&history_key).await.unwrap();
        let declined_history_after: HashMap<String, u64> =
            conn.hgetall(&declined_history_key).await.unwrap();
        let ordering_after: Vec<(String, f64)> =
            conn.zrange_withscores(&ordering_key, 0, -1).await.unwrap();
        let total_after: u64 = conn.get(&total_key).await.unwrap();
        let declined_total_after: Option<u64> = conn.get(&declined_total_key).await.unwrap();
        let limit_after: u64 = conn.get(&limit_key).await.unwrap();
        let factor_after: Option<String> = conn.get(&factor_key).await.unwrap();
        let limit_ttl_after: i64 = conn.pttl(&limit_key).await.unwrap();
        let factor_ttl_after: i64 = conn.pttl(&factor_key).await.unwrap();
        let active_score_after: Option<f64> = conn
            .zscore(key_generator.get_active_entities_key(), k.to_string())
            .await
            .unwrap();

        assert_eq!(history_after, history_before, "history changed on no-match");
        assert_eq!(
            declined_history_after, declined_history_before,
            "declined history changed on no-match"
        );
        assert_eq!(
            ordering_after, ordering_before,
            "ordering changed on no-match"
        );
        assert_eq!(total_after, total_before, "total changed on no-match");
        assert_eq!(
            declined_total_after, declined_total_before,
            "declined total changed on no-match"
        );
        assert_eq!(limit_after, limit_before, "limit changed on no-match");
        assert_eq!(factor_after, factor_before, "factor changed on no-match");
        assert_eq!(
            active_score_after, active_score_before,
            "active-entity score changed on no-match"
        );
        assert!(
            limit_ttl_after > 0 && limit_ttl_after <= limit_ttl_before - 20,
            "limit TTL was refreshed on no-match: before={limit_ttl_before}, after={limit_ttl_after}"
        );
        assert!(
            factor_ttl_after > 0 && factor_ttl_after <= factor_ttl_before - 20,
            "factor TTL was refreshed on no-match: before={factor_ttl_before}, after={factor_ttl_after}"
        );
    });
}

#[test]
fn redis_state_hybrid_suppressed_preserves_requested_history_edge() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let rl = build_limiter(&url, 60, 1, 1.0, 100, 25, prefix.clone()).await;
        let rate = RateLimit::try_from(100f64).unwrap();

        for (name, preservation, expected_reduced, expected_increased) in [
            (
                "newest",
                HistoryPreservation::PreserveNewest,
                vec![2_u64, 6],
                vec![2_u64, 9],
            ),
            (
                "oldest",
                HistoryPreservation::PreserveOldest,
                vec![4_u64, 4],
                vec![7_u64, 4],
            ),
        ] {
            let k = key(name);
            assert_eq!(
                rl.hybrid()
                    .suppressed()
                    .set_if(&k, &rate, RateLimitComparator::Nil, 4)
                    .await
                    .unwrap(),
                (4, 0)
            );
            runtime::async_sleep(Duration::from_millis(3)).await;
            assert!(matches!(
                rl.hybrid().suppressed().inc(&k, &rate, 5).await.unwrap(),
                RateLimitDecision::Allowed
            ));
            assert_eq!(
                rl.hybrid()
                    .suppressed()
                    .set_if_preserve_history(
                        &k,
                        &rate,
                        RateLimitComparator::Eq(9),
                        9,
                        preservation,
                    )
                    .await
                    .unwrap(),
                (9, 9)
            );
            runtime::async_sleep(Duration::from_millis(3)).await;
            assert!(matches!(
                rl.hybrid().suppressed().inc(&k, &rate, 6).await.unwrap(),
                RateLimitDecision::Allowed
            ));
            assert_eq!(
                rl.hybrid()
                    .suppressed()
                    .set_if_preserve_history(
                        &k,
                        &rate,
                        RateLimitComparator::Eq(15),
                        15,
                        preservation,
                    )
                    .await
                    .unwrap(),
                (15, 15)
            );
            assert_eq!(rl.hybrid().suppressed().get(&k).await.unwrap().total, 15);

            assert_eq!(
                rl.hybrid()
                    .suppressed()
                    .set_if_preserve_history(
                        &k,
                        &rate,
                        RateLimitComparator::Eq(15),
                        8,
                        preservation,
                    )
                    .await
                    .unwrap(),
                (8, 15)
            );

            let mut conn = redis::Client::open(url.as_str())
                .unwrap()
                .get_multiplexed_async_connection()
                .await
                .unwrap();
            let ordering_key = redis_key(&prefix, &k, "a");
            let history_key = redis_key(&prefix, &k, "h");
            let declined_history_key = redis_key(&prefix, &k, "hd");
            let fields: Vec<String> = conn.zrange(&ordering_key, 0, -1).await.unwrap();
            let counts: Vec<u64> = redis::cmd("HMGET")
                .arg(&history_key)
                .arg(&fields)
                .query_async(&mut conn)
                .await
                .unwrap();
            assert_eq!(counts, expected_reduced);
            let declined: Vec<Option<u64>> = redis::cmd("HMGET")
                .arg(&declined_history_key)
                .arg(&fields)
                .query_async(&mut conn)
                .await
                .unwrap();
            assert_eq!(declined, vec![None; fields.len()]);

            assert_eq!(
                rl.hybrid()
                    .suppressed()
                    .set_if_preserve_history(
                        &k,
                        &rate,
                        RateLimitComparator::Eq(8),
                        11,
                        preservation,
                    )
                    .await
                    .unwrap(),
                (11, 8)
            );
            let fields: Vec<String> = conn.zrange(&ordering_key, 0, -1).await.unwrap();
            let counts: Vec<u64> = redis::cmd("HMGET")
                .arg(&history_key)
                .arg(&fields)
                .query_async(&mut conn)
                .await
                .unwrap();
            assert_eq!(counts, expected_increased);
            let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
            let declined_total: Option<u64> = conn.get(redis_key(&prefix, &k, "d")).await.unwrap();
            assert_eq!(total, 11);
            assert_eq!(declined_total.unwrap_or_default(), 0);
        }
    });
}

#[test]
fn redis_state_hybrid_suppressed_zero_target_removes_all_entity_state() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let rl = build_limiter(&url, 6, 1000, 1.0, 100, 25, prefix.clone()).await;
        let rate = RateLimit::try_from(10f64).unwrap();
        let key_generator = key_gen(&prefix, RateType::HybridSuppressed);

        for (name, preservation) in [
            ("replace", None),
            ("preserve", Some(HistoryPreservation::PreserveNewest)),
        ] {
            let k = key(name);
            let missing_result = match preservation {
                Some(preservation) => rl
                    .hybrid()
                    .suppressed()
                    .set_if_preserve_history(&k, &rate, RateLimitComparator::Eq(0), 0, preservation)
                    .await
                    .unwrap(),
                None => rl
                    .hybrid()
                    .suppressed()
                    .set_if(&k, &rate, RateLimitComparator::Eq(0), 0)
                    .await
                    .unwrap(),
            };
            assert_eq!(missing_result, (0, 0));

            assert_eq!(
                rl.hybrid()
                    .suppressed()
                    .set_if(&k, &rate, RateLimitComparator::Nil, 17)
                    .await
                    .unwrap(),
                (17, 0)
            );

            let result = match preservation {
                Some(preservation) => rl
                    .hybrid()
                    .suppressed()
                    .set_if_preserve_history(&k, &rate, RateLimitComparator::Nil, 0, preservation)
                    .await
                    .unwrap(),
                None => rl
                    .hybrid()
                    .suppressed()
                    .set_if(&k, &rate, RateLimitComparator::Nil, 0)
                    .await
                    .unwrap(),
            };
            assert_eq!(result, (0, 17));

            let mut conn = redis::Client::open(url.as_str())
                .unwrap()
                .get_multiplexed_async_connection()
                .await
                .unwrap();
            for entity_key in key_generator.get_all_entity_keys(&k) {
                let exists: bool = conn.exists(&entity_key).await.unwrap();
                assert!(!exists, "unexpected entity key: {entity_key}");
            }
            let score: Option<f64> = conn
                .zscore(key_generator.get_active_entities_key(), k.as_str())
                .await
                .unwrap();
            assert!(score.is_none(), "unexpected active membership for {name}");
        }
    });
}

#[test]
fn redis_state_hybrid_suppressed_get_keeps_unknown_entity_absent() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let rl = build_limiter(&url, 6, 1000, 1.0, 100, 25, prefix.clone()).await;
        let k = key("k");
        let rate = RateLimit::try_from(10f64).unwrap();
        let key_generator = key_gen(&prefix, RateType::HybridSuppressed);

        assert_eq!(rl.hybrid().suppressed().get(&k).await.unwrap().total, 0);
        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        for entity_key in key_generator.get_all_entity_keys(&k) {
            let exists: bool = conn.exists(&entity_key).await.unwrap();
            assert!(!exists, "unknown get created {entity_key}");
        }
        let score: Option<f64> = conn
            .zscore(key_generator.get_active_entities_key(), k.as_str())
            .await
            .unwrap();
        assert!(score.is_none());

        rl.hybrid()
            .suppressed()
            .set_if(&k, &rate, RateLimitComparator::Nil, 3)
            .await
            .unwrap();
        let _: u64 = conn
            .zrem(key_generator.get_active_entities_key(), k.as_str())
            .await
            .unwrap();
        assert_eq!(rl.hybrid().suppressed().get(&k).await.unwrap().total, 3);
        let score: Option<f64> = conn
            .zscore(key_generator.get_active_entities_key(), k.as_str())
            .await
            .unwrap();
        assert!(score.is_some(), "known get must refresh active membership");
    });
}
