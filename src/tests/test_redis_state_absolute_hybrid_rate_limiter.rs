//! Redis state inspection tests for the **absolute hybrid** rate limiter.
//!
//! The hybrid limiter accumulates increments locally and commits to Redis only when the
//! local accept budget is exhausted (overflow commit) or on periodic background flushes.
//! These tests verify that Redis state is correct after commits are flushed.
//!
//! # Redis data model (hybrid_absolute, per user-key `K`, prefix `P`)
//!
//! | Redis key                         | Type        | Meaning                                    |
//! |-----------------------------------|-------------|--------------------------------------------|
//! | `P:K:hybrid_absolute:h`           | Hash        | `timestamp_ms → count` buckets             |
//! | `P:K:hybrid_absolute:a`           | Sorted set  | Active bucket timestamps (scores = ts_ms)  |
//! | `P:K:hybrid_absolute:w`           | String      | Stored window limit                        |
//! | `P:K:hybrid_absolute:t`           | String      | Running total count                        |
//! | `P:hybrid_absolute:active_entities` | Sorted set  | All active user-keys (for cleanup)        |
//!
//! **Important:** because the hybrid limiter batches writes, tests must wait for the
//! background committer to flush (`wait_for_hybrid_sync`) before inspecting Redis state.

use std::{collections::HashMap, time::Duration};

use redis::AsyncCommands;

use super::runtime;
use super::common::{redis_url, unique_prefix, key, key_gen, wait_for_hybrid_sync};

use crate::common::{RateType, SuppressionFactorCacheMs};
use crate::hybrid::SyncIntervalMs;
use crate::{
    HardLimitFactor, LocalRateLimiterOptions, RateGroupSizeMs, RateLimit, RateLimitDecision,
    RateLimiter, RateLimiterOptions, RedisKey, RedisRateLimiterOptions, WindowSizeSeconds,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn build_limiter(
    url: &str,
    window_size_seconds: u64,
    rate_group_size_ms: u64,
    sync_interval_ms: u64,
    prefix: RedisKey,
) -> std::sync::Arc<RateLimiter> {
    let client = redis::Client::open(url).unwrap();
    let cm = client.get_connection_manager().await.unwrap();

    let options = RateLimiterOptions {
        local: LocalRateLimiterOptions {
            window_size_seconds: WindowSizeSeconds::try_from(window_size_seconds).unwrap(),
            rate_group_size_ms: RateGroupSizeMs::try_from(rate_group_size_ms).unwrap(),
            hard_limit_factor: HardLimitFactor::default(),
            suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
        },
        redis: RedisRateLimiterOptions {
            connection_manager: cm,
            prefix: Some(prefix),
            window_size_seconds: WindowSizeSeconds::try_from(window_size_seconds).unwrap(),
            rate_group_size_ms: RateGroupSizeMs::try_from(rate_group_size_ms).unwrap(),
            hard_limit_factor: HardLimitFactor::default(),
            suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
            sync_interval_ms: SyncIntervalMs::try_from(sync_interval_ms).unwrap(),
        },
    };

    std::sync::Arc::new(RateLimiter::new(options))
}

/// Construct the canonical Redis key for a given suffix using the key generator.
fn redis_key(prefix: &RedisKey, user_key: &RedisKey, suffix: &str) -> String {
    let kg = key_gen(prefix, RateType::HybridAbsolute);
    match suffix {
        "h" => kg.get_hash_key(user_key),
        "a" => kg.get_active_keys(user_key),
        "w" => kg.get_window_limit_key(user_key),
        "t" => kg.get_total_count_key(user_key),
        _   => panic!("unknown suffix for hybrid_absolute rate type: {suffix}"),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Before the local accept budget is exhausted, no hybrid_absolute Redis keys should exist
/// because the hybrid limiter has not yet committed any state.
#[test]
fn redis_state_hybrid_absolute_no_redis_keys_before_overflow() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 1_u64;
        let sync_interval_ms = 2000_u64; // very slow tick so no background flush occurs

        let rl = build_limiter(&url, window_size_seconds, 1000, sync_interval_ms, prefix.clone()).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        // capacity = 1 * 5 = 5; fill all 5 slots locally.
        for _ in 0..5 {
            let d = rl.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
            assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");
        }

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // No commit should have happened yet.
        let total: Option<u64> = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        assert!(
            total.is_none(),
            "total count key should not exist before the local budget overflows"
        );
        let hash_len: u64 = conn.hlen(redis_key(&prefix, &k, "h")).await.unwrap();
        assert_eq!(hash_len, 0, "hash should be empty before overflow");
    });
}

/// After the local budget overflows (triggering a commit), the committed count must be
/// visible in Redis once the background committer flushes.
#[test]
fn redis_state_hybrid_absolute_commit_writes_total_count_after_overflow() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 1_u64;
        let sync_interval_ms = 25_u64;

        let rl = build_limiter(&url, window_size_seconds, 1000, sync_interval_ms, prefix.clone()).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let cap = (window_size_seconds as f64 * *rate_limit) as u64; // 5

        // Fill the local budget.
        for _ in 0..cap {
            let d = rl.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
            assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");
        }

        // Trigger overflow — this queues the commit.
        let d_overflow = rl.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(
            matches!(d_overflow, RateLimitDecision::Rejected { .. }),
            "d_overflow: {d_overflow:?}"
        );

        // Wait for the committer to flush.
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
        assert!(
            total <= cap,
            "total count ({total}) should not exceed capacity ({cap})"
        );

        // The hash must have at least one bucket.
        let hash: HashMap<String, u64> = conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        assert!(!hash.is_empty(), "hash must have at least one bucket after commit");

        // The active sorted set must have at least one member.
        let active_count: u64 = conn.zcard(redis_key(&prefix, &k, "a")).await.unwrap();
        assert!(
            active_count > 0,
            "active sorted set must be non-empty after commit"
        );
    });
}

/// After a commit, the window limit key must be set and reflect the correct capacity.
#[test]
fn redis_state_hybrid_absolute_window_limit_key_is_set_after_commit() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 2_u64;
        let sync_interval_ms = 25_u64;

        let rl = build_limiter(&url, window_size_seconds, 1000, sync_interval_ms, prefix.clone()).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(3f64).unwrap();
        // capacity = 2 * 3 = 6
        let expected_window_limit = 6_u64;

        for _ in 0..expected_window_limit {
            let _ = rl.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        }
        // Overflow to trigger commit.
        let _ = rl.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        wait_for_hybrid_sync(sync_interval_ms).await;

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let stored_limit: u64 = conn.get(redis_key(&prefix, &k, "w")).await.unwrap();
        assert_eq!(
            stored_limit, expected_window_limit,
            "stored window limit should equal capacity"
        );
    });
}

/// Two limiters sharing the same prefix should observe each other's committed state.
/// Once limiter A overflows and commits, limiter B must see a non-zero total in Redis.
#[test]
fn redis_state_hybrid_absolute_committed_state_is_visible_to_another_instance() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 1_u64;
        let sync_interval_ms = 25_u64;

        let rl_a = build_limiter(&url, window_size_seconds, 1000, sync_interval_ms, prefix.clone()).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let cap = (window_size_seconds as f64 * *rate_limit) as u64;

        // A fills and overflows.
        for _ in 0..cap {
            let _ = rl_a.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        }
        let _ = rl_a.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        wait_for_hybrid_sync(sync_interval_ms).await;

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        assert!(
            total > 0,
            "total must be visible in Redis after A commits, got {total}"
        );
    });
}

/// The hash bucket sum must always equal the total count key after a commit.
#[test]
fn redis_state_hybrid_absolute_hash_sum_matches_total_count_after_commit() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 1_u64;
        let sync_interval_ms = 25_u64;

        let rl = build_limiter(&url, window_size_seconds, 1000, sync_interval_ms, prefix.clone()).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let cap = (window_size_seconds as f64 * *rate_limit) as u64;

        for _ in 0..cap {
            let _ = rl.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        }
        let _ = rl.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        wait_for_hybrid_sync(sync_interval_ms).await;

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        let hash: HashMap<String, u64> = conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        let hash_sum: u64 = hash.values().sum();

        assert_eq!(
            hash_sum, total,
            "hash sum ({hash_sum}) must equal total count ({total}) after commit"
        );
    });
}

/// After the window expires and a new commit is made, stale buckets must be evicted so
/// the total count reflects only the fresh increment.
#[test]
fn redis_state_hybrid_absolute_evicts_expired_buckets_on_next_commit() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 1_u64;
        let sync_interval_ms = 25_u64;

        let rl = build_limiter(&url, window_size_seconds, 1000, sync_interval_ms, prefix.clone()).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let cap = (window_size_seconds as f64 * *rate_limit) as u64;

        // First burst: fill and overflow to commit.
        for _ in 0..cap {
            let _ = rl.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        }
        let _ = rl.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        wait_for_hybrid_sync(sync_interval_ms).await;

        // Wait for the window to expire.
        runtime::async_sleep(Duration::from_millis(window_size_seconds * 1000 + 100)).await;

        // Second burst: this read_state call in the hybrid limiter triggers Redis eviction.
        let d = rl.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(d, RateLimitDecision::Allowed), "d after expiry: {d:?}");
        // Let the new commit flush if needed.
        wait_for_hybrid_sync(sync_interval_ms).await;

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        assert!(
            total <= cap,
            "total count ({total}) after window expiry must not exceed capacity ({cap})"
        );
    });
}

/// Two separate prefixes must maintain independent Redis namespaces.  Usage committed
/// under prefix A must not appear under prefix B.
#[test]
fn redis_state_hybrid_absolute_different_prefixes_are_isolated() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix_a = unique_prefix();
        let prefix_b = unique_prefix();
        let window_size_seconds = 1_u64;
        let sync_interval_ms = 25_u64;

        let rl_a = build_limiter(&url, window_size_seconds, 1000, sync_interval_ms, prefix_a.clone()).await;
        let rl_b = build_limiter(&url, window_size_seconds, 1000, sync_interval_ms, prefix_b.clone()).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let cap = (window_size_seconds as f64 * *rate_limit) as u64;

        // Overflow A to trigger commit.
        for _ in 0..cap {
            let _ = rl_a.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        }
        let _ = rl_a.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        wait_for_hybrid_sync(sync_interval_ms).await;

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // B's namespace must be empty.
        let total_b: Option<u64> = conn.get(redis_key(&prefix_b, &k, "t")).await.unwrap();
        assert!(
            total_b.is_none(),
            "prefix B should not have a total count after prefix A's commit"
        );

        // Sanity: B can still operate independently.
        let d_b = rl_b.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(d_b, RateLimitDecision::Allowed), "d_b: {d_b:?}");
    });
}

/// The active sorted set scores (bucket timestamps) must be monotonically non-decreasing
/// after multiple overflow-and-commit cycles within the same window.
#[test]
fn redis_state_hybrid_absolute_active_sorted_set_scores_are_ordered() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 10_u64;
        // Small group size so each commit lands in a new bucket.
        let rate_group_size_ms = 100_u64;
        let sync_interval_ms = 50_u64;

        let rl = build_limiter(&url, window_size_seconds, rate_group_size_ms, sync_interval_ms, prefix.clone()).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(10f64).unwrap();

        // Trigger two separate overflow+commit cycles with a gap between them.
        for _ in 0..10 {
            let _ = rl.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        }
        let _ = rl.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        wait_for_hybrid_sync(sync_interval_ms).await;

        // A brief pause ensures the next commit lands in a later ms bucket.
        runtime::async_sleep(Duration::from_millis(200)).await;

        // Reset local state so we can accumulate more.
        let rl2 = build_limiter(&url, window_size_seconds, rate_group_size_ms, sync_interval_ms, prefix.clone()).await;
        for _ in 0..10 {
            let _ = rl2.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        }
        let _ = rl2.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        wait_for_hybrid_sync(sync_interval_ms).await;

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let members_with_scores: Vec<(String, f64)> = conn
            .zrange_withscores(redis_key(&prefix, &k, "a"), 0isize, -1isize)
            .await
            .unwrap();

        assert!(
            !members_with_scores.is_empty(),
            "active sorted set should not be empty after commits"
        );

        let scores: Vec<f64> = members_with_scores.iter().map(|(_, s)| *s).collect();
        for i in 1..scores.len() {
            assert!(
                scores[i] >= scores[i - 1],
                "scores must be non-decreasing: {scores:?}"
            );
        }
    });
}

// ---------------------------------------------------------------------------
// Cleanup tests
// ---------------------------------------------------------------------------

/// After cleanup with a stale threshold the entity has exceeded, all per-entity Redis keys
/// must be deleted and the entity must be removed from the `active_entities` sorted set.
#[test]
fn redis_state_hybrid_absolute_cleanup_removes_all_redis_keys_for_stale_entity() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 5_u64;
        let sync_interval_ms = 25_u64;
        let stale_after_ms = 150_u64;

        let rl = build_limiter(&url, window_size_seconds, 1000, sync_interval_ms, prefix.clone()).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let cap = (window_size_seconds as f64 * *rate_limit) as u64;

        // Overflow to trigger a Redis commit.
        for _ in 0..cap {
            let _ = rl.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        }
        let _ = rl.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        wait_for_hybrid_sync(sync_interval_ms).await;

        let active_entities_key = key_gen(&prefix, RateType::HybridAbsolute).get_active_entities_key();

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let kg = key_gen(&prefix, RateType::HybridAbsolute);

        // Verify all keys exist before cleanup.
        for entity_key in kg.get_all_entity_keys(&k) {
            // Absolute limiter only writes h, a, w, t — others are trivially absent.
            // We check only the ones that must exist.
            if entity_key == kg.get_hash_key(&k)
                || entity_key == kg.get_active_keys(&k)
                || entity_key == kg.get_window_limit_key(&k)
                || entity_key == kg.get_total_count_key(&k)
            {
                let exists: bool = conn.exists(&entity_key).await.unwrap();
                assert!(exists, "key {entity_key} must exist before cleanup");
            }
        }
        let score: Option<f64> = conn.zscore(&active_entities_key, k.as_str()).await.unwrap();
        assert!(score.is_some(), "entity must be in active_entities before cleanup");

        // Wait until the entity is stale.
        runtime::async_sleep(Duration::from_millis(stale_after_ms + 50)).await;

        rl.hybrid().absolute().cleanup(stale_after_ms).await.unwrap();

        // All per-entity keys must be deleted.
        for entity_key in kg.get_all_entity_keys(&k) {
            let exists: bool = conn.exists(&entity_key).await.unwrap();
            assert!(!exists, "key {entity_key} must be deleted after cleanup");
        }

        // Entity must be removed from active_entities.
        let score_after: Option<f64> = conn.zscore(&active_entities_key, k.as_str()).await.unwrap();
        assert!(score_after.is_none(), "entity must be removed from active_entities after cleanup");
    });
}

/// An entity whose last-commit timestamp is within `stale_after_ms` must survive cleanup.
#[test]
fn redis_state_hybrid_absolute_cleanup_does_not_remove_active_entity() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 5_u64;
        let sync_interval_ms = 25_u64;
        let stale_after_ms = 5_000_u64; // very long — entity will not be stale yet

        let rl = build_limiter(&url, window_size_seconds, 1000, sync_interval_ms, prefix.clone()).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let cap = (window_size_seconds as f64 * *rate_limit) as u64;

        // Overflow and sync — entity is recent.
        for _ in 0..cap {
            let _ = rl.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        }
        let _ = rl.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        wait_for_hybrid_sync(sync_interval_ms).await;

        // Immediately cleanup with a long threshold — nothing should be removed.
        rl.hybrid().absolute().cleanup(stale_after_ms).await.unwrap();

        let active_entities_key = key_gen(&prefix, RateType::HybridAbsolute).get_active_entities_key();

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let t_exists: bool = conn.exists(redis_key(&prefix, &k, "t")).await.unwrap();
        assert!(t_exists, "total count key must still exist for active entity");
        let score: Option<f64> = conn.zscore(&active_entities_key, k.as_str()).await.unwrap();
        assert!(score.is_some(), "active entity must remain in active_entities after cleanup");
    });
}

/// After cleanup removes Redis state, a subsequent `inc` for the same key must be allowed
/// (the in-memory state must also be cleared so the limiter starts fresh from Redis).
#[test]
fn redis_state_hybrid_absolute_cleanup_allows_fresh_requests_after_cleanup() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 5_u64;
        let sync_interval_ms = 25_u64;
        let stale_after_ms = 150_u64;

        let rl = build_limiter(&url, window_size_seconds, 1000, sync_interval_ms, prefix.clone()).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(2f64).unwrap();
        let cap = (window_size_seconds as f64 * *rate_limit) as u64; // 10

        // Fill capacity then overflow — entity ends up in Rejecting state.
        for _ in 0..cap {
            let _ = rl.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        }
        let rejected = rl.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(
            matches!(rejected, RateLimitDecision::Rejected { .. }),
            "expected Rejected after overflow, got {rejected:?}"
        );
        wait_for_hybrid_sync(sync_interval_ms).await;

        // Wait until entity is stale, then clean up.
        runtime::async_sleep(Duration::from_millis(stale_after_ms + 50)).await;
        rl.hybrid().absolute().cleanup(stale_after_ms).await.unwrap();

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let t_exists: bool = conn.exists(redis_key(&prefix, &k, "t")).await.unwrap();
        assert!(!t_exists, "total count key must be deleted after cleanup");

        // The next request must be allowed — stale in-memory Rejecting state must be cleared.
        let decision = rl.hybrid().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "expected Allowed after cleanup but got {decision:?}"
        );
    });
}

/// When multiple entities exist under the same prefix, cleanup must only remove stale ones
/// and leave recently-active entities intact.
#[test]
fn redis_state_hybrid_absolute_cleanup_multiple_entities_mixed() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 5_u64;
        let sync_interval_ms = 25_u64;
        let stale_after_ms = 150_u64;

        let rl = build_limiter(&url, window_size_seconds, 1000, sync_interval_ms, prefix.clone()).await;
        let stale = key("stale_user");
        let active = key("active_user");
        let rate_limit = RateLimit::try_from(2f64).unwrap();
        let cap = (window_size_seconds as f64 * *rate_limit) as u64;

        // Overflow stale_user and sync.
        for _ in 0..cap {
            let _ = rl.hybrid().absolute().inc(&stale, &rate_limit, 1).await.unwrap();
        }
        let _ = rl.hybrid().absolute().inc(&stale, &rate_limit, 1).await.unwrap();
        wait_for_hybrid_sync(sync_interval_ms).await;

        // Wait for stale_user to become stale.
        runtime::async_sleep(Duration::from_millis(stale_after_ms + 50)).await;

        // Now overflow active_user — its commit timestamp is recent.
        let rl2 = build_limiter(&url, window_size_seconds, 1000, sync_interval_ms, prefix.clone()).await;
        for _ in 0..cap {
            let _ = rl2.hybrid().absolute().inc(&active, &rate_limit, 1).await.unwrap();
        }
        let _ = rl2.hybrid().absolute().inc(&active, &rate_limit, 1).await.unwrap();
        wait_for_hybrid_sync(sync_interval_ms).await;

        rl2.hybrid().absolute().cleanup(stale_after_ms).await.unwrap();

        let active_entities_key = key_gen(&prefix, RateType::HybridAbsolute).get_active_entities_key();

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // stale_user keys must be gone.
        let stale_t: bool = conn.exists(redis_key(&prefix, &stale, "t")).await.unwrap();
        assert!(!stale_t, "stale_user total count key must be deleted");
        let stale_score: Option<f64> = conn.zscore(&active_entities_key, stale.as_str()).await.unwrap();
        assert!(stale_score.is_none(), "stale_user must be removed from active_entities");

        // active_user keys must still exist.
        let active_t: bool = conn.exists(redis_key(&prefix, &active, "t")).await.unwrap();
        assert!(active_t, "active_user total count key must still exist");
        let active_score: Option<f64> = conn.zscore(&active_entities_key, active.as_str()).await.unwrap();
        assert!(active_score.is_some(), "active_user must remain in active_entities");
    });
}

/// A limiter that has not yet overflowed writes nothing to the hybrid_absolute keyspace,
/// while another limiter using the same prefix (but different rate type, e.g. redis absolute)
/// must not contaminate the hybrid_absolute namespace.
#[test]
fn redis_state_hybrid_absolute_redis_absolute_keys_do_not_contaminate_hybrid_keyspace() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 1_u64;
        let sync_interval_ms = 2000_u64; // slow tick

        let rl = build_limiter(&url, window_size_seconds, 1000, sync_interval_ms, prefix.clone()).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();

        // Use only the redis (non-hybrid) absolute limiter — this writes to `absolute:*` keys.
        for _ in 0..5 {
            let _ = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        }

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // The hybrid_absolute keyspace must still be empty.
        let hybrid_total: Option<u64> = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        assert!(
            hybrid_total.is_none(),
            "hybrid_absolute keyspace must not be contaminated by redis absolute writes"
        );
    });
}
