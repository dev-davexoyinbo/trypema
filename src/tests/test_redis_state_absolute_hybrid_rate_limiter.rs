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

use super::common::{key, key_gen, redis_url, unique_prefix, wait_for_hybrid_sync};
use super::runtime;

use crate::common::{RateType, SuppressionFactorCachePeriod};
use crate::hybrid::SyncInterval;
use crate::{
    BucketSize, HardLimitFactor, HistoryPreservation, LocalRateLimiterOptions, RateLimit,
    RateLimitComparator, RateLimitDecision, RateLimiter, RateLimiterOptions, RedisKey,
    RedisRateLimiterOptions, WindowSize,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn build_limiter(
    url: &str,
    window_size: u64,
    bucket_size: u64,
    sync_interval: u64,
    prefix: RedisKey,
) -> std::sync::Arc<RateLimiter> {
    let client = redis::Client::open(url).unwrap();
    let cm = client.get_connection_manager().await.unwrap();

    let options = RateLimiterOptions {
        local: LocalRateLimiterOptions {
            window_size: WindowSize::seconds(window_size).unwrap(),
            bucket_size: BucketSize::milliseconds(bucket_size).unwrap(),
            hard_limit_factor: HardLimitFactor::default(),
            suppression_factor_cache_period: SuppressionFactorCachePeriod::default(),
        },
        redis: RedisRateLimiterOptions {
            connection_manager: cm,
            prefix: Some(prefix),
            window_size: WindowSize::seconds(window_size).unwrap(),
            bucket_size: BucketSize::milliseconds(bucket_size).unwrap(),
            hard_limit_factor: HardLimitFactor::default(),
            suppression_factor_cache_period: SuppressionFactorCachePeriod::default(),
            sync_interval: SyncInterval::milliseconds(sync_interval).unwrap(),
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
        _ => panic!("unknown suffix for hybrid_absolute rate type: {suffix}"),
    }
}

fn assert_allowed(decision: RateLimitDecision, context: &str) {
    assert!(
        matches!(decision, RateLimitDecision::Allowed),
        "{context}: expected allowed decision, got {decision:?}"
    );
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
        let window_size = 1_u64;
        let sync_interval = 2000_u64; // very slow tick so no background flush occurs

        let rl = build_limiter(&url, window_size, 1000, sync_interval, prefix.clone()).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(5f64).unwrap();
        // capacity = 1 * 5 = 5; fill all 5 slots locally.
        for _ in 0..5 {
            let d = rl
                .hybrid()
                .absolute()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");
        }

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // No commit should have happened yet.
        let key_generator = key_gen(&prefix, RateType::HybridAbsolute);
        for entity_key in key_generator.get_all_entity_keys(&k) {
            let exists: bool = conn.exists(&entity_key).await.unwrap();
            assert!(!exists, "local-only usage created {entity_key}");
        }
        let active_score: Option<f64> = conn
            .zscore(key_generator.get_active_entities_key(), k.as_str())
            .await
            .unwrap();
        assert!(
            active_score.is_none(),
            "local-only usage marked the key active"
        );
    });
}

/// After the local budget overflows (triggering a commit), the committed count must be
/// visible in Redis once the background committer flushes.
#[test]
fn redis_state_hybrid_absolute_commit_writes_total_count_after_overflow() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size = 1_u64;
        let sync_interval = 25_u64;

        let rl = build_limiter(&url, window_size, 1000, sync_interval, prefix.clone()).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(5f64).unwrap();
        let cap = (window_size as f64 * *rate_limit) as u64; // 5

        // Fill the local budget.
        for _ in 0..cap {
            let d = rl
                .hybrid()
                .absolute()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");
        }

        // Trigger overflow — the accepted capacity is committed synchronously.
        let d_overflow = rl
            .hybrid()
            .absolute()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(
            matches!(d_overflow, RateLimitDecision::Rejected { .. }),
            "d_overflow: {d_overflow:?}"
        );

        // Wait for the committer to flush.
        wait_for_hybrid_sync(sync_interval).await;

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        assert_eq!(total, cap, "the rejected overflow must not be committed");

        // The hash must have at least one bucket.
        let hash: HashMap<String, u64> = conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        assert_eq!(hash.values().sum::<u64>(), cap);

        // The active sorted set must have at least one member.
        let active_count: u64 = conn.zcard(redis_key(&prefix, &k, "a")).await.unwrap();
        assert_eq!(active_count, 1, "one grouped commit must create one bucket");
    });
}

/// After a commit, the window limit key must be set and reflect the correct capacity.
#[test]
fn redis_state_hybrid_absolute_window_limit_key_is_set_after_commit() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size = 2_u64;
        let sync_interval = 25_u64;

        let rl = build_limiter(&url, window_size, 1000, sync_interval, prefix.clone()).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(3f64).unwrap();
        // capacity = 2 * 3 = 6
        let expected_window_limit = 6_u64;

        for _ in 0..expected_window_limit {
            assert_allowed(
                rl.hybrid()
                    .absolute()
                    .inc(&k, &rate_limit, 1)
                    .await
                    .unwrap(),
                "window-limit setup increment",
            );
        }
        // Overflow to trigger commit.
        let overflow = rl
            .hybrid()
            .absolute()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(matches!(overflow, RateLimitDecision::Rejected { .. }));
        wait_for_hybrid_sync(sync_interval).await;

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
        let window_size = 1_u64;
        let sync_interval = 25_u64;

        let rl_a = build_limiter(&url, window_size, 1000, sync_interval, prefix.clone()).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(5f64).unwrap();
        let cap = (window_size as f64 * *rate_limit) as u64;

        // A fills and overflows.
        for _ in 0..cap {
            assert_allowed(
                rl_a.hybrid()
                    .absolute()
                    .inc(&k, &rate_limit, 1)
                    .await
                    .unwrap(),
                "shared-prefix setup increment",
            );
        }
        let overflow = rl_a
            .hybrid()
            .absolute()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(matches!(overflow, RateLimitDecision::Rejected { .. }));
        wait_for_hybrid_sync(sync_interval).await;

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        assert_eq!(total, cap, "the complete accepted volume must be visible");
    });
}

/// The hash bucket sum must always equal the total count key after a commit.
#[test]
fn redis_state_hybrid_absolute_hash_sum_matches_total_count_after_commit() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size = 1_u64;
        let sync_interval = 25_u64;

        let rl = build_limiter(&url, window_size, 1000, sync_interval, prefix.clone()).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(5f64).unwrap();
        let cap = (window_size as f64 * *rate_limit) as u64;

        for _ in 0..cap {
            assert_allowed(
                rl.hybrid()
                    .absolute()
                    .inc(&k, &rate_limit, 1)
                    .await
                    .unwrap(),
                "hash-total setup increment",
            );
        }
        let overflow = rl
            .hybrid()
            .absolute()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(matches!(overflow, RateLimitDecision::Rejected { .. }));
        wait_for_hybrid_sync(sync_interval).await;

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        let hash: HashMap<String, u64> = conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        let hash_sum: u64 = hash.values().sum();

        assert_eq!(
            total, cap,
            "the rejected overflow must not change the total"
        );
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
        let window_size = 1_u64;
        let sync_interval = 25_u64;

        let rl = build_limiter(&url, window_size, 1000, sync_interval, prefix.clone()).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(5f64).unwrap();
        let cap = (window_size as f64 * *rate_limit) as u64;

        // First burst: fill and overflow to commit.
        for _ in 0..cap {
            assert_allowed(
                rl.hybrid()
                    .absolute()
                    .inc(&k, &rate_limit, 1)
                    .await
                    .unwrap(),
                "expiry setup increment",
            );
        }
        let overflow = rl
            .hybrid()
            .absolute()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(matches!(overflow, RateLimitDecision::Rejected { .. }));
        wait_for_hybrid_sync(sync_interval).await;

        // Wait for the window to expire.
        runtime::async_sleep(Duration::from_millis(window_size * 1000 + 100)).await;

        // Second burst: this read_state call in the hybrid limiter triggers Redis eviction.
        let d = rl
            .hybrid()
            .absolute()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(
            matches!(d, RateLimitDecision::Allowed),
            "d after expiry: {d:?}"
        );
        // Let the new commit flush if needed.
        wait_for_hybrid_sync(sync_interval).await;

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        assert_eq!(total, 1, "expired usage must be fully evicted");
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
        let window_size = 1_u64;
        let sync_interval = 25_u64;

        let rl_a = build_limiter(&url, window_size, 1000, sync_interval, prefix_a.clone()).await;
        let rl_b = build_limiter(&url, window_size, 1000, sync_interval, prefix_b.clone()).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(5f64).unwrap();
        let cap = (window_size as f64 * *rate_limit) as u64;

        // Overflow A to trigger commit.
        for _ in 0..cap {
            assert_allowed(
                rl_a.hybrid()
                    .absolute()
                    .inc(&k, &rate_limit, 1)
                    .await
                    .unwrap(),
                "prefix A setup increment",
            );
        }
        let overflow = rl_a
            .hybrid()
            .absolute()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(matches!(overflow, RateLimitDecision::Rejected { .. }));
        wait_for_hybrid_sync(sync_interval).await;

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
        let d_b = rl_b
            .hybrid()
            .absolute()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
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
        let window_size = 10_u64;
        // Small group size so each commit lands in a new bucket.
        let bucket_size = 100_u64;
        let sync_interval = 50_u64;

        let rl = build_limiter(
            &url,
            window_size,
            bucket_size,
            sync_interval,
            prefix.clone(),
        )
        .await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(10f64).unwrap();

        // Trigger two separate periodic commit cycles with a gap between them.
        for _ in 0..10 {
            assert_allowed(
                rl.hybrid()
                    .absolute()
                    .inc(&k, &rate_limit, 1)
                    .await
                    .unwrap(),
                "first ordered-bucket setup increment",
            );
        }
        assert_allowed(
            rl.hybrid()
                .absolute()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap(),
            "first ordered-bucket final increment",
        );
        wait_for_hybrid_sync(sync_interval).await;

        // A brief pause ensures the next commit lands in a later ms bucket.
        runtime::async_sleep(Duration::from_millis(200)).await;

        // Reset local state so we can accumulate more.
        let rl2 = build_limiter(
            &url,
            window_size,
            bucket_size,
            sync_interval,
            prefix.clone(),
        )
        .await;
        for _ in 0..10 {
            assert_allowed(
                rl2.hybrid()
                    .absolute()
                    .inc(&k, &rate_limit, 1)
                    .await
                    .unwrap(),
                "second ordered-bucket setup increment",
            );
        }
        assert_allowed(
            rl2.hybrid()
                .absolute()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap(),
            "second ordered-bucket final increment",
        );
        wait_for_hybrid_sync(sync_interval).await;

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let members_with_scores: Vec<(String, f64)> = conn
            .zrange_withscores(redis_key(&prefix, &k, "a"), 0isize, -1isize)
            .await
            .unwrap();

        assert_eq!(
            members_with_scores.len(),
            2,
            "the two separated commits must create two ordered buckets"
        );

        let scores: Vec<f64> = members_with_scores.iter().map(|(_, s)| *s).collect();
        for i in 1..scores.len() {
            assert!(
                scores[i] > scores[i - 1],
                "separated bucket scores must be strictly increasing: {scores:?}"
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
        let window_size = 5_u64;
        let sync_interval = 25_u64;
        let stale_after_ms = 150_u64;

        let rl = build_limiter(&url, window_size, 1000, sync_interval, prefix.clone()).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(5f64).unwrap();
        let cap = (window_size as f64 * *rate_limit) as u64;

        // Overflow to trigger a Redis commit.
        for _ in 0..cap {
            assert_allowed(
                rl.hybrid()
                    .absolute()
                    .inc(&k, &rate_limit, 1)
                    .await
                    .unwrap(),
                "stale cleanup setup increment",
            );
        }
        let overflow = rl
            .hybrid()
            .absolute()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(matches!(overflow, RateLimitDecision::Rejected { .. }));
        wait_for_hybrid_sync(sync_interval).await;

        let active_entities_key =
            key_gen(&prefix, RateType::HybridAbsolute).get_active_entities_key();

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
        assert!(
            score.is_some(),
            "entity must be in active_entities before cleanup"
        );

        // Wait until the entity is stale.
        runtime::async_sleep(Duration::from_millis(stale_after_ms + 50)).await;

        rl.hybrid()
            .absolute()
            .cleanup(stale_after_ms)
            .await
            .unwrap();

        // All per-entity keys must be deleted.
        for entity_key in kg.get_all_entity_keys(&k) {
            let exists: bool = conn.exists(&entity_key).await.unwrap();
            assert!(!exists, "key {entity_key} must be deleted after cleanup");
        }

        // Entity must be removed from active_entities.
        let score_after: Option<f64> = conn.zscore(&active_entities_key, k.as_str()).await.unwrap();
        assert!(
            score_after.is_none(),
            "entity must be removed from active_entities after cleanup"
        );
    });
}

/// An entity whose last-commit timestamp is within `stale_after_ms` must survive cleanup.
#[test]
fn redis_state_hybrid_absolute_cleanup_does_not_remove_active_entity() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size = 5_u64;
        let sync_interval = 25_u64;
        let stale_after_ms = 5_000_u64; // very long — entity will not be stale yet

        let rl = build_limiter(&url, window_size, 1000, sync_interval, prefix.clone()).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(5f64).unwrap();
        let cap = (window_size as f64 * *rate_limit) as u64;

        // Overflow and sync — entity is recent.
        for _ in 0..cap {
            assert_allowed(
                rl.hybrid()
                    .absolute()
                    .inc(&k, &rate_limit, 1)
                    .await
                    .unwrap(),
                "active cleanup setup increment",
            );
        }
        let overflow = rl
            .hybrid()
            .absolute()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(matches!(overflow, RateLimitDecision::Rejected { .. }));
        wait_for_hybrid_sync(sync_interval).await;

        // Immediately cleanup with a long threshold — nothing should be removed.
        rl.hybrid()
            .absolute()
            .cleanup(stale_after_ms)
            .await
            .unwrap();

        let active_entities_key =
            key_gen(&prefix, RateType::HybridAbsolute).get_active_entities_key();

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
fn redis_state_hybrid_absolute_cleanup_allows_fresh_requests_after_cleanup() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size = 1_u64;
        let sync_interval = 25_u64;
        let stale_after_ms = 150_u64;

        let rl = build_limiter(&url, window_size, 1000, sync_interval, prefix.clone()).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(2f64).unwrap();
        let cap = (window_size as f64 * *rate_limit) as u64;

        // Fill capacity then overflow — entity ends up in Rejecting state.
        for _ in 0..cap {
            assert_allowed(
                rl.hybrid()
                    .absolute()
                    .inc(&k, &rate_limit, 1)
                    .await
                    .unwrap(),
                "cleanup reset setup increment",
            );
        }
        let rejected = rl
            .hybrid()
            .absolute()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(
            matches!(rejected, RateLimitDecision::Rejected { .. }),
            "expected Rejected after overflow, got {rejected:?}"
        );
        wait_for_hybrid_sync(sync_interval).await;

        // Wait until the retry TTL and its following stale horizon have elapsed.
        runtime::async_sleep(Duration::from_millis(
            window_size * 1000 + stale_after_ms + 100,
        ))
        .await;
        rl.hybrid()
            .absolute()
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

        // The next request must be allowed after the post-TTL stale horizon has elapsed.
        let decision = rl
            .hybrid()
            .absolute()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
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
        let window_size = 5_u64;
        let sync_interval = 25_u64;
        let stale_after_ms = 150_u64;

        let rl = build_limiter(&url, window_size, 1000, sync_interval, prefix.clone()).await;
        let stale = key("stale_user");
        let active = key("active_user");
        let rate_limit = RateLimit::per_second(2f64).unwrap();
        let cap = (window_size as f64 * *rate_limit) as u64;

        // Overflow stale_user and sync.
        for _ in 0..cap {
            assert_allowed(
                rl.hybrid()
                    .absolute()
                    .inc(&stale, &rate_limit, 1)
                    .await
                    .unwrap(),
                "stale entity setup increment",
            );
        }
        let stale_overflow = rl
            .hybrid()
            .absolute()
            .inc(&stale, &rate_limit, 1)
            .await
            .unwrap();
        assert!(matches!(stale_overflow, RateLimitDecision::Rejected { .. }));
        wait_for_hybrid_sync(sync_interval).await;

        // Wait for stale_user to become stale.
        runtime::async_sleep(Duration::from_millis(stale_after_ms + 50)).await;

        // Now overflow active_user — its commit timestamp is recent.
        let rl2 = build_limiter(&url, window_size, 1000, sync_interval, prefix.clone()).await;
        for _ in 0..cap {
            assert_allowed(
                rl2.hybrid()
                    .absolute()
                    .inc(&active, &rate_limit, 1)
                    .await
                    .unwrap(),
                "active entity setup increment",
            );
        }
        let active_overflow = rl2
            .hybrid()
            .absolute()
            .inc(&active, &rate_limit, 1)
            .await
            .unwrap();
        assert!(matches!(
            active_overflow,
            RateLimitDecision::Rejected { .. }
        ));
        wait_for_hybrid_sync(sync_interval).await;

        rl2.hybrid()
            .absolute()
            .cleanup(stale_after_ms)
            .await
            .unwrap();

        let active_entities_key =
            key_gen(&prefix, RateType::HybridAbsolute).get_active_entities_key();

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // stale_user keys must be gone.
        let stale_t: bool = conn.exists(redis_key(&prefix, &stale, "t")).await.unwrap();
        assert!(!stale_t, "stale_user total count key must be deleted");
        let stale_score: Option<f64> = conn
            .zscore(&active_entities_key, stale.as_str())
            .await
            .unwrap();
        assert!(
            stale_score.is_none(),
            "stale_user must be removed from active_entities"
        );

        // active_user keys must still exist.
        let active_t: bool = conn.exists(redis_key(&prefix, &active, "t")).await.unwrap();
        assert!(active_t, "active_user total count key must still exist");
        let active_score: Option<f64> = conn
            .zscore(&active_entities_key, active.as_str())
            .await
            .unwrap();
        assert!(
            active_score.is_some(),
            "active_user must remain in active_entities"
        );
    });
}

/// A limiter that has not yet overflowed writes nothing to the hybrid_absolute keyspace,
/// while another limiter using the same prefix (but different rate type, e.g. redis absolute)
/// A matched `set_if` replaces the window contents with exactly one bucket holding the
/// written count, sets the running total to that count, and (re)defines the window limit.
#[test]
fn redis_state_hybrid_absolute_set_if_writes_single_bucket_total_and_window_limit() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size = 6_u64;

        let rl = build_limiter(&url, window_size, 1000, 25, prefix.clone()).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(10f64).unwrap();

        let (new_total, old_total) = rl
            .hybrid()
            .absolute()
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
        assert_eq!(total, 40, "running total must equal the written count");

        let window_limit: u64 = conn.get(redis_key(&prefix, &k, "w")).await.unwrap();
        assert_eq!(
            window_limit, 60,
            "window limit must be window_size * rate (6 * 10)"
        );

        let buckets: HashMap<String, u64> =
            conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        assert_eq!(buckets.len(), 1, "exactly one bucket expected: {buckets:?}");
        assert_eq!(
            buckets.values().sum::<u64>(),
            40,
            "the single bucket must hold the written count"
        );

        let ordering_key = redis_key(&prefix, &k, "a");
        let ordering: Vec<String> = conn.zrange(&ordering_key, 0, -1).await.unwrap();
        assert_eq!(ordering.len(), 1, "exactly one active bucket expected");
        assert!(
            buckets.contains_key(&ordering[0]),
            "ordering member must reference the history bucket: {ordering:?} {buckets:?}"
        );

        let limit_ttl_ms: i64 = conn.pttl(redis_key(&prefix, &k, "w")).await.unwrap();
        assert!(
            limit_ttl_ms > 0 && limit_ttl_ms <= window_size as i64 * 1_000,
            "window-limit TTL must be live and bounded by the window: {limit_ttl_ms}"
        );

        let active_score: Option<f64> = conn
            .zscore(
                key_gen(&prefix, RateType::HybridAbsolute).get_active_entities_key(),
                k.as_str(),
            )
            .await
            .unwrap();
        assert!(active_score.is_some(), "entity must be marked active");
    });
}

#[test]
fn redis_state_hybrid_absolute_preserves_requested_history_edge() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let sync_interval = 25_u64;
        let rl = build_limiter(&url, 60, 1, sync_interval, prefix.clone()).await;
        let rate = RateLimit::per_second(100f64).unwrap();

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
                    .absolute()
                    .set_if(&k, &rate, RateLimitComparator::Nil, 4)
                    .await
                    .unwrap(),
                (4, 0)
            );
            runtime::async_sleep(Duration::from_millis(3)).await;
            assert!(matches!(
                rl.hybrid().absolute().inc(&k, &rate, 5).await.unwrap(),
                RateLimitDecision::Allowed
            ));
            assert_eq!(
                rl.hybrid()
                    .absolute()
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
                rl.hybrid().absolute().inc(&k, &rate, 6).await.unwrap(),
                RateLimitDecision::Allowed
            ));
            assert_eq!(
                rl.hybrid()
                    .absolute()
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
            assert_eq!(rl.hybrid().absolute().get(&k).await.unwrap(), 15);

            assert_eq!(
                rl.hybrid()
                    .absolute()
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
            let fields: Vec<String> = conn.zrange(&ordering_key, 0, -1).await.unwrap();
            let counts: Vec<u64> = redis::cmd("HMGET")
                .arg(&history_key)
                .arg(&fields)
                .query_async(&mut conn)
                .await
                .unwrap();
            assert_eq!(counts, expected_reduced);

            assert_eq!(
                rl.hybrid()
                    .absolute()
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
            assert_eq!(total, 11);
        }
    });
}

/// An unmatched `set_if` must leave buckets, totals, and the stored limit untouched.
#[test]
fn redis_state_hybrid_absolute_set_if_no_match_leaves_buckets_and_total_untouched() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size = 6_u64;

        let rl = build_limiter(&url, window_size, 1000, 25, prefix.clone()).await;
        let k = key("k");

        // Seed exact state through set_if itself (single bucket of 17, window limit 6*10=60).
        let rate_seed = RateLimit::per_second(10f64).unwrap();
        assert_eq!(
            rl.hybrid()
                .absolute()
                .set_if(&k, &rate_seed, RateLimitComparator::Nil, 17)
                .await
                .unwrap(),
            (17, 0)
        );

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let key_generator = key_gen(&prefix, RateType::HybridAbsolute);
        let history_key = redis_key(&prefix, &k, "h");
        let ordering_key = redis_key(&prefix, &k, "a");
        let total_key = redis_key(&prefix, &k, "t");
        let limit_key = redis_key(&prefix, &k, "w");
        let history_before: HashMap<String, u64> = conn.hgetall(&history_key).await.unwrap();
        let ordering_before: Vec<(String, f64)> =
            conn.zrange_withscores(&ordering_key, 0, -1).await.unwrap();
        let total_before: u64 = conn.get(&total_key).await.unwrap();
        let limit_before: u64 = conn.get(&limit_key).await.unwrap();
        let limit_ttl_before: i64 = conn.pttl(&limit_key).await.unwrap();
        let active_score_before: Option<f64> = conn
            .zscore(key_generator.get_active_entities_key(), k.to_string())
            .await
            .unwrap();

        runtime::async_sleep(Duration::from_millis(50)).await;

        // Guard cannot match (17 is not > 1000); a different rate is ignored.
        let rate_new = RateLimit::per_second(20f64).unwrap();
        let (new_total, old_total) = rl
            .hybrid()
            .absolute()
            .set_if(&k, &rate_new, RateLimitComparator::Gt(1000), 5)
            .await
            .unwrap();
        assert_eq!((new_total, old_total), (17, 17));

        let history_after: HashMap<String, u64> = conn.hgetall(&history_key).await.unwrap();
        let ordering_after: Vec<(String, f64)> =
            conn.zrange_withscores(&ordering_key, 0, -1).await.unwrap();
        let total_after: u64 = conn.get(&total_key).await.unwrap();
        let limit_after: u64 = conn.get(&limit_key).await.unwrap();
        let limit_ttl_after: i64 = conn.pttl(&limit_key).await.unwrap();
        let active_score_after: Option<f64> = conn
            .zscore(key_generator.get_active_entities_key(), k.to_string())
            .await
            .unwrap();

        assert_eq!(history_after, history_before, "history changed on no-match");
        assert_eq!(
            ordering_after, ordering_before,
            "ordering changed on no-match"
        );
        assert_eq!(total_after, total_before, "total changed on no-match");
        assert_eq!(limit_after, limit_before, "limit changed on no-match");
        assert_eq!(
            active_score_after, active_score_before,
            "active-entity score changed on no-match"
        );
        assert!(
            limit_ttl_after > 0 && limit_ttl_after <= limit_ttl_before - 20,
            "limit TTL was refreshed on no-match: before={limit_ttl_before}, after={limit_ttl_after}"
        );
    });
}

#[test]
fn redis_state_hybrid_absolute_conditional_set_uses_live_history_before_mutating() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size = 2_u64;
        let sync_interval = 25_u64;
        let rl = build_limiter(&url, window_size, 1, sync_interval, prefix.clone()).await;
        let k = key("k");
        let rate = RateLimit::per_second(100f64).unwrap();

        assert_eq!(
            rl.hybrid()
                .absolute()
                .set_if(&k, &rate, RateLimitComparator::Nil, 4)
                .await
                .unwrap(),
            (4, 0)
        );
        runtime::async_sleep(Duration::from_millis(1_000)).await;
        assert_allowed(
            rl.hybrid().absolute().inc(&k, &rate, 6).await.unwrap(),
            "fresh bucket setup increment",
        );
        wait_for_hybrid_sync(sync_interval).await;
        runtime::async_sleep(Duration::from_millis(1_050)).await;

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let key_generator = key_gen(&prefix, RateType::HybridAbsolute);
        let history_key = key_generator.get_hash_key(&k);
        let ordering_key = key_generator.get_active_keys(&k);
        let total_key = key_generator.get_total_count_key(&k);
        let limit_key = key_generator.get_window_limit_key(&k);
        let active_entities_key = key_generator.get_active_entities_key();

        let history_before: HashMap<String, u64> = conn.hgetall(&history_key).await.unwrap();
        let ordering_before: Vec<(String, f64)> =
            conn.zrange_withscores(&ordering_key, 0, -1).await.unwrap();
        let total_before: u64 = conn.get(&total_key).await.unwrap();
        let limit_before: u64 = conn.get(&limit_key).await.unwrap();
        let limit_ttl_before: i64 = conn.pttl(&limit_key).await.unwrap();
        let active_score_before: Option<f64> =
            conn.zscore(&active_entities_key, k.as_str()).await.unwrap();
        assert_eq!(history_before.len(), 2, "setup must create two buckets");
        assert_eq!(ordering_before.len(), 2, "setup must order two buckets");
        assert_eq!(
            total_before, 10,
            "stored total still includes expired history"
        );

        assert_eq!(
            rl.hybrid()
                .absolute()
                .set_if(&k, &rate, RateLimitComparator::Gt(100), 5)
                .await
                .unwrap(),
            (6, 6),
            "the comparator must see only the fresh six-unit bucket"
        );

        let history_after_miss: HashMap<String, u64> = conn.hgetall(&history_key).await.unwrap();
        let ordering_after_miss: Vec<(String, f64)> =
            conn.zrange_withscores(&ordering_key, 0, -1).await.unwrap();
        let total_after_miss: u64 = conn.get(&total_key).await.unwrap();
        let limit_after_miss: u64 = conn.get(&limit_key).await.unwrap();
        let limit_ttl_after_miss: i64 = conn.pttl(&limit_key).await.unwrap();
        let active_score_after_miss: Option<f64> =
            conn.zscore(&active_entities_key, k.as_str()).await.unwrap();
        assert_eq!(history_after_miss, history_before);
        assert_eq!(ordering_after_miss, ordering_before);
        assert_eq!(total_after_miss, total_before);
        assert_eq!(limit_after_miss, limit_before);
        assert_eq!(active_score_after_miss, active_score_before);
        assert!(
            limit_ttl_after_miss > 0 && limit_ttl_after_miss <= limit_ttl_before,
            "a guard miss must not refresh the limit TTL: before={limit_ttl_before}, after={limit_ttl_after_miss}"
        );

        assert_eq!(
            rl.hybrid()
                .absolute()
                .set_if_preserve_history(
                    &k,
                    &rate,
                    RateLimitComparator::Eq(6),
                    4,
                    HistoryPreservation::PreserveNewest,
                )
                .await
                .unwrap(),
            (4, 6)
        );

        let history_after_match: HashMap<String, u64> = conn.hgetall(&history_key).await.unwrap();
        let ordering_after_match: Vec<String> = conn.zrange(&ordering_key, 0, -1).await.unwrap();
        let total_after_match: u64 = conn.get(&total_key).await.unwrap();
        assert_eq!(history_after_match.len(), 1);
        assert_eq!(ordering_after_match.len(), 1);
        assert_eq!(history_after_match.get(&ordering_after_match[0]), Some(&4));
        assert_eq!(total_after_match, 4);
    });
}

/// must not contaminate the hybrid_absolute namespace.
#[test]
fn redis_state_hybrid_absolute_redis_absolute_keys_do_not_contaminate_hybrid_keyspace() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size = 1_u64;
        let sync_interval = 2000_u64; // slow tick

        let rl = build_limiter(&url, window_size, 1000, sync_interval, prefix.clone()).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(5f64).unwrap();

        // Use only the redis (non-hybrid) absolute limiter — this writes to `absolute:*` keys.
        for _ in 0..5 {
            assert_allowed(
                rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap(),
                "pure Redis namespace setup increment",
            );
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

#[test]
fn redis_state_hybrid_absolute_zero_target_removes_all_entity_state() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let rl = build_limiter(&url, 6, 1000, 25, prefix.clone()).await;
        let rate = RateLimit::per_second(10f64).unwrap();
        let key_generator = key_gen(&prefix, RateType::HybridAbsolute);

        for (name, preservation) in [
            ("replace", None),
            ("preserve", Some(HistoryPreservation::PreserveNewest)),
        ] {
            let k = key(name);
            let missing_result = match preservation {
                Some(preservation) => rl
                    .hybrid()
                    .absolute()
                    .set_if_preserve_history(&k, &rate, RateLimitComparator::Eq(0), 0, preservation)
                    .await
                    .unwrap(),
                None => rl
                    .hybrid()
                    .absolute()
                    .set_if(&k, &rate, RateLimitComparator::Eq(0), 0)
                    .await
                    .unwrap(),
            };
            assert_eq!(missing_result, (0, 0));

            let mut conn = redis::Client::open(url.as_str())
                .unwrap()
                .get_multiplexed_async_connection()
                .await
                .unwrap();
            for entity_key in key_generator.get_all_entity_keys(&k) {
                let exists: bool = conn.exists(&entity_key).await.unwrap();
                assert!(
                    !exists,
                    "missing zero target unexpectedly created {entity_key}"
                );
            }
            let score: Option<f64> = conn
                .zscore(key_generator.get_active_entities_key(), k.as_str())
                .await
                .unwrap();
            assert!(score.is_none(), "missing zero target marked {name} active");

            assert_eq!(
                rl.hybrid()
                    .absolute()
                    .set_if(&k, &rate, RateLimitComparator::Nil, 17)
                    .await
                    .unwrap(),
                (17, 0)
            );

            let result = match preservation {
                Some(preservation) => rl
                    .hybrid()
                    .absolute()
                    .set_if_preserve_history(&k, &rate, RateLimitComparator::Nil, 0, preservation)
                    .await
                    .unwrap(),
                None => rl
                    .hybrid()
                    .absolute()
                    .set_if(&k, &rate, RateLimitComparator::Nil, 0)
                    .await
                    .unwrap(),
            };
            assert_eq!(result, (0, 17));

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
fn redis_state_hybrid_absolute_get_keeps_unknown_entity_absent() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let rl = build_limiter(&url, 6, 1000, 25, prefix.clone()).await;
        let k = key("k");
        let rate = RateLimit::per_second(10f64).unwrap();
        let key_generator = key_gen(&prefix, RateType::HybridAbsolute);

        assert_eq!(rl.hybrid().absolute().get_inferred(&k).await.unwrap(), 0);
        assert_eq!(rl.hybrid().absolute().get(&k).await.unwrap(), 0);
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
            .absolute()
            .set_if(&k, &rate, RateLimitComparator::Nil, 3)
            .await
            .unwrap();
        let _: u64 = conn
            .zrem(key_generator.get_active_entities_key(), k.as_str())
            .await
            .unwrap();

        assert_eq!(rl.hybrid().absolute().get_inferred(&k).await.unwrap(), 3);
        let score: Option<f64> = conn
            .zscore(key_generator.get_active_entities_key(), k.as_str())
            .await
            .unwrap();
        assert!(
            score.is_some(),
            "state refresh must restore active membership"
        );

        let _: u64 = conn
            .zrem(key_generator.get_active_entities_key(), k.as_str())
            .await
            .unwrap();
        assert_eq!(rl.hybrid().absolute().get_inferred(&k).await.unwrap(), 3);
        let score: Option<f64> = conn
            .zscore(key_generator.get_active_entities_key(), k.as_str())
            .await
            .unwrap();
        assert!(
            score.is_none(),
            "local inference fast path must not read or mutate Redis"
        );

        assert_eq!(rl.hybrid().absolute().get(&k).await.unwrap(), 3);
        let score: Option<f64> = conn
            .zscore(key_generator.get_active_entities_key(), k.as_str())
            .await
            .unwrap();
        assert!(score.is_some(), "known get must refresh active membership");
    });
}
