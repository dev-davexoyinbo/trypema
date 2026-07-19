//! Redis state inspection tests for the **absolute Redis** rate limiter.
//!
//! These tests verify that the Lua scripts maintain the correct Redis data model after
//! each operation.  Every test connects directly to Redis (using the same `REDIS_URL`
//! env var required by the integration tests) and reads the raw keys written by the
//! limiter so that internal state is observable independently of the public API.
//!
//! # Redis data model (absolute, per user-key `K`, prefix `P`)
//!
//! | Redis key                    | Type        | Meaning                                       |
//! |------------------------------|-------------|-----------------------------------------------|
//! | `P:K:absolute:h`             | Hash        | `timestamp_ms → count` buckets                |
//! | `P:K:absolute:a`             | Sorted set  | Active bucket timestamps (scores = ts_ms)     |
//! | `P:K:absolute:w`             | String      | Stored window limit                           |
//! | `P:K:absolute:t`             | String      | Running total count                           |
//! | `P:absolute:active_entities` | Sorted set  | All active user-keys (for cleanup)            |

use std::{collections::HashMap, thread, time::Duration};

use redis::AsyncCommands;

use super::common::{key, key_gen, redis_url, unique_prefix};
use super::runtime;

use crate::common::{RateType, SuppressionFactorCacheMs};
use crate::hybrid::SyncIntervalMs;
use crate::{
    HardLimitFactor, HistoryPreservation, LocalRateLimiterOptions, RateGroupSizeMs, RateLimit,
    RateLimitComparator, RateLimitDecision, RateLimiter, RateLimiterOptions, RedisKey,
    RedisRateLimiterOptions, WindowSizeSeconds,
};

/// Build a rate limiter and also return its unique prefix so tests can construct Redis keys.
async fn build_limiter(
    url: &str,
    window_size_seconds: u64,
    rate_group_size_ms: u64,
) -> (std::sync::Arc<RateLimiter>, RedisKey) {
    let client = redis::Client::open(url).unwrap();
    let cm = client.get_connection_manager().await.unwrap();
    let prefix = unique_prefix();

    let options = RateLimiterOptions {
        local: LocalRateLimiterOptions {
            window_size_seconds: WindowSizeSeconds::try_from(window_size_seconds).unwrap(),
            rate_group_size_ms: RateGroupSizeMs::try_from(rate_group_size_ms).unwrap(),
            hard_limit_factor: HardLimitFactor::default(),
            suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
        },
        redis: RedisRateLimiterOptions {
            connection_manager: cm,
            prefix: Some(prefix.clone()),
            window_size_seconds: WindowSizeSeconds::try_from(window_size_seconds).unwrap(),
            rate_group_size_ms: RateGroupSizeMs::try_from(rate_group_size_ms).unwrap(),
            hard_limit_factor: HardLimitFactor::default(),
            suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
            sync_interval_ms: SyncIntervalMs::default(),
        },
    };

    (std::sync::Arc::new(RateLimiter::new(options)), prefix)
}

/// Construct the canonical Redis key for a given suffix using the key generator.
fn redis_key(prefix: &RedisKey, user_key: &RedisKey, suffix: &str) -> String {
    let kg = key_gen(prefix, RateType::Absolute);
    match suffix {
        "h" => kg.get_hash_key(user_key),
        "a" => kg.get_active_keys(user_key),
        "w" => kg.get_window_limit_key(user_key),
        "t" => kg.get_total_count_key(user_key),
        _ => panic!("unknown suffix for absolute rate type: {suffix}"),
    }
}

fn active_entities_key(prefix: &RedisKey) -> String {
    key_gen(prefix, RateType::Absolute).get_active_entities_key()
}

fn assert_allowed(decision: RateLimitDecision, context: &str) {
    assert!(
        matches!(&decision, RateLimitDecision::Allowed),
        "{context}: {decision:?}"
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// After one allowed increment the hash contains exactly one bucket with the correct count,
/// the active sorted set has exactly one member, the window limit key is set, and the total
/// count key equals the increment value.
#[test]
fn redis_state_after_single_allowed_inc() {
    let url = redis_url();

    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 10, 1000).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();

        let d = rl.redis().absolute().inc(&k, &rate_limit, 3).await.unwrap();
        assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // Total count must equal the incremented value.
        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        assert_eq!(total, 3, "total count should be 3");

        // Window limit must equal window_size * rate_limit = 10 * 5 = 50.
        let window_limit: u64 = conn.get(redis_key(&prefix, &k, "w")).await.unwrap();
        assert_eq!(window_limit, 50, "window limit should be 50");

        // The hash must contain exactly one bucket whose value equals the increment.
        let hash: HashMap<String, u64> = conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        assert_eq!(hash.len(), 1, "hash should have exactly one bucket");
        let bucket_count: u64 = *hash.values().next().unwrap();
        assert_eq!(bucket_count, 3, "bucket count should be 3");

        // The sorted set must contain exactly one member.
        let active_count: u64 = conn.zcard(redis_key(&prefix, &k, "a")).await.unwrap();
        assert_eq!(active_count, 1, "active sorted set should have one member");

        // The global active_entities sorted set must include the user key.
        let entity_score: Option<f64> = conn
            .zscore(active_entities_key(&prefix), &**k)
            .await
            .unwrap();
        assert!(entity_score.is_some(), "key should be in active_entities");
    });
}

/// The first accepted increment stores the sticky window limit. Later `inc` calls must use and
/// retain that value even when their supplied rate differs.
#[test]
fn redis_state_inc_retains_the_first_window_limit() {
    let url = redis_url();

    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 1, 1000).await;
        let k = key("k");
        let initial_rate = RateLimit::try_from(2.5f64).unwrap();
        let later_rate = RateLimit::try_from(100f64).unwrap();

        assert_allowed(
            rl.redis()
                .absolute()
                .inc(&k, &initial_rate, 2)
                .await
                .unwrap(),
            "filling the truncated initial capacity",
        );
        let decision = rl.redis().absolute().inc(&k, &later_rate, 1).await.unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Rejected { .. }),
            "the later rate must not replace the sticky limit: {decision:?}"
        );

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let stored_limit: f64 = conn.get(redis_key(&prefix, &k, "w")).await.unwrap();
        assert_eq!(
            stored_limit, 2.5,
            "the original fractional limit must be retained"
        );
        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        assert_eq!(total, 2);
    });
}

/// Multiple increments within the same rate-group window are coalesced into a single bucket.
/// The hash must still contain exactly one entry whose value is the sum of both increments.
#[test]
fn redis_state_coalesces_increments_within_rate_group() {
    let url = redis_url();

    runtime::block_on(async {
        // Large rate_group_size_ms so both increments land in the same group.
        let (rl, prefix) = build_limiter(&url, 10, 2000).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(10f64).unwrap();

        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 2).await.unwrap(),
            "creating the grouped bucket",
        );
        // Sleep well within the 2-second group window.
        thread::sleep(Duration::from_millis(50));
        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 3).await.unwrap(),
            "coalescing into the grouped bucket",
        );

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // Total count must be 5.
        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        assert_eq!(total, 5, "total count should be 5");

        // Hash must have exactly one bucket (coalesced) with count = 5.
        let hash: HashMap<String, u64> = conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        assert_eq!(hash.len(), 1, "coalesced — hash should have one bucket");
        let bucket_count: u64 = *hash.values().next().unwrap();
        assert_eq!(bucket_count, 5, "coalesced bucket should hold 5");

        // Active sorted set also has one member.
        let active_count: u64 = conn.zcard(redis_key(&prefix, &k, "a")).await.unwrap();
        assert_eq!(active_count, 1, "active sorted set should have one member");
    });
}

/// Increments separated by more than `rate_group_size_ms` create distinct buckets.
/// Both the hash and the active sorted set must reflect two separate entries.
#[test]
fn redis_state_creates_distinct_buckets_across_rate_groups() {
    let url = redis_url();

    runtime::block_on(async {
        // Small rate_group_size_ms so a 200ms sleep guarantees a new bucket.
        let (rl, prefix) = build_limiter(&url, 10, 100).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(10f64).unwrap();

        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap(),
            "creating the oldest bucket",
        );
        thread::sleep(Duration::from_millis(150));
        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 2).await.unwrap(),
            "creating the newest bucket",
        );

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // Total count = 3.
        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        assert_eq!(total, 3);

        // Two separate buckets in the hash.
        let hash: HashMap<String, u64> = conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        assert_eq!(hash.len(), 2, "two buckets should exist: {hash:?}");

        // Two entries in the active sorted set.
        let active_count: u64 = conn.zcard(redis_key(&prefix, &k, "a")).await.unwrap();
        assert_eq!(active_count, 2, "active sorted set should have two members");
    });
}

/// A rejected increment must not alter usage history, ordering, totals, or the sticky limit.
#[test]
fn redis_state_rejected_inc_does_not_mutate_usage_or_limit_state() {
    let url = redis_url();

    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 1, 1000).await;
        let k = key("k");
        // capacity = 1s * 2/s = 2
        let rate_limit = RateLimit::try_from(2f64).unwrap();

        // Fill capacity.
        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 2).await.unwrap(),
            "filling the window",
        );

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let total_before: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        let hash_before: HashMap<String, u64> =
            conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        let ordering_before: Vec<(String, f64)> = conn
            .zrange_withscores(redis_key(&prefix, &k, "a"), 0, -1)
            .await
            .unwrap();
        let limit_before: f64 = conn.get(redis_key(&prefix, &k, "w")).await.unwrap();

        // Attempt a rejected increment.
        let d = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(d, RateLimitDecision::Rejected { .. }), "d: {d:?}");

        let total_after: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        let hash_after: HashMap<String, u64> =
            conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        let ordering_after: Vec<(String, f64)> = conn
            .zrange_withscores(redis_key(&prefix, &k, "a"), 0, -1)
            .await
            .unwrap();
        let limit_after: f64 = conn.get(redis_key(&prefix, &k, "w")).await.unwrap();

        assert_eq!(
            total_before, total_after,
            "total count must not change on rejection"
        );
        assert_eq!(hash_before, hash_after, "hash must not change on rejection");
        assert_eq!(
            ordering_before, ordering_after,
            "bucket ordering must not change on rejection"
        );
        assert_eq!(
            limit_before, limit_after,
            "sticky limit changed on rejection"
        );
    });
}

/// After the window expires, `inc` must evict the stale buckets from both the hash and the
/// active sorted set, and reduce the total count accordingly so a new burst is admitted.
#[test]
fn redis_state_evicts_expired_buckets_after_window() {
    let url = redis_url();

    runtime::block_on(async {
        // 1-second window; small group so sleep creates a separate bucket.
        let (rl, prefix) = build_limiter(&url, 1, 200).await;
        let k = key("k");
        // capacity = 1s * 3/s = 3
        let rate_limit = RateLimit::try_from(3f64).unwrap();

        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 3).await.unwrap(),
            "filling the initial window",
        );

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // Confirm we are at capacity.
        let total_before: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        assert_eq!(total_before, 3, "at capacity before expiry");

        // Wait past the 1-second window.
        thread::sleep(Duration::from_millis(1100));

        // A new increment triggers eviction inside the Lua script.
        let d = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");

        // The evicted buckets must have been removed from both hash and sorted set,
        // and the total count must reflect only the new increment.
        let hash: HashMap<String, u64> = conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        let total_after: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        let active_count: u64 = conn.zcard(redis_key(&prefix, &k, "a")).await.unwrap();

        assert_eq!(
            total_after, 1,
            "total count must reflect only the new increment after eviction"
        );
        assert_eq!(
            hash.len(),
            1,
            "hash must contain only the new bucket after eviction"
        );
        assert_eq!(
            active_count, 1,
            "active sorted set must contain only the new bucket after eviction"
        );
    });
}

/// `is_allowed` on a saturated key evicts only expired buckets and decrements the exact total.
#[test]
fn redis_state_is_allowed_evicts_expired_buckets() {
    let url = redis_url();

    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 1, 100).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(2f64).unwrap();

        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap(),
            "creating the oldest bucket",
        );
        runtime::async_sleep(Duration::from_millis(500)).await;
        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap(),
            "creating the fresh bucket and refreshing the limit TTL",
        );

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let total_before: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        assert_eq!(total_before, 2);
        let buckets_before: HashMap<String, u64> =
            conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        assert_eq!(buckets_before.len(), 2);

        // Only the first bucket expires. The second increment keeps the sticky-limit key alive,
        // so this call must execute the eviction path rather than return early for an unknown key.
        runtime::async_sleep(Duration::from_millis(550)).await;
        let decision = rl.redis().absolute().is_allowed(&k).await.unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "evicting the oldest bucket should move the key below capacity: {decision:?}"
        );

        let total_after: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        assert_eq!(total_after, 1, "only the fresh bucket should remain");
        let buckets_after: HashMap<String, u64> =
            conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        assert_eq!(buckets_after.len(), 1);
        assert_eq!(buckets_after.values().sum::<u64>(), 1);
        let ordering_after: Vec<String> = conn
            .zrange(redis_key(&prefix, &k, "a"), 0, -1)
            .await
            .unwrap();
        assert_eq!(ordering_after.len(), 1);
        assert!(buckets_after.contains_key(&ordering_after[0]));
    });
}

/// Two different user-keys under the same prefix must maintain completely independent
/// Redis state: separate hashes, sorted sets, total counts, and window limits.
#[test]
fn redis_state_per_key_state_is_independent() {
    let url = redis_url();

    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 10, 1000).await;
        let a = key("a");
        let b = key("b");
        let rate_limit = RateLimit::try_from(5f64).unwrap();

        assert_allowed(
            rl.redis().absolute().inc(&a, &rate_limit, 3).await.unwrap(),
            "seeding key a",
        );
        assert_allowed(
            rl.redis().absolute().inc(&b, &rate_limit, 7).await.unwrap(),
            "seeding key b",
        );

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let total_a: u64 = conn.get(redis_key(&prefix, &a, "t")).await.unwrap();
        let total_b: u64 = conn.get(redis_key(&prefix, &b, "t")).await.unwrap();

        assert_eq!(total_a, 3, "total for key a should be 3");
        assert_eq!(total_b, 7, "total for key b should be 7");

        // Hashes must be independent.
        let hash_a: HashMap<String, u64> = conn.hgetall(redis_key(&prefix, &a, "h")).await.unwrap();
        let hash_b: HashMap<String, u64> = conn.hgetall(redis_key(&prefix, &b, "h")).await.unwrap();

        let sum_a: u64 = hash_a.values().sum();
        let sum_b: u64 = hash_b.values().sum();

        assert_eq!(sum_a, 3, "hash sum for key a should be 3");
        assert_eq!(sum_b, 7, "hash sum for key b should be 7");
    });
}

/// The hash bucket values sum must always equal the total count key.  This invariant
/// must hold across multiple increments within the same window.
#[test]
fn redis_state_hash_sum_matches_total_count() {
    let url = redis_url();

    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 10, 100).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(100f64).unwrap();

        // Multiple increments across multiple buckets.
        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 5).await.unwrap(),
            "creating the oldest bucket",
        );
        thread::sleep(Duration::from_millis(150));
        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 3).await.unwrap(),
            "creating the middle bucket",
        );
        thread::sleep(Duration::from_millis(150));
        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 7).await.unwrap(),
            "creating the newest bucket",
        );

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
            "hash sum ({hash_sum}) must equal total count ({total})"
        );
        assert_eq!(total, 15, "total count should be 15");
    });
}

/// The active sorted set scores must be monotonically non-decreasing (earlier buckets have
/// lower or equal scores than later ones).
#[test]
fn redis_state_active_sorted_set_scores_are_ordered() {
    let url = redis_url();

    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 10, 100).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(100f64).unwrap();

        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap(),
            "creating the oldest bucket",
        );
        thread::sleep(Duration::from_millis(150));
        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap(),
            "creating the middle bucket",
        );
        thread::sleep(Duration::from_millis(150));
        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap(),
            "creating the newest bucket",
        );

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // ZRANGE returns members in ascending score order.
        let members_with_scores: Vec<(String, f64)> = conn
            .zrange_withscores(redis_key(&prefix, &k, "a"), 0isize, -1isize)
            .await
            .unwrap();

        assert_eq!(members_with_scores.len(), 3, "should have 3 buckets");

        let scores: Vec<f64> = members_with_scores.iter().map(|(_, s)| *s).collect();
        for i in 1..scores.len() {
            assert!(
                scores[i] >= scores[i - 1],
                "scores must be non-decreasing: {scores:?}"
            );
        }
    });
}

/// The `active_entities` sorted set registers a key after `inc` accepts usage.
#[test]
fn redis_state_active_entities_registers_allowed_inc() {
    let url = redis_url();

    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 10, 1000).await;
        let k = key("myentity");
        let rate_limit = RateLimit::try_from(10f64).unwrap();

        let ae_key = active_entities_key(&prefix);

        // Not present before any call.
        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let score_before: Option<f64> = conn.zscore(&ae_key, &**k).await.unwrap();
        assert!(
            score_before.is_none(),
            "key should not be in active_entities before inc"
        );

        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap(),
            "registering the active entity",
        );

        let score_after: Option<f64> = conn.zscore(&ae_key, &**k).await.unwrap();
        assert!(
            score_after.is_some(),
            "key should be in active_entities after inc"
        );
    });
}

/// The window limit key must persist with a TTL roughly equal to `window_size_seconds`.
/// This test confirms the key exists and has a positive TTL (meaning EXPIRE was called).
#[test]
fn redis_state_window_limit_key_has_ttl() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size_seconds = 5_u64;
        let (rl, prefix) = build_limiter(&url, window_size_seconds, 1000).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(10f64).unwrap();

        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap(),
            "creating state with a window-limit TTL",
        );

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let ttl: i64 = conn.ttl(redis_key(&prefix, &k, "w")).await.unwrap();

        assert!(
            ttl > 0,
            "window limit key should have a positive TTL (EXPIRE was called), got {ttl}"
        );
        assert!(
            ttl <= window_size_seconds as i64,
            "TTL should be <= window_size_seconds={window_size_seconds}, got {ttl}"
        );
    });
}

/// A matched `set_if` replaces the window contents with exactly one bucket holding the
/// written count, sets the running total to that count, and (re)defines the window limit —
/// all in the `absolute` keyspace.
#[test]
fn redis_state_absolute_set_if_writes_single_bucket_total_and_window_limit() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size_seconds = 6_u64;
        let (rl, prefix) = build_limiter(&url, window_size_seconds, 100).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(10f64).unwrap();

        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 3).await.unwrap(),
            "creating the oldest bucket",
        );
        runtime::async_sleep(Duration::from_millis(150)).await;
        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 4).await.unwrap(),
            "creating the newest bucket",
        );

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let buckets_before: HashMap<String, u64> =
            conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        assert_eq!(buckets_before.len(), 2, "setup must create two buckets");

        let (new_total, old_total) = rl
            .redis()
            .absolute()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(40), 40)
            .await
            .unwrap();
        assert_eq!((new_total, old_total), (40, 7));

        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        assert_eq!(total, 40, "running total must equal the written count");

        let window_limit: u64 = conn.get(redis_key(&prefix, &k, "w")).await.unwrap();
        assert_eq!(
            window_limit, 60,
            "window limit must be window_size_seconds * rate (6 * 10)"
        );

        let ttl: i64 = conn.ttl(redis_key(&prefix, &k, "w")).await.unwrap();
        assert!(ttl > 0, "window limit key must have a TTL, got {ttl}");

        let buckets: HashMap<String, u64> =
            conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        assert_eq!(buckets.len(), 1, "exactly one bucket expected: {buckets:?}");
        assert_eq!(buckets.values().sum::<u64>(), 40);

        let ordering: Vec<String> = conn
            .zrange(redis_key(&prefix, &k, "a"), 0, -1)
            .await
            .unwrap();
        assert_eq!(ordering.len(), 1, "exactly one active bucket expected");
        assert!(
            buckets.contains_key(&ordering[0]),
            "the ordered bucket must exist in history"
        );
    });
}

/// An unmatched `set_if` must leave all state untouched.
#[test]
fn redis_state_absolute_set_if_no_match_leaves_buckets_and_total_untouched() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size_seconds = 6_u64;
        let (rl, prefix) = build_limiter(&url, window_size_seconds, 1000).await;
        let k = key("k");

        // Seed exact state through set_if itself (single bucket of 17, window limit 6*10=60).
        let rate_seed = RateLimit::try_from(10f64).unwrap();
        assert_eq!(
            rl.redis()
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
            .zscore(active_entities_key(&prefix), k.to_string())
            .await
            .unwrap();

        runtime::async_sleep(Duration::from_millis(50)).await;

        // Guard cannot match (17 is not > 1000); the different rate must be ignored.
        let rate_new = RateLimit::try_from(20f64).unwrap();
        let (new_total, old_total) = rl
            .redis()
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
            .zscore(active_entities_key(&prefix), k.to_string())
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

/// Conditional sets compare against the live total. A miss must leave expired history untouched;
/// a match must prune it before applying a history-preserving update.
#[test]
fn redis_state_absolute_set_if_handles_expired_history_only_after_a_match() {
    let url = redis_url();

    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 2, 100).await;
        let rate = RateLimit::try_from(10f64).unwrap();
        let miss_key = key("miss");
        let match_key = key("match");

        for k in [&miss_key, &match_key] {
            assert_allowed(
                rl.redis().absolute().inc(k, &rate, 4).await.unwrap(),
                "creating an oldest bucket",
            );
        }
        runtime::async_sleep(Duration::from_millis(900)).await;
        for k in [&miss_key, &match_key] {
            assert_allowed(
                rl.redis().absolute().inc(k, &rate, 6).await.unwrap(),
                "creating a fresh bucket",
            );
        }
        runtime::async_sleep(Duration::from_millis(1_200)).await;

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let miss_history_key = redis_key(&prefix, &miss_key, "h");
        let miss_ordering_key = redis_key(&prefix, &miss_key, "a");
        let miss_total_key = redis_key(&prefix, &miss_key, "t");
        let miss_limit_key = redis_key(&prefix, &miss_key, "w");
        let miss_history_before: HashMap<String, u64> =
            conn.hgetall(&miss_history_key).await.unwrap();
        let miss_ordering_before: Vec<(String, f64)> = conn
            .zrange_withscores(&miss_ordering_key, 0, -1)
            .await
            .unwrap();
        let miss_total_before: u64 = conn.get(&miss_total_key).await.unwrap();
        let miss_limit_before: f64 = conn.get(&miss_limit_key).await.unwrap();
        let miss_ttl_before: i64 = conn.pttl(&miss_limit_key).await.unwrap();
        let miss_active_score_before: Option<f64> = conn
            .zscore(active_entities_key(&prefix), miss_key.to_string())
            .await
            .unwrap();
        assert_eq!(miss_history_before.len(), 2);
        assert_eq!(miss_total_before, 10);
        assert!(
            miss_ttl_before > 0,
            "fresh bucket should keep the limit alive"
        );

        let miss_result = rl
            .redis()
            .absolute()
            .set_if(&miss_key, &rate, RateLimitComparator::Gt(100), 1)
            .await
            .unwrap();
        assert_eq!(
            miss_result,
            (6, 6),
            "the comparator must exclude the expired count of 4"
        );
        let miss_history_after: HashMap<String, u64> =
            conn.hgetall(&miss_history_key).await.unwrap();
        let miss_ordering_after: Vec<(String, f64)> = conn
            .zrange_withscores(&miss_ordering_key, 0, -1)
            .await
            .unwrap();
        let miss_total_after: u64 = conn.get(&miss_total_key).await.unwrap();
        let miss_limit_after: f64 = conn.get(&miss_limit_key).await.unwrap();
        let miss_ttl_after: i64 = conn.pttl(&miss_limit_key).await.unwrap();
        let miss_active_score_after: Option<f64> = conn
            .zscore(active_entities_key(&prefix), miss_key.to_string())
            .await
            .unwrap();
        assert_eq!(miss_history_after, miss_history_before);
        assert_eq!(miss_ordering_after, miss_ordering_before);
        assert_eq!(miss_total_after, miss_total_before);
        assert_eq!(miss_limit_after, miss_limit_before);
        assert_eq!(miss_active_score_after, miss_active_score_before);
        assert!(
            miss_ttl_after > 0 && miss_ttl_after <= miss_ttl_before,
            "a guard miss must not refresh the limit TTL: before={miss_ttl_before}, after={miss_ttl_after}"
        );

        let match_result = rl
            .redis()
            .absolute()
            .set_if_preserve_history(
                &match_key,
                &rate,
                RateLimitComparator::Eq(6),
                4,
                HistoryPreservation::PreserveNewest,
            )
            .await
            .unwrap();
        assert_eq!(match_result, (4, 6));
        let matched_history: HashMap<String, u64> = conn
            .hgetall(redis_key(&prefix, &match_key, "h"))
            .await
            .unwrap();
        assert_eq!(matched_history.len(), 1);
        assert_eq!(matched_history.values().sum::<u64>(), 4);
        let matched_ordering: Vec<String> = conn
            .zrange(redis_key(&prefix, &match_key, "a"), 0, -1)
            .await
            .unwrap();
        assert_eq!(matched_ordering.len(), 1);
        assert!(matched_history.contains_key(&matched_ordering[0]));
        let matched_total: u64 = conn.get(redis_key(&prefix, &match_key, "t")).await.unwrap();
        assert_eq!(matched_total, 4);
    });
}

#[test]
fn redis_state_absolute_preserves_requested_history_edge() {
    let url = redis_url();

    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 60, 1).await;
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
            assert!(matches!(
                rl.redis().absolute().inc(&k, &rate, 4).await.unwrap(),
                RateLimitDecision::Allowed
            ));
            runtime::async_sleep(Duration::from_millis(3)).await;
            assert!(matches!(
                rl.redis().absolute().inc(&k, &rate, 5).await.unwrap(),
                RateLimitDecision::Allowed
            ));
            runtime::async_sleep(Duration::from_millis(3)).await;
            assert!(matches!(
                rl.redis().absolute().inc(&k, &rate, 6).await.unwrap(),
                RateLimitDecision::Allowed
            ));

            assert_eq!(
                rl.redis()
                    .absolute()
                    .set_if_preserve_history(&k, &rate, RateLimitComparator::Nil, 8, preservation,)
                    .await
                    .unwrap(),
                (8, 15)
            );

            let mut conn = redis::Client::open(url.as_str())
                .unwrap()
                .get_multiplexed_async_connection()
                .await
                .unwrap();
            let fields: Vec<String> = conn
                .zrange(redis_key(&prefix, &k, "a"), 0, -1)
                .await
                .unwrap();
            let counts: Vec<u64> = redis::cmd("HMGET")
                .arg(redis_key(&prefix, &k, "h"))
                .arg(&fields)
                .query_async(&mut conn)
                .await
                .unwrap();
            assert_eq!(counts, expected_reduced);

            assert_eq!(
                rl.redis()
                    .absolute()
                    .set_if_preserve_history(&k, &rate, RateLimitComparator::Nil, 11, preservation,)
                    .await
                    .unwrap(),
                (11, 8)
            );
            let fields: Vec<String> = conn
                .zrange(redis_key(&prefix, &k, "a"), 0, -1)
                .await
                .unwrap();
            let counts: Vec<u64> = redis::cmd("HMGET")
                .arg(redis_key(&prefix, &k, "h"))
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

#[test]
fn redis_state_absolute_missing_zero_and_guard_miss_write_nothing() {
    let url = redis_url();
    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 60, 10).await;
        let rate = RateLimit::try_from(100f64).unwrap();

        for (name, preserve) in [("replace", false), ("preserve", true)] {
            let k = key(name);
            let result = if preserve {
                rl.redis()
                    .absolute()
                    .set_if_preserve_history(
                        &k,
                        &rate,
                        RateLimitComparator::Eq(0),
                        0,
                        HistoryPreservation::PreserveNewest,
                    )
                    .await
                    .unwrap()
            } else {
                rl.redis()
                    .absolute()
                    .set_if(&k, &rate, RateLimitComparator::Eq(0), 0)
                    .await
                    .unwrap()
            };
            assert_eq!(result, (0, 0));

            let guard_miss = if preserve {
                rl.redis()
                    .absolute()
                    .set_if_preserve_history(
                        &k,
                        &rate,
                        RateLimitComparator::Eq(1),
                        5,
                        HistoryPreservation::PreserveOldest,
                    )
                    .await
                    .unwrap()
            } else {
                rl.redis()
                    .absolute()
                    .set_if(&k, &rate, RateLimitComparator::Eq(1), 5)
                    .await
                    .unwrap()
            };
            assert_eq!(guard_miss, (0, 0));

            let mut conn = redis::Client::open(url.as_str())
                .unwrap()
                .get_multiplexed_async_connection()
                .await
                .unwrap();
            for suffix in ["h", "a", "w", "t"] {
                let exists: bool = conn.exists(redis_key(&prefix, &k, suffix)).await.unwrap();
                assert!(!exists, "unexpected {suffix} key");
            }
            let active_score: Option<f64> = conn
                .zscore(active_entities_key(&prefix), k.to_string())
                .await
                .unwrap();
            assert!(
                active_score.is_none(),
                "missing conditional set created membership"
            );
        }
    });
}

/// A matched zero target removes every per-entity key and its cleanup membership.
#[test]
fn redis_state_absolute_conditional_set_zero_removes_entity_state() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size_seconds = 6_u64;
        let (rl, prefix) = build_limiter(&url, window_size_seconds, 1000).await;
        let rate_limit = RateLimit::try_from(10f64).unwrap();

        for (name, preserve) in [("replace", false), ("preserve", true)] {
            let k = key(name);
            assert_eq!(
                rl.redis()
                    .absolute()
                    .set_if(&k, &rate_limit, RateLimitComparator::Nil, 17)
                    .await
                    .unwrap(),
                (17, 0)
            );

            let result = if preserve {
                rl.redis()
                    .absolute()
                    .set_if_preserve_history(
                        &k,
                        &rate_limit,
                        RateLimitComparator::Nil,
                        0,
                        HistoryPreservation::PreserveOldest,
                    )
                    .await
                    .unwrap()
            } else {
                rl.redis()
                    .absolute()
                    .set_if(&k, &rate_limit, RateLimitComparator::Nil, 0)
                    .await
                    .unwrap()
            };
            assert_eq!(result, (0, 17));

            let mut conn = redis::Client::open(url.as_str())
                .unwrap()
                .get_multiplexed_async_connection()
                .await
                .unwrap();
            for suffix in ["h", "a", "w", "t"] {
                let exists: bool = conn.exists(redis_key(&prefix, &k, suffix)).await.unwrap();
                assert!(!exists, "unexpected {suffix} key for {name}");
            }
            let score: Option<f64> = conn
                .zscore(active_entities_key(&prefix), k.to_string())
                .await
                .unwrap();
            assert!(score.is_none(), "unexpected active membership for {name}");
        }
    });
}

/// Unknown reads and admission checks stay absent; reads of existing state refresh membership.
#[test]
fn redis_state_absolute_unknown_reads_stay_absent_and_known_get_refreshes_membership() {
    let url = redis_url();

    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 6, 1000).await;
        let k = key("k");
        let rate = RateLimit::try_from(10f64).unwrap();

        let total = rl.redis().absolute().get(&k).await.unwrap();
        assert_eq!(total, 0);
        let decision = rl.redis().absolute().is_allowed(&k).await.unwrap();
        assert!(matches!(decision, RateLimitDecision::Allowed));

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        for suffix in ["h", "a", "w", "t"] {
            let exists: bool = conn.exists(redis_key(&prefix, &k, suffix)).await.unwrap();
            assert!(!exists, "unknown read created the {suffix} key");
        }

        let score: Option<f64> = conn
            .zscore(active_entities_key(&prefix), k.to_string())
            .await
            .unwrap();
        assert!(score.is_none(), "unknown reads must not create state");

        assert_eq!(
            rl.redis()
                .absolute()
                .set_if(&k, &rate, RateLimitComparator::Nil, 3)
                .await
                .unwrap(),
            (3, 0)
        );
        let _: u64 = conn
            .zrem(active_entities_key(&prefix), k.to_string())
            .await
            .unwrap();

        assert_eq!(rl.redis().absolute().get(&k).await.unwrap(), 3);
        let score: Option<f64> = conn
            .zscore(active_entities_key(&prefix), k.to_string())
            .await
            .unwrap();
        assert!(score.is_some(), "known get must refresh active membership");
    });
}

/// Cleanup removes stale entities completely while preserving recently accessed state.
#[test]
fn redis_state_absolute_cleanup_removes_only_stale_entities() {
    let url = redis_url();

    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 10, 1000).await;
        let rate = RateLimit::try_from(10f64).unwrap();
        let stale_key = key("stale");
        let active_key = key("active");

        assert_allowed(
            rl.redis()
                .absolute()
                .inc(&stale_key, &rate, 3)
                .await
                .unwrap(),
            "seeding the stale entity",
        );
        assert_allowed(
            rl.redis()
                .absolute()
                .inc(&active_key, &rate, 4)
                .await
                .unwrap(),
            "seeding the active entity",
        );
        runtime::async_sleep(Duration::from_millis(250)).await;
        assert_eq!(rl.redis().absolute().get(&active_key).await.unwrap(), 4);

        rl.redis().absolute().cleanup(100).await.unwrap();

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        for suffix in ["h", "a", "w", "t"] {
            let stale_exists: bool = conn
                .exists(redis_key(&prefix, &stale_key, suffix))
                .await
                .unwrap();
            assert!(!stale_exists, "cleanup retained the stale {suffix} key");

            let active_exists: bool = conn
                .exists(redis_key(&prefix, &active_key, suffix))
                .await
                .unwrap();
            assert!(active_exists, "cleanup removed the active {suffix} key");
        }

        let stale_score: Option<f64> = conn
            .zscore(active_entities_key(&prefix), stale_key.to_string())
            .await
            .unwrap();
        assert!(stale_score.is_none());
        let active_score: Option<f64> = conn
            .zscore(active_entities_key(&prefix), active_key.to_string())
            .await
            .unwrap();
        assert!(active_score.is_some());
        assert_eq!(rl.redis().absolute().get(&active_key).await.unwrap(), 4);
    });
}
