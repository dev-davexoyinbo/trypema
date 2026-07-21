//! Redis state inspection tests for the **suppressed Redis** rate limiter.
//!
//! These tests connect directly to Redis and read raw keys written by the suppressed-strategy
//! Lua scripts, so that internal state is independently observable.
//!
//! # Redis data model (suppressed, per user-key `K`, prefix `P`)
//!
//! | Redis key                    | Type        | Meaning                                         |
//! |------------------------------|-------------|-------------------------------------------------|
//! | `P:K:suppressed:h`           | Hash        | `timestamp_ms → count` (total increments)       |
//! | `P:K:suppressed:hd`          | Hash        | `timestamp_ms → declined_count`                 |
//! | `P:K:suppressed:a`           | Sorted set  | Active bucket timestamps (scores = ts_ms)       |
//! | `P:K:suppressed:w`           | String      | Stored hard window limit                        |
//! | `P:K:suppressed:t`           | String      | Running total count (allowed + denied)          |
//! | `P:K:suppressed:d`           | String      | Running total declined count                    |
//! | `P:K:suppressed:sf`          | String      | Cached suppression factor (with PX TTL)         |
//! | `P:suppressed:active_entities` | Sorted set  | All active user-keys (for cleanup)            |

use std::{collections::HashMap, thread, time::Duration};

use redis::AsyncCommands;

use super::common::{key, key_gen, redis_url, unique_prefix};
use super::runtime;

use crate::common::{RateType, SuppressionFactorCachePeriod};
use crate::{
    BucketSize, HardLimitFactor, HistoryPreservation, RateLimit, RateLimitComparator,
    RateLimitDecision, RateLimiterBuilder, WindowSize,
    redis::{RedisKey, RedisRateLimiterProvider},
};

/// Build a rate limiter and return its unique prefix.
async fn build_limiter(
    url: &str,
    window_size: u64,
    bucket_size: u64,
    hard_limit_factor: f64,
    suppression_factor_cache_period: u64,
) -> (std::sync::Arc<RedisRateLimiterProvider>, RedisKey) {
    let client = redis::Client::open(url).unwrap();
    let cm = client.get_connection_manager().await.unwrap();
    let prefix = unique_prefix();

    let provider = RedisRateLimiterProvider::builder(cm)
        .prefix(prefix.clone())
        .window_size(WindowSize::seconds(window_size).unwrap())
        .bucket_size(BucketSize::milliseconds(bucket_size).unwrap())
        .hard_limit_factor(HardLimitFactor::try_from(hard_limit_factor).unwrap())
        .suppression_factor_cache_period(
            SuppressionFactorCachePeriod::milliseconds(suppression_factor_cache_period).unwrap(),
        )
        .cleanup_enabled(false)
        .build()
        .unwrap();

    (provider, prefix)
}

/// Construct the canonical Redis key for a given suffix using the key generator.
fn redis_key(prefix: &RedisKey, user_key: &RedisKey, suffix: &str) -> String {
    let kg = key_gen(prefix, RateType::Suppressed);
    match suffix {
        "h" => kg.get_hash_key(user_key),
        "hd" => kg.get_hash_declined_key(user_key),
        "a" => kg.get_active_keys(user_key),
        "w" => kg.get_window_limit_key(user_key),
        "t" => kg.get_total_count_key(user_key),
        "d" => kg.get_total_declined_key(user_key),
        "sf" => kg.get_suppression_factor_key(user_key),
        _ => panic!("unknown suffix for suppressed rate type: {suffix}"),
    }
}

fn active_entities_key(prefix: &RedisKey) -> String {
    key_gen(prefix, RateType::Suppressed).get_active_entities_key()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// After a single allowed increment (below the soft limit) the total count key equals the
/// increment value and the declined count key is zero.
#[test]
fn redis_state_suppressed_allowed_inc_sets_total_count_and_zero_declined() {
    let url = redis_url();

    runtime::block_on(async {
        // hard_limit_factor=2 => soft_window_limit=10, hard_window_limit=20.
        let (rl, prefix) = build_limiter(&url, 10, 1000, 2.0, 100).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(1f64).unwrap();

        let d = rl.suppressed().inc(&k, &rate_limit, 3).await.unwrap();
        assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // Total count must equal the increment.
        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        assert_eq!(total, 3, "total count should be 3");

        // Declined count must be 0 (or the key may not exist).
        let declined: u64 = conn.get(redis_key(&prefix, &k, "d")).await.unwrap_or(0u64);
        assert_eq!(declined, 0, "declined count should be 0");

        // The main hash must have the increment recorded.
        let hash: HashMap<String, u64> = conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        let hash_sum: u64 = hash.values().sum();
        assert_eq!(hash_sum, 3, "hash sum should equal the increment");

        // The declined hash must be empty.
        let hash_d: HashMap<String, u64> =
            conn.hgetall(redis_key(&prefix, &k, "hd")).await.unwrap();
        assert!(hash_d.is_empty(), "declined hash should be empty");
    });
}

/// Landing exactly on the hard limit is allowed and caches full suppression. The next call is
/// denied, while both observed and declined Redis state remain internally exact.
#[test]
fn redis_state_suppressed_exact_hard_caches_one_and_next_call_records_decline() {
    let url = redis_url();

    runtime::block_on(async {
        let cache_ms = 60_000_u64;
        let (rl, prefix) = build_limiter(&url, 10, 1000, 2.0, cache_ms).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(1f64).unwrap();
        let hard_window_limit = 20_u64;

        let reaches_hard = rl
            .suppressed()
            .inc(&k, &rate_limit, hard_window_limit)
            .await
            .unwrap();
        assert!(
            matches!(reaches_hard, RateLimitDecision::Allowed),
            "reaches_hard: {reaches_hard:?}"
        );

        let next = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(
            matches!(
                next,
                RateLimitDecision::Suppressed {
                    suppression_factor,
                    is_allowed: false
                } if (suppression_factor - 1.0).abs() < 1e-12
            ),
            "next: {next:?}"
        );

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let declined: u64 = conn.get(redis_key(&prefix, &k, "d")).await.unwrap();
        assert_eq!(declined, 1);

        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        assert_eq!(total, hard_window_limit + 1);

        let history: HashMap<String, u64> =
            conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        assert_eq!(history.values().sum::<u64>(), hard_window_limit + 1);

        let hash_d: HashMap<String, u64> =
            conn.hgetall(redis_key(&prefix, &k, "hd")).await.unwrap();
        assert_eq!(hash_d.values().sum::<u64>(), 1);

        let ordering_count: u64 = conn.zcard(redis_key(&prefix, &k, "a")).await.unwrap();
        assert_eq!(
            ordering_count, 1,
            "grouped increments should share one bucket"
        );

        let stored_limit: u64 = conn.get(redis_key(&prefix, &k, "w")).await.unwrap();
        assert_eq!(stored_limit, hard_window_limit);

        let factor: f64 = conn.get(redis_key(&prefix, &k, "sf")).await.unwrap();
        assert!((factor - 1.0).abs() < 1e-12, "factor: {factor}");
        let factor_ttl: i64 = conn.pttl(redis_key(&prefix, &k, "sf")).await.unwrap();
        assert!(factor_ttl > 0 && factor_ttl <= cache_ms as i64);

        let active_score: Option<f64> = conn
            .zscore(active_entities_key(&prefix), k.to_string())
            .await
            .unwrap();
        assert!(active_score.is_some(), "key should be registered as active");
    });
}

/// The suppression factor cache key (`sf`) is written after the first `inc` that enters the
/// suppression zone (above soft limit).  Its value must be a float string in [0, 1].
#[test]
fn redis_state_suppressed_sf_cache_key_is_set_in_suppression_zone() {
    let url = redis_url();

    runtime::block_on(async {
        // window=10s, rate=1, hard_limit_factor=2 => soft=10, hard=20.
        //
        // Use a cache_ms that is large enough for the `sf` key to survive the Redis
        // round-trip that asserts its value, but short enough that we can wait for it
        // to expire before calling `get_suppression_factor` so that function is forced
        // to recompute (and therefore write) a fresh value.
        //
        // Previously this test used cache_ms=1, which created a race: `get_suppression_factor`
        // wrote the `sf` key with a 1ms TTL, but by the time the assertion opened a new
        // connection and issued GET the key had already expired, causing intermittent failures.
        let cache_ms = 500_u64;
        let (rl, prefix) = build_limiter(&url, 10, 100, 2.0, cache_ms).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(1f64).unwrap();

        for accepted_after in 1..=10_u64 {
            let decision = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();
            assert!(
                matches!(decision, RateLimitDecision::Allowed),
                "accepted_after={accepted_after}, decision={decision:?}"
            );
        }

        let above_soft = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(
            matches!(
                above_soft,
                RateLimitDecision::Suppressed {
                    suppression_factor,
                    ..
                } if (suppression_factor - 0.9).abs() < 1e-12
            ),
            "above_soft: {above_soft:?}"
        );

        // Wait for the `sf` key written by the inc loop to expire, so that the
        // subsequent `get_suppression_factor` call is guaranteed to recompute and
        // re-write the `sf` key with a fresh cache_ms TTL.
        runtime::async_sleep(Duration::from_millis(cache_ms + 10)).await;

        // Trigger a fresh computation. This writes `sf` with `cache_ms` TTL.
        let sf = rl.suppressed().get_suppression_factor(&k).await.unwrap();

        // Verify the return value directly — no separate Redis read needed here.
        assert!(
            sf > 0.0,
            "suppression factor should be > 0 past the soft limit, got {sf}"
        );
        assert!(
            (0.0..=1.0).contains(&sf),
            "suppression factor must be in [0, 1], got {sf}"
        );

        // Also verify the `sf` key was physically written to Redis.  The key now has
        // a full cache_ms TTL so there is no race with the assertion below.
        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let sf_raw: Option<String> = conn.get(redis_key(&prefix, &k, "sf")).await.unwrap();
        assert!(
            sf_raw.is_some(),
            "suppression factor cache key should be set when in the suppression zone"
        );

        let sf_stored: f64 = sf_raw.unwrap().parse().expect("sf should be a valid float");
        assert!(
            (sf_stored - sf).abs() < 1e-9,
            "stored sf value ({sf_stored}) should match the value returned by get_suppression_factor ({sf})"
        );
    });
}

/// The suppression factor cache key has a positive TTL (set via PX).
#[test]
fn redis_state_suppressed_sf_cache_key_has_ttl() {
    let url = redis_url();

    runtime::block_on(async {
        let cache_ms = 500_u64;
        let (rl, prefix) = build_limiter(&url, 10, 100, 2.0, cache_ms).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(1f64).unwrap();

        for accepted_after in 1..=10_u64 {
            let decision = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();
            assert!(
                matches!(decision, RateLimitDecision::Allowed),
                "accepted_after={accepted_after}, decision={decision:?}"
            );
        }

        let above_soft = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(
            matches!(
                above_soft,
                RateLimitDecision::Suppressed {
                    suppression_factor,
                    ..
                } if (suppression_factor - 0.9).abs() < 1e-12
            ),
            "above_soft: {above_soft:?}"
        );

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // PTTL returns -2 (not found), -1 (no expiry), or positive ms.
        let pttl: i64 = conn.pttl(redis_key(&prefix, &k, "sf")).await.unwrap();

        assert!(
            pttl > 0,
            "suppression factor cache key should have a positive TTL (PX set), got {pttl}"
        );
        assert!(
            pttl <= cache_ms as i64,
            "TTL should be <= cache_ms={cache_ms}, got {pttl}"
        );
    });
}

/// Invalid cached factors are ignored and replaced with a freshly calculated value.
#[test]
fn redis_state_suppressed_invalid_cached_factors_are_recomputed() {
    let url = redis_url();

    runtime::block_on(async {
        let cache_ms = 60_000_u64;
        let (rl, prefix) = build_limiter(&url, 10, 100, 2.0, cache_ms).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(1f64).unwrap();

        assert_eq!(
            rl.suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Always, 11)
                .await
                .unwrap(),
            (11, 0)
        );

        let factor_key = redis_key(&prefix, &k, "sf");
        let expected = 1.0_f64 - (1.0_f64 / 11.0_f64);
        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        for invalid in [-0.25_f64, 1.25_f64] {
            let _: () = conn.set(&factor_key, invalid).await.unwrap();

            let factor = rl.suppressed().get_suppression_factor(&k).await.unwrap();
            assert!(
                (factor - expected).abs() < 1e-12,
                "invalid={invalid}, factor={factor}, expected={expected}"
            );

            let stored: f64 = conn.get(&factor_key).await.unwrap();
            assert!((stored - expected).abs() < 1e-12);
            let ttl: i64 = conn.pttl(&factor_key).await.unwrap();
            assert!(ttl > 0 && ttl <= cache_ms as i64, "ttl: {ttl}");
        }
    });
}

/// The `w` key preserves the untruncated hard window limit so that the soft capacity can be
/// derived before either operational boundary is truncated.
#[test]
fn redis_state_suppressed_window_limit_key_equals_hard_limit() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size = 6_u64;
        let rate_limit_value = 0.5_f64;
        let hard_limit_factor = 1.5_f64;
        // raw hard window = 6 * 0.5 * 1.5 = 4.5; capacities are soft=3 and hard=4.
        let expected_hard_limit = 4.5_f64;

        let (rl, prefix) = build_limiter(&url, window_size, 1000, hard_limit_factor, 100).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(rate_limit_value).unwrap();

        let decision = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(decision, RateLimitDecision::Allowed));

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

#[test]
fn redis_state_suppressed_inc_retains_first_hard_window_limit() {
    let url = redis_url();

    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 1, 1000, 1.0, 60_000).await;
        let k = key("k");
        let initial_rate = RateLimit::per_second(2f64).unwrap();
        let later_rate = RateLimit::per_second(10f64).unwrap();

        let decision = rl.suppressed().inc(&k, &initial_rate, 2).await.unwrap();
        assert!(matches!(decision, RateLimitDecision::Allowed));
        let decision = rl.suppressed().inc(&k, &later_rate, 1).await.unwrap();
        assert!(
            matches!(
                decision,
                RateLimitDecision::Suppressed {
                    suppression_factor: 1.0,
                    is_allowed: false,
                }
            ),
            "the later rate must not replace the stored hard limit: {decision:?}"
        );

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let hard_window_limit: f64 = conn.get(redis_key(&prefix, &k, "w")).await.unwrap();
        assert_eq!(hard_window_limit, 2.0);
    });
}

/// The hash sum (`h`) must always equal the total count key (`t`), and the declined
/// hash sum (`hd`) must equal the declined count key (`d`).
#[test]
fn redis_state_suppressed_hash_sums_match_counter_keys() {
    let url = redis_url();

    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 10, 100, 10.0, 1).await;
        let k = key("k");
        // With hard_limit_factor=10 and rate=1: hard_window_limit = 100.
        // We'll drive past the hard limit so some calls are denied.
        let rate_limit = RateLimit::per_second(1f64).unwrap();

        let reaches_hard = rl.suppressed().inc(&k, &rate_limit, 100).await.unwrap();
        assert!(
            matches!(reaches_hard, RateLimitDecision::Allowed),
            "reaches_hard: {reaches_hard:?}"
        );

        for denied_after in 1..=5_u64 {
            let decision = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();
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

        assert_eq!(total, 105);
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

/// After the window expires, both the total hash and the declined hash must be evicted,
/// and both counter keys must reflect the eviction.
#[test]
fn redis_state_suppressed_evicts_expired_buckets_from_both_hashes() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size = 1_u64;
        // hard_limit_factor=10 so the hard limit is 10×rate = 10.
        let (rl, prefix) = build_limiter(&url, window_size, 1000, 10.0, 1).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(1f64).unwrap();

        let reaches_hard = rl.suppressed().inc(&k, &rate_limit, 10).await.unwrap();
        assert!(matches!(reaches_hard, RateLimitDecision::Allowed));

        let declined = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();
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

        // Wait for the window to expire.
        thread::sleep(Duration::from_millis(window_size * 1000 + 50));
        runtime::async_sleep(Duration::from_millis(10)).await;

        // A fresh inc triggers eviction inside the Lua script.
        let d = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        let hash: HashMap<String, u64> = conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        let active_count: u64 = conn.zcard(redis_key(&prefix, &k, "a")).await.unwrap();

        // After eviction + one new increment, total must equal 1.
        assert_eq!(
            total, 1,
            "total count must be 1 after eviction (only the fresh increment remains)"
        );
        assert_eq!(
            hash.len(),
            1,
            "hash must have exactly one bucket after eviction"
        );
        assert_eq!(
            active_count, 1,
            "active sorted set must have one entry after eviction"
        );
    });
}

/// `get_suppression_factor` on an unknown key returns 0.0 and must NOT write any Redis keys.
#[test]
fn redis_state_suppressed_get_factor_on_unknown_key_writes_no_state() {
    let url = redis_url();

    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 10, 1000, 2.0, 100).await;
        let k = key("brand_new");

        let sf = rl.suppressed().get_suppression_factor(&k).await.unwrap();
        assert!((sf - 0.0).abs() < 1e-12, "sf: {sf}");

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        for suffix in ["h", "hd", "a", "w", "t", "d", "sf"] {
            let exists: bool = conn.exists(redis_key(&prefix, &k, suffix)).await.unwrap();
            assert!(
                !exists,
                "unknown suppression-factor read created the {suffix} key"
            );
        }

        let active_score: Option<f64> = conn
            .zscore(active_entities_key(&prefix), k.to_string())
            .await
            .unwrap();
        assert_eq!(
            active_score, None,
            "unknown suppression-factor read registered an active entity"
        );
    });
}

/// Two different user-keys under the same prefix maintain independent state.
#[test]
fn redis_state_suppressed_per_key_state_is_independent() {
    let url = redis_url();

    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 10, 1000, 2.0, 100).await;
        let a = key("a");
        let b = key("b");
        let rate_limit = RateLimit::per_second(1f64).unwrap();

        let decision = rl.suppressed().inc(&a, &rate_limit, 4).await.unwrap();
        assert!(matches!(decision, RateLimitDecision::Allowed));
        let decision = rl.suppressed().inc(&b, &rate_limit, 9).await.unwrap();
        assert!(matches!(decision, RateLimitDecision::Allowed));

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let total_a: u64 = conn.get(redis_key(&prefix, &a, "t")).await.unwrap();
        let total_b: u64 = conn.get(redis_key(&prefix, &b, "t")).await.unwrap();

        assert_eq!(total_a, 4, "total for key a should be 4");
        assert_eq!(total_b, 9, "total for key b should be 9");
    });
}

/// The `active_entities` sorted set is updated on each `inc` call.
#[test]
fn redis_state_suppressed_active_entities_updated_on_inc() {
    let url = redis_url();

    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 10, 1000, 2.0, 100).await;
        let k = key("entity");
        let rate_limit = RateLimit::per_second(1f64).unwrap();

        let ae_key = active_entities_key(&prefix);

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // Not present before any call.
        let score_before: Option<f64> = conn.zscore(&ae_key, k.as_str()).await.unwrap();
        assert!(
            score_before.is_none(),
            "key should not be in active_entities before inc"
        );

        let decision = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(decision, RateLimitDecision::Allowed));

        let score_after: Option<f64> = conn.zscore(&ae_key, k.as_str()).await.unwrap();
        assert!(
            score_after.is_some(),
            "key should be in active_entities after inc"
        );
    });
}

/// A matched `set_if` replaces the window with a single bucket holding the count, resets
/// the declined counters, drops the cached suppression factor, and (re)defines the hard
/// hard window limit (`window * rate * hard_limit_factor`).
#[test]
fn redis_state_suppressed_set_if_writes_single_bucket_and_resets_declines() {
    let url = redis_url();

    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 6, 1000, 2.0, 100).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(10f64).unwrap();

        let outcome = rl
            .suppressed()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(40), 40)
            .await
            .unwrap();
        let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
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

        let ordering: Vec<String> = conn
            .zrange(redis_key(&prefix, &k, "a"), 0, -1)
            .await
            .unwrap();
        assert_eq!(ordering.len(), 1, "exactly one active bucket expected");
        assert!(
            buckets.contains_key(&ordering[0]),
            "the ordered bucket must exist in history"
        );

        let declined_total: Option<u64> = conn.get(redis_key(&prefix, &k, "d")).await.unwrap();
        assert!(
            declined_total.is_none(),
            "declined total must be reset, got {declined_total:?}"
        );

        let declined_len: u64 = conn.hlen(redis_key(&prefix, &k, "hd")).await.unwrap();
        assert_eq!(declined_len, 0, "declined bucket hash must be empty");

        let factor: Option<String> = conn.get(redis_key(&prefix, &k, "sf")).await.unwrap();
        assert!(
            factor.is_none(),
            "cached suppression factor must be dropped, got {factor:?}"
        );

        let hard_window_limit: u64 = conn.get(redis_key(&prefix, &k, "w")).await.unwrap();
        assert_eq!(
            hard_window_limit, 120,
            "hard window limit must be window * rate * hard_limit_factor (6 * 10 * 2)"
        );
        let ttl: i64 = conn.ttl(redis_key(&prefix, &k, "w")).await.unwrap();
        assert!(ttl > 0 && ttl <= 6, "hard window limit TTL: {ttl}");
        let active_score: Option<f64> = conn
            .zscore(active_entities_key(&prefix), k.to_string())
            .await
            .unwrap();
        assert!(active_score.is_some(), "set_if must register the entity");
    });
}

/// An unmatched `set_if` leaves all state untouched.
#[test]
fn redis_state_suppressed_set_if_no_match_is_true_noop() {
    let url = redis_url();

    runtime::block_on(async {
        // Long-lived factor cache so the sf key persists until set_if drops it.
        let (rl, prefix) = build_limiter(&url, 6, 1000, 1.0, 60_000).await;
        let k = key("k");

        let rate_seed = RateLimit::per_second(10f64).unwrap();
        assert_eq!(
            rl.suppressed()
                .set_if(&k, &rate_seed, RateLimitComparator::Always, 17)
                .await
                .unwrap(),
            (17, 0)
        );

        // A below-soft increment does not create a cache entry, matching suppressed-local.
        assert!(matches!(
            rl.suppressed().inc(&k, &rate_seed, 1).await.unwrap(),
            RateLimitDecision::Allowed
        ));

        // A public factor read computes and caches the current factor so the guard-miss
        // assertion below can prove that set_if leaves valid cache metadata untouched.
        assert_eq!(
            rl.suppressed().get_suppression_factor(&k).await.unwrap(),
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
            .zscore(active_entities_key(&prefix), k.to_string())
            .await
            .unwrap();
        assert!(
            factor_before.is_some(),
            "the public factor read should have cached a factor"
        );

        runtime::async_sleep(Duration::from_millis(50)).await;

        let rate_new = RateLimit::per_second(20f64).unwrap();
        let outcome = rl
            .suppressed()
            .set_if(&k, &rate_new, RateLimitComparator::Gt(1000), 5)
            .await
            .unwrap();
        let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
        assert_eq!((new_total, old_total), (18, 18));

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
            .zscore(active_entities_key(&prefix), k.to_string())
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

/// Conditional sets compare against the live total. A miss leaves expired history and every
/// piece of suppression metadata untouched; a match prunes expired history before preserving.
#[test]
fn redis_state_suppressed_set_if_handles_expired_history_only_after_a_match() {
    let url = redis_url();

    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 2, 100, 1.0, 60_000).await;
        let rate = RateLimit::per_second(10f64).unwrap();
        let miss_key = key("miss");
        let match_key = key("match");

        for k in [&miss_key, &match_key] {
            let decision = rl.suppressed().inc(k, &rate, 4).await.unwrap();
            assert!(
                matches!(decision, RateLimitDecision::Allowed),
                "creating an oldest bucket: {decision:?}"
            );
        }
        runtime::async_sleep(Duration::from_millis(900)).await;
        for k in [&miss_key, &match_key] {
            let decision = rl.suppressed().inc(k, &rate, 6).await.unwrap();
            assert!(
                matches!(decision, RateLimitDecision::Allowed),
                "creating a fresh bucket: {decision:?}"
            );
            assert_eq!(
                rl.suppressed().get_suppression_factor(k).await.unwrap(),
                0.0
            );
        }
        runtime::async_sleep(Duration::from_millis(1_200)).await;

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let history_key = redis_key(&prefix, &miss_key, "h");
        let declined_history_key = redis_key(&prefix, &miss_key, "hd");
        let ordering_key = redis_key(&prefix, &miss_key, "a");
        let total_key = redis_key(&prefix, &miss_key, "t");
        let declined_total_key = redis_key(&prefix, &miss_key, "d");
        let limit_key = redis_key(&prefix, &miss_key, "w");
        let factor_key = redis_key(&prefix, &miss_key, "sf");

        let history_before: HashMap<String, u64> = conn.hgetall(&history_key).await.unwrap();
        let declined_history_before: HashMap<String, u64> =
            conn.hgetall(&declined_history_key).await.unwrap();
        let ordering_before: Vec<(String, f64)> =
            conn.zrange_withscores(&ordering_key, 0, -1).await.unwrap();
        let total_before: u64 = conn.get(&total_key).await.unwrap();
        let declined_total_before: Option<u64> = conn.get(&declined_total_key).await.unwrap();
        let limit_before: f64 = conn.get(&limit_key).await.unwrap();
        let factor_before: Option<String> = conn.get(&factor_key).await.unwrap();
        let limit_ttl_before: i64 = conn.pttl(&limit_key).await.unwrap();
        let factor_ttl_before: i64 = conn.pttl(&factor_key).await.unwrap();
        let active_score_before: Option<f64> = conn
            .zscore(active_entities_key(&prefix), miss_key.to_string())
            .await
            .unwrap();
        assert_eq!(history_before.len(), 2);
        assert!(declined_history_before.is_empty());
        assert_eq!(total_before, 10);
        assert_eq!(declined_total_before, None);
        assert!(factor_before.is_some(), "setup must cache a factor");
        assert!(limit_ttl_before > 0);
        assert!(factor_ttl_before > 0);

        let result = rl
            .suppressed()
            .set_if(&miss_key, &rate, RateLimitComparator::Gt(100), 1)
            .await
            .unwrap();
        assert_eq!(
            result,
            (6, 6),
            "the comparator must exclude the expired count of 4"
        );

        let history_after: HashMap<String, u64> = conn.hgetall(&history_key).await.unwrap();
        let declined_history_after: HashMap<String, u64> =
            conn.hgetall(&declined_history_key).await.unwrap();
        let ordering_after: Vec<(String, f64)> =
            conn.zrange_withscores(&ordering_key, 0, -1).await.unwrap();
        let total_after: u64 = conn.get(&total_key).await.unwrap();
        let declined_total_after: Option<u64> = conn.get(&declined_total_key).await.unwrap();
        let limit_after: f64 = conn.get(&limit_key).await.unwrap();
        let factor_after: Option<String> = conn.get(&factor_key).await.unwrap();
        let limit_ttl_after: i64 = conn.pttl(&limit_key).await.unwrap();
        let factor_ttl_after: i64 = conn.pttl(&factor_key).await.unwrap();
        let active_score_after: Option<f64> = conn
            .zscore(active_entities_key(&prefix), miss_key.to_string())
            .await
            .unwrap();
        assert_eq!(history_after, history_before);
        assert_eq!(declined_history_after, declined_history_before);
        assert_eq!(ordering_after, ordering_before);
        assert_eq!(total_after, total_before);
        assert_eq!(declined_total_after, declined_total_before);
        assert_eq!(limit_after, limit_before);
        assert_eq!(factor_after, factor_before);
        assert_eq!(active_score_after, active_score_before);
        assert!(limit_ttl_after > 0 && limit_ttl_after <= limit_ttl_before);
        assert!(factor_ttl_after > 0 && factor_ttl_after <= factor_ttl_before);

        let result = rl
            .suppressed()
            .set_if_preserve_history(
                &match_key,
                &rate,
                RateLimitComparator::Eq(6),
                4,
                HistoryPreservation::PreserveNewest,
            )
            .await
            .unwrap();
        assert_eq!(result, (4, 6));

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
        let matched_declined_history_len: u64 = conn
            .hlen(redis_key(&prefix, &match_key, "hd"))
            .await
            .unwrap();
        assert_eq!(matched_declined_history_len, 0);
        let matched_declined_total: Option<u64> =
            conn.get(redis_key(&prefix, &match_key, "d")).await.unwrap();
        assert_eq!(matched_declined_total, None);
        let matched_factor: Option<String> = conn
            .get(redis_key(&prefix, &match_key, "sf"))
            .await
            .unwrap();
        assert_eq!(matched_factor, None);
        let matched_limit: f64 = conn.get(redis_key(&prefix, &match_key, "w")).await.unwrap();
        assert_eq!(matched_limit, 20.0);
        let matched_limit_ttl: i64 = conn
            .pttl(redis_key(&prefix, &match_key, "w"))
            .await
            .unwrap();
        assert!(matched_limit_ttl > 0 && matched_limit_ttl <= 2_000);
        let matched_active_score: Option<f64> = conn
            .zscore(active_entities_key(&prefix), match_key.to_string())
            .await
            .unwrap();
        assert!(matched_active_score.is_some());
    });
}

#[test]
fn redis_state_suppressed_preserve_history_scales_declines() {
    let url = redis_url();
    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 1, 100, 1.0, 1).await;
        let low_rate = RateLimit::per_second(4f64).unwrap();
        let medium_rate = RateLimit::per_second(12f64).unwrap();
        let high_rate = RateLimit::per_second(100f64).unwrap();

        for (
            name,
            preservation,
            old_total,
            reduced_total,
            reduced_counts,
            reduced_declines,
            increased_total,
            increased_counts,
        ) in [
            (
                "newest",
                HistoryPreservation::PreserveNewest,
                17_u64,
                15_u64,
                vec![4_u64, 5, 6],
                vec![1_u64, 0, 0],
                18_u64,
                vec![4_u64, 5, 9],
            ),
            (
                "oldest",
                HistoryPreservation::PreserveOldest,
                15_u64,
                13_u64,
                vec![4_u64, 5, 4],
                vec![0_u64, 0, 2],
                16_u64,
                vec![7_u64, 5, 4],
            ),
        ] {
            let k = key(name);

            match preservation {
                HistoryPreservation::PreserveNewest => {
                    assert_eq!(
                        rl.suppressed()
                            .set_if(&k, &low_rate, RateLimitComparator::Always, 4)
                            .await
                            .unwrap(),
                        (4, 0)
                    );
                    assert_eq!(
                        rl.suppressed().get_suppression_factor(&k).await.unwrap(),
                        1.0
                    );
                    let declined = rl.suppressed().inc(&k, &low_rate, 2).await.unwrap();
                    assert!(matches!(
                        declined,
                        RateLimitDecision::Suppressed {
                            suppression_factor: 1.0,
                            is_allowed: false
                        }
                    ));
                    assert_eq!(
                        rl.suppressed()
                            .set_if_preserve_history(
                                &k,
                                &high_rate,
                                RateLimitComparator::Eq(6),
                                6,
                                HistoryPreservation::PreserveNewest,
                            )
                            .await
                            .unwrap(),
                        (6, 6)
                    );
                    runtime::async_sleep(Duration::from_millis(120)).await;
                    assert!(matches!(
                        rl.suppressed().inc(&k, &high_rate, 5).await.unwrap(),
                        RateLimitDecision::Allowed
                    ));
                    runtime::async_sleep(Duration::from_millis(120)).await;
                    assert!(matches!(
                        rl.suppressed().inc(&k, &high_rate, 6).await.unwrap(),
                        RateLimitDecision::Allowed
                    ));
                }
                HistoryPreservation::PreserveOldest => {
                    assert_eq!(
                        rl.suppressed()
                            .set_if(&k, &high_rate, RateLimitComparator::Always, 4)
                            .await
                            .unwrap(),
                        (4, 0)
                    );
                    runtime::async_sleep(Duration::from_millis(120)).await;
                    assert!(matches!(
                        rl.suppressed().inc(&k, &high_rate, 5).await.unwrap(),
                        RateLimitDecision::Allowed
                    ));
                    assert_eq!(
                        rl.suppressed()
                            .set_if_preserve_history(
                                &k,
                                &medium_rate,
                                RateLimitComparator::Eq(9),
                                9,
                                HistoryPreservation::PreserveOldest,
                            )
                            .await
                            .unwrap(),
                        (9, 9)
                    );
                    runtime::async_sleep(Duration::from_millis(120)).await;
                    assert!(matches!(
                        rl.suppressed().inc(&k, &medium_rate, 3).await.unwrap(),
                        RateLimitDecision::Allowed
                    ));
                    runtime::async_sleep(Duration::from_millis(5)).await;
                    assert_eq!(
                        rl.suppressed().get_suppression_factor(&k).await.unwrap(),
                        1.0
                    );
                    let declined = rl.suppressed().inc(&k, &medium_rate, 3).await.unwrap();
                    assert!(matches!(
                        declined,
                        RateLimitDecision::Suppressed {
                            suppression_factor: 1.0,
                            is_allowed: false
                        }
                    ));
                }
            }

            assert_eq!(rl.suppressed().get(&k).await.unwrap().total, old_total);

            assert_eq!(
                rl.suppressed()
                    .set_if_preserve_history(
                        &k,
                        &high_rate,
                        RateLimitComparator::Eq(old_total),
                        reduced_total,
                        preservation,
                    )
                    .await
                    .unwrap(),
                (reduced_total, old_total)
            );

            let mut conn = redis::Client::open(url.as_str())
                .unwrap()
                .get_multiplexed_async_connection()
                .await
                .unwrap();
            let h = redis_key(&prefix, &k, "h");
            let hd = redis_key(&prefix, &k, "hd");
            let a = redis_key(&prefix, &k, "a");
            let retained_fields: Vec<String> = conn.zrange(&a, 0, -1).await.unwrap();
            let counts: Vec<u64> = redis::cmd("HMGET")
                .arg(&h)
                .arg(&retained_fields)
                .query_async(&mut conn)
                .await
                .unwrap();
            let declines: Vec<Option<u64>> = redis::cmd("HMGET")
                .arg(&hd)
                .arg(&retained_fields)
                .query_async(&mut conn)
                .await
                .unwrap();
            let declines = declines
                .into_iter()
                .map(|declined| declined.unwrap_or(0))
                .collect::<Vec<_>>();
            assert_eq!(counts, reduced_counts);
            assert_eq!(declines, reduced_declines);
            let total_declined: u64 = conn.get(redis_key(&prefix, &k, "d")).await.unwrap();
            assert_eq!(total_declined, reduced_declines.iter().sum::<u64>());
            let factor: Option<String> = conn.get(redis_key(&prefix, &k, "sf")).await.unwrap();
            assert!(factor.is_none());

            assert_eq!(
                rl.suppressed()
                    .set_if_preserve_history(
                        &k,
                        &high_rate,
                        RateLimitComparator::Eq(reduced_total),
                        increased_total,
                        preservation,
                    )
                    .await
                    .unwrap(),
                (increased_total, reduced_total)
            );
            let retained_fields: Vec<String> = conn.zrange(&a, 0, -1).await.unwrap();
            let counts: Vec<u64> = redis::cmd("HMGET")
                .arg(&h)
                .arg(&retained_fields)
                .query_async(&mut conn)
                .await
                .unwrap();
            assert_eq!(counts, increased_counts);
            let declines: Vec<Option<u64>> = redis::cmd("HMGET")
                .arg(&hd)
                .arg(&retained_fields)
                .query_async(&mut conn)
                .await
                .unwrap();
            let declines = declines
                .into_iter()
                .map(|declined| declined.unwrap_or(0))
                .collect::<Vec<_>>();
            assert_eq!(
                declines, reduced_declines,
                "accepted additions must not add declines"
            );
            let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
            assert_eq!(total, increased_total);
            let total_declined: u64 = conn.get(redis_key(&prefix, &k, "d")).await.unwrap();
            assert_eq!(total_declined, reduced_declines.iter().sum::<u64>());
        }
    });
}

#[test]
fn redis_state_suppressed_missing_zero_and_guard_miss_write_nothing() {
    let url = redis_url();

    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 60, 10, 1.0, 100).await;
        let rate = RateLimit::per_second(100f64).unwrap();

        for (name, preserve) in [("replace", false), ("preserve", true)] {
            let k = key(name);
            let result = if preserve {
                rl.suppressed()
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
                rl.suppressed()
                    .set_if(&k, &rate, RateLimitComparator::Eq(0), 0)
                    .await
                    .unwrap()
            };
            assert_eq!(result, (0, 0));

            let guard_miss = if preserve {
                rl.suppressed()
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
                rl.suppressed()
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
            for suffix in ["h", "hd", "a", "w", "t", "d", "sf"] {
                let exists: bool = conn.exists(redis_key(&prefix, &k, suffix)).await.unwrap();
                assert!(!exists, "unexpected {suffix} key for {name}");
            }
            let active_score: Option<f64> = conn
                .zscore(active_entities_key(&prefix), k.to_string())
                .await
                .unwrap();
            assert!(
                active_score.is_none(),
                "missing conditional set created membership for {name}"
            );
        }
    });
}

/// A matched zero target removes every per-entity key and its cleanup membership.
#[test]
fn redis_state_suppressed_conditional_set_zero_removes_entity_state() {
    let url = redis_url();

    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 6, 1000, 1.0, 100).await;
        let rate_limit = RateLimit::per_second(10f64).unwrap();

        for (name, preserve) in [("replace", false), ("preserve", true)] {
            let k = key(name);
            assert_eq!(
                rl.suppressed()
                    .set_if(&k, &rate_limit, RateLimitComparator::Always, 17)
                    .await
                    .unwrap(),
                (17, 0)
            );

            let result = if preserve {
                rl.suppressed()
                    .set_if_preserve_history(
                        &k,
                        &rate_limit,
                        RateLimitComparator::Always,
                        0,
                        HistoryPreservation::PreserveOldest,
                    )
                    .await
                    .unwrap()
            } else {
                rl.suppressed()
                    .set_if(&k, &rate_limit, RateLimitComparator::Always, 0)
                    .await
                    .unwrap()
            };
            assert_eq!(result, (0, 17));

            let mut conn = redis::Client::open(url.as_str())
                .unwrap()
                .get_multiplexed_async_connection()
                .await
                .unwrap();
            for suffix in ["h", "hd", "a", "w", "t", "d", "sf"] {
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

/// Unknown reads stay absent; reads of existing state refresh cleanup membership.
#[test]
fn redis_state_suppressed_get_only_registers_existing_entity_as_active() {
    let url = redis_url();

    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 6, 1000, 1.0, 100).await;
        let k = key("k");
        let rate = RateLimit::per_second(10f64).unwrap();

        assert_eq!(rl.suppressed().get(&k).await.unwrap().total, 0);

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        for suffix in ["h", "hd", "a", "w", "t", "d", "sf"] {
            let exists: bool = conn.exists(redis_key(&prefix, &k, suffix)).await.unwrap();
            assert!(!exists, "unknown get created the {suffix} key");
        }
        let score: Option<f64> = conn
            .zscore(active_entities_key(&prefix), k.to_string())
            .await
            .unwrap();
        assert!(score.is_none(), "unknown get must not create state");

        rl.suppressed()
            .set_if(&k, &rate, RateLimitComparator::Always, 3)
            .await
            .unwrap();
        let _: u64 = conn
            .zrem(active_entities_key(&prefix), k.to_string())
            .await
            .unwrap();

        assert_eq!(rl.suppressed().get(&k).await.unwrap().total, 3);
        let score: Option<f64> = conn
            .zscore(active_entities_key(&prefix), k.to_string())
            .await
            .unwrap();
        assert!(score.is_some(), "known get must refresh active membership");
    });
}

#[test]
fn redis_state_suppressed_get_eviction_invalidates_cached_factor() {
    let url = redis_url();

    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 1, 1_000, 1.0, 5_000).await;
        let k = key("k");
        let rate = RateLimit::per_second(1f64).unwrap();

        assert_eq!(
            rl.suppressed()
                .set_if(&k, &rate, RateLimitComparator::Always, 1)
                .await
                .unwrap(),
            (1, 0)
        );
        assert_eq!(
            rl.suppressed().get_suppression_factor(&k).await.unwrap(),
            1.0
        );

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let cached: bool = conn.exists(redis_key(&prefix, &k, "sf")).await.unwrap();
        assert!(cached, "setup must create a long-lived suppression cache");

        runtime::async_sleep(Duration::from_millis(1_050)).await;

        assert_eq!(rl.suppressed().get(&k).await.unwrap().total, 0);
        let cached: bool = conn.exists(redis_key(&prefix, &k, "sf")).await.unwrap();
        assert!(
            !cached,
            "history eviction must invalidate the cached factor"
        );

        let decision = rl.suppressed().inc(&k, &rate, 1).await.unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "fresh usage after eviction must not use the stale full-suppression cache: {decision:?}"
        );
    });
}

/// Cleanup removes every suppressed key for stale entities while preserving recently read state.
#[test]
fn redis_state_suppressed_cleanup_removes_only_stale_entities() {
    let url = redis_url();

    runtime::block_on(async {
        let (rl, prefix) = build_limiter(&url, 10, 1000, 1.0, 60_000).await;
        let rate = RateLimit::per_second(1f64).unwrap();
        let stale_key = key("stale");
        let active_key = key("active");

        for k in [&stale_key, &active_key] {
            let decision = rl.suppressed().inc(k, &rate, 10).await.unwrap();
            assert!(
                matches!(decision, RateLimitDecision::Allowed),
                "seeding exact-hard usage: {decision:?}"
            );
            let decision = rl.suppressed().inc(k, &rate, 1).await.unwrap();
            assert!(
                matches!(
                    decision,
                    RateLimitDecision::Suppressed {
                        suppression_factor: 1.0,
                        is_allowed: false,
                    }
                ),
                "seeding declined usage: {decision:?}"
            );
        }

        runtime::async_sleep(Duration::from_millis(250)).await;
        let snapshot = rl.suppressed().get(&active_key).await.unwrap();
        assert_eq!((snapshot.total, snapshot.total_declined), (11, 1));

        rl.suppressed().cleanup(100).await.unwrap();

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        for suffix in ["h", "hd", "a", "w", "t", "d", "sf"] {
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
        let snapshot = rl.suppressed().get(&active_key).await.unwrap();
        assert_eq!((snapshot.total, snapshot.total_declined), (11, 1));
    });
}
