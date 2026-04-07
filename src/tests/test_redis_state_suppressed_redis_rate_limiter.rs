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

use super::runtime;
use super::common::{redis_url, unique_prefix, key, key_gen};

use crate::common::{RateType, SuppressionFactorCacheMs};
use crate::hybrid::SyncIntervalMs;
use crate::{
    HardLimitFactor, LocalRateLimiterOptions, RateGroupSizeMs, RateLimit, RateLimitDecision,
    RateLimiter, RateLimiterOptions, RedisKey, RedisRateLimiterOptions, WindowSizeSeconds,
};

/// Build a rate limiter and return its unique prefix.
async fn build_limiter(
    url: &str,
    window_size_seconds: u64,
    rate_group_size_ms: u64,
    hard_limit_factor: f64,
    suppression_factor_cache_ms: u64,
) -> (std::sync::Arc<RateLimiter>, RedisKey) {
    let client = redis::Client::open(url).unwrap();
    let cm = client.get_connection_manager().await.unwrap();
    let prefix = unique_prefix();

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
            prefix: Some(prefix.clone()),
            window_size_seconds: WindowSizeSeconds::try_from(window_size_seconds).unwrap(),
            rate_group_size_ms: RateGroupSizeMs::try_from(rate_group_size_ms).unwrap(),
            hard_limit_factor: HardLimitFactor::try_from(hard_limit_factor).unwrap(),
            suppression_factor_cache_ms: SuppressionFactorCacheMs::try_from(
                suppression_factor_cache_ms,
            )
            .unwrap(),
            sync_interval_ms: SyncIntervalMs::default(),
        },
    };

    (std::sync::Arc::new(RateLimiter::new(options)), prefix)
}

/// Construct the canonical Redis key for a given suffix using the key generator.
fn redis_key(prefix: &RedisKey, user_key: &RedisKey, suffix: &str) -> String {
    let kg = key_gen(prefix, RateType::Suppressed);
    match suffix {
        "h"  => kg.get_hash_key(user_key),
        "hd" => kg.get_hash_declined_key(user_key),
        "a"  => kg.get_active_keys(user_key),
        "w"  => kg.get_window_limit_key(user_key),
        "t"  => kg.get_total_count_key(user_key),
        "d"  => kg.get_total_declined_key(user_key),
        "sf" => kg.get_suppression_factor_key(user_key),
        _    => panic!("unknown suffix for suppressed rate type: {suffix}"),
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
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        let d = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 3)
            .await
            .unwrap();
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
        let declined: u64 = conn
            .get(redis_key(&prefix, &k, "d"))
            .await
            .unwrap_or(0u64);
        assert_eq!(declined, 0, "declined count should be 0");

        // The main hash must have the increment recorded.
        let hash: HashMap<String, u64> =
            conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        let hash_sum: u64 = hash.values().sum();
        assert_eq!(hash_sum, 3, "hash sum should equal the increment");

        // The declined hash must be empty.
        let hash_d: HashMap<String, u64> =
            conn.hgetall(redis_key(&prefix, &k, "hd")).await.unwrap();
        assert!(hash_d.is_empty(), "declined hash should be empty");
    });
}

/// Every call (allowed or denied) increments the total count key.  Denied calls also
/// increment the declined count key and record in the declined hash.
#[test]
fn redis_state_suppressed_denied_calls_increment_both_total_and_declined() {
    let url = redis_url();

    runtime::block_on(async {
        // hard_limit_factor=1 => soft == hard; any request past the window limit is denied.
        // Small cache so suppression_factor is recomputed on the very next call.
        let (rl, prefix) = build_limiter(&url, 10, 1000, 10.0, 1).await;
        let k = key("k");
        // window_limit = 10 * 1 * 10 = 100; hard cap is easy to hit with a big batch.
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        // Drive total_count past the hard_limit in one shot so sf=1.0 on the next call.
        let _ = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 100)
            .await
            .unwrap();

        // Allow the sf cache to expire.
        runtime::async_sleep(Duration::from_millis(10)).await;

        // Next call should be denied (sf == 1.0).
        let d = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(
            matches!(
                d,
                RateLimitDecision::Suppressed {
                    suppression_factor,
                    is_allowed: false
                } if suppression_factor == 1.0
            ),
            "d: {d:?}"
        );

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // Declined count must be > 0.
        let declined: u64 = conn.get(redis_key(&prefix, &k, "d")).await.unwrap();
        assert!(declined > 0, "declined count should be > 0 after a denial");

        // Declined hash must have at least one entry.
        let hash_d: HashMap<String, u64> =
            conn.hgetall(redis_key(&prefix, &k, "hd")).await.unwrap();
        assert!(
            !hash_d.is_empty(),
            "declined hash must have entries after a denial"
        );

        // Total count must be >= declined count.
        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        assert!(
            total >= declined,
            "total ({total}) must be >= declined ({declined})"
        );
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
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        // Drive total just past soft limit (11) to enter suppression zone.
        for _ in 0..11 {
            let _ = rl
                .redis()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
        }

        // Wait for the `sf` key written by the inc loop to expire, so that the
        // subsequent `get_suppression_factor` call is guaranteed to recompute and
        // re-write the `sf` key with a fresh cache_ms TTL.
        runtime::async_sleep(Duration::from_millis(cache_ms + 10)).await;

        // Trigger a fresh computation. This writes `sf` with `cache_ms` TTL.
        let sf = rl
            .redis()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();

        // Verify the return value directly — no separate Redis read needed here.
        assert!(sf > 0.0, "suppression factor should be > 0 past the soft limit, got {sf}");
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
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        // Push past soft limit to trigger sf caching.
        for _ in 0..11 {
            let _ = rl
                .redis()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
        }

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // PTTL returns -2 (not found), -1 (no expiry), or positive ms.
        let pttl: i64 = conn
            .pttl(redis_key(&prefix, &k, "sf"))
            .await
            .unwrap();

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

/// The hard window limit stored in the `w` key equals
/// `floor(window_size_seconds * rate_limit * hard_limit_factor)`.
#[test]
fn redis_state_suppressed_window_limit_key_equals_hard_limit() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size_seconds = 5_u64;
        let rate_limit_value = 4f64;
        let hard_limit_factor = 3.0_f64;
        // hard_window_limit = floor(5 * 4 * 3) = 60
        let expected_hard_limit = 60_u64;

        let (rl, prefix) = build_limiter(&url, window_size_seconds, 1000, hard_limit_factor, 100).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(rate_limit_value).unwrap();

        rl.redis().suppressed().inc(&k, &rate_limit, 1).await.unwrap();

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
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        // Fill past the hard limit so denials occur.
        let _ = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 100)
            .await
            .unwrap();
        runtime::async_sleep(Duration::from_millis(10)).await;

        // Make several more calls — some will be denied.
        for _ in 0..5 {
            let _ = rl
                .redis()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
        }

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
    });
}

/// After the window expires, both the total hash and the declined hash must be evicted,
/// and both counter keys must reflect the eviction.
#[test]
fn redis_state_suppressed_evicts_expired_buckets_from_both_hashes() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size_seconds = 1_u64;
        // hard_limit_factor=10 so the hard limit is 10×rate = 10.
        let (rl, prefix) = build_limiter(&url, window_size_seconds, 1000, 10.0, 1).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        // Drive past hard limit so there are entries in both hashes.
        let _ = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 10)
            .await
            .unwrap();
        runtime::async_sleep(Duration::from_millis(5)).await;
        let _ = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();

        // Wait for the window to expire.
        thread::sleep(Duration::from_millis(window_size_seconds * 1000 + 50));
        runtime::async_sleep(Duration::from_millis(10)).await;

        // A fresh inc triggers eviction inside the Lua script.
        let d = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let total: u64 = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        let hash: HashMap<String, u64> =
            conn.hgetall(redis_key(&prefix, &k, "h")).await.unwrap();
        let active_count: u64 = conn
            .zcard(redis_key(&prefix, &k, "a"))
            .await
            .unwrap();

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

        let sf = rl
            .redis()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();
        assert!((sf - 0.0).abs() < 1e-12, "sf: {sf}");

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // None of the data keys should exist.
        let total: Option<u64> = conn.get(redis_key(&prefix, &k, "t")).await.unwrap();
        let hash_len: u64 = conn
            .hlen(redis_key(&prefix, &k, "h"))
            .await
            .unwrap();
        assert!(
            total.is_none(),
            "total count key should not exist for an unknown key"
        );
        assert_eq!(
            hash_len, 0,
            "hash should be empty for an unknown key"
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
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        rl.redis().suppressed().inc(&a, &rate_limit, 4).await.unwrap();
        rl.redis().suppressed().inc(&b, &rate_limit, 9).await.unwrap();

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
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        let ae_key = active_entities_key(&prefix);

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // Not present before any call.
        let score_before: Option<f64> = conn.zscore(&ae_key, &**k).await.unwrap();
        assert!(score_before.is_none(), "key should not be in active_entities before inc");

        rl.redis().suppressed().inc(&k, &rate_limit, 1).await.unwrap();

        let score_after: Option<f64> = conn.zscore(&ae_key, &**k).await.unwrap();
        assert!(
            score_after.is_some(),
            "key should be in active_entities after inc"
        );
    });
}
