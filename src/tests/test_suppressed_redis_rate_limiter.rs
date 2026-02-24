use std::{env, time::Duration};

use redis::AsyncCommands;

use crate::common::SuppressionFactorCacheMs;
use crate::{
    HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision, RedisKey, RedisKeyGenerator,
    RedisRateLimiterOptions, SuppressedRedisRateLimiter, WindowSizeSeconds, common::RateType,
};

fn redis_url() -> String {
    env::var("REDIS_URL")
        .expect("REDIS_URL must be set to run redis integration tests (try `make test-redis`)")
}

fn unique_prefix() -> RedisKey {
    let n: u64 = rand::random();
    RedisKey::try_from(format!("trypema_test_{n}")).unwrap()
}

fn key(s: &str) -> RedisKey {
    RedisKey::try_from(s.to_string()).unwrap()
}

fn keygen(prefix: &RedisKey, rate_type: RateType) -> RedisKeyGenerator {
    RedisKeyGenerator::new(prefix.clone(), rate_type)
}

async fn suppressed_keys_exist(
    conn: &mut redis::aio::ConnectionManager,
    kg: &RedisKeyGenerator,
    key: &RedisKey,
) -> (bool, bool, bool, bool, bool) {
    let h_exists: bool = conn.exists(kg.get_hash_key(key)).await.unwrap();
    let a_exists: bool = conn.exists(kg.get_active_keys(key)).await.unwrap();
    let w_exists: bool = conn.exists(kg.get_window_limit_key(key)).await.unwrap();
    let t_exists: bool = conn.exists(kg.get_total_count_key(key)).await.unwrap();
    let sf_exists: bool = conn
        .exists(kg.get_suppression_factor_key(key))
        .await
        .unwrap();
    (h_exists, a_exists, w_exists, t_exists, sf_exists)
}

async fn get_total(
    conn: &mut redis::aio::ConnectionManager,
    kg: &RedisKeyGenerator,
    key: &RedisKey,
) -> u64 {
    conn.hget::<_, _, Option<u64>>(kg.get_total_count_key(key), "count")
        .await
        .unwrap()
        .unwrap_or(0)
}

async fn get_declined(
    conn: &mut redis::aio::ConnectionManager,
    kg: &RedisKeyGenerator,
    key: &RedisKey,
) -> u64 {
    conn.hget::<_, _, Option<u64>>(kg.get_total_count_key(key), "declined")
        .await
        .unwrap()
        .unwrap_or(0)
}

async fn set_zset_score_ms_ago(
    conn: &mut redis::aio::ConnectionManager,
    zset_key: &str,
    member: &RedisKey,
    score_ms_ago: u64,
) {
    let script = redis::Script::new(
        r#"
        local time_array = redis.call("TIME")
        local now_ms = tonumber(time_array[1]) * 1000 + math.floor(tonumber(time_array[2]) / 1000)

        local member = ARGV[1]
        local ms_ago = tonumber(ARGV[2])
        local score = now_ms - ms_ago

        redis.call("ZADD", KEYS[1], score, member)
        return score
    "#,
    );

    let _: u64 = script
        .key(zset_key)
        .arg(member.to_string())
        .arg(score_ms_ago)
        .invoke_async(conn)
        .await
        .unwrap();
}

async fn build_limiter(
    url: &str,
    window_size_seconds: u64,
    rate_group_size_ms: u128,
    hard_limit_factor: f64,
) -> (
    SuppressedRedisRateLimiter,
    redis::aio::ConnectionManager,
    RedisKey,
) {
    build_limiter_with_cache_ms(
        url,
        window_size_seconds,
        rate_group_size_ms,
        hard_limit_factor,
        *SuppressionFactorCacheMs::default(),
    )
    .await
}

async fn build_limiter_with_cache_ms(
    url: &str,
    window_size_seconds: u64,
    rate_group_size_ms: u128,
    hard_limit_factor: f64,
    suppression_factor_cache_ms: u64,
) -> (
    SuppressedRedisRateLimiter,
    redis::aio::ConnectionManager,
    RedisKey,
) {
    let client = redis::Client::open(url).unwrap();
    let cm = client.get_connection_manager().await.unwrap();
    let prefix = unique_prefix();

    let limiter = SuppressedRedisRateLimiter::new(RedisRateLimiterOptions {
        client: cm.clone(),
        prefix: Some(prefix.clone()),
        window_size_seconds: WindowSizeSeconds::try_from(window_size_seconds).unwrap(),
        rate_group_size_ms: RateGroupSizeMs::try_from(rate_group_size_ms).unwrap(),
        hard_limit_factor: HardLimitFactor::try_from(hard_limit_factor).unwrap(),
        suppression_factor_cache_ms: SuppressionFactorCacheMs::try_from(
            suppression_factor_cache_ms,
        )
        .unwrap(),
    });

    (limiter, cm, prefix)
}

#[test]
fn get_suppression_factor_fresh_key_returns_zero_and_sets_cache_ttl() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let cache_ms = 500_u64;
        let (limiter, cm, prefix) =
            build_limiter_with_cache_ms(&url, 10, 100, 2f64, cache_ms).await;
        let k = key("k");
        let kg = keygen(&prefix, RateType::Suppressed);

        let mut conn = cm.clone();
        let sf_key = kg.get_suppression_factor_key(&k);
        let _: u64 = conn.del(&sf_key).await.unwrap();

        // With no window_limit key set yet, suppression factor resolves to 0.
        let sf = limiter.get_suppression_factor(&k).await.unwrap();
        assert!((sf - 0.0).abs() < 1e-12, "sf: {sf}");

        let pttl: i64 = conn.pttl(&sf_key).await.unwrap();
        assert!(pttl > 0, "expected sf key to have positive TTL");
        assert!(
            pttl <= cache_ms as i64 + 200,
            "expected sf TTL <= cache_ms (+slack), got {pttl}"
        );
    });
}

#[test]
fn get_suppression_factor_returns_cached_when_in_range_and_does_not_overwrite_ttl() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 10, 100, 2f64).await;
        let k = key("k");
        let kg = keygen(&prefix, RateType::Suppressed);

        let mut conn = cm.clone();
        let sf_key = kg.get_suppression_factor_key(&k);

        // Even if the underlying state would imply a different suppression factor, a valid
        // cached value must be returned as-is.
        let cached_val = 0.123_f64;
        let _: () = conn.set_ex(&sf_key, cached_val, 60).await.unwrap();

        // Seed state that would otherwise drive suppression_factor to 1.0.
        let w_key = kg.get_window_limit_key(&k);
        let t_key = kg.get_total_count_key(&k);
        let _: () = conn.set(&w_key, 10_u64).await.unwrap();
        let _: () = conn.hset(&t_key, "count", 10_u64).await.unwrap();

        let sf = limiter.get_suppression_factor(&k).await.unwrap();
        assert!((sf - cached_val).abs() < 1e-12, "sf: {sf}");

        // TTL should remain ~60s (not overwritten to suppression_factor_cache_ms).
        let pttl: i64 = conn.pttl(&sf_key).await.unwrap();
        assert!(
            pttl > 50_000,
            "expected cached TTL to remain large, got {pttl}"
        );
    });
}

#[test]
fn get_suppression_factor_invalid_cached_negative_is_recomputed_and_overwritten() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let cache_ms = 500_u64;
        let (limiter, cm, prefix) =
            build_limiter_with_cache_ms(&url, 10, 100, 2f64, cache_ms).await;
        let k = key("k");
        let kg = keygen(&prefix, RateType::Suppressed);

        let mut conn = cm.clone();
        let sf_key = kg.get_suppression_factor_key(&k);
        let w_key = kg.get_window_limit_key(&k);
        let t_key = kg.get_total_count_key(&k);

        // Create state that forces suppression_factor to 1.0.
        let _: () = conn.set(&w_key, 10_u64).await.unwrap();
        let _: () = conn.hset(&t_key, "count", 10_u64).await.unwrap();

        // Write invalid cached value.
        let _: () = conn.set_ex(&sf_key, -0.5_f64, 60).await.unwrap();

        let sf = limiter.get_suppression_factor(&k).await.unwrap();
        assert!((sf - 1.0).abs() < 1e-12, "sf: {sf}");

        let cached: Option<f64> = conn.get(&sf_key).await.unwrap();
        let cached = cached.expect("expected sf to be cached");
        assert!((cached - 1.0).abs() < 1e-12, "cached: {cached}");

        let pttl: i64 = conn.pttl(&sf_key).await.unwrap();
        assert!(pttl > 0, "expected overwritten sf key to have TTL");
        assert!(
            pttl <= cache_ms as i64 + 200,
            "expected overwritten TTL <= cache_ms (+slack), got {pttl}"
        );
    });
}

#[test]
fn get_suppression_factor_invalid_cached_gt_one_is_recomputed_and_overwritten() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let cache_ms = 500_u64;
        let (limiter, cm, prefix) =
            build_limiter_with_cache_ms(&url, 10, 100, 2f64, cache_ms).await;
        let k = key("k");
        let kg = keygen(&prefix, RateType::Suppressed);

        let mut conn = cm.clone();
        let sf_key = kg.get_suppression_factor_key(&k);
        let w_key = kg.get_window_limit_key(&k);
        let t_key = kg.get_total_count_key(&k);

        // Create state that forces suppression_factor to 1.0.
        let _: () = conn.set(&w_key, 10_u64).await.unwrap();
        let _: () = conn.hset(&t_key, "count", 10_u64).await.unwrap();

        // Write invalid cached value.
        let _: () = conn.set_ex(&sf_key, 2.0_f64, 60).await.unwrap();

        let sf = limiter.get_suppression_factor(&k).await.unwrap();
        assert!((sf - 1.0).abs() < 1e-12, "sf: {sf}");

        let cached: Option<f64> = conn.get(&sf_key).await.unwrap();
        let cached = cached.expect("expected sf to be cached");
        assert!((cached - 1.0).abs() < 1e-12, "cached: {cached}");

        let pttl: i64 = conn.pttl(&sf_key).await.unwrap();
        assert!(pttl > 0, "expected overwritten sf key to have TTL");
        assert!(
            pttl <= cache_ms as i64 + 200,
            "expected overwritten TTL <= cache_ms (+slack), got {pttl}"
        );
    });
}

#[test]
fn get_suppression_factor_returns_zero_when_window_limit_missing_even_if_totals_exist() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 10, 100, 2f64).await;
        let k = key("k");
        let kg = keygen(&prefix, RateType::Suppressed);

        let mut conn = cm.clone();
        let w_key = kg.get_window_limit_key(&k);
        let t_key = kg.get_total_count_key(&k);
        let sf_key = kg.get_suppression_factor_key(&k);

        // Simulate partial state: totals present, but no window_limit stored.
        let _: () = conn.hset(&t_key, "count", 999_u64).await.unwrap();
        let _: u64 = conn.del(&w_key).await.unwrap();
        let _: u64 = conn.del(&sf_key).await.unwrap();

        let sf = limiter.get_suppression_factor(&k).await.unwrap();
        assert!((sf - 0.0).abs() < 1e-12, "sf: {sf}");
    });
}

#[test]
fn get_suppression_factor_missing_total_count_field_is_treated_as_zero() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 10, 100, 2f64).await;
        let k = key("k");
        let kg = keygen(&prefix, RateType::Suppressed);

        let mut conn = cm.clone();
        let w_key = kg.get_window_limit_key(&k);
        let t_key = kg.get_total_count_key(&k);
        let sf_key = kg.get_suppression_factor_key(&k);

        // Hard window limit (window_size * rate * hard_limit_factor).
        let _: () = conn.set(&w_key, 20_u64).await.unwrap();

        // Write only declined; omit the "count" field.
        let _: () = conn.hset(&t_key, "declined", 5_u64).await.unwrap();
        let _: u64 = conn.del(&sf_key).await.unwrap();

        let sf = limiter.get_suppression_factor(&k).await.unwrap();
        assert!((sf - 0.0).abs() < 1e-12, "sf: {sf}");
    });
}

#[test]
fn get_suppression_factor_computed_uses_last_second_peak_rate_at_threshold_boundary() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // window_size=10s, hard_limit_factor=2 => window_limit=20 and threshold is 10.
        // We drive total_count to exactly 10 with a burst in < 1s so perceived_rate uses
        // last-second peak (10 req/s) instead of average (1 req/s).
        let cache_ms = 50_u64;
        let (limiter, _cm, _prefix) =
            build_limiter_with_cache_ms(&url, 10, 100, 2f64, cache_ms).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        for _ in 0..10 {
            let _ = limiter.inc(&k, &rate_limit, 1).await.unwrap();
        }

        // Ensure any cached suppression_factor from the burst expires so we recompute.
        tokio::time::sleep(Duration::from_millis(cache_ms + 25)).await;

        let sf = limiter.get_suppression_factor(&k).await.unwrap();

        // rate_limit = 1 req/s; perceived_rate = 10 req/s => sf = 1 - 1/10 = 0.9
        let expected = 1.0_f64 - (1.0_f64 / 10.0_f64);
        assert!(
            (sf - expected).abs() < 1e-12,
            "sf: {sf}, expected: {expected}"
        );
    });
}

#[test]
fn get_suppression_factor_evicts_out_of_window_usage_and_resets_admission() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let window_size_seconds = 1_u64;
        let cache_ms = 50_u64;
        let (limiter, _cm, _prefix) =
            build_limiter_with_cache_ms(&url, window_size_seconds, 1000, 2f64, cache_ms).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        // window_limit = window_size * rate_limit * hard_limit_factor = 1 * 1 * 2 = 2.
        // We record 2 calls in the window, then wait for the full window to pass. If eviction
        // does not occur, the next increment would see total_count >= window_limit and go to full
        // suppression (sf=1.0). If eviction occurs, the next increment is Allowed.
        let d1 = limiter.inc(&k, &rate_limit, 2).await.unwrap();

        assert!(matches!(d1, RateLimitDecision::Allowed), "d1: {:?}", d1);

        tokio::time::sleep(Duration::from_millis(window_size_seconds * 1000 + 50)).await;
        tokio::time::sleep(Duration::from_millis(cache_ms + 25)).await;

        eprintln!(">>>>>>>>>>>>>");
        let sf = limiter.get_suppression_factor(&k).await.unwrap();
        eprintln!("sf: {sf}");
        assert!((sf - 0.0).abs() < 1e-12, "sf: {sf}");

        let d2 = limiter.inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(d2, RateLimitDecision::Allowed), "d2: {:?}", d2);
    });
}

#[test]
fn suppression_factor_one_returns_suppressed_denied_and_increments_declined() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 1, 1000, 10f64).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();

        let kg = keygen(&prefix, RateType::Suppressed);

        // Force suppression factor and deny.
        let mut conn = cm.clone();
        let sf_key = kg.get_suppression_factor_key(&k);
        let _: () = conn.set_ex(&sf_key, 1.0_f64, 60).await.unwrap();

        let decision = limiter.inc(&k, &rate_limit, 1).await.unwrap();

        assert!(matches!(
            decision,
            RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: false
            } if (suppression_factor - 1.0).abs() < 1e-12
        ));

        let mut conn = cm.clone();
        assert_eq!(get_total(&mut conn, &kg, &k).await, 1);
        assert_eq!(get_declined(&mut conn, &kg, &k).await, 1);
    });
}

#[test]
fn suppression_factor_zero_bypasses_suppression_and_declined_not_incremented() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 1, 1000, 10f64).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();

        let kg = keygen(&prefix, RateType::Suppressed);

        let mut conn = cm.clone();
        let sf_key = kg.get_suppression_factor_key(&k);
        let _: () = conn.set_ex(&sf_key, 0.0_f64, 60).await.unwrap();

        let decision = limiter.inc(&k, &rate_limit, 1).await.unwrap();

        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "decision: {:?}",
            decision
        );

        let mut conn = cm.clone();
        assert_eq!(get_total(&mut conn, &kg, &k).await, 1);
        assert_eq!(get_declined(&mut conn, &kg, &k).await, 0);
    });
}

#[test]
fn hard_limit_factor_one_never_allows_when_suppression_factor_is_forced_to_one() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // hard_limit_factor=1 => hard cutoff at the base window capacity.
        let (limiter, cm, prefix) = build_limiter(&url, 1, 1000, 1f64).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        let kg = keygen(&prefix, RateType::Suppressed);

        let mut conn = cm.clone();
        let sf_key = kg.get_suppression_factor_key(&k);
        let _: () = conn.set_ex(&sf_key, 1.0_f64, 60).await.unwrap();

        // Force suppression to 100% so we get deterministic non-admission.
        let d1 = limiter.inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(
            d1,
            RateLimitDecision::Suppressed {
                is_allowed: false,
                suppression_factor,
            } if (suppression_factor - 1.0).abs() < 1e-12
        ));

        let d2 = limiter.inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(
            d2,
            RateLimitDecision::Suppressed {
                is_allowed: false,
                suppression_factor,
            } if (suppression_factor - 1.0).abs() < 1e-12
        ));

        let mut conn = cm.clone();
        assert_eq!(get_total(&mut conn, &kg, &k).await, 2);
        assert_eq!(get_declined(&mut conn, &kg, &k).await, 2);
    });
}

#[test]
fn suppression_factor_gt_one_is_invalid_and_is_recomputed() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 1, 1000, 10f64).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();

        let kg = keygen(&prefix, RateType::Suppressed);
        let mut conn = cm.clone();
        let sf_key = kg.get_suppression_factor_key(&k);
        let _: () = conn.set_ex(&sf_key, 2.0_f64, 60).await.unwrap();

        let decision = limiter.inc(&k, &rate_limit, 1).await.unwrap();

        // Invalid cached suppression factors should be ignored and recomputed.
        assert!(matches!(decision, RateLimitDecision::Allowed));

        let cached: Option<f64> = conn.get(&sf_key).await.unwrap();
        let cached = cached.expect("expected suppression factor to be cached");
        assert!(
            (0.0..=1.0).contains(&cached),
            "cached sf in valid range: {cached}"
        );
        assert!((cached - 0.0).abs() < 1e-12, "cached sf: {cached}");
    });
}

#[test]
fn suppression_factor_negative_is_invalid_and_is_recomputed() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 1, 1000, 10f64).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();

        let kg = keygen(&prefix, RateType::Suppressed);

        let mut conn = cm.clone();
        let sf_key = kg.get_suppression_factor_key(&k);
        let _: () = conn.set_ex(&sf_key, -0.01_f64, 60).await.unwrap();

        let decision = limiter.inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(decision, RateLimitDecision::Allowed));

        let cached: Option<f64> = conn.get(&sf_key).await.unwrap();
        let cached = cached.expect("expected suppression factor to be cached");
        assert!(
            (0.0..=1.0).contains(&cached),
            "cached sf in valid range: {cached}"
        );
        assert!((cached - 0.0).abs() < 1e-12, "cached sf: {cached}");

        let mut conn = cm.clone();
        let total = get_total(&mut conn, &kg, &k).await;
        assert_eq!(total, 1, "expected total to be 1, instead got {total}");

        let declined = get_declined(&mut conn, &kg, &k).await;
        assert_eq!(
            declined, 0,
            "expected declined to be 0, instead got {declined}"
        );
    });
}

#[test]
fn suppression_factor_cache_is_written_and_has_positive_ttl() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let rate_group_size_ms = 200_u128;
        let (limiter, cm, prefix) = build_limiter(&url, 10, rate_group_size_ms, 10f64).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        let kg = keygen(&prefix, RateType::Suppressed);
        let mut conn = cm.clone();
        let sf_key = kg.get_suppression_factor_key(&k);
        let _: u64 = conn.del(&sf_key).await.unwrap();

        // With no usage, suppression_factor should resolve to 0 and bypass suppression.
        let decision = limiter.inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(decision, RateLimitDecision::Allowed));

        let pttl: i64 = conn.pttl(&sf_key).await.unwrap();
        assert!(pttl > 0, "expected sf key to have positive TTL");
        assert!(
            pttl <= *SuppressionFactorCacheMs::default() as i64 + 100,
            "expected sf TTL <= suppression_factor_cache_ms (+slack), got {pttl}"
        );
    });
}

#[test]
fn suppression_factor_recompute_does_not_error_when_no_recent_activity_in_last_1s() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Large window so buckets can be >1s old but still in-window.
        let (limiter, cm, prefix) = build_limiter(&url, 10, 50, 10f64).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        // Seed usage in the suppressed limiter keyspace.
        limiter.inc(&k, &rate_limit, 10).await.unwrap();

        let kg = keygen(&prefix, RateType::Suppressed);

        // Force all accepted activity to be older than 1s.
        let mut conn = cm.clone();
        let ak_key = kg.get_active_keys(&k);
        let members: Vec<String> = conn.zrange(&ak_key, 0, -1).await.unwrap();
        for m in members {
            // Make each member score older than 1s but within window.
            // Member strings are bucket timestamps; treat as raw ZSET members.
            let script = redis::Script::new(
                r#"
                local time_array = redis.call("TIME")
                local now_ms = tonumber(time_array[1]) * 1000 + math.floor(tonumber(time_array[2]) / 1000)
                local member = ARGV[1]
                local score = now_ms - 1500
                redis.call("ZADD", KEYS[1], score, member)
                return score
            "#,
            );
            let _: u64 = script
                .key(&ak_key)
                .arg(m)
                .invoke_async(&mut conn)
                .await
                .unwrap();
        }

        // Force suppression-factor recompute.
        let sf_key = kg.get_suppression_factor_key(&k);
        let _: u64 = conn.del(&sf_key).await.unwrap();

        let res = limiter.inc(&k, &rate_limit, 1).await;
        assert!(res.is_ok(), "expected recompute to not error, instead got {res:?}");
    });
}

#[test]
fn cleanup_removes_stale_entities_for_both_keyspaces_including_sf() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 10, 200, 10f64).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(1000f64).unwrap();

        // Create some state.
        let _ = limiter.inc(&k, &rate_limit, 1).await.unwrap();

        let kg = keygen(&prefix, RateType::Suppressed);

        // Ensure sf exists so we can assert it is deleted.
        let mut conn = cm.clone();
        let sf_key = kg.get_suppression_factor_key(&k);
        let _: () = conn.set_ex(&sf_key, 0.5_f64, 60).await.unwrap();
        let sf_exists_before: bool = conn.exists(&sf_key).await.unwrap();
        assert!(sf_exists_before, "expected sf to exist before cleanup");

        // Force staleness in the shared active_entities index.
        let ae_key = kg.get_active_entities_key();
        set_zset_score_ms_ago(&mut conn, &ae_key, &k, 1050).await;

        limiter.cleanup(1000).await.unwrap();

        let (h, ak, w, t, sf) = suppressed_keys_exist(&mut conn, &kg, &k).await;
        assert!(!h && !ak && !w && !t && !sf, "expected keyspace deleted");
    });
}

#[test]
fn verify_suppression_factor_calculation_spread_redis() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, _cm, _prefix) = build_limiter(&url, 10, 100, 10f64).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        // fill up in the first 3 seconds
        for _ in 0..20 {
            let _ = limiter.inc(&k, &rate_limit, 1).await.unwrap();
            tokio::time::sleep(Duration::from_millis(3000 / 20)).await;
        }

        // wait for 1.5 seconds
        tokio::time::sleep(Duration::from_millis(1200)).await;

        let expected_suppression_factor = 1f64 - (1f64 / 2f64);

        let decision = limiter.inc(&k, &rate_limit, 1).await.unwrap();

        eprintln!("decision: {:?}", decision);

        assert!(
            matches!(
                decision,
                RateLimitDecision::Suppressed {
                    suppression_factor,
                    ..
                } if (suppression_factor - expected_suppression_factor).abs() < 1e-12
            ),
            "decision: {:?}",
            decision
        );
    });
}

#[test]
fn verify_suppression_factor_calculation_last_second_redis() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, _cm, _prefix) = build_limiter(&url, 10, 100, 10f64).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        let _ = limiter.inc(&k, &rate_limit, 10).await.unwrap();
        // wait for 1s to pass
        tokio::time::sleep(Duration::from_millis(1001)).await;

        let _ = limiter.inc(&k, &rate_limit, 20).await.unwrap();
        // Allow time for the suppression_factor to expire
        tokio::time::sleep(Duration::from_millis(101)).await;

        let expected_suppression_factor = 1f64 - (1f64 / 20f64);

        let decision = limiter.inc(&k, &rate_limit, 1).await.unwrap();

        assert!(
            matches!(
                decision,
                RateLimitDecision::Suppressed {
                    suppression_factor,
                    ..
                } if (suppression_factor - expected_suppression_factor).abs() < 1e-12
            ),
            "decision: {:?}, expected sf: {expected_suppression_factor}",
            decision
        );
    });
}

#[test]
fn verify_hard_limit_rejects() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, _cm, _prefix) = build_limiter(&url, 10, 100, 10f64).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        let _ = limiter.inc(&k, &rate_limit, 100).await.unwrap();
        // wait for 1s to pass
        tokio::time::sleep(Duration::from_millis(1001)).await;

        let _ = limiter.inc(&k, &rate_limit, 20).await.unwrap();

        let decision = limiter.inc(&k, &rate_limit, 1).await.unwrap();

        assert!(
            matches!(
                decision,
                RateLimitDecision::Suppressed {
                    suppression_factor,
                    is_allowed: false,
                } if suppression_factor == 1.0f64
            ),
            "decision: {:?}",
            decision
        );
    });
}
