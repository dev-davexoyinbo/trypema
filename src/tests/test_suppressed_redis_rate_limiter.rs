use std::{env, time::Duration};

use redis::AsyncCommands;

use crate::{
    AbsoluteRedisRateLimiter, HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision,
    RedisKey, RedisKeyGenerator, RedisRateLimiterOptions, SuppressedRedisRateLimiter,
    WindowSizeSeconds, common::RateType,
};
use crate::common::SuppressionFactorCacheMs;

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
) -> Option<u64> {
    conn.get(kg.get_total_count_key(key)).await.unwrap()
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
    rate_group_size_ms: u64,
    hard_limit_factor: f64,
) -> (
    SuppressedRedisRateLimiter,
    redis::aio::ConnectionManager,
    RedisKey,
) {
    let client = redis::Client::open(url).unwrap();
    let cm = client.get_connection_manager().await.unwrap();
    let prefix = unique_prefix();

    let limiter = SuppressedRedisRateLimiter::new(RedisRateLimiterOptions {
        connection_manager: cm.clone(),
        prefix: Some(prefix.clone()),
        window_size_seconds: WindowSizeSeconds::try_from(window_size_seconds).unwrap(),
        rate_group_size_ms: RateGroupSizeMs::try_from(rate_group_size_ms).unwrap(),
        hard_limit_factor: HardLimitFactor::try_from(hard_limit_factor).unwrap(),
        suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
    });

    (limiter, cm, prefix)
}

#[test]
fn denied_path_returns_suppressed_and_does_not_increment_accepted() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 1, 1000, 10f64).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();

        let kg_observed = keygen(&prefix, RateType::SuppressedObserved);
        let kg_accepted = keygen(&prefix, RateType::Suppressed);

        // Force suppression factor and deny.
        let mut conn = cm.clone();
        let sf_key = kg_observed.get_suppression_factor_key(&k);
        let _: () = conn.set_ex(&sf_key, 0.25_f64, 60).await.unwrap();

        let mut rng = |_p: f64| false;
        let decision = limiter
            .inc_with_rng(&k, &rate_limit, 1, &mut rng)
            .await
            .unwrap();

        assert!(matches!(
            decision,
            RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: false
            } if (suppression_factor - 0.25).abs() < 1e-12
        ));

        let mut conn = cm.clone();
        assert_eq!(get_total(&mut conn, &kg_observed, &k).await, Some(1));
        assert_eq!(get_total(&mut conn, &kg_accepted, &k).await, None);
    });
}

#[test]
fn allowed_path_returns_suppressed_and_increments_both() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 1, 1000, 10f64).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();

        let kg_observed = keygen(&prefix, RateType::SuppressedObserved);
        let kg_accepted = keygen(&prefix, RateType::Suppressed);

        let mut conn = cm.clone();
        let sf_key = kg_observed.get_suppression_factor_key(&k);
        let _: () = conn.set_ex(&sf_key, 0.25_f64, 60).await.unwrap();

        let mut rng = |_p: f64| true;
        let decision = limiter
            .inc_with_rng(&k, &rate_limit, 1, &mut rng)
            .await
            .unwrap();

        assert!(
            matches!(
                decision,
                RateLimitDecision::Suppressed {
                    suppression_factor,
                    ..
                } if (suppression_factor - 0.25).abs() < 1e-12
            ),
            "expected suppression factor to be 0.25 instead got {decision:?}"
        );

        let mut conn = cm.clone();
        assert_eq!(get_total(&mut conn, &kg_observed, &k).await, Some(1));
        assert_eq!(get_total(&mut conn, &kg_accepted, &k).await, Some(1));
    });
}

#[test]
fn hard_limit_rejection_is_returned_as_rejected() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // hard_limit_factor=1 => hard cutoff at the base window capacity.
        let (limiter, cm, prefix) = build_limiter(&url, 1, 1000, 1f64).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        let kg_observed = keygen(&prefix, RateType::SuppressedObserved);
        let kg_accepted = keygen(&prefix, RateType::Suppressed);

        let mut conn = cm.clone();
        let sf_key = kg_observed.get_suppression_factor_key(&k);
        let _: () = conn.set_ex(&sf_key, 0.25_f64, 60).await.unwrap();

        let mut rng = |_p: f64| true;

        let d1 = limiter
            .inc_with_rng(&k, &rate_limit, 1, &mut rng)
            .await
            .unwrap();
        assert!(matches!(
            d1,
            RateLimitDecision::Suppressed {
                is_allowed: true,
                ..
            }
        ));

        let d2 = limiter
            .inc_with_rng(&k, &rate_limit, 1, &mut rng)
            .await
            .unwrap();
        assert!(matches!(d2, RateLimitDecision::Rejected { .. }));

        let mut conn = cm.clone();
        assert_eq!(get_total(&mut conn, &kg_observed, &k).await, Some(2));
        assert_eq!(get_total(&mut conn, &kg_accepted, &k).await, Some(1));
    });
}

#[test]
fn suppression_factor_is_clamped_to_one() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 1, 1000, 10f64).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();

        let kg_observed = keygen(&prefix, RateType::SuppressedObserved);
        let mut conn = cm.clone();
        let sf_key = kg_observed.get_suppression_factor_key(&k);
        let _: () = conn.set_ex(&sf_key, 2.0_f64, 60).await.unwrap();

        let mut rng = |p: f64| {
            assert!((p - 0.0).abs() < 1e-12);
            false
        };

        let decision = limiter
            .inc_with_rng(&k, &rate_limit, 1, &mut rng)
            .await
            .unwrap();
        assert!(matches!(
            decision,
            RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: false
            } if (suppression_factor - 1.0).abs() < 1e-12
        ));
    });
}

#[test]
fn nonpositive_suppression_factor_bypasses_suppression_rng_not_called() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 1, 1000, 10f64).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();

        let kg_observed = keygen(&prefix, RateType::SuppressedObserved);
        let kg_accepted = keygen(&prefix, RateType::Suppressed);

        let mut conn = cm.clone();
        let sf_key = kg_observed.get_suppression_factor_key(&k);
        let _: () = conn.set_ex(&sf_key, -0.01_f64, 60).await.unwrap();

        let mut rng = |_p: f64| panic!("rng should not be called when suppression_factor <= 0");
        let decision = limiter
            .inc_with_rng(&k, &rate_limit, 1, &mut rng)
            .await
            .unwrap();
        assert!(matches!(decision, RateLimitDecision::Allowed));

        let mut conn = cm.clone();
        assert_eq!(get_total(&mut conn, &kg_observed, &k).await, Some(1));
        assert_eq!(get_total(&mut conn, &kg_accepted, &k).await, Some(1));
    });
}

#[test]
fn suppression_factor_cache_is_written_and_has_positive_ttl() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let rate_group_size_ms = 200_u64;
        let (limiter, cm, prefix) = build_limiter(&url, 10, rate_group_size_ms, 10f64).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        let kg_observed = keygen(&prefix, RateType::SuppressedObserved);
        let mut conn = cm.clone();
        let sf_key = kg_observed.get_suppression_factor_key(&k);
        let _: u64 = conn.del(&sf_key).await.unwrap();

        // With no accepted usage, suppression_factor should resolve to 0 and bypass suppression.
        let mut rng = |_p: f64| panic!("rng should not be used when suppression_factor <= 0");
        let decision = limiter
            .inc_with_rng(&k, &rate_limit, 1, &mut rng)
            .await
            .unwrap();
        assert!(matches!(decision, RateLimitDecision::Allowed));

        let pttl: i64 = conn.pttl(&sf_key).await.unwrap();
        assert!(pttl > 0, "expected sf key to have positive TTL");
        assert!(
            pttl <= rate_group_size_ms as i64 + 50,
            "expected sf TTL <= rate_group_size_ms (+small slack), got {pttl}"
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

        // Seed accepted usage to capacity using the accepted limiter directly.
        let accepted = AbsoluteRedisRateLimiter::with_rate_type(
            RateType::Suppressed,
            RedisRateLimiterOptions {
                connection_manager: cm.clone(),
                prefix: Some(prefix.clone()),
                window_size_seconds: WindowSizeSeconds::try_from(10).unwrap(),
                rate_group_size_ms: RateGroupSizeMs::try_from(50).unwrap(),
                hard_limit_factor: HardLimitFactor::try_from(10f64).unwrap(),
                suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
            },
        );
        accepted.inc(&k, &rate_limit, 10).await.unwrap();

        let kg_accepted = keygen(&prefix, RateType::Suppressed);
        let kg_observed = keygen(&prefix, RateType::SuppressedObserved);

        // Force all accepted activity to be older than 1s.
        let mut conn = cm.clone();
        let ak_key = kg_accepted.get_active_keys(&k);
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
        let sf_key = kg_observed.get_suppression_factor_key(&k);
        let _: u64 = conn.del(&sf_key).await.unwrap();

        let mut rng = |_p: f64| true;
        let res = limiter.inc_with_rng(&k, &rate_limit, 1, &mut rng).await;
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
        let mut rng = |_p: f64| true;
        let _ = limiter
            .inc_with_rng(&k, &rate_limit, 1, &mut rng)
            .await
            .unwrap();

        let kg_accepted = keygen(&prefix, RateType::Suppressed);
        let kg_observed = keygen(&prefix, RateType::SuppressedObserved);

        // Ensure sf exists so we can assert it is deleted.
        let mut conn = cm.clone();
        let sf_key = kg_observed.get_suppression_factor_key(&k);
        let _: () = conn.set_ex(&sf_key, 0.5_f64, 60).await.unwrap();
        let sf_exists_before: bool = conn.exists(&sf_key).await.unwrap();
        assert!(sf_exists_before, "expected sf to exist before cleanup");

        // Force staleness in the shared active_entities index.
        let ae_key = kg_accepted.get_active_entities_key();
        set_zset_score_ms_ago(&mut conn, &ae_key, &k, 1050).await;

        limiter.cleanup(1000).await.unwrap();

        // Accepted keys should be deleted.
        let (a_h, a_ak, a_w, a_t, a_sf) = suppressed_keys_exist(&mut conn, &kg_accepted, &k).await;
        assert!(
            !a_h && !a_ak && !a_w && !a_t,
            "expected accepted keyspace deleted"
        );
        // Accepted keyspace should not have sf key, but assert false anyway.
        assert!(!a_sf);

        // Observed keys should be deleted, including sf.
        let (o_h, o_ak, o_w, o_t, o_sf) = suppressed_keys_exist(&mut conn, &kg_observed, &k).await;
        assert!(
            !o_h && !o_ak && !o_w && !o_t && !o_sf,
            "expected observed keyspace (including sf) deleted"
        );
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

        let expected_suppression_factor = 1f64 - (1f64 / 2.1f64);

        let decision = limiter.inc(&k, &rate_limit, 1).await.unwrap();

        eprintln!("decision: {:?}", decision);

        assert!(
            matches!(
                decision,
                RateLimitDecision::Suppressed {
                    suppression_factor,
                    ..
                } if suppression_factor - expected_suppression_factor < 1e-12
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

        let expected_suppression_factor = 1f64 - (1f64 / 21f64);

        let decision = limiter.inc(&k, &rate_limit, 1).await.unwrap();

        assert!(
            matches!(
                decision,
                RateLimitDecision::Suppressed {
                    suppression_factor,
                    ..
                } if suppression_factor - expected_suppression_factor < 1e-12
            ),
            "decision: {:?}",
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
            matches!(decision, RateLimitDecision::Rejected { .. } )
            ||
            matches!(decision, RateLimitDecision::Suppressed { suppression_factor, .. } if suppression_factor == 1.0f64),
            "decision: {:?}",
            decision
        );
    });
}
