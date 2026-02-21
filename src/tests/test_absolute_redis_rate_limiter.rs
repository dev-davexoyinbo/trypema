use std::{env, thread, time::Duration};

use redis::AsyncCommands;

use crate::common::SuppressionFactorCacheMs;
use crate::{
    AbsoluteRedisRateLimiter, RateGroupSizeMs, RateLimit, RateLimitDecision, RedisKey,
    RedisKeyGenerator, RedisRateLimiterOptions, WindowSizeSeconds, common::RateType,
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

fn keygen(prefix: &RedisKey) -> RedisKeyGenerator {
    RedisKeyGenerator::new(prefix.clone(), RateType::Absolute)
}

async fn absolute_keys_exist(
    conn: &mut redis::aio::ConnectionManager,
    kg: &RedisKeyGenerator,
    key: &RedisKey,
) -> (bool, bool, bool, bool) {
    let h_exists: bool = conn.exists(kg.get_hash_key(key)).await.unwrap();
    let a_exists: bool = conn.exists(kg.get_active_keys(key)).await.unwrap();
    let w_exists: bool = conn.exists(kg.get_window_limit_key(key)).await.unwrap();
    let t_exists: bool = conn.exists(kg.get_total_count_key(key)).await.unwrap();
    (h_exists, a_exists, w_exists, t_exists)
}

async fn sum_hash_counts(conn: &mut redis::aio::ConnectionManager, hash_key: String) -> u64 {
    let values: Vec<String> = conn.hvals(hash_key).await.unwrap();
    values
        .into_iter()
        .map(|v| v.parse::<u64>().unwrap())
        .sum::<u64>()
}

async fn set_active_entity_score_ms_ago(
    conn: &mut redis::aio::ConnectionManager,
    active_entities_key: &str,
    entity: &RedisKey,
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
        .key(active_entities_key)
        .arg(entity.to_string())
        .arg(score_ms_ago)
        .invoke_async(conn)
        .await
        .unwrap();
}

async fn build_limiter(
    url: &str,
    window_size_seconds: u64,
    rate_group_size_ms: u64,
) -> (
    AbsoluteRedisRateLimiter,
    redis::aio::ConnectionManager,
    RedisKey,
) {
    let client = redis::Client::open(url).unwrap();
    let cm = client.get_connection_manager().await.unwrap();
    let prefix = unique_prefix();

    let limiter = AbsoluteRedisRateLimiter::new(RedisRateLimiterOptions {
        connection_manager: cm.clone(),
        prefix: Some(prefix.clone()),
        window_size_seconds: WindowSizeSeconds::try_from(window_size_seconds).unwrap(),
        rate_group_size_ms: RateGroupSizeMs::try_from(rate_group_size_ms).unwrap(),
        // Unused by absolute limiter, but required by the options type.
        hard_limit_factor: crate::HardLimitFactor::default(),
        suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
    });

    (limiter, cm, prefix)
}

#[test]
fn rejects_at_exact_window_limit() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, _cm, _prefix) = build_limiter(&url, 1, 1000).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(2f64).unwrap();

        assert!(matches!(
            limiter.inc(&k, &rate_limit, 1).await.unwrap(),
            RateLimitDecision::Allowed
        ));

        assert!(matches!(
            limiter.inc(&k, &rate_limit, 1).await.unwrap(),
            RateLimitDecision::Allowed
        ));

        // The third call is over the 1s window capacity (1 * 2 = 2).
        let decision = limiter.inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(decision, RateLimitDecision::Rejected { .. }));
    });
}

#[test]
fn per_key_state_is_independent() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, _cm, _prefix) = build_limiter(&url, 1, 1000).await;

        let a = key("a");
        let b = key("b");
        let rate_limit = RateLimit::try_from(2f64).unwrap();

        // Saturate key a.
        limiter.inc(&a, &rate_limit, 2).await.unwrap();
        let decision_a = limiter.inc(&a, &rate_limit, 1).await.unwrap();
        assert!(matches!(decision_a, RateLimitDecision::Rejected { .. }));

        // Key b should still be allowed.
        let decision_b = limiter.inc(&b, &rate_limit, 1).await.unwrap();
        assert!(matches!(decision_b, RateLimitDecision::Allowed));
    });
}

#[test]
fn rate_grouping_merges_within_group() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 10, 200).await;
        let kg = keygen(&prefix);

        let k = key("k");
        let rate_limit = RateLimit::try_from(1000f64).unwrap();

        limiter.inc(&k, &rate_limit, 1).await.unwrap();
        thread::sleep(Duration::from_millis(50));
        limiter.inc(&k, &rate_limit, 1).await.unwrap();

        let mut conn = cm.clone();
        let hlen: u64 = conn.hlen(kg.get_hash_key(&k)).await.unwrap();
        let zcard: u64 = conn.zcard(kg.get_active_keys(&k)).await.unwrap();

        // Increments within the coalescing window should share a single bucket.
        assert_eq!(hlen, 1, "hlen should be 1");
        assert_eq!(zcard, 1, "zcard should be 1");
    });
}

#[test]
fn rate_grouping_separates_beyond_group() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 10, 200).await;
        let kg = keygen(&prefix);

        let k = key("k");
        let rate_limit = RateLimit::try_from(1000f64).unwrap();

        limiter.inc(&k, &rate_limit, 1).await.unwrap();
        thread::sleep(Duration::from_millis(250));
        limiter.inc(&k, &rate_limit, 1).await.unwrap();

        let mut conn = cm.clone();
        let hlen: u64 = conn.hlen(kg.get_hash_key(&k)).await.unwrap();
        let zcard: u64 = conn.zcard(kg.get_active_keys(&k)).await.unwrap();

        assert_eq!(hlen, 2);
        assert_eq!(zcard, 2);
    });
}

#[test]
fn unblocks_after_window_expires() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, _cm, _prefix) = build_limiter(&url, 1, 1000).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(3f64).unwrap();

        assert!(
            matches!(
                limiter.inc(&k, &rate_limit, 3).await.unwrap(),
                RateLimitDecision::Allowed
            ),
            "first increment should be allowed"
        );

        assert!(
            matches!(
                limiter.inc(&k, &rate_limit, 1).await.unwrap(),
                RateLimitDecision::Rejected { .. }
            ),
            "second increment should be rejected"
        );

        thread::sleep(Duration::from_millis(1100));

        assert!(
            matches!(
                limiter.inc(&k, &rate_limit, 1).await.unwrap(),
                RateLimitDecision::Allowed
            ),
            "third increment should be allowed"
        );
    });
}

#[test]
fn rejected_includes_retry_after_and_remaining_after_waiting() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, _cm, _prefix) = build_limiter(&url, 6, 200).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        // Create two buckets.
        limiter.inc(&k, &rate_limit, 2).await.unwrap();
        thread::sleep(Duration::from_millis(250));
        limiter.inc(&k, &rate_limit, 4).await.unwrap();

        // At capacity (6 * 1 = 6). Next increment should be rejected.
        let decision = limiter.inc(&k, &rate_limit, 1).await.unwrap();
        let RateLimitDecision::Rejected {
            window_size_seconds,
            retry_after_ms,
            remaining_after_waiting,
        } = decision
        else {
            panic!("expected rejected decision");
        };

        assert_eq!(window_size_seconds, 6, "window size should be 6");
        eprintln!("retry after: {retry_after_ms}");
        assert!(
            retry_after_ms <= 6000,
            "retry after should be less than 6000, instead got {retry_after_ms}"
        );
        // After the oldest bucket (count=2) expires, then 2 spots should remain
        assert_eq!(
            remaining_after_waiting, 2,
            "remaining after waiting should be 4 instead got {remaining_after_waiting}"
        );
    });
}

#[test]
fn is_allowed_unknown_key_is_allowed() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, _cm, _prefix) = build_limiter(&url, 1, 50).await;

        let k = key("missing");
        let decision = limiter.is_allowed(&k).await.unwrap();
        assert!(matches!(decision, RateLimitDecision::Allowed));
    });
}

#[test]
fn is_allowed_unknown_key_does_not_create_redis_state() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 10, 200).await;
        let kg = keygen(&prefix);

        let k = key("missing");
        let _ = limiter.is_allowed(&k).await;

        let mut conn = cm.clone();
        let h_exists: bool = conn.exists(kg.get_hash_key(&k)).await.unwrap();
        let a_exists: bool = conn.exists(kg.get_active_keys(&k)).await.unwrap();
        let w_exists: bool = conn.exists(kg.get_window_limit_key(&k)).await.unwrap();
        let t_exists: bool = conn.exists(kg.get_total_count_key(&k)).await.unwrap();

        assert!(!h_exists);
        assert!(!a_exists);
        assert!(!w_exists);
        assert!(!t_exists);
    });
}

#[test]
fn inc_rejects_when_count_would_push_over_limit() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 1, 1000).await;
        let kg = keygen(&prefix);

        let k = key("k");
        let rate_limit = RateLimit::try_from(2f64).unwrap();

        assert!(matches!(
            limiter.inc(&k, &rate_limit, 1).await.unwrap(),
            RateLimitDecision::Allowed
        ));

        // Capacity is 2 (1s * 2/s). current_total=1; count=2 would push to 3, so reject.
        let decision = limiter.inc(&k, &rate_limit, 2).await.unwrap();
        assert!(matches!(decision, RateLimitDecision::Rejected { .. }));

        // Rejected increments must not mutate state.
        let mut conn = cm.clone();
        let total: Option<u64> = conn.get(kg.get_total_count_key(&k)).await.unwrap();
        assert_eq!(total, Some(1));

        let sum = sum_hash_counts(&mut conn, kg.get_hash_key(&k)).await;
        assert_eq!(sum, 1);
    });
}

#[test]
fn is_allowed_evicts_old_buckets_and_updates_total_count() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 2, 200).await;
        let kg = keygen(&prefix);

        let k = key("k");
        let rate_limit = RateLimit::try_from(1000f64).unwrap();

        // Two buckets (sleep > group size).
        limiter.inc(&k, &rate_limit, 1).await.unwrap();
        thread::sleep(Duration::from_millis(250));
        limiter.inc(&k, &rate_limit, 1).await.unwrap();

        // Wait until the first bucket is out of window (2s) but the second is still in-window.
        thread::sleep(Duration::from_millis(1850));

        let decision = limiter.is_allowed(&k).await.unwrap();
        assert!(matches!(decision, RateLimitDecision::Allowed));

        let mut conn = cm.clone();
        let hlen: u64 = conn.hlen(kg.get_hash_key(&k)).await.unwrap();
        let zcard: u64 = conn.zcard(kg.get_active_keys(&k)).await.unwrap();
        let total: Option<u64> = conn.get(kg.get_total_count_key(&k)).await.unwrap();

        assert_eq!(hlen, 1, "expected oldest bucket to be evicted");
        assert_eq!(zcard, 1, "expected oldest bucket to be evicted");
        assert_eq!(total, Some(1));

        let sum = sum_hash_counts(&mut conn, kg.get_hash_key(&k)).await;
        assert_eq!(sum, 1);
    });
}

#[test]
fn is_allowed_does_not_change_total_count_when_no_eviction() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 10, 200).await;
        let kg = keygen(&prefix);

        let k = key("k");
        let rate_limit = RateLimit::try_from(1000f64).unwrap();

        limiter.inc(&k, &rate_limit, 1).await.unwrap();
        thread::sleep(Duration::from_millis(250));
        limiter.inc(&k, &rate_limit, 1).await.unwrap();

        let mut conn = cm.clone();
        let total_before: Option<u64> = conn.get(kg.get_total_count_key(&k)).await.unwrap();
        let sum_before = sum_hash_counts(&mut conn, kg.get_hash_key(&k)).await;

        let _ = limiter.is_allowed(&k).await.unwrap();

        let total_after: Option<u64> = conn.get(kg.get_total_count_key(&k)).await.unwrap();
        let sum_after = sum_hash_counts(&mut conn, kg.get_hash_key(&k)).await;

        assert_eq!(total_before, total_after);
        assert_eq!(sum_before, sum_after);
    });
}

#[test]
fn inc_evicts_expired_buckets_and_total_matches_hash_sum() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 1, 200).await;
        let kg = keygen(&prefix);

        let k = key("k");
        let rate_limit = RateLimit::try_from(1000f64).unwrap();

        // Two buckets.
        limiter.inc(&k, &rate_limit, 1).await.unwrap();
        thread::sleep(Duration::from_millis(250));
        limiter.inc(&k, &rate_limit, 1).await.unwrap();

        // Wait past the window so both buckets are expired.
        thread::sleep(Duration::from_millis(1200));
        assert!(matches!(
            limiter.inc(&k, &rate_limit, 1).await.unwrap(),
            RateLimitDecision::Allowed
        ));

        let mut conn = cm.clone();
        let sum = sum_hash_counts(&mut conn, kg.get_hash_key(&k)).await;
        let total: Option<u64> = conn.get(kg.get_total_count_key(&k)).await.unwrap();
        assert_eq!(total, Some(sum));

        let hlen: u64 = conn.hlen(kg.get_hash_key(&k)).await.unwrap();
        let zcard: u64 = conn.zcard(kg.get_active_keys(&k)).await.unwrap();
        assert_eq!(hlen, 1);
        assert_eq!(zcard, 1);
    });
}

#[test]
fn is_allowed_reflects_rejected_after_hitting_limit_then_allows_after_expiry() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, _cm, _prefix) = build_limiter(&url, 1, 1000).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(2f64).unwrap();

        limiter.inc(&k, &rate_limit, 2).await.unwrap();
        let d1 = limiter.is_allowed(&k).await.unwrap();
        assert!(matches!(d1, RateLimitDecision::Rejected { .. }));

        thread::sleep(Duration::from_millis(1100));
        let d2 = limiter.is_allowed(&k).await.unwrap();
        assert!(matches!(d2, RateLimitDecision::Allowed));
    });
}

#[test]
fn is_allowed_does_not_mutate_bucket_counts() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 10, 200).await;
        let kg = keygen(&prefix);

        let k = key("k");
        let rate_limit = RateLimit::try_from(1000f64).unwrap();

        // Create two buckets.
        limiter.inc(&k, &rate_limit, 1).await.unwrap();
        thread::sleep(Duration::from_millis(250));
        limiter.inc(&k, &rate_limit, 1).await.unwrap();

        let mut conn = cm.clone();
        let hlen_before: u64 = conn.hlen(kg.get_hash_key(&k)).await.unwrap();
        let zcard_before: u64 = conn.zcard(kg.get_active_keys(&k)).await.unwrap();

        let _ = limiter.is_allowed(&k).await;

        let hlen_after: u64 = conn.hlen(kg.get_hash_key(&k)).await.unwrap();
        let zcard_after: u64 = conn.zcard(kg.get_active_keys(&k)).await.unwrap();

        assert_eq!(hlen_before, hlen_after);
        assert_eq!(zcard_before, zcard_after);
    });
}

#[test]
fn is_allowed_rejected_includes_retry_after_and_remaining_after_waiting() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, _cm, _prefix) = build_limiter(&url, 6, 200).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        limiter.inc(&k, &rate_limit, 2).await.unwrap();
        thread::sleep(Duration::from_millis(250));
        limiter.inc(&k, &rate_limit, 4).await.unwrap();

        let decision = limiter.is_allowed(&k).await.unwrap();
        let RateLimitDecision::Rejected {
            window_size_seconds,
            retry_after_ms,
            remaining_after_waiting,
        } = decision
        else {
            panic!("expected rejected decision");
        };

        assert_eq!(
            window_size_seconds, 6,
            "window size should be 6 instead got {window_size_seconds}"
        );
        assert!(
            retry_after_ms <= 6000,
            "retry after should be less than 6000, instead got {retry_after_ms}"
        );
        assert_eq!(
            remaining_after_waiting, 2,
            "remaining after waiting should be 2 instead got {remaining_after_waiting}"
        );
    });
}

#[test]
fn is_allowed_returns_allowed_when_below_limit() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, _cm, _prefix) = build_limiter(&url, 6, 200).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        limiter.inc(&k, &rate_limit, 5).await.unwrap();
        let decision = limiter.is_allowed(&k).await.unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "should be allowed"
        );
    });
}

#[test]
fn cleanup_deletes_state_and_index_for_stale_entities() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 10, 200).await;
        let kg = keygen(&prefix);

        let a = key("a");
        let b = key("b");
        let c = key("c");
        let rate_limit = RateLimit::try_from(1000f64).unwrap();

        limiter.inc(&a, &rate_limit, 1).await.unwrap();
        limiter.inc(&b, &rate_limit, 1).await.unwrap();
        limiter.inc(&c, &rate_limit, 1).await.unwrap();

        let mut conn = cm.clone();
        let ae_key = kg.get_active_entities_key();

        // Seed suppression-factor keys to verify cleanup deletes them too.
        let sf_a = kg.get_suppression_factor_key(&a);
        let sf_b = kg.get_suppression_factor_key(&b);
        let sf_c = kg.get_suppression_factor_key(&c);
        let _: () = conn.set(&sf_a, 0.5_f64).await.unwrap();
        let _: () = conn.set(&sf_b, 0.5_f64).await.unwrap();
        let _: () = conn.set(&sf_c, 0.5_f64).await.unwrap();

        // Force staleness deterministically based on Redis server time.
        set_active_entity_score_ms_ago(&mut conn, &ae_key, &a, 1050).await;
        set_active_entity_score_ms_ago(&mut conn, &ae_key, &b, 1050).await;
        set_active_entity_score_ms_ago(&mut conn, &ae_key, &c, 0).await;

        limiter.cleanup(1000).await.unwrap();

        let (a_h, a_ak, a_w, a_t) = absolute_keys_exist(&mut conn, &kg, &a).await;
        assert!(
            !a_h && !a_ak && !a_w && !a_t,
            "expected all keys for a deleted"
        );

        let a_sf: bool = conn.exists(&sf_a).await.unwrap();
        assert!(!a_sf, "expected suppression-factor key for a deleted");

        let (b_h, b_ak, b_w, b_t) = absolute_keys_exist(&mut conn, &kg, &b).await;
        assert!(
            !b_h && !b_ak && !b_w && !b_t,
            "expected all keys for b deleted"
        );

        let b_sf: bool = conn.exists(&sf_b).await.unwrap();
        assert!(!b_sf, "expected suppression-factor key for b deleted");

        let (c_h, c_ak, c_w, c_t) = absolute_keys_exist(&mut conn, &kg, &c).await;
        assert!(
            c_h && c_ak && c_w && c_t,
            "expected all keys for c retained"
        );

        let c_sf: bool = conn.exists(&sf_c).await.unwrap();
        assert!(c_sf, "expected suppression-factor key for c retained");

        let a_score: Option<f64> = conn.zscore(&ae_key, &*a).await.unwrap();
        let b_score: Option<f64> = conn.zscore(&ae_key, &*b).await.unwrap();
        let c_score: Option<f64> = conn.zscore(&ae_key, &*c).await.unwrap();

        assert!(a_score.is_none(), "expected a removed from active_entities");
        assert!(b_score.is_none(), "expected b removed from active_entities");
        assert!(c_score.is_some(), "expected c retained in active_entities");
    });
}

#[test]
fn cleanup_removes_multiple_stale_entities() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 10, 200).await;
        let kg = keygen(&prefix);

        let a = key("a");
        let b = key("b");
        let rate_limit = RateLimit::try_from(1000f64).unwrap();

        limiter.inc(&a, &rate_limit, 1).await.unwrap();
        limiter.inc(&b, &rate_limit, 1).await.unwrap();

        let mut conn = cm.clone();
        let ae_key = kg.get_active_entities_key();
        set_active_entity_score_ms_ago(&mut conn, &ae_key, &a, 1050).await;
        set_active_entity_score_ms_ago(&mut conn, &ae_key, &b, 1050).await;

        limiter.cleanup(1000).await.unwrap();

        let (a_h, a_ak, a_w, a_t) = absolute_keys_exist(&mut conn, &kg, &a).await;
        let (b_h, b_ak, b_w, b_t) = absolute_keys_exist(&mut conn, &kg, &b).await;
        assert!(!a_h && !a_ak && !a_w && !a_t);
        assert!(!b_h && !b_ak && !b_w && !b_t);

        let a_score: Option<f64> = conn.zscore(&ae_key, &*a).await.unwrap();
        let b_score: Option<f64> = conn.zscore(&ae_key, &*b).await.unwrap();
        assert!(a_score.is_none());
        assert!(b_score.is_none());
    });
}

#[test]
fn cleanup_noop_when_no_stale_entities() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 10, 200).await;
        let kg = keygen(&prefix);

        let a = key("a");
        let rate_limit = RateLimit::try_from(1000f64).unwrap();
        limiter.inc(&a, &rate_limit, 1).await.unwrap();

        let mut conn = cm.clone();
        let ae_key = kg.get_active_entities_key();
        set_active_entity_score_ms_ago(&mut conn, &ae_key, &a, 0).await;

        limiter.cleanup(1000).await.unwrap();

        let (a_h, a_ak, a_w, a_t) = absolute_keys_exist(&mut conn, &kg, &a).await;
        assert!(a_h && a_ak && a_w && a_t, "expected all keys retained");

        let a_score: Option<f64> = conn.zscore(&ae_key, &*a).await.unwrap();
        assert!(a_score.is_some(), "expected active_entities retained");
    });
}

#[test]
fn cleanup_noop_on_empty_prefix() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 10, 200).await;
        let kg = keygen(&prefix);

        limiter.cleanup(1000).await.unwrap();

        let mut conn = cm.clone();
        let exists: bool = conn.exists(kg.get_active_entities_key()).await.unwrap();
        assert!(!exists, "cleanup should not create active_entities key");
    });
}

#[test]
fn cleanup_is_prefix_scoped() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter1, cm1, prefix1) = build_limiter(&url, 10, 200).await;
        let (limiter2, cm2, prefix2) = build_limiter(&url, 10, 200).await;

        let kg1 = keygen(&prefix1);
        let kg2 = keygen(&prefix2);

        let k1 = key("k");
        let k2 = key("k");
        let rate_limit = RateLimit::try_from(1000f64).unwrap();

        limiter1.inc(&k1, &rate_limit, 1).await.unwrap();
        limiter2.inc(&k2, &rate_limit, 1).await.unwrap();

        let mut conn1 = cm1.clone();
        let ae1 = kg1.get_active_entities_key();
        set_active_entity_score_ms_ago(&mut conn1, &ae1, &k1, 1050).await;

        limiter1.cleanup(1000).await.unwrap();

        let (k1_h, k1_ak, k1_w, k1_t) = absolute_keys_exist(&mut conn1, &kg1, &k1).await;
        assert!(
            !k1_h && !k1_ak && !k1_w && !k1_t,
            "expected prefix1 keys deleted"
        );
        let k1_score: Option<f64> = conn1.zscore(&ae1, &*k1).await.unwrap();
        assert!(k1_score.is_none(), "expected prefix1 index entry removed");

        let mut conn2 = cm2.clone();
        let (k2_h, k2_ak, k2_w, k2_t) = absolute_keys_exist(&mut conn2, &kg2, &k2).await;
        assert!(
            k2_h && k2_ak && k2_w && k2_t,
            "expected prefix2 keys retained"
        );
        let k2_score: Option<f64> = conn2
            .zscore(kg2.get_active_entities_key(), &*k2)
            .await
            .unwrap();
        assert!(k2_score.is_some(), "expected prefix2 index entry retained");
    });
}
