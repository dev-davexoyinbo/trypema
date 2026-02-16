use std::{env, thread, time::Duration};

use redis::AsyncCommands;

use crate::{
    AbsoluteRedisRateLimiter, RateGroupSizeMs, RateLimit, RateLimitDecision, RedisKey,
    RedisRateLimiterOptions, WindowSizeSeconds,
};

fn redis_url() -> Option<String> {
    env::var("REDIS_URL").ok()
}

fn unique_prefix() -> RedisKey {
    let n: u64 = rand::random();
    RedisKey::try_from(format!("trypema_test_{n}")).unwrap()
}

fn key(s: &str) -> RedisKey {
    RedisKey::try_from(s.to_string()).unwrap()
}

fn hash_key(prefix: &RedisKey, key: &RedisKey) -> String {
    format!("{}:{}:absolute:h", **prefix, **key)
}

fn active_keys_key(prefix: &RedisKey, key: &RedisKey) -> String {
    format!("{}:{}:absolute:a", **prefix, **key)
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
    });

    (limiter, cm, prefix)
}

#[test]
fn rejects_at_exact_window_limit() {
    let Some(url) = redis_url() else {
        eprintln!("skipping redis integration tests (REDIS_URL not set)");
        return;
    };

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
    let Some(url) = redis_url() else {
        eprintln!("skipping redis integration tests (REDIS_URL not set)");
        return;
    };

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
    let Some(url) = redis_url() else {
        eprintln!("skipping redis integration tests (REDIS_URL not set)");
        return;
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 10, 200).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(1000f64).unwrap();

        limiter.inc(&k, &rate_limit, 1).await.unwrap();
        thread::sleep(Duration::from_millis(50));
        limiter.inc(&k, &rate_limit, 1).await.unwrap();

        let mut conn = cm.clone();
        let hlen: u64 = conn.hlen(hash_key(&prefix, &k)).await.unwrap();
        let zcard: u64 = conn.zcard(active_keys_key(&prefix, &k)).await.unwrap();

        // Increments within the coalescing window should share a single bucket.
        assert_eq!(hlen, 1, "hlen should be 1");
        assert_eq!(zcard, 1, "zcard should be 1");
    });
}

#[test]
fn rate_grouping_separates_beyond_group() {
    eprintln!(">>>>>> skipping redis integration tests (REDIS_URL not set)");
    let Some(url) = redis_url() else {
        eprintln!("skipping redis integration tests (REDIS_URL not set)");
        return;
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, cm, prefix) = build_limiter(&url, 10, 200).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(1000f64).unwrap();

        limiter.inc(&k, &rate_limit, 1).await.unwrap();
        thread::sleep(Duration::from_millis(250));
        limiter.inc(&k, &rate_limit, 1).await.unwrap();

        let mut conn = cm.clone();
        let hlen: u64 = conn.hlen(hash_key(&prefix, &k)).await.unwrap();
        let zcard: u64 = conn.zcard(active_keys_key(&prefix, &k)).await.unwrap();

        assert_eq!(hlen, 2);
        assert_eq!(zcard, 2);
    });
}

#[test]
fn unblocks_after_window_expires() {
    let Some(url) = redis_url() else {
        eprintln!("skipping redis integration tests (REDIS_URL not set)");
        return;
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, _cm, _prefix) = build_limiter(&url, 1, 1000).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(3f64).unwrap();

        assert!(matches!(
            limiter.inc(&k, &rate_limit, 3).await.unwrap(),
            RateLimitDecision::Allowed
        ));

        assert!(matches!(
            limiter.inc(&k, &rate_limit, 1).await.unwrap(),
            RateLimitDecision::Rejected { .. }
        ));

        thread::sleep(Duration::from_millis(1100));

        assert!(matches!(
            limiter.inc(&k, &rate_limit, 1).await.unwrap(),
            RateLimitDecision::Allowed
        ));
    });
}

#[test]
fn rejected_includes_retry_after_and_remaining_after_waiting() {
    let Some(url) = redis_url() else {
        eprintln!("skipping redis integration tests (REDIS_URL not set)");
        return;
    };

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
    let Some(url) = redis_url() else {
        eprintln!("skipping redis integration tests (REDIS_URL not set)");
        return;
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (limiter, _cm, _prefix) = build_limiter(&url, 1, 50).await;

        let k = key("missing");
        let decision = limiter.is_allowed(&k).await.unwrap();
        assert!(matches!(decision, RateLimitDecision::Allowed));
    });
}
