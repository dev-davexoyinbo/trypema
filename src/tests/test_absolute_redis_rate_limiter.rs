use std::{env, future::Future, thread, time::Duration};

use crate::common::SuppressionFactorCacheMs;
use crate::{
    HardLimitFactor, LocalRateLimiterOptions, RateGroupSizeMs, RateLimit, RateLimitDecision,
    RateLimiter, RateLimiterOptions, RedisKey, RedisRateLimiterOptions, WindowSizeSeconds,
};

#[cfg(feature = "redis-tokio")]
fn block_on<F, T>(f: F) -> T
where
    F: Future<Output = T>,
{
    tokio::runtime::Runtime::new().unwrap().block_on(f)
}

#[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
fn block_on<F, T>(f: F) -> T
where
    F: Future<Output = T>,
{
    smol::block_on(f)
}

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

async fn build_limiter(
    url: &str,
    window_size_seconds: u64,
    rate_group_size_ms: u64,
) -> std::sync::Arc<RateLimiter> {
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
        },
    };

    std::sync::Arc::new(RateLimiter::new(options))
}

#[test]
fn rejects_at_exact_window_limit() {
    let Some(url) = redis_url() else { return };

    block_on(async {
        let rl = build_limiter(&url, 1, 1000).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(2f64).unwrap();

        assert!(matches!(
            rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap(),
            RateLimitDecision::Allowed
        ));

        assert!(matches!(
            rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap(),
            RateLimitDecision::Allowed
        ));

        // The third call is over the 1s window capacity (1 * 2 = 2).
        let decision = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(decision, RateLimitDecision::Rejected { .. }));
    });
}

#[test]
fn per_key_state_is_independent() {
    let Some(url) = redis_url() else { return };

    block_on(async {
        let rl = build_limiter(&url, 1, 1000).await;

        let a = key("a");
        let b = key("b");
        let rate_limit = RateLimit::try_from(2f64).unwrap();

        // Saturate key a.
        rl.redis().absolute().inc(&a, &rate_limit, 2).await.unwrap();
        let decision_a = rl.redis().absolute().inc(&a, &rate_limit, 1).await.unwrap();
        assert!(
            matches!(decision_a, RateLimitDecision::Rejected { .. }),
            "decision_a: {:?}",
            decision_a
        );

        // Key b should still be allowed.
        let decision_b = rl.redis().absolute().inc(&b, &rate_limit, 1).await.unwrap();
        assert!(
            matches!(decision_b, RateLimitDecision::Allowed),
            "decision_b: {:?}",
            decision_b
        );
    });
}

#[test]
fn rate_grouping_merges_within_group_affects_remaining_after_waiting() {
    let Some(url) = redis_url() else { return };

    block_on(async {
        let rl = build_limiter(&url, 6, 300).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        // Create a single bucket by staying within the rate-group coalescing window.
        rl.redis().absolute().inc(&k, &rate_limit, 2).await.unwrap();
        thread::sleep(Duration::from_millis(50));
        rl.redis().absolute().inc(&k, &rate_limit, 4).await.unwrap();

        // At capacity (6 * 1 = 6). Next increment should be rejected.
        let decision = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        let RateLimitDecision::Rejected {
            remaining_after_waiting,
            ..
        } = decision
        else {
            panic!("expected rejected decision");
        };

        // When usage is merged into one bucket, waiting for the oldest bucket to expire clears
        // the full capacity.
        assert_eq!(remaining_after_waiting, 6);
    });
}

#[test]
fn unblocks_after_window_expires() {
    let Some(url) = redis_url() else { return };

    block_on(async {
        let rl = build_limiter(&url, 1, 1000).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(3f64).unwrap();

        assert!(
            matches!(
                rl.redis().absolute().inc(&k, &rate_limit, 3).await.unwrap(),
                RateLimitDecision::Allowed
            ),
            "first increment should be allowed"
        );

        assert!(
            matches!(
                rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap(),
                RateLimitDecision::Rejected { .. }
            ),
            "second increment should be rejected"
        );

        thread::sleep(Duration::from_millis(1100));

        assert!(
            matches!(
                rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap(),
                RateLimitDecision::Allowed
            ),
            "third increment should be allowed"
        );
    });
}

#[test]
fn rejected_includes_retry_after_and_remaining_after_waiting() {
    let Some(url) = redis_url() else { return };

    block_on(async {
        let rl = build_limiter(&url, 6, 200).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        // Create two buckets.
        rl.redis().absolute().inc(&k, &rate_limit, 2).await.unwrap();
        thread::sleep(Duration::from_millis(250));
        rl.redis().absolute().inc(&k, &rate_limit, 4).await.unwrap();

        // At capacity (6 * 1 = 6). Next increment should be rejected.
        let decision = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
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
    let Some(url) = redis_url() else { return };

    block_on(async {
        let rl = build_limiter(&url, 1, 50).await;

        let k = key("missing");
        let decision = rl.redis().absolute().is_allowed(&k).await.unwrap();
        assert!(matches!(decision, RateLimitDecision::Allowed));
    });
}

#[test]
fn rejected_inc_does_not_consume_capacity() {
    let Some(url) = redis_url() else { return };

    block_on(async {
        let rl = build_limiter(&url, 1, 1000).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(2f64).unwrap();

        assert!(matches!(
            rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap(),
            RateLimitDecision::Allowed
        ));

        // Capacity is 2 (1s * 2/s). current_total=1; count=2 would push to 3, so reject.
        let decision = rl.redis().absolute().inc(&k, &rate_limit, 2).await.unwrap();
        assert!(matches!(decision, RateLimitDecision::Rejected { .. }));

        // If the rejected increment mutated state, this would be rejected.
        let decision2 = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(decision2, RateLimitDecision::Allowed));
    });
}

#[test]
fn is_allowed_evicts_old_buckets_and_updates_total_count() {
    let Some(url) = redis_url() else { return };

    block_on(async {
        let rl = build_limiter(&url, 2, 200).await;
        let k = key("k");
        // Use a rate limit that makes behavior differ depending on whether eviction happens.
        // window=2s, rate=1/s -> capacity=2.
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        // Two buckets (sleep > group size).
        rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        thread::sleep(Duration::from_millis(250));
        rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();

        // At exact capacity: should be rejected.
        let d0 = rl.redis().absolute().is_allowed(&k).await.unwrap();
        assert!(matches!(d0, RateLimitDecision::Rejected { .. }));

        // Wait until the first bucket is out of window (2s) but the second is still in-window.
        thread::sleep(Duration::from_millis(1850));

        let decision = rl.redis().absolute().is_allowed(&k).await.unwrap();
        assert!(matches!(decision, RateLimitDecision::Allowed));
    });
}

#[test]
fn inc_evicts_expired_buckets_and_total_matches_hash_sum() {
    let Some(url) = redis_url() else { return };

    block_on(async {
        let rl = build_limiter(&url, 1, 200).await;
        let k = key("k");
        // window=1s, rate=2/s -> capacity=2.
        // If eviction does not happen, the post-sleep increment would push total over capacity.
        let rate_limit = RateLimit::try_from(2f64).unwrap();

        // Two buckets.
        rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        thread::sleep(Duration::from_millis(250));
        rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();

        // Wait past the window so both buckets are expired.
        thread::sleep(Duration::from_millis(1200));
        let decision = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "fourth increment should be allowed, instead got {decision:?}"
        );
    });
}

#[test]
fn is_allowed_reflects_rejected_after_hitting_limit_then_allows_after_expiry() {
    let Some(url) = redis_url() else { return };

    block_on(async {
        let rl = build_limiter(&url, 1, 1000).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(2f64).unwrap();

        rl.redis().absolute().inc(&k, &rate_limit, 2).await.unwrap();
        let d1 = rl.redis().absolute().is_allowed(&k).await.unwrap();
        assert!(matches!(d1, RateLimitDecision::Rejected { .. }));

        thread::sleep(Duration::from_millis(1100));
        let d2 = rl.redis().absolute().is_allowed(&k).await.unwrap();
        assert!(matches!(d2, RateLimitDecision::Allowed));
    });
}

#[test]
fn is_allowed_rejected_includes_retry_after_and_remaining_after_waiting() {
    let Some(url) = redis_url() else { return };

    block_on(async {
        let rl = build_limiter(&url, 6, 200).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        rl.redis().absolute().inc(&k, &rate_limit, 2).await.unwrap();
        thread::sleep(Duration::from_millis(250));
        rl.redis().absolute().inc(&k, &rate_limit, 4).await.unwrap();

        let decision = rl.redis().absolute().is_allowed(&k).await.unwrap();
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
    let Some(url) = redis_url() else { return };

    block_on(async {
        let rl = build_limiter(&url, 6, 200).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        rl.redis().absolute().inc(&k, &rate_limit, 5).await.unwrap();
        let decision = rl.redis().absolute().is_allowed(&k).await.unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "should be allowed"
        );
    });
}
