use std::{env, future::Future, thread, time::Duration};

use crate::common::SuppressionFactorCacheMs;
use crate::hybrid::SyncIntervalMs;
use crate::{
    HardLimitFactor, LocalRateLimiterOptions, RateGroupSizeMs, RateLimit, RateLimitDecision,
    RateLimiter, RateLimiterOptions, RedisKey, RedisRateLimiterOptions, WindowSizeSeconds,
};

fn window_capacity(window_size_seconds: u64, rate_limit: &RateLimit) -> u64 {
    ((window_size_seconds as f64) * **rate_limit) as u64
}

fn record_decision(
    decision: RateLimitDecision,
    count: u64,
    accepted_volume: &mut u64,
    rejected_volume: &mut u64,
    allowed_ops: &mut u64,
    rejected_ops: &mut u64,
) {
    match decision {
        RateLimitDecision::Allowed => {
            *accepted_volume += count;
            *allowed_ops += 1;
        }
        RateLimitDecision::Rejected { .. } => {
            *rejected_volume += count;
            *rejected_ops += 1;
        }
        RateLimitDecision::Suppressed { .. } => {
            panic!("suppressed decision is not expected in absolute strategy")
        }
    }
}

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
            sync_interval_ms: SyncIntervalMs::default(),
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
        thread::sleep(Duration::from_millis(100));

        // At capacity (6 * 1 = 6). Next increment should be rejected.
        let decision = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        let RateLimitDecision::Rejected {
            remaining_after_waiting,
            ..
        } = decision
        else {
            panic!("expected rejected decision, got {decision:?}");
        };

        // When usage is merged into one bucket, waiting for the oldest bucket to expire clears
        // the full capacity.
        assert_eq!(
            remaining_after_waiting, 6,
            "remaining_after_waiting: {remaining_after_waiting}, decision: {decision:?}"
        );
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
        assert!(
            matches!(decision, RateLimitDecision::Rejected { .. }),
            "should be rejected, received decision: {decision:?}"
        );

        // If the rejected increment mutated state, this would be rejected.
        let decision2 = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(
            matches!(decision2, RateLimitDecision::Allowed),
            "should be allowed, received decision: {decision2:?}"
        );
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
        assert!(
            matches!(d0, RateLimitDecision::Rejected { .. }),
            "should be rejected, instead got {d0:?}"
        );

        // Wait until the first bucket is out of window (2s) but the second is still in-window.
        thread::sleep(Duration::from_millis(1850));

        let decision = rl.redis().absolute().is_allowed(&k).await.unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "should be allowed, instead got {decision:?}"
        );
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

#[test]
fn volume_unit_increments_accepts_exact_capacity_then_rejects_rest() {
    let Some(url) = redis_url() else { return };

    block_on(async {
        let window_size_seconds = 1_u64;
        let rl = build_limiter(&url, window_size_seconds, 1000).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(50f64).unwrap();
        let capacity = window_capacity(window_size_seconds, &rate_limit);
        assert_eq!(capacity, 50);

        let mut accepted_volume = 0_u64;
        let mut rejected_volume = 0_u64;
        let mut allowed_ops = 0_u64;
        let mut rejected_ops = 0_u64;

        for _ in 0..80_u64 {
            let decision = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
            record_decision(
                decision,
                1,
                &mut accepted_volume,
                &mut rejected_volume,
                &mut allowed_ops,
                &mut rejected_ops,
            );
        }

        assert_eq!(accepted_volume, capacity);
        assert_eq!(rejected_volume, 80 - capacity);
        assert_eq!(allowed_ops, capacity);
        assert_eq!(rejected_ops, 80 - capacity);
    });
}

#[test]
fn volume_batch_increment_is_all_or_nothing_and_matches_expected_volumes() {
    let Some(url) = redis_url() else { return };

    block_on(async {
        let window_size_seconds = 1_u64;
        let rl = build_limiter(&url, window_size_seconds, 1000).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(10f64).unwrap();
        let capacity = window_capacity(window_size_seconds, &rate_limit);
        assert_eq!(capacity, 10);

        let mut accepted_volume = 0_u64;
        let mut rejected_volume = 0_u64;
        let mut allowed_ops = 0_u64;
        let mut rejected_ops = 0_u64;

        // Allowed: consumes 9 of 10.
        let d1 = rl.redis().absolute().inc(&k, &rate_limit, 9).await.unwrap();
        record_decision(
            d1,
            9,
            &mut accepted_volume,
            &mut rejected_volume,
            &mut allowed_ops,
            &mut rejected_ops,
        );

        // Rejected: would push total to 11.
        let d2 = rl.redis().absolute().inc(&k, &rate_limit, 2).await.unwrap();
        assert!(
            matches!(d2, RateLimitDecision::Rejected { .. }),
            "d2: {d2:?}"
        );
        record_decision(
            d2,
            2,
            &mut accepted_volume,
            &mut rejected_volume,
            &mut allowed_ops,
            &mut rejected_ops,
        );

        // Allowed: proves the rejected batch did not consume capacity.
        let d3 = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        record_decision(
            d3,
            1,
            &mut accepted_volume,
            &mut rejected_volume,
            &mut allowed_ops,
            &mut rejected_ops,
        );

        assert_eq!(accepted_volume, capacity);
        assert_eq!(rejected_volume, 2);
        assert_eq!(allowed_ops, 2);
        assert_eq!(rejected_ops, 1);
    });
}

#[test]
fn volume_rejections_do_not_consume_and_capacity_resets_after_window_expiry() {
    let Some(url) = redis_url() else { return };

    block_on(async {
        let window_size_seconds = 1_u64;
        let rl = build_limiter(&url, window_size_seconds, 1000).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(2f64).unwrap();
        let capacity = window_capacity(window_size_seconds, &rate_limit);
        assert_eq!(capacity, 2);

        // Fill capacity.
        for _ in 0..capacity {
            let decision = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
            assert!(
                matches!(decision, RateLimitDecision::Allowed),
                "decision: {decision:?}"
            );
        }

        // Many rejected attempts should not change what we can do after the window expires.
        let mut rejected_ops = 0_u64;
        for _ in 0..20_u64 {
            let decision = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
            assert!(
                matches!(decision, RateLimitDecision::Rejected { .. }),
                "decision: {decision:?}"
            );
            rejected_ops += 1;
        }
        assert_eq!(rejected_ops, 20);

        thread::sleep(Duration::from_millis(1100));

        let mut accepted_after_expiry = 0_u64;
        for _ in 0..capacity {
            let decision = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
            assert!(
                matches!(decision, RateLimitDecision::Allowed),
                "decision: {decision:?}"
            );
            accepted_after_expiry += 1;
        }
        assert_eq!(accepted_after_expiry, capacity);
    });
}

#[test]
fn volume_non_integer_rate_uses_truncating_capacity() {
    let Some(url) = redis_url() else { return };

    block_on(async {
        let window_size_seconds = 1_u64;
        let rl = build_limiter(&url, window_size_seconds, 1000).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(2.9f64).unwrap();
        let capacity = window_capacity(window_size_seconds, &rate_limit);
        assert_eq!(capacity, 2);

        for _ in 0..capacity {
            let decision = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
            assert!(
                matches!(decision, RateLimitDecision::Allowed),
                "decision: {decision:?}"
            );
        }

        let decision = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Rejected { .. }),
            "decision: {decision:?}"
        );
    });
}
