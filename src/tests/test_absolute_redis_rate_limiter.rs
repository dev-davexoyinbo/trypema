use std::{thread, time::Duration};

use super::{
    common::{key, redis_url, unique_prefix},
    runtime,
};

use crate::common::SuppressionFactorCacheMs;
use crate::hybrid::SyncIntervalMs;
use crate::{
    HardLimitFactor, HistoryPreservation, LocalRateLimiterOptions, RateGroupSizeMs, RateLimit,
    RateLimitComparator, RateLimitDecision, RateLimiter, RateLimiterOptions,
    RedisRateLimiterOptions, WindowSizeSeconds,
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

fn assert_allowed(decision: RateLimitDecision, context: &str) {
    assert!(
        matches!(&decision, RateLimitDecision::Allowed),
        "{context}: {decision:?}"
    );
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
    let url = redis_url();

    runtime::block_on(async {
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
fn inc_uses_the_first_rate_limit_for_existing_keys() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 1, 1000).await;
        let low_then_high = key("low-then-high");
        let low_rate = RateLimit::try_from(2f64).unwrap();
        let high_rate = RateLimit::try_from(10f64).unwrap();

        assert_allowed(
            rl.redis()
                .absolute()
                .inc(&low_then_high, &low_rate, 2)
                .await
                .unwrap(),
            "the first increment should fill the original capacity",
        );
        let decision = rl
            .redis()
            .absolute()
            .inc(&low_then_high, &high_rate, 1)
            .await
            .unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Rejected { .. }),
            "a later larger rate must not increase the sticky capacity: {decision:?}"
        );
        assert_eq!(rl.redis().absolute().get(&low_then_high).await.unwrap(), 2);

        let high_then_low = key("high-then-low");
        assert_allowed(
            rl.redis()
                .absolute()
                .inc(&high_then_low, &high_rate, 8)
                .await
                .unwrap(),
            "the first increment should establish the larger capacity",
        );
        assert_allowed(
            rl.redis()
                .absolute()
                .inc(&high_then_low, &low_rate, 2)
                .await
                .unwrap(),
            "a later smaller rate must not reduce the sticky capacity",
        );
        assert_eq!(rl.redis().absolute().get(&high_then_low).await.unwrap(), 10);
    });
}

#[test]
fn per_key_state_is_independent() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 1, 1000).await;

        let a = key("a");
        let b = key("b");
        let rate_limit = RateLimit::try_from(2f64).unwrap();

        // Saturate key a.
        assert_allowed(
            rl.redis().absolute().inc(&a, &rate_limit, 2).await.unwrap(),
            "filling key a",
        );
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
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 6, 300).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        // Create a single bucket by staying within the rate-group coalescing window.
        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 2).await.unwrap(),
            "creating the oldest grouped usage",
        );
        thread::sleep(Duration::from_millis(50));
        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 4).await.unwrap(),
            "coalescing usage into the oldest bucket",
        );
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
    let url = redis_url();

    runtime::block_on(async {
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
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 10, 200).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        // Create two buckets.
        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 3).await.unwrap(),
            "creating the oldest bucket",
        );
        thread::sleep(Duration::from_millis(250));
        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 7).await.unwrap(),
            "creating the newest bucket",
        );

        // At capacity (10 * 1 = 10). Next increment should be rejected.
        let decision = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        let RateLimitDecision::Rejected {
            window_size_seconds,
            retry_after_ms,
            remaining_after_waiting,
        } = decision
        else {
            panic!("expected rejected decision");
        };

        assert_eq!(window_size_seconds, 10, "window size should be 10");
        assert!(
            retry_after_ms >= 8_500 && retry_after_ms <= 10_000,
            "retry after should be the remaining lifetime of the oldest bucket, got {retry_after_ms}"
        );
        // The oldest bucket releases exactly its count when it expires.
        assert_eq!(
            remaining_after_waiting, 3,
            "three count units should become available, got {remaining_after_waiting}"
        );
    });
}

#[test]
fn is_allowed_unknown_key_is_allowed() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 1, 50).await;

        let k = key("missing");
        let decision = rl.redis().absolute().is_allowed(&k).await.unwrap();
        assert!(matches!(decision, RateLimitDecision::Allowed));
    });
}

#[test]
fn rejected_inc_does_not_consume_capacity() {
    let url = redis_url();

    runtime::block_on(async {
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
fn oversized_request_on_empty_key_has_no_backoff_wait() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size_seconds = 1_u64;
        let rl = build_limiter(&url, window_size_seconds, 1_000).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let capacity = window_capacity(window_size_seconds, &rate_limit);

        let decision = rl
            .redis()
            .absolute()
            .inc(&k, &rate_limit, capacity + 1)
            .await
            .unwrap();
        let RateLimitDecision::Rejected {
            retry_after_ms,
            remaining_after_waiting,
            ..
        } = decision
        else {
            panic!("expected oversized request to be rejected, got {decision:?}");
        };

        assert_eq!(retry_after_ms, 0, "no existing bucket needs to expire");
        assert_eq!(remaining_after_waiting, 0, "no bucket releases capacity");
        assert_eq!(rl.redis().absolute().get(&k).await.unwrap(), 0);
    });
}

#[test]
fn is_allowed_evicts_old_buckets_and_updates_total_count() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 2, 200).await;
        let k = key("k");
        // Use a rate limit that makes behavior differ depending on whether eviction happens.
        // window=2s, rate=1/s -> capacity=2.
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        // Two buckets (sleep > group size).
        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap(),
            "creating the oldest bucket",
        );
        thread::sleep(Duration::from_millis(750));
        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap(),
            "creating the newest bucket",
        );

        // At exact capacity: should be rejected.
        let d0 = rl.redis().absolute().is_allowed(&k).await.unwrap();
        assert!(
            matches!(d0, RateLimitDecision::Rejected { .. }),
            "should be rejected, instead got {d0:?}"
        );

        // Wait until the first bucket is out of window (2s) but the second is still in-window.
        thread::sleep(Duration::from_millis(1350));

        let decision = rl.redis().absolute().is_allowed(&k).await.unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "should be allowed, instead got {decision:?}"
        );
        assert_eq!(
            rl.redis().absolute().get(&k).await.unwrap(),
            1,
            "only the newest bucket should remain live"
        );
    });
}

#[test]
fn inc_evicts_expired_buckets_before_admission() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 1, 200).await;
        let k = key("k");
        // window=1s, rate=2/s -> capacity=2.
        // If eviction does not happen, the post-sleep increment would push total over capacity.
        let rate_limit = RateLimit::try_from(2f64).unwrap();

        // Two buckets.
        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap(),
            "creating the oldest bucket",
        );
        thread::sleep(Duration::from_millis(250));
        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap(),
            "creating the newest bucket",
        );

        // Wait past the window so both buckets are expired.
        thread::sleep(Duration::from_millis(1200));
        let decision = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "post-expiry increment should be allowed, instead got {decision:?}"
        );
        assert_eq!(rl.redis().absolute().get(&k).await.unwrap(), 1);
    });
}

#[test]
fn is_allowed_reflects_rejected_after_hitting_limit_then_allows_after_expiry() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 1, 1000).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(2f64).unwrap();

        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 2).await.unwrap(),
            "filling the window",
        );
        let d1 = rl.redis().absolute().is_allowed(&k).await.unwrap();
        assert!(matches!(d1, RateLimitDecision::Rejected { .. }));

        thread::sleep(Duration::from_millis(1100));
        let d2 = rl.redis().absolute().is_allowed(&k).await.unwrap();
        assert!(matches!(d2, RateLimitDecision::Allowed));
    });
}

#[test]
fn is_allowed_rejected_includes_retry_after_and_remaining_after_waiting() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 10, 200).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 3).await.unwrap(),
            "creating the oldest bucket",
        );
        thread::sleep(Duration::from_millis(250));
        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 7).await.unwrap(),
            "creating the newest bucket",
        );

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
            window_size_seconds, 10,
            "window size should be 10 instead got {window_size_seconds}"
        );
        assert!(
            retry_after_ms >= 8_500 && retry_after_ms <= 10_000,
            "retry after should be the remaining lifetime of the oldest bucket, got {retry_after_ms}"
        );
        assert_eq!(
            remaining_after_waiting, 3,
            "three count units should become available, got {remaining_after_waiting}"
        );
    });
}

#[test]
fn is_allowed_returns_allowed_when_below_limit() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 6, 200).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 5).await.unwrap(),
            "seeding usage below the limit",
        );
        let decision = rl.redis().absolute().is_allowed(&k).await.unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "should be allowed"
        );
    });
}

#[test]
fn volume_unit_increments_accepts_exact_capacity_then_rejects_rest() {
    let url = redis_url();

    runtime::block_on(async {
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
    let url = redis_url();

    runtime::block_on(async {
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
    let url = redis_url();

    runtime::block_on(async {
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
    let url = redis_url();

    runtime::block_on(async {
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

        let decision = rl.redis().absolute().is_allowed(&k).await.unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Rejected { .. }),
            "the truncated capacity must be full: {decision:?}"
        );

        let decision = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Rejected { .. }),
            "decision: {decision:?}"
        );
    });
}

#[test]
fn get_returns_zero_for_untouched_key() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 6, 1000).await;

        let total = rl.redis().absolute().get(&key("k")).await.unwrap();
        assert_eq!(total, 0);
    });
}

#[test]
fn get_returns_exact_window_total() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 6, 1000).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(100f64).unwrap();

        for _ in 0..3 {
            let d = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
            assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");
        }

        // Pure Redis provider: every inc is committed immediately, so get is exact.
        let total = rl.redis().absolute().get(&k).await.unwrap();
        assert_eq!(total, 3);
    });
}

#[test]
fn get_evicts_expired_buckets() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size_seconds = 1_u64;
        let rl = build_limiter(&url, window_size_seconds, 1000).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(100f64).unwrap();

        assert_allowed(
            rl.redis().absolute().inc(&k, &rate_limit, 5).await.unwrap(),
            "seeding usage before expiry",
        );
        assert_eq!(rl.redis().absolute().get(&k).await.unwrap(), 5);

        // Wait for the window to pass; get must observe the evicted (empty) window.
        runtime::async_sleep(Duration::from_millis(1_100)).await;
        assert_eq!(rl.redis().absolute().get(&k).await.unwrap(), 0);
    });
}

#[test]
fn set_if_lt_primes_empty_key_and_reprime_is_noop() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 6, 1000).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(100f64).unwrap();

        let (new_total, old_total) = rl
            .redis()
            .absolute()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(100), 100)
            .await
            .unwrap();
        assert_eq!((new_total, old_total), (100, 0));

        let (new_total, old_total) = rl
            .redis()
            .absolute()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(100), 100)
            .await
            .unwrap();
        assert_eq!((new_total, old_total), (100, 100));

        assert_eq!(rl.redis().absolute().get(&k).await.unwrap(), 100);
    });
}

#[test]
fn set_if_lt_with_lower_target_is_noop() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 6, 1000).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(100f64).unwrap();

        assert_eq!(
            rl.redis()
                .absolute()
                .set_if(&k, &rate_limit, RateLimitComparator::Lt(100), 100)
                .await
                .unwrap(),
            (100, 0)
        );

        let (new_total, old_total) = rl
            .redis()
            .absolute()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(50), 50)
            .await
            .unwrap();
        assert_eq!((new_total, old_total), (100, 100));
    });
}

#[test]
fn set_if_nil_overwrites_unconditionally_including_lowering() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 6, 1000).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(100f64).unwrap();

        assert_eq!(
            rl.redis()
                .absolute()
                .set_if(&k, &rate_limit, RateLimitComparator::Nil, 100)
                .await
                .unwrap(),
            (100, 0)
        );

        let (new_total, old_total) = rl
            .redis()
            .absolute()
            .set_if(&k, &rate_limit, RateLimitComparator::Nil, 30)
            .await
            .unwrap();
        assert_eq!((new_total, old_total), (30, 100));
        assert_eq!(rl.redis().absolute().get(&k).await.unwrap(), 30);

        // Overwriting to 0 clears the window; admission resumes from empty.
        let (new_total, old_total) = rl
            .redis()
            .absolute()
            .set_if(&k, &rate_limit, RateLimitComparator::Nil, 0)
            .await
            .unwrap();
        assert_eq!((new_total, old_total), (0, 30));
        assert_eq!(rl.redis().absolute().get(&k).await.unwrap(), 0);

        let d = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");
    });
}

#[test]
fn set_if_eq_zero_sets_only_when_window_is_empty() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 6, 1000).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(100f64).unwrap();

        let (new_total, old_total) = rl
            .redis()
            .absolute()
            .set_if(&k, &rate_limit, RateLimitComparator::Eq(0), 25)
            .await
            .unwrap();
        assert_eq!((new_total, old_total), (25, 0));

        let (new_total, old_total) = rl
            .redis()
            .absolute()
            .set_if(&k, &rate_limit, RateLimitComparator::Eq(0), 99)
            .await
            .unwrap();
        assert_eq!((new_total, old_total), (25, 25));
    });
}

#[test]
fn set_if_gt_and_ne_guards_follow_current_total() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 6, 1000).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(100f64).unwrap();

        assert_eq!(
            rl.redis()
                .absolute()
                .set_if(&k, &rate_limit, RateLimitComparator::Nil, 10)
                .await
                .unwrap(),
            (10, 0)
        );

        // Gt(5): 10 > 5 matches → lowered to 3.
        let (new_total, old_total) = rl
            .redis()
            .absolute()
            .set_if(&k, &rate_limit, RateLimitComparator::Gt(5), 3)
            .await
            .unwrap();
        assert_eq!((new_total, old_total), (3, 10));

        // Ne(3): current is exactly 3 → no match.
        let (new_total, old_total) = rl
            .redis()
            .absolute()
            .set_if(&k, &rate_limit, RateLimitComparator::Ne(3), 7)
            .await
            .unwrap();
        assert_eq!((new_total, old_total), (3, 3));

        // Ne(5): current is 3 → match.
        let (new_total, old_total) = rl
            .redis()
            .absolute()
            .set_if(&k, &rate_limit, RateLimitComparator::Ne(5), 7)
            .await
            .unwrap();
        assert_eq!((new_total, old_total), (7, 3));
    });
}

#[test]
fn set_if_prime_then_inc_enforces_remaining_budget() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size_seconds = 6_u64;
        let rl = build_limiter(&url, window_size_seconds, 1000).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let capacity = window_capacity(window_size_seconds, &rate_limit);
        assert_eq!(capacity, 30);

        // Prime 27 of 30: exactly 3 units of budget remain.
        let (new_total, _) = rl
            .redis()
            .absolute()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(27), 27)
            .await
            .unwrap();
        assert_eq!(new_total, 27);

        for i in 0..3_u64 {
            let d = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
            assert!(matches!(d, RateLimitDecision::Allowed), "i: {i}, d: {d:?}");
        }

        let d = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(d, RateLimitDecision::Rejected { .. }), "d: {d:?}");
    });
}

#[test]
fn set_if_prime_at_capacity_rejects_inc_and_is_allowed() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size_seconds = 6_u64;
        let rl = build_limiter(&url, window_size_seconds, 1000).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let capacity = window_capacity(window_size_seconds, &rate_limit);

        let (new_total, _) = rl
            .redis()
            .absolute()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(capacity), capacity)
            .await
            .unwrap();
        assert_eq!(new_total, capacity);

        let d = rl.redis().absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(d, RateLimitDecision::Rejected { .. }), "d: {d:?}");

        // is_allowed reads the window limit stored by set_if.
        let d = rl.redis().absolute().is_allowed(&k).await.unwrap();
        assert!(matches!(d, RateLimitDecision::Rejected { .. }), "d: {d:?}");
    });
}

#[test]
fn set_if_preserve_history_creates_missing_positive_keys_in_both_directions() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 6, 1000).await;
        let rate_limit = RateLimit::try_from(10f64).unwrap();

        for (name, preservation) in [
            ("newest", HistoryPreservation::PreserveNewest),
            ("oldest", HistoryPreservation::PreserveOldest),
        ] {
            let k = key(name);
            let result = rl
                .redis()
                .absolute()
                .set_if_preserve_history(
                    &k,
                    &rate_limit,
                    RateLimitComparator::Eq(0),
                    5,
                    preservation,
                )
                .await
                .unwrap();
            assert_eq!(result, (5, 0));
            assert_eq!(rl.redis().absolute().get(&k).await.unwrap(), 5);
        }
    });
}

#[test]
fn set_if_preserve_history_redefines_limit_when_total_is_unchanged() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 1, 1000).await;
        let k = key("k");
        let initial_rate = RateLimit::try_from(10f64).unwrap();
        let replacement_rate = RateLimit::try_from(6f64).unwrap();

        assert_allowed(
            rl.redis()
                .absolute()
                .inc(&k, &initial_rate, 5)
                .await
                .unwrap(),
            "seeding usage under the initial limit",
        );
        assert_eq!(
            rl.redis()
                .absolute()
                .set_if_preserve_history(
                    &k,
                    &replacement_rate,
                    RateLimitComparator::Eq(5),
                    5,
                    HistoryPreservation::PreserveNewest,
                )
                .await
                .unwrap(),
            (5, 5)
        );

        assert_allowed(
            rl.redis()
                .absolute()
                .inc(&k, &initial_rate, 1)
                .await
                .unwrap(),
            "one unit should remain under the redefined capacity",
        );
        let decision = rl
            .redis()
            .absolute()
            .inc(&k, &initial_rate, 1)
            .await
            .unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Rejected { .. }),
            "the matched conditional set must redefine the sticky limit: {decision:?}"
        );
    });
}

#[test]
fn set_if_and_get_do_not_cross_provider_keyspaces() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 6, 1000).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(100f64).unwrap();

        // Write through the pure Redis provider only.
        assert_eq!(
            rl.redis()
                .absolute()
                .set_if(&k, &rate_limit, RateLimitComparator::Nil, 40)
                .await
                .unwrap(),
            (40, 0)
        );

        // The hybrid provider (same prefix) uses a separate keyspace and must see nothing.
        assert_eq!(rl.hybrid().absolute().get(&k).await.unwrap(), 0);
        assert_eq!(rl.redis().absolute().get(&k).await.unwrap(), 40);

        // And the reverse: hybrid writes stay invisible to the pure Redis provider.
        assert_eq!(
            rl.hybrid()
                .absolute()
                .set_if(&k, &rate_limit, RateLimitComparator::Nil, 7)
                .await
                .unwrap(),
            (7, 0)
        );
        assert_eq!(rl.redis().absolute().get(&k).await.unwrap(), 40);
        assert_eq!(rl.hybrid().absolute().get(&k).await.unwrap(), 7);
    });
}
