use std::{
    sync::{Arc, Barrier, atomic::Ordering, mpsc},
    thread,
    time::Duration,
};

use crate::common::{
    BucketSize, HardLimitFactor, HistoryPreservation, RateLimit, RateLimitComparator,
    RateLimitDecision, SuppressionFactorCachePeriod, WindowSize,
};
use crate::{builder::ProviderConfig, local::AbsoluteLocalRateLimiter};

fn window_limit(window_size: u64, rate_limit: &RateLimit) -> f64 {
    window_size as f64 * rate_limit.as_per_second()
}

fn window_capacity(window_size: u64, rate_limit: &RateLimit) -> u64 {
    window_limit(window_size, rate_limit) as u64
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

fn limiter(window_size: u64, bucket_size: u64) -> AbsoluteLocalRateLimiter {
    AbsoluteLocalRateLimiter::new(ProviderConfig {
        window_size: WindowSize::seconds(window_size).unwrap(),
        bucket_size: BucketSize::milliseconds(bucket_size).unwrap(),
        hard_limit_factor: HardLimitFactor::default(),
        suppression_factor_cache_period: SuppressionFactorCachePeriod::default(),
    })
}

fn assert_retry_after_between(retry_after: Duration, min_ms: u128, max_ms: u128) {
    let retry_after = retry_after.as_millis();
    assert!(
        retry_after >= min_ms && retry_after <= max_ms,
        "retry_after={retry_after}, expected {min_ms} <= retry_after <= {max_ms}"
    );
}

#[test]
fn is_allowed_unknown_key_is_allowed() {
    let limiter = limiter(1, 50);

    assert!(matches!(
        limiter.is_allowed("missing"),
        RateLimitDecision::Allowed
    ));
}

#[test]
fn rejects_at_exact_window_limit() {
    let limiter = limiter(1, 1000);
    let key = "k";
    let rate_limit = RateLimit::per_second(2f64).unwrap();

    assert!(matches!(
        limiter.inc(key, &rate_limit, 1),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Allowed
    ));

    assert!(matches!(
        limiter.inc(key, &rate_limit, 1),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Rejected { .. }
    ));
}

#[test]
fn rejects_at_exact_window_limit_window_6() {
    let limiter = limiter(6, 1000);
    let key = "k";
    let rate_limit = RateLimit::per_second(2f64).unwrap();

    // window_limit = 6 * 2 = 12
    assert!(matches!(
        limiter.inc(key, &rate_limit, 11),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Allowed
    ));

    assert!(matches!(
        limiter.inc(key, &rate_limit, 1),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Rejected { .. }
    ));
}

#[test]
fn rejects_at_exact_window_limit_window_10() {
    let limiter = limiter(10, 1000);
    let key = "k";
    let rate_limit = RateLimit::per_second(2f64).unwrap();

    // window_limit = 10 * 2 = 20
    assert!(matches!(
        limiter.inc(key, &rate_limit, 19),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Allowed
    ));

    assert!(matches!(
        limiter.inc(key, &rate_limit, 1),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Rejected { .. }
    ));
}

#[test]
fn per_key_state_is_independent() {
    let limiter = limiter(1, 1000);
    let rate_limit = RateLimit::per_second(2f64).unwrap();

    assert!(matches!(
        limiter.inc("a", &rate_limit, 2),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        limiter.is_allowed("a"),
        RateLimitDecision::Rejected { .. }
    ));

    assert!(matches!(
        limiter.is_allowed("b"),
        RateLimitDecision::Allowed
    ));
}

#[test]
fn window_limit_for_key_is_not_updated_after_first_inc() {
    let window_size = 6;
    let limiter = limiter(window_size, 1000);
    let key = "k";
    let strict_rate_limit = RateLimit::per_second(1f64).unwrap();
    let loose_rate_limit = RateLimit::per_second(100f64).unwrap();

    // Seed key with a strict limit.
    assert!(matches!(
        limiter.inc(key, &strict_rate_limit, 0),
        RateLimitDecision::Allowed
    ));
    assert_eq!(
        limiter.series().get(key).unwrap().window_limit,
        window_limit(window_size, &strict_rate_limit),
        "the series must store the computed window limit, not the per-second rate"
    );

    // If the stored window limit were updated from 6 to 600 here, the
    // resulting total would remain below capacity.
    assert!(matches!(
        limiter.inc(key, &loose_rate_limit, 6),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Rejected { .. }
    ));
}

#[test]
fn rejected_includes_retry_after_and_remaining_after_waiting() {
    let limiter = limiter(1, 10);
    let key = "k";
    let rate_limit = RateLimit::per_second(5f64).unwrap();

    assert!(matches!(
        limiter.inc(key, &rate_limit, 1),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(20));
    assert!(matches!(
        limiter.inc(key, &rate_limit, 4),
        RateLimitDecision::Allowed
    ));

    let decision = limiter.is_allowed(key);
    let RateLimitDecision::Rejected {
        window_size,
        retry_after,
        remaining_after_waiting,
    } = decision
    else {
        panic!("expected rejected decision");
    };

    assert_eq!(window_size.as_seconds(), 1);
    assert_eq!(remaining_after_waiting, 1);
    assert_retry_after_between(retry_after, 800, 1_000);
}

#[test]
fn rejected_includes_retry_after_and_remaining_after_waiting_window_6() {
    let limiter = limiter(6, 1);
    let key = "k";
    let rate_limit = RateLimit::per_second(1f64).unwrap();

    assert!(matches!(
        limiter.inc(key, &rate_limit, 2),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(10));
    assert!(matches!(
        limiter.inc(key, &rate_limit, 4),
        RateLimitDecision::Allowed
    ));

    let decision = limiter.is_allowed(key);
    let RateLimitDecision::Rejected {
        window_size,
        retry_after,
        remaining_after_waiting,
    } = decision
    else {
        panic!("expected rejected decision");
    };

    assert_eq!(window_size.as_seconds(), 6);
    assert_eq!(remaining_after_waiting, 2);
    assert_retry_after_between(retry_after, 5_000, 6_000);
}

#[test]
fn rejected_includes_retry_after_and_remaining_after_waiting_window_10() {
    let limiter = limiter(10, 1);
    let key = "k";
    let rate_limit = RateLimit::per_second(1f64).unwrap();

    assert!(matches!(
        limiter.inc(key, &rate_limit, 3),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(10));
    assert!(matches!(
        limiter.inc(key, &rate_limit, 7),
        RateLimitDecision::Allowed
    ));

    let decision = limiter.is_allowed(key);
    let RateLimitDecision::Rejected {
        window_size,
        retry_after,
        remaining_after_waiting,
    } = decision
    else {
        panic!("expected rejected decision");
    };

    assert_eq!(window_size.as_seconds(), 10);
    assert_eq!(remaining_after_waiting, 3);
    assert_retry_after_between(retry_after, 9_000, 10_000);

    let decision = limiter.inc(key, &rate_limit, 1);
    let RateLimitDecision::Rejected {
        remaining_after_waiting,
        ..
    } = decision
    else {
        panic!("expected rejected increment, got {decision:?}");
    };
    assert_eq!(remaining_after_waiting, 3);
    assert_eq!(
        limiter.get(key),
        10,
        "a rejected increment must not consume capacity"
    );
}

#[test]
fn rejected_metadata_window_6_with_nonzero_grouping_separates_buckets() {
    let limiter = limiter(6, 200);
    let key = "k";
    let rate_limit = RateLimit::per_second(1f64).unwrap();

    // Two buckets (sleep > group size). window_limit = 6 * 1 = 6
    assert!(matches!(
        limiter.inc(key, &rate_limit, 2),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(250));
    assert!(matches!(
        limiter.inc(key, &rate_limit, 4),
        RateLimitDecision::Allowed
    ));

    let decision = limiter.is_allowed(key);
    let RateLimitDecision::Rejected {
        window_size,
        retry_after,
        remaining_after_waiting,
    } = decision
    else {
        panic!("expected rejected decision");
    };

    assert_eq!(window_size.as_seconds(), 6);
    assert_eq!(remaining_after_waiting, 2);
    assert_retry_after_between(retry_after, 5_000, 6_000);
}

#[test]
fn rejected_metadata_window_10_with_nonzero_grouping_separates_buckets() {
    let limiter = limiter(10, 200);
    let key = "k";
    let rate_limit = RateLimit::per_second(1f64).unwrap();

    // Two buckets (sleep > group size). window_limit = 10 * 1 = 10
    assert!(matches!(
        limiter.inc(key, &rate_limit, 3),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(250));
    assert!(matches!(
        limiter.inc(key, &rate_limit, 7),
        RateLimitDecision::Allowed
    ));

    let decision = limiter.is_allowed(key);
    let RateLimitDecision::Rejected {
        window_size,
        retry_after,
        remaining_after_waiting,
    } = decision
    else {
        panic!("expected rejected decision");
    };

    assert_eq!(window_size.as_seconds(), 10);
    assert_eq!(remaining_after_waiting, 3);
    assert_retry_after_between(retry_after, 9_000, 10_000);
}

#[test]
fn rejected_metadata_window_6_with_nonzero_grouping_merges_within_group() {
    let limiter = limiter(6, 200);
    let key = "k";
    let rate_limit = RateLimit::per_second(1f64).unwrap();

    // Same bucket (sleep < group size). window_limit = 6 * 1 = 6
    assert!(matches!(
        limiter.inc(key, &rate_limit, 3),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(100));
    assert!(matches!(
        limiter.inc(key, &rate_limit, 3),
        RateLimitDecision::Allowed
    ));

    let decision = limiter.is_allowed(key);
    let RateLimitDecision::Rejected {
        retry_after,
        remaining_after_waiting,
        ..
    } = decision
    else {
        panic!("expected rejected decision");
    };

    assert_eq!(remaining_after_waiting, 6);
    assert_retry_after_between(retry_after, 5_000, 6_000);
}

#[test]
fn rejected_metadata_window_10_with_nonzero_grouping_merges_within_group() {
    let limiter = limiter(10, 200);
    let key = "k";
    let rate_limit = RateLimit::per_second(1f64).unwrap();

    // Same bucket (sleep < group size). window_limit = 10 * 1 = 10
    assert!(matches!(
        limiter.inc(key, &rate_limit, 5),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(100));
    assert!(matches!(
        limiter.inc(key, &rate_limit, 5),
        RateLimitDecision::Allowed
    ));

    let decision = limiter.is_allowed(key);
    let RateLimitDecision::Rejected {
        retry_after,
        remaining_after_waiting,
        ..
    } = decision
    else {
        panic!("expected rejected decision");
    };

    assert_eq!(remaining_after_waiting, 10);
    assert_retry_after_between(retry_after, 9_000, 10_000);
}

#[test]
fn unblocks_after_window_expires() {
    let limiter = limiter(1, 1000);
    let key = "k";
    let rate_limit = RateLimit::per_second(3f64).unwrap();

    assert!(matches!(
        limiter.inc(key, &rate_limit, 3),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Rejected { .. }
    ));

    // is_allowed eviction uses `elapsed().as_millis()`; for a 1s window it should
    // unblock shortly after ~1000ms.
    thread::sleep(Duration::from_millis(1100));

    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Allowed
    ));
}

#[test]
fn unblocks_after_window_expires_window_2_with_nonzero_grouping() {
    let limiter = limiter(2, 200);
    let key = "k";
    let rate_limit = RateLimit::per_second(1f64).unwrap();

    // window_limit = 2 * 1 = 2
    assert!(matches!(
        limiter.inc(key, &rate_limit, 2),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Rejected { .. }
    ));

    // Eviction uses `elapsed().as_millis()`; for a 2s window it should unblock
    // shortly after ~2000ms.
    thread::sleep(Duration::from_millis(2100));

    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Allowed
    ));
}

#[test]
fn evicts_oldest_bucket_but_keeps_newer_bucket_window_2_with_grouping() {
    let limiter = limiter(2, 200);
    let key = "k";
    let rate_limit = RateLimit::per_second(1f64).unwrap();

    assert!(matches!(
        limiter.inc(key, &rate_limit, 1),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(300));
    assert!(matches!(
        limiter.inc(key, &rate_limit, 1),
        RateLimitDecision::Allowed
    ));

    // The first bucket expires while the second remains inside the window.
    thread::sleep(Duration::from_millis(1_800));

    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Allowed
    ));

    let series = limiter.series().get(key).expect("series should remain");
    assert_eq!(series.buckets.len(), 1);
    assert_eq!(
        series
            .buckets
            .front()
            .unwrap()
            .count
            .load(Ordering::Acquire),
        1
    );
    assert_eq!(series.total_count.load(Ordering::Acquire), 1);
}

#[test]
fn rate_grouping_merges_within_group() {
    let limiter = limiter(1, 50);
    let key = "k";
    let rate_limit = RateLimit::per_second(6f64).unwrap();

    assert!(matches!(
        limiter.inc(key, &rate_limit, 3),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(10));
    assert!(matches!(
        limiter.inc(key, &rate_limit, 3),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Rejected { .. }
    ));

    let series = limiter.series().get(key).expect("series should exist");
    assert_eq!(
        series.buckets.len(),
        1,
        "increments should share one bucket"
    );
    assert_eq!(
        series
            .buckets
            .front()
            .unwrap()
            .count
            .load(Ordering::Acquire),
        6
    );
    assert_eq!(series.total_count.load(Ordering::Acquire), 6);
}

#[test]
fn rate_grouping_separates_beyond_group() {
    let limiter = limiter(1, 50);
    let key = "k";
    let rate_limit = RateLimit::per_second(10f64).unwrap();

    assert!(matches!(
        limiter.inc(key, &rate_limit, 2),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(75));
    assert!(matches!(
        limiter.inc(key, &rate_limit, 3),
        RateLimitDecision::Allowed
    ));

    let series = limiter
        .series()
        .get(key)
        .expect("expected key to exist in limiter");
    assert_eq!(series.buckets.len(), 2);
    let counts = series
        .buckets
        .iter()
        .map(|bucket| bucket.count.load(Ordering::Acquire))
        .collect::<Vec<_>>();
    assert_eq!(counts, vec![2, 3]);
    assert_eq!(series.total_count.load(Ordering::Acquire), 5);
    drop(series);
    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Allowed
    ));
}

#[test]
fn concurrent_inc_eventually_rejects_without_panicking() {
    let limiter = Arc::new(limiter(1, 1000));
    let key = "k";
    let rate_limit = RateLimit::per_second(10f64).unwrap();

    let threads: Vec<_> = (0..8)
        .map(|_| {
            let limiter = limiter.clone();

            thread::spawn(move || {
                for _ in 0..25 {
                    limiter.inc(key, &rate_limit, 1);
                    let _ = limiter.is_allowed(key);
                }
            })
        })
        .collect();

    for t in threads {
        t.join().expect("thread panicked");
    }

    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Rejected { .. }
    ));
}

#[test]
fn cleanup_removes_stale_keys_by_last_activity() {
    let limiter = limiter(60, 1000);
    let limit = RateLimit::per_second(1f64).unwrap();

    assert!(matches!(
        limiter.inc("stale", &limit, 1),
        RateLimitDecision::Allowed
    ));
    assert!(limiter.series().contains_key("stale"));
    thread::sleep(Duration::from_millis(1_100));

    limiter.cleanup(1_000);
    assert!(!limiter.series().contains_key("stale"));
}

#[test]
fn cleanup_keeps_fresh_keys() {
    let limiter = limiter(60, 1000);
    let limit = RateLimit::per_second(1f64).unwrap();

    assert!(matches!(
        limiter.inc("fresh", &limit, 1),
        RateLimitDecision::Allowed
    ));
    limiter.cleanup(10_000);
    assert!(limiter.series().contains_key("fresh"));
}

#[test]
fn cleanup_uses_most_recent_bucket_not_oldest() {
    let limiter = limiter(60, 1000);
    let limit = RateLimit::per_second(1f64).unwrap();

    assert!(matches!(
        limiter.inc("mixed", &limit, 1),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(1_100));
    assert!(matches!(
        limiter.inc("mixed", &limit, 1),
        RateLimitDecision::Allowed
    ));

    limiter.cleanup(1_000);
    assert!(limiter.series().contains_key("mixed"));
}

#[test]
fn cleanup_removal_makes_key_unknown_and_allowed() {
    let limiter = limiter(60, 1000);
    let key = "k";
    let limit = RateLimit::per_second(1f64).unwrap();

    // window_limit = 60 * 1 = 60.
    assert!(matches!(
        limiter.inc(key, &limit, 60),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Rejected { .. }
    ));

    thread::sleep(Duration::from_millis(20));
    limiter.cleanup(10);
    assert!(!limiter.series().contains_key(key));
    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Allowed
    ));
}

#[test]
fn cleanup_is_millisecond_granularity() {
    let limiter = limiter(60, 1000);
    let limit = RateLimit::per_second(1f64).unwrap();

    assert!(matches!(
        limiter.inc("ms", &limit, 1),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(150));
    limiter.cleanup(100);
    assert!(!limiter.series().contains_key("ms"));
}

#[test]
fn cleanup_concurrent_is_allowed_smoke_does_not_panic() {
    let limiter = Arc::new(limiter(60, 1));
    let limit = RateLimit::per_second(1f64).unwrap();

    // Seed some state through the limiter API.
    assert!(matches!(
        limiter.inc("a", &limit, 1),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        limiter.inc("b", &limit, 1),
        RateLimitDecision::Allowed
    ));

    let reader = {
        let limiter = limiter.clone();
        thread::spawn(move || {
            for i in 0..50_000u64 {
                let k = if i % 2 == 0 { "a" } else { "b" };
                let _ = limiter.is_allowed(k);

                // Occasionally re-seed a key to race with cleanup.
                if i % 5000 == 0 {
                    let _ = limiter.inc(k, &limit, 1);
                }
            }
        })
    };

    let cleaner = {
        let limiter = limiter.clone();
        thread::spawn(move || {
            for _ in 0..5_000u64 {
                limiter.cleanup(0);
            }
        })
    };

    reader.join().expect("reader thread panicked");
    cleaner.join().expect("cleaner thread panicked");
}

#[test]
fn volume_unit_increments_accepts_exact_capacity_then_rejects_rest() {
    let window_size = 1_u64;
    let limiter = limiter(window_size, 1000);

    let k = "k";
    let rate_limit = RateLimit::per_second(50f64).unwrap();
    let capacity = window_capacity(window_size, &rate_limit);
    assert_eq!(capacity, 50);

    let mut accepted_volume = 0_u64;
    let mut rejected_volume = 0_u64;
    let mut allowed_ops = 0_u64;
    let mut rejected_ops = 0_u64;

    for _ in 0..80_u64 {
        let decision = limiter.inc(k, &rate_limit, 1);
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
    assert_eq!(limiter.get(k), capacity);
}

#[test]
fn volume_batch_increment_matches_expected_volumes_under_local_semantics() {
    let window_size = 1_u64;
    let limiter = limiter(window_size, 1000);

    let k = "k";
    let rate_limit = RateLimit::per_second(10f64).unwrap();
    let capacity = window_capacity(window_size, &rate_limit);
    assert_eq!(capacity, 10);

    let mut accepted_volume = 0_u64;
    let mut rejected_volume = 0_u64;
    let mut allowed_ops = 0_u64;
    let mut rejected_ops = 0_u64;

    // Allowed: consumes 9 of 10.
    let d1 = limiter.inc(k, &rate_limit, 9);
    record_decision(
        d1,
        9,
        &mut accepted_volume,
        &mut rejected_volume,
        &mut allowed_ops,
        &mut rejected_ops,
    );

    // NOTE: The local absolute limiter admission check does not take a `count` parameter,
    // so batch increments can overshoot capacity. This test asserts accounting invariants
    // of the decisions that are returned.
    let d2 = limiter.inc(k, &rate_limit, 2);
    record_decision(
        d2,
        2,
        &mut accepted_volume,
        &mut rejected_volume,
        &mut allowed_ops,
        &mut rejected_ops,
    );

    // Next increment should be rejected once capacity is exceeded.
    let d3 = limiter.inc(k, &rate_limit, 1);
    record_decision(
        d3,
        1,
        &mut accepted_volume,
        &mut rejected_volume,
        &mut allowed_ops,
        &mut rejected_ops,
    );

    assert_eq!(accepted_volume, 11);
    assert_eq!(rejected_volume, 1);
    assert_eq!(allowed_ops, 2);
    assert_eq!(rejected_ops, 1);
    assert_eq!(limiter.get(k), 11);
}

#[test]
fn volume_rejections_do_not_consume_and_capacity_resets_after_window_expiry() {
    let window_size = 1_u64;
    let limiter = limiter(window_size, 1000);

    let k = "k";
    let rate_limit = RateLimit::per_second(2f64).unwrap();
    let capacity = window_capacity(window_size, &rate_limit);
    assert_eq!(capacity, 2);

    // Fill capacity.
    for _ in 0..capacity {
        let decision = limiter.inc(k, &rate_limit, 1);
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "decision: {decision:?}"
        );
    }

    // Many rejected attempts should not change what we can do after the window expires.
    let mut rejected_ops = 0_u64;
    for _ in 0..20_u64 {
        let decision = limiter.inc(k, &rate_limit, 1);
        assert!(
            matches!(decision, RateLimitDecision::Rejected { .. }),
            "decision: {decision:?}"
        );
        rejected_ops += 1;
    }
    assert_eq!(rejected_ops, 20);
    assert_eq!(limiter.get(k), capacity, "rejections must not consume");

    thread::sleep(Duration::from_millis(1100));

    let mut accepted_after_expiry = 0_u64;
    for _ in 0..capacity {
        let decision = limiter.inc(k, &rate_limit, 1);
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "decision: {decision:?}"
        );
        accepted_after_expiry += 1;
    }
    assert_eq!(accepted_after_expiry, capacity);
    assert_eq!(limiter.get(k), capacity);
}

#[test]
fn volume_non_integer_rate_uses_truncating_capacity() {
    let window_size = 1_u64;
    let limiter = limiter(window_size, 1000);

    let k = "k";
    let rate_limit = RateLimit::per_second(2.9f64).unwrap();
    let capacity = window_capacity(window_size, &rate_limit);
    assert_eq!(capacity, 2);

    for _ in 0..capacity {
        let decision = limiter.inc(k, &rate_limit, 1);
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "decision: {decision:?}"
        );
    }

    let decision = limiter.inc(k, &rate_limit, 1);
    assert!(
        matches!(decision, RateLimitDecision::Rejected { .. }),
        "decision: {decision:?}"
    );
    assert_eq!(limiter.get(k), capacity);
}

#[test]
fn get_unknown_key_returns_zero() {
    let limiter = limiter(6, 1000);

    assert_eq!(limiter.get("k"), 0);
    assert!(limiter.series().is_empty());
}

#[test]
fn get_returns_current_window_total() {
    let limiter = limiter(6, 1000);
    let rate_limit = RateLimit::per_second(100f64).unwrap();

    for _ in 0..3 {
        let decision = limiter.inc("k", &rate_limit, 1);
        assert!(matches!(decision, RateLimitDecision::Allowed));
    }

    assert_eq!(limiter.get("k"), 3);
}

#[test]
fn get_excludes_and_evicts_expired_buckets() {
    let window_size = 1_u64;
    let limiter = limiter(window_size, 50);
    let rate_limit = RateLimit::per_second(100f64).unwrap();

    assert!(matches!(
        limiter.inc("k2", &rate_limit, 5),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        limiter.inc("k", &rate_limit, 5),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(200));
    assert!(matches!(
        limiter.inc("k", &rate_limit, 3),
        RateLimitDecision::Allowed
    ));

    // The first bucket for `k` and the only bucket for `k2` expire while the
    // second bucket for `k` remains live.
    thread::sleep(Duration::from_millis(850));

    assert_eq!(limiter.get("k"), 3, "expired bucket must be ignored");
    {
        let series = limiter.series().get("k").expect("series should remain");
        assert_eq!(series.buckets.len(), 1, "expired history must be evicted");
        assert_eq!(
            series
                .buckets
                .front()
                .unwrap()
                .count
                .load(Ordering::Acquire),
            3
        );
        assert_eq!(series.total_count.load(Ordering::Acquire), 3);
    }

    assert_eq!(limiter.get("k2"), 0);
    let series = limiter.series().get("k2").expect("series should remain");
    assert!(series.buckets.is_empty(), "expired history must be evicted");
    assert_eq!(series.total_count.load(Ordering::Acquire), 0);
}

#[test]
fn get_fresh_key_uses_shared_dashmap_lock() {
    let limiter = Arc::new(limiter(6, 1000));
    let rate_limit = RateLimit::per_second(100f64).unwrap();
    assert!(matches!(
        limiter.inc("k", &rate_limit, 3),
        RateLimitDecision::Allowed
    ));

    let held_read_guard = limiter.series().get("k").expect("series should exist");
    let barrier = Arc::new(Barrier::new(2));
    let (sender, receiver) = mpsc::channel();

    let reader = {
        let limiter = Arc::clone(&limiter);
        let barrier = Arc::clone(&barrier);

        thread::spawn(move || {
            barrier.wait();
            sender.send(limiter.get("k")).unwrap();
        })
    };

    barrier.wait();
    let result = receiver.recv_timeout(Duration::from_secs(2));

    // Release the guard before joining so a regression to get_mut cannot strand
    // the worker after the timeout.
    drop(held_read_guard);
    reader.join().expect("reader thread panicked");

    assert_eq!(
        result.expect("fresh-key get blocked on an existing shared DashMap guard"),
        3
    );
}

#[test]
fn conditional_set_guard_miss_uses_shared_dashmap_lock() {
    let limiter = Arc::new(limiter(6, 1000));
    let rate = RateLimit::per_second(100f64).unwrap();
    assert!(matches!(
        limiter.inc("k", &rate, 3),
        RateLimitDecision::Allowed
    ));

    let held_read_guard = limiter.series().get("k").expect("series should exist");
    let barrier = Arc::new(Barrier::new(2));
    let (sender, receiver) = mpsc::channel();

    let worker = {
        let limiter = Arc::clone(&limiter);
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier.wait();
            let replace = limiter.set_if("k", &rate, RateLimitComparator::Eq(99), 10);
            let preserve = limiter.set_if_preserve_history(
                "k",
                &rate,
                RateLimitComparator::Eq(99),
                10,
                HistoryPreservation::PreserveNewest,
            );
            sender.send((replace, preserve)).unwrap();
        })
    };

    barrier.wait();
    let result = receiver.recv_timeout(Duration::from_secs(2));
    drop(held_read_guard);
    worker.join().expect("worker thread panicked");

    let (replace, preserve) =
        result.expect("guard-miss conditional sets blocked on a shared DashMap guard");
    assert_eq!(replace, (3, 3));
    assert_eq!(preserve, (3, 3));
}

#[test]
fn conditional_set_guard_miss_leaves_expired_history_untouched() {
    let limiter = limiter(1, 1);
    let rate = RateLimit::per_second(100f64).unwrap();
    assert!(matches!(
        limiter.inc("k", &rate, 4),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(200));
    assert!(matches!(
        limiter.inc("k", &rate, 5),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(850));

    assert_eq!(
        limiter.set_if("k", &rate, RateLimitComparator::Eq(99), 10),
        (5, 5)
    );
    assert_eq!(
        limiter.set_if_preserve_history(
            "k",
            &rate,
            RateLimitComparator::Eq(99),
            10,
            HistoryPreservation::PreserveNewest,
        ),
        (5, 5)
    );

    let series = limiter.series().get("k").expect("series should remain");
    assert_eq!(series.buckets.len(), 2, "miss must not prune history");
    assert_eq!(
        series.total_count.load(Ordering::Acquire),
        9,
        "miss must not rewrite stored counters"
    );
}

#[test]
fn set_if_lt_primes_empty_key_and_reprime_is_noop() {
    let limiter = limiter(6, 1000);
    let rate_limit = RateLimit::per_second(100f64).unwrap();

    let outcome = limiter.set_if("k", &rate_limit, RateLimitComparator::Lt(100), 100);
    let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
    assert_eq!((new_total, old_total), (100, 0));

    let outcome = limiter.set_if("k", &rate_limit, RateLimitComparator::Lt(100), 100);
    let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
    assert_eq!((new_total, old_total), (100, 100));

    assert_eq!(limiter.get("k"), 100);
}

#[test]
fn set_if_lt_with_lower_target_is_noop() {
    let limiter = limiter(6, 1000);
    let rate_limit = RateLimit::per_second(100f64).unwrap();

    assert_eq!(
        limiter.set_if("k", &rate_limit, RateLimitComparator::Lt(100), 100),
        (100, 0)
    );

    let outcome = limiter.set_if("k", &rate_limit, RateLimitComparator::Lt(50), 50);
    let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
    assert_eq!((new_total, old_total), (100, 100));
    assert_eq!(limiter.get("k"), 100);
    let series = limiter.series().get("k").expect("series should exist");
    assert_eq!(series.buckets.len(), 1);
    assert_eq!(
        series
            .buckets
            .front()
            .unwrap()
            .count
            .load(Ordering::Acquire),
        100
    );
    assert_eq!(series.total_count.load(Ordering::Acquire), 100);
}

#[test]
fn set_if_always_overwrites_unconditionally_including_lowering() {
    let limiter = limiter(6, 1000);
    let rate_limit = RateLimit::per_second(100f64).unwrap();

    assert_eq!(
        limiter.set_if("k", &rate_limit, RateLimitComparator::Always, 100),
        (100, 0)
    );

    let outcome = limiter.set_if("k", &rate_limit, RateLimitComparator::Always, 30);
    let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
    assert_eq!((new_total, old_total), (30, 100));
    assert_eq!(limiter.get("k"), 30);
    {
        let series = limiter.series().get("k").expect("series should exist");
        assert_eq!(series.buckets.len(), 1);
        assert_eq!(
            series
                .buckets
                .front()
                .unwrap()
                .count
                .load(Ordering::Acquire),
            30
        );
        assert_eq!(series.total_count.load(Ordering::Acquire), 30);
    }

    // Overwriting to 0 removes the key entirely; admission resumes from empty.
    let outcome = limiter.set_if("k", &rate_limit, RateLimitComparator::Always, 0);
    let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
    assert_eq!((new_total, old_total), (0, 30));
    assert_eq!(limiter.get("k"), 0);
    assert!(!limiter.series().contains_key("k"));

    let decision = limiter.inc("k", &rate_limit, 1);
    assert!(matches!(decision, RateLimitDecision::Allowed));
}

#[test]
fn set_if_eq_zero_sets_only_when_window_is_empty() {
    let limiter = limiter(6, 1000);
    let rate_limit = RateLimit::per_second(100f64).unwrap();

    let outcome = limiter.set_if("k", &rate_limit, RateLimitComparator::Eq(0), 25);
    let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
    assert_eq!((new_total, old_total), (25, 0));

    let outcome = limiter.set_if("k", &rate_limit, RateLimitComparator::Eq(0), 99);
    let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
    assert_eq!((new_total, old_total), (25, 25));
    assert_eq!(limiter.get("k"), 25);
}

#[test]
fn set_if_gt_and_ne_guards_follow_current_total() {
    let limiter = limiter(6, 1000);
    let rate_limit = RateLimit::per_second(100f64).unwrap();

    assert_eq!(
        limiter.set_if("k", &rate_limit, RateLimitComparator::Always, 10),
        (10, 0)
    );

    // Gt(5): 10 > 5 matches → lowered to 3.
    let outcome = limiter.set_if("k", &rate_limit, RateLimitComparator::Gt(5), 3);
    let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
    assert_eq!((new_total, old_total), (3, 10));

    // Ne(3): current is exactly 3 → no match.
    let outcome = limiter.set_if("k", &rate_limit, RateLimitComparator::Ne(3), 7);
    let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
    assert_eq!((new_total, old_total), (3, 3));

    // Ne(5): current is 3 → match.
    let outcome = limiter.set_if("k", &rate_limit, RateLimitComparator::Ne(5), 7);
    let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
    assert_eq!((new_total, old_total), (7, 3));
    assert_eq!(limiter.get("k"), 7);
}

#[test]
fn set_if_prime_then_inc_enforces_remaining_budget() {
    let window_size = 6_u64;
    let limiter = limiter(window_size, 1000);
    let rate_limit = RateLimit::per_second(5f64).unwrap();
    let capacity = window_capacity(window_size, &rate_limit);
    assert_eq!(capacity, 30);

    // Prime 27 of 30: exactly 3 units of budget remain.
    let outcome = limiter.set_if("k", &rate_limit, RateLimitComparator::Lt(27), 27);
    let new_total = outcome.current_total;
    assert_eq!(new_total, 27);

    for i in 0..3_u64 {
        let decision = limiter.inc("k", &rate_limit, 1);
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "i: {i}, decision: {decision:?}"
        );
    }
    assert_eq!(limiter.get("k"), capacity);

    let decision = limiter.inc("k", &rate_limit, 1);
    assert!(
        matches!(decision, RateLimitDecision::Rejected { .. }),
        "decision: {decision:?}"
    );
    assert_eq!(limiter.get("k"), capacity, "rejection must not consume");
}

#[test]
fn set_if_prime_at_capacity_rejects_next_inc() {
    let window_size = 6_u64;
    let limiter = limiter(window_size, 1000);
    let rate_limit = RateLimit::per_second(5f64).unwrap();
    let capacity = window_capacity(window_size, &rate_limit);

    let outcome = limiter.set_if(
        "k",
        &rate_limit,
        RateLimitComparator::Lt(capacity),
        capacity,
    );
    let new_total = outcome.current_total;
    assert_eq!(new_total, capacity);

    let decision = limiter.inc("k", &rate_limit, 1);
    assert!(
        matches!(decision, RateLimitDecision::Rejected { .. }),
        "decision: {decision:?}"
    );

    let decision = limiter.is_allowed("k");
    assert!(
        matches!(decision, RateLimitDecision::Rejected { .. }),
        "decision: {decision:?}"
    );
    assert_eq!(limiter.get("k"), capacity);
}

#[test]
fn set_if_redefines_sticky_rate_limit() {
    let window_size = 6_u64;
    let limiter = limiter(window_size, 1000);

    // Fill to the original capacity (6 * 1 = 6); the key is now rejecting.
    let rate_one = RateLimit::per_second(1f64).unwrap();
    for _ in 0..6 {
        let decision = limiter.inc("k", &rate_one, 1);
        assert!(matches!(decision, RateLimitDecision::Allowed));
    }
    let decision = limiter.inc("k", &rate_one, 1);
    assert!(matches!(decision, RateLimitDecision::Rejected { .. }));

    // Unlike inc (sticky limit), set_if redefines the stored window limit: 6 * 10 = 60.
    let rate_ten = RateLimit::per_second(10f64).unwrap();
    assert_eq!(
        limiter.set_if("k", &rate_ten, RateLimitComparator::Always, 1),
        (1, 6)
    );

    {
        let series = limiter.series().get("k").expect("series should remain");
        assert_eq!(
            series.window_limit,
            window_limit(window_size, &rate_ten),
            "matched set_if must redefine the stored window limit"
        );
        assert_eq!(series.buckets.len(), 1);
        assert_eq!(
            series
                .buckets
                .front()
                .unwrap()
                .count
                .load(Ordering::Acquire),
            1
        );
        assert_eq!(series.total_count.load(Ordering::Acquire), 1);
    }

    // The old capacity of 6 no longer applies (the inc rate argument is still
    // ignored — the stored limit now comes from set_if).
    for i in 0..12_u64 {
        let decision = limiter.inc("k", &rate_one, 1);
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "i: {i}, decision: {decision:?}"
        );
    }
}

#[test]
fn set_if_evicts_expired_buckets_before_comparing() {
    let window_size = 1_u64;
    let limiter = limiter(window_size, 1000);
    let rate_limit = RateLimit::per_second(100f64).unwrap();

    assert!(matches!(
        limiter.inc("k", &rate_limit, 50),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(1_100));

    // Eq(0) must observe the post-eviction total.
    let outcome = limiter.set_if("k", &rate_limit, RateLimitComparator::Eq(0), 9);
    let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
    assert_eq!((new_total, old_total), (9, 0));
    let series = limiter.series().get("k").expect("series should exist");
    assert_eq!(series.buckets.len(), 1);
    assert_eq!(
        series
            .buckets
            .front()
            .unwrap()
            .count
            .load(Ordering::Acquire),
        9
    );
    assert_eq!(series.total_count.load(Ordering::Acquire), 9);
}

#[test]
fn set_if_unmatched_guard_on_unknown_key_writes_no_counts() {
    let limiter = limiter(6, 1000);
    let rate_limit = RateLimit::per_second(100f64).unwrap();

    let outcome = limiter.set_if("k", &rate_limit, RateLimitComparator::Gt(5), 9);
    let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
    assert_eq!((new_total, old_total), (0, 0));
    assert_eq!(limiter.get("k"), 0);
    assert!(!limiter.series().contains_key("k"));
}

#[test]
fn set_if_preserve_newest_reduces_from_front_and_increases_newest() {
    let limiter = limiter(10, 1);
    let rate = RateLimit::per_second(100f64).unwrap();
    assert!(matches!(
        limiter.inc("k", &rate, 4),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(5));
    assert!(matches!(
        limiter.inc("k", &rate, 5),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(5));
    assert!(matches!(
        limiter.inc("k", &rate, 6),
        RateLimitDecision::Allowed
    ));

    let result = limiter.set_if_preserve_history(
        "k",
        &rate,
        RateLimitComparator::Always,
        8,
        HistoryPreservation::PreserveNewest,
    );
    assert_eq!(result, (8, 15));

    let series = limiter.series().get("k").unwrap();
    let counts = series
        .buckets
        .iter()
        .map(|bucket| bucket.count.load(Ordering::Acquire))
        .collect::<Vec<_>>();
    assert_eq!(counts, vec![2, 6]);
    assert_eq!(series.total_count.load(Ordering::Acquire), 8);
    assert_eq!(series.window_limit, window_limit(10, &rate));
    drop(series);

    let result = limiter.set_if_preserve_history(
        "k",
        &rate,
        RateLimitComparator::Always,
        11,
        HistoryPreservation::PreserveNewest,
    );
    assert_eq!(result, (11, 8));
    let series = limiter.series().get("k").unwrap();
    let counts = series
        .buckets
        .iter()
        .map(|bucket| bucket.count.load(Ordering::Acquire))
        .collect::<Vec<_>>();
    assert_eq!(counts, vec![2, 9]);
    assert_eq!(series.total_count.load(Ordering::Acquire), 11);
}

#[test]
fn set_if_preserve_oldest_reduces_from_back_and_increases_oldest() {
    let limiter = limiter(10, 1);
    let rate = RateLimit::per_second(100f64).unwrap();
    assert!(matches!(
        limiter.inc("k", &rate, 4),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(5));
    assert!(matches!(
        limiter.inc("k", &rate, 5),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(5));
    assert!(matches!(
        limiter.inc("k", &rate, 6),
        RateLimitDecision::Allowed
    ));

    assert_eq!(
        limiter.set_if_preserve_history(
            "k",
            &rate,
            RateLimitComparator::Always,
            8,
            HistoryPreservation::PreserveOldest,
        ),
        (8, 15)
    );
    let series = limiter.series().get("k").unwrap();
    let counts = series
        .buckets
        .iter()
        .map(|bucket| bucket.count.load(Ordering::Acquire))
        .collect::<Vec<_>>();
    assert_eq!(counts, vec![4, 4]);
    assert_eq!(series.total_count.load(Ordering::Acquire), 8);
    assert_eq!(series.window_limit, window_limit(10, &rate));
    drop(series);

    assert_eq!(
        limiter.set_if_preserve_history(
            "k",
            &rate,
            RateLimitComparator::Always,
            11,
            HistoryPreservation::PreserveOldest,
        ),
        (11, 8)
    );
    let series = limiter.series().get("k").unwrap();
    let counts = series
        .buckets
        .iter()
        .map(|bucket| bucket.count.load(Ordering::Acquire))
        .collect::<Vec<_>>();
    assert_eq!(counts, vec![7, 4]);
    assert_eq!(series.total_count.load(Ordering::Acquire), 11);
}

#[test]
fn set_if_preserve_history_redefines_limit_even_when_total_is_unchanged() {
    let limiter = limiter(6, 1);
    let old_rate = RateLimit::per_second(1f64).unwrap();
    let new_rate = RateLimit::per_second(10f64).unwrap();
    assert!(matches!(
        limiter.inc("k", &old_rate, 2),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(5));
    assert!(matches!(
        limiter.inc("k", &old_rate, 3),
        RateLimitDecision::Allowed
    ));

    assert_eq!(
        limiter.set_if_preserve_history(
            "k",
            &new_rate,
            RateLimitComparator::Eq(5),
            5,
            HistoryPreservation::PreserveNewest,
        ),
        (5, 5)
    );

    let series = limiter.series().get("k").expect("series should exist");
    assert_eq!(
        series.window_limit,
        window_limit(6, &new_rate),
        "matched preserve must redefine the stored window limit"
    );
    let counts = series
        .buckets
        .iter()
        .map(|bucket| bucket.count.load(Ordering::Acquire))
        .collect::<Vec<_>>();
    assert_eq!(counts, vec![2, 3], "unchanged total must preserve history");
    assert_eq!(series.total_count.load(Ordering::Acquire), 5);
}

#[test]
fn set_if_preserve_history_prunes_expired_before_directional_reduction() {
    let limiter = limiter(1, 1);
    let rate = RateLimit::per_second(100f64).unwrap();
    assert!(matches!(
        limiter.inc("k", &rate, 4),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(200));
    assert!(matches!(
        limiter.inc("k", &rate, 5),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(200));
    assert!(matches!(
        limiter.inc("k", &rate, 6),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(700));

    assert_eq!(
        limiter.set_if_preserve_history(
            "k",
            &rate,
            RateLimitComparator::Eq(11),
            8,
            HistoryPreservation::PreserveNewest,
        ),
        (8, 11)
    );

    let series = limiter.series().get("k").expect("series should exist");
    let counts = series
        .buckets
        .iter()
        .map(|bucket| bucket.count.load(Ordering::Acquire))
        .collect::<Vec<_>>();
    assert_eq!(counts, vec![2, 6]);
    assert_eq!(series.total_count.load(Ordering::Acquire), 8);
}

#[test]
fn set_if_preserve_history_zero_removes_key() {
    let limiter = limiter(10, 1);
    let rate = RateLimit::per_second(100f64).unwrap();
    assert!(matches!(
        limiter.inc("k", &rate, 4),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(5));
    assert!(matches!(
        limiter.inc("k", &rate, 5),
        RateLimitDecision::Allowed
    ));

    assert_eq!(
        limiter.set_if_preserve_history(
            "k",
            &rate,
            RateLimitComparator::Always,
            0,
            HistoryPreservation::PreserveOldest,
        ),
        (0, 9)
    );

    assert_eq!(limiter.get("k"), 0);
    assert!(!limiter.series().contains_key("k"));
}

#[test]
fn conditional_set_missing_zero_leaves_key_absent() {
    let limiter = limiter(6, 1000);
    let rate = RateLimit::per_second(100f64).unwrap();

    assert_eq!(
        limiter.set_if("k", &rate, RateLimitComparator::Eq(0), 0),
        (0, 0)
    );
    assert_eq!(
        limiter.set_if_preserve_history(
            "k2",
            &rate,
            RateLimitComparator::Eq(0),
            0,
            HistoryPreservation::PreserveNewest,
        ),
        (0, 0)
    );
    assert!(limiter.series().is_empty());
}

#[test]
fn conditional_set_present_zero_removes_key() {
    let limiter = limiter(6, 1);
    let rate = RateLimit::per_second(100f64).unwrap();

    assert!(matches!(
        limiter.inc("replace", &rate, 3),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        limiter.inc("preserve", &rate, 4),
        RateLimitDecision::Allowed
    ));

    assert_eq!(
        limiter.set_if("replace", &rate, RateLimitComparator::Always, 0),
        (0, 3)
    );
    assert_eq!(
        limiter.set_if_preserve_history(
            "preserve",
            &rate,
            RateLimitComparator::Always,
            0,
            HistoryPreservation::PreserveNewest,
        ),
        (0, 4)
    );

    assert_eq!(limiter.get("replace"), 0);
    assert_eq!(limiter.get("preserve"), 0);
    assert!(!limiter.series().contains_key("replace"));
    assert!(!limiter.series().contains_key("preserve"));
}

#[test]
fn set_if_preserve_history_creates_missing_positive_target_for_both_directions() {
    let limiter = limiter(6, 1);
    let rate = RateLimit::per_second(100f64).unwrap();

    for (key, preservation) in [
        ("newest", HistoryPreservation::PreserveNewest),
        ("oldest", HistoryPreservation::PreserveOldest),
    ] {
        assert_eq!(
            limiter.set_if_preserve_history(
                key,
                &rate,
                RateLimitComparator::Eq(0),
                9,
                preservation,
            ),
            (9, 0)
        );

        let series = limiter.series().get(key).expect("series should be created");
        assert_eq!(series.window_limit, window_limit(6, &rate));
        assert_eq!(series.buckets.len(), 1);
        assert_eq!(
            series
                .buckets
                .front()
                .unwrap()
                .count
                .load(Ordering::Acquire),
            9
        );
        assert_eq!(series.total_count.load(Ordering::Acquire), 9);
    }
}
