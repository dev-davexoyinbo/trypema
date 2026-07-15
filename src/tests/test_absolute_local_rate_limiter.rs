use std::{
    collections::VecDeque,
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

use crate::common::{
    HardLimitFactor, HistoryPreservation, InstantRate, RateGroupSizeMs, RateLimit,
    RateLimitComparator, RateLimitDecision, SuppressionFactorCacheMs, WindowSizeSeconds,
};
use crate::local::AbsoluteRateLimitSeries as RateLimitSeries;
use crate::{AbsoluteLocalRateLimiter, LocalRateLimiterOptions};

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

fn limiter(window_size_seconds: u64, rate_group_size_ms: u64) -> AbsoluteLocalRateLimiter {
    AbsoluteLocalRateLimiter::new(LocalRateLimiterOptions {
        window_size_seconds: WindowSizeSeconds::try_from(window_size_seconds).unwrap(),
        rate_group_size_ms: RateGroupSizeMs::try_from(rate_group_size_ms).unwrap(),
        hard_limit_factor: HardLimitFactor::default(),
        suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
    })
}

fn insert_series(
    limiter: &AbsoluteLocalRateLimiter,
    key: &str,
    limit: RateLimit,
    buckets: &[(u64, u64)],
) {
    let total = buckets.iter().map(|(count, _)| *count).sum::<u64>();
    let series = buckets
        .iter()
        .map(|(count, ms_ago)| InstantRate {
            count: AtomicU64::new(*count),
            declined: AtomicU64::new(0),
            timestamp: Instant::now() - Duration::from_millis(*ms_ago),
        })
        .collect::<VecDeque<_>>();

    limiter.series().insert(
        key.to_string(),
        RateLimitSeries {
            limit,
            series,
            total: AtomicU64::new(total),
        },
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
    let rate_limit = RateLimit::try_from(2f64).unwrap();

    limiter.inc(key, &rate_limit, 1);
    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Allowed
    ));

    limiter.inc(key, &rate_limit, 1);
    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Rejected { .. }
    ));
}

#[test]
fn rejects_at_exact_window_limit_window_6() {
    let limiter = limiter(6, 1000);
    let key = "k";
    let rate_limit = RateLimit::try_from(2f64).unwrap();

    // window_limit = 6 * 2 = 12
    limiter.inc(key, &rate_limit, 11);
    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Allowed
    ));

    limiter.inc(key, &rate_limit, 1);
    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Rejected { .. }
    ));
}

#[test]
fn rejects_at_exact_window_limit_window_10() {
    let limiter = limiter(10, 1000);
    let key = "k";
    let rate_limit = RateLimit::try_from(2f64).unwrap();

    // window_limit = 10 * 2 = 20
    limiter.inc(key, &rate_limit, 19);
    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Allowed
    ));

    limiter.inc(key, &rate_limit, 1);
    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Rejected { .. }
    ));
}

#[test]
fn per_key_state_is_independent() {
    let limiter = limiter(1, 1000);
    let rate_limit = RateLimit::try_from(2f64).unwrap();

    limiter.inc("a", &rate_limit, 2);
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
fn rate_limit_for_key_is_not_updated_after_first_inc() {
    let limiter = limiter(1, 1000);
    let key = "k";
    let strict_rate_limit = RateLimit::try_from(1f64).unwrap();
    let loose_rate_limit = RateLimit::try_from(100f64).unwrap();

    // Seed key with a strict limit.
    limiter.inc(key, &strict_rate_limit, 0);

    // If the limit were updated to 100 here, this would be allowed.
    limiter.inc(key, &loose_rate_limit, 1);
    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Rejected { .. }
    ));
}

#[test]
fn rejected_includes_retry_after_and_remaining_after_waiting() {
    let limiter = limiter(1, 10);
    let key = "k";
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    // Create two buckets so that after the oldest expires, some usage remains.
    // Keep the first bucket below the limit so the second increment is applied.
    limiter.inc(key, &rate_limit, 1);
    thread::sleep(Duration::from_millis(20));
    limiter.inc(key, &rate_limit, 4);

    let decision = limiter.is_allowed(key);
    let RateLimitDecision::Rejected {
        window_size_seconds,
        retry_after_ms,
        remaining_after_waiting,
    } = decision
    else {
        panic!("expected rejected decision");
    };

    assert_eq!(window_size_seconds, 1);
    assert!(retry_after_ms <= 1000);
    assert_eq!(remaining_after_waiting, 4);
}

#[test]
fn rejected_includes_retry_after_and_remaining_after_waiting_window_6() {
    let limiter = limiter(6, 1);
    let key = "k";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    // Two buckets; after the oldest expires, some usage remains.
    limiter.inc(key, &rate_limit, 2);
    thread::sleep(Duration::from_millis(2));
    limiter.inc(key, &rate_limit, 4);

    let decision = limiter.is_allowed(key);
    let RateLimitDecision::Rejected {
        window_size_seconds,
        retry_after_ms,
        remaining_after_waiting,
    } = decision
    else {
        panic!("expected rejected decision");
    };

    assert_eq!(window_size_seconds, 6);
    assert!(retry_after_ms <= 6000);
    assert_eq!(remaining_after_waiting, 4);
}

#[test]
fn rejected_includes_retry_after_and_remaining_after_waiting_window_10() {
    let limiter = limiter(10, 1);
    let key = "k";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    // Two buckets; after the oldest expires, some usage remains.
    // Put the second bucket at the exact limit.
    limiter.inc(key, &rate_limit, 3);
    thread::sleep(Duration::from_millis(2));
    limiter.inc(key, &rate_limit, 7);

    let decision = limiter.is_allowed(key);
    let RateLimitDecision::Rejected {
        window_size_seconds,
        retry_after_ms,
        remaining_after_waiting,
    } = decision
    else {
        panic!("expected rejected decision");
    };

    assert_eq!(window_size_seconds, 10);
    assert!(retry_after_ms <= 10000);
    assert_eq!(remaining_after_waiting, 7);
}

#[test]
fn rejected_metadata_window_6_with_nonzero_grouping_separates_buckets() {
    let limiter = limiter(6, 200);
    let key = "k";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    // Two buckets (sleep > group size). window_limit = 6 * 1 = 6
    limiter.inc(key, &rate_limit, 2);
    thread::sleep(Duration::from_millis(250));
    limiter.inc(key, &rate_limit, 4);

    let decision = limiter.is_allowed(key);
    let RateLimitDecision::Rejected {
        window_size_seconds,
        retry_after_ms,
        remaining_after_waiting,
    } = decision
    else {
        panic!("expected rejected decision");
    };

    assert_eq!(window_size_seconds, 6);
    assert!(retry_after_ms <= 6000);
    assert_eq!(remaining_after_waiting, 4);
}

#[test]
fn rejected_metadata_window_10_with_nonzero_grouping_separates_buckets() {
    let limiter = limiter(10, 200);
    let key = "k";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    // Two buckets (sleep > group size). window_limit = 10 * 1 = 10
    limiter.inc(key, &rate_limit, 3);
    thread::sleep(Duration::from_millis(250));
    limiter.inc(key, &rate_limit, 7);

    let decision = limiter.is_allowed(key);
    let RateLimitDecision::Rejected {
        window_size_seconds,
        retry_after_ms,
        remaining_after_waiting,
    } = decision
    else {
        panic!("expected rejected decision");
    };

    assert_eq!(window_size_seconds, 10);
    assert!(retry_after_ms <= 10000);
    assert_eq!(remaining_after_waiting, 7);
}

#[test]
fn rejected_metadata_window_6_with_nonzero_grouping_merges_within_group() {
    let limiter = limiter(6, 200);
    let key = "k";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    // Same bucket (sleep < group size). window_limit = 6 * 1 = 6
    limiter.inc(key, &rate_limit, 3);
    thread::sleep(Duration::from_millis(100));
    limiter.inc(key, &rate_limit, 3);

    let decision = limiter.is_allowed(key);
    let RateLimitDecision::Rejected {
        retry_after_ms,
        remaining_after_waiting,
        ..
    } = decision
    else {
        panic!("expected rejected decision");
    };

    assert!(retry_after_ms <= 6000);
    // If grouped, the oldest bucket includes all usage, so nothing remains after waiting.
    assert_eq!(remaining_after_waiting, 0);
}

#[test]
fn rejected_metadata_window_10_with_nonzero_grouping_merges_within_group() {
    let limiter = limiter(10, 200);
    let key = "k";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    // Same bucket (sleep < group size). window_limit = 10 * 1 = 10
    limiter.inc(key, &rate_limit, 5);
    thread::sleep(Duration::from_millis(100));
    limiter.inc(key, &rate_limit, 5);

    let decision = limiter.is_allowed(key);
    let RateLimitDecision::Rejected {
        retry_after_ms,
        remaining_after_waiting,
        ..
    } = decision
    else {
        panic!("expected rejected decision");
    };

    assert!(retry_after_ms <= 10000);
    assert_eq!(remaining_after_waiting, 0);
}

#[test]
fn unblocks_after_window_expires() {
    let limiter = limiter(1, 1000);
    let key = "k";
    let rate_limit = RateLimit::try_from(3f64).unwrap();

    limiter.inc(key, &rate_limit, 3);
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
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    // window_limit = 2 * 1 = 2
    limiter.inc(key, &rate_limit, 2);
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
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    // Two buckets (sleep > group size) so that after the oldest expires, some usage remains.
    // window_limit = 2 * 1 = 2
    limiter.inc(key, &rate_limit, 1);
    thread::sleep(Duration::from_millis(250));
    limiter.inc(key, &rate_limit, 1);

    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Rejected { .. }
    ));

    // Wait long enough that:
    // - the oldest bucket is evicted (elapsed > 2000ms)
    // - the newer bucket is still in-window (elapsed <= 2000ms)
    thread::sleep(Duration::from_millis(2050));

    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Allowed
    ));
}

#[test]
fn rate_grouping_merges_within_group() {
    let limiter = limiter(1, 50);
    let key = "k";
    let rate_limit = RateLimit::try_from(3f64).unwrap();

    limiter.inc(key, &rate_limit, 3);
    thread::sleep(Duration::from_millis(10));
    limiter.inc(key, &rate_limit, 3);
    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Rejected { .. }
    ));

    // If increments were grouped, they share the oldest timestamp and expire together.
    // If they were incorrectly split into separate buckets, the newer bucket would still
    // be within the window and this would remain rejected.
    thread::sleep(Duration::from_millis(2005));

    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Allowed
    ));
}

#[test]
fn rate_grouping_separates_beyond_group() {
    let limiter = limiter(1, 50);
    let key = "k";
    let rate_limit = RateLimit::try_from(3f64).unwrap();

    // Create an initial bucket without consuming capacity. This ensures we can
    // later create a second bucket at the exact limit (3) and still have two buckets
    // so eviction behavior is observable.
    limiter.inc(key, &rate_limit, 0);

    // Ensure the second increment is beyond the grouping threshold but still within the
    // 1s window, so we end up with two buckets.
    thread::sleep(Duration::from_millis(150));

    // Put the second bucket at the exact limit.
    limiter.inc(key, &rate_limit, 3);
    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Rejected { .. }
    ));

    let series = limiter
        .series()
        .get(key)
        .expect("expected key to exist in limiter");
    assert_eq!(series.series.len(), 2);
    drop(series);

    // Wait until the first bucket is evictable but the second is still in-window.
    let start = std::time::Instant::now();
    loop {
        let series = limiter
            .series()
            .get(key)
            .expect("expected key to exist in limiter");

        let first_elapsed_ms = series
            .series
            .front()
            .expect("expected first bucket")
            .timestamp
            .elapsed()
            .as_millis();
        let second_elapsed_ms = series
            .series
            .back()
            .expect("expected second bucket")
            .timestamp
            .elapsed()
            .as_millis();
        drop(series);

        if first_elapsed_ms > 1000 && second_elapsed_ms <= 1000 {
            break;
        }

        if start.elapsed() > Duration::from_secs(5) {
            panic!(
                "timed out waiting for bucket ages; first_elapsed_ms={first_elapsed_ms} second_elapsed_ms={second_elapsed_ms}"
            );
        }

        thread::sleep(Duration::from_millis(10));
    }

    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Rejected { .. }
    ));
}

#[test]
fn concurrent_inc_eventually_rejects_without_panicking() {
    let limiter = Arc::new(limiter(1, 1000));
    let key = "k";
    let rate_limit = RateLimit::try_from(10f64).unwrap();

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
    let limit = RateLimit::try_from(1f64).unwrap();

    insert_series(&limiter, "stale", limit, &[(1, 5_000)]);
    assert!(limiter.series().contains_key("stale"));

    limiter.cleanup(1_000);
    assert!(!limiter.series().contains_key("stale"));
}

#[test]
fn cleanup_keeps_fresh_keys() {
    let limiter = limiter(60, 1000);
    let limit = RateLimit::try_from(1f64).unwrap();

    insert_series(&limiter, "fresh", limit, &[(1, 10)]);
    limiter.cleanup(10_000);
    assert!(limiter.series().contains_key("fresh"));
}

#[test]
fn cleanup_drops_empty_series_entries() {
    let limiter = limiter(60, 1000);
    let limit = RateLimit::try_from(1f64).unwrap();

    limiter
        .series()
        .insert("empty".to_string(), RateLimitSeries::new(limit));
    assert!(limiter.series().contains_key("empty"));

    limiter.cleanup(10_000);
    assert!(!limiter.series().contains_key("empty"));
}

#[test]
fn cleanup_uses_most_recent_bucket_not_oldest() {
    let limiter = limiter(60, 1000);
    let limit = RateLimit::try_from(1f64).unwrap();

    // Oldest is very old, but most recent bucket is fresh.
    insert_series(&limiter, "mixed", limit, &[(1, 60_000), (1, 10)]);

    limiter.cleanup(1_000);
    assert!(limiter.series().contains_key("mixed"));
}

#[test]
fn cleanup_removal_makes_key_unknown_and_allowed() {
    let limiter = limiter(60, 1000);
    let key = "k";
    let limit = RateLimit::try_from(1f64).unwrap();

    // window_limit = 60 * 1 = 60; total=61 => rejected.
    insert_series(&limiter, key, limit, &[(61, 50)]);
    assert!(matches!(
        limiter.is_allowed(key),
        RateLimitDecision::Rejected { .. }
    ));

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
    let limit = RateLimit::try_from(1f64).unwrap();

    insert_series(&limiter, "ms", limit, &[(1, 150)]);
    limiter.cleanup(100);
    assert!(!limiter.series().contains_key("ms"));
}

#[test]
fn cleanup_concurrent_is_allowed_smoke_does_not_panic() {
    let limiter = Arc::new(limiter(60, 1));
    let limit = RateLimit::try_from(1f64).unwrap();

    // Seed some state.
    insert_series(&limiter, "a", limit, &[(1, 0)]);
    insert_series(&limiter, "b", limit, &[(1, 0)]);

    let reader = {
        let limiter = limiter.clone();
        thread::spawn(move || {
            for i in 0..50_000u64 {
                let k = if i % 2 == 0 { "a" } else { "b" };
                let _ = limiter.is_allowed(k);

                // Occasionally re-seed a key to race with cleanup.
                if i % 5000 == 0 {
                    insert_series(&limiter, k, limit, &[(1, 0)]);
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
    let window_size_seconds = 1_u64;
    let limiter = limiter(window_size_seconds, 1000);

    let k = "k";
    let rate_limit = RateLimit::try_from(50f64).unwrap();
    let capacity = window_capacity(window_size_seconds, &rate_limit);
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
}

#[test]
fn volume_batch_increment_matches_expected_volumes_under_local_semantics() {
    let window_size_seconds = 1_u64;
    let limiter = limiter(window_size_seconds, 1000);

    let k = "k";
    let rate_limit = RateLimit::try_from(10f64).unwrap();
    let capacity = window_capacity(window_size_seconds, &rate_limit);
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

    assert!(
        accepted_volume >= capacity,
        "accepted_volume={accepted_volume} capacity={capacity}"
    );
    assert!(
        accepted_volume <= capacity + 2,
        "accepted_volume={accepted_volume} capacity={capacity}"
    );
    assert_eq!(accepted_volume + rejected_volume, 12);
    assert_eq!(allowed_ops + rejected_ops, 3);
}

#[test]
fn volume_rejections_do_not_consume_and_capacity_resets_after_window_expiry() {
    let window_size_seconds = 1_u64;
    let limiter = limiter(window_size_seconds, 1000);

    let k = "k";
    let rate_limit = RateLimit::try_from(2f64).unwrap();
    let capacity = window_capacity(window_size_seconds, &rate_limit);
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
}

#[test]
fn volume_non_integer_rate_uses_truncating_capacity() {
    let window_size_seconds = 1_u64;
    let limiter = limiter(window_size_seconds, 1000);

    let k = "k";
    let rate_limit = RateLimit::try_from(2.9f64).unwrap();
    let capacity = window_capacity(window_size_seconds, &rate_limit);
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
    let rate_limit = RateLimit::try_from(100f64).unwrap();

    for _ in 0..3 {
        let decision = limiter.inc("k", &rate_limit, 1);
        assert!(matches!(decision, RateLimitDecision::Allowed));
    }

    assert_eq!(limiter.get("k"), 3);
}

#[test]
fn get_ignores_expired_buckets() {
    let window_size_seconds = 1_u64;
    let limiter = limiter(window_size_seconds, 50);
    let rate_limit = RateLimit::try_from(100f64).unwrap();

    // One bucket well past the 1s window, one recent bucket.
    insert_series(&limiter, "k", rate_limit, &[(5, 2_000), (3, 100)]);

    assert_eq!(limiter.get("k"), 3, "expired bucket must be ignored");
    {
        let series = limiter.series().get("k").expect("series should remain");
        assert_eq!(series.series.len(), 2, "get must not evict history");
        assert_eq!(series.total.load(Ordering::Relaxed), 8);
    }

    // Only the expired bucket for this key.
    insert_series(&limiter, "k2", rate_limit, &[(5, 2_000)]);
    assert_eq!(limiter.get("k2"), 0);
    let series = limiter.series().get("k2").expect("series should remain");
    assert_eq!(series.series.len(), 1, "get must not evict history");
    assert_eq!(series.total.load(Ordering::Relaxed), 5);
}

#[test]
fn get_fresh_key_uses_shared_dashmap_lock() {
    let limiter = Arc::new(limiter(6, 1000));
    let rate_limit = RateLimit::try_from(100f64).unwrap();
    let _ = limiter.inc("k", &rate_limit, 3);

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
fn get_expired_key_uses_shared_dashmap_lock() {
    let limiter = Arc::new(limiter(1, 50));
    let rate_limit = RateLimit::try_from(100f64).unwrap();
    insert_series(&limiter, "k", rate_limit, &[(5, 2_000)]);

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

    drop(held_read_guard);
    reader.join().expect("reader thread panicked");

    assert_eq!(
        result.expect("expired-key get blocked on an existing shared DashMap guard"),
        0
    );

    let series = limiter.series().get("k").expect("series should remain");
    assert_eq!(series.series.len(), 1, "get must not evict history");
    assert_eq!(series.total.load(Ordering::Relaxed), 5);
}

#[test]
fn set_if_lt_primes_empty_key_and_reprime_is_noop() {
    let limiter = limiter(6, 1000);
    let rate_limit = RateLimit::try_from(100f64).unwrap();

    let (new_total, old_total) =
        limiter.set_if("k", &rate_limit, RateLimitComparator::Lt(100), 100);
    assert_eq!((new_total, old_total), (100, 0));

    let (new_total, old_total) =
        limiter.set_if("k", &rate_limit, RateLimitComparator::Lt(100), 100);
    assert_eq!((new_total, old_total), (100, 100));

    assert_eq!(limiter.get("k"), 100);
}

#[test]
fn set_if_lt_with_lower_target_is_noop() {
    let limiter = limiter(6, 1000);
    let rate_limit = RateLimit::try_from(100f64).unwrap();

    let _ = limiter.set_if("k", &rate_limit, RateLimitComparator::Lt(100), 100);

    let (new_total, old_total) = limiter.set_if("k", &rate_limit, RateLimitComparator::Lt(50), 50);
    assert_eq!((new_total, old_total), (100, 100));
}

#[test]
fn set_if_nil_overwrites_unconditionally_including_lowering() {
    let limiter = limiter(6, 1000);
    let rate_limit = RateLimit::try_from(100f64).unwrap();

    let _ = limiter.set_if("k", &rate_limit, RateLimitComparator::Nil, 100);

    let (new_total, old_total) = limiter.set_if("k", &rate_limit, RateLimitComparator::Nil, 30);
    assert_eq!((new_total, old_total), (30, 100));
    assert_eq!(limiter.get("k"), 30);

    // Overwriting to 0 clears the window entirely; admission resumes from empty.
    let (new_total, old_total) = limiter.set_if("k", &rate_limit, RateLimitComparator::Nil, 0);
    assert_eq!((new_total, old_total), (0, 30));
    assert_eq!(limiter.get("k"), 0);

    let decision = limiter.inc("k", &rate_limit, 1);
    assert!(matches!(decision, RateLimitDecision::Allowed));
}

#[test]
fn set_if_eq_zero_sets_only_when_window_is_empty() {
    let limiter = limiter(6, 1000);
    let rate_limit = RateLimit::try_from(100f64).unwrap();

    let (new_total, old_total) = limiter.set_if("k", &rate_limit, RateLimitComparator::Eq(0), 25);
    assert_eq!((new_total, old_total), (25, 0));

    let (new_total, old_total) = limiter.set_if("k", &rate_limit, RateLimitComparator::Eq(0), 99);
    assert_eq!((new_total, old_total), (25, 25));
}

#[test]
fn set_if_gt_and_ne_guards_follow_current_total() {
    let limiter = limiter(6, 1000);
    let rate_limit = RateLimit::try_from(100f64).unwrap();

    let _ = limiter.set_if("k", &rate_limit, RateLimitComparator::Nil, 10);

    // Gt(5): 10 > 5 matches → lowered to 3.
    let (new_total, old_total) = limiter.set_if("k", &rate_limit, RateLimitComparator::Gt(5), 3);
    assert_eq!((new_total, old_total), (3, 10));

    // Ne(3): current is exactly 3 → no match.
    let (new_total, old_total) = limiter.set_if("k", &rate_limit, RateLimitComparator::Ne(3), 7);
    assert_eq!((new_total, old_total), (3, 3));

    // Ne(5): current is 3 → match.
    let (new_total, old_total) = limiter.set_if("k", &rate_limit, RateLimitComparator::Ne(5), 7);
    assert_eq!((new_total, old_total), (7, 3));
}

#[test]
fn set_if_prime_then_inc_enforces_remaining_budget() {
    let window_size_seconds = 6_u64;
    let limiter = limiter(window_size_seconds, 1000);
    let rate_limit = RateLimit::try_from(5f64).unwrap();
    let capacity = window_capacity(window_size_seconds, &rate_limit);
    assert_eq!(capacity, 30);

    // Prime 27 of 30: exactly 3 units of budget remain.
    let (new_total, _) = limiter.set_if("k", &rate_limit, RateLimitComparator::Lt(27), 27);
    assert_eq!(new_total, 27);

    for i in 0..3_u64 {
        let decision = limiter.inc("k", &rate_limit, 1);
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "i: {i}, decision: {decision:?}"
        );
    }

    let decision = limiter.inc("k", &rate_limit, 1);
    assert!(
        matches!(decision, RateLimitDecision::Rejected { .. }),
        "decision: {decision:?}"
    );
}

#[test]
fn set_if_prime_at_capacity_rejects_next_inc() {
    let window_size_seconds = 6_u64;
    let limiter = limiter(window_size_seconds, 1000);
    let rate_limit = RateLimit::try_from(5f64).unwrap();
    let capacity = window_capacity(window_size_seconds, &rate_limit);

    let (new_total, _) = limiter.set_if(
        "k",
        &rate_limit,
        RateLimitComparator::Lt(capacity),
        capacity,
    );
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
}

#[test]
fn set_if_redefines_sticky_rate_limit() {
    let window_size_seconds = 6_u64;
    let limiter = limiter(window_size_seconds, 1000);

    // Fill to the original capacity (6 * 1 = 6); the key is now rejecting.
    let rate_one = RateLimit::try_from(1f64).unwrap();
    for _ in 0..6 {
        let decision = limiter.inc("k", &rate_one, 1);
        assert!(matches!(decision, RateLimitDecision::Allowed));
    }
    let decision = limiter.inc("k", &rate_one, 1);
    assert!(matches!(decision, RateLimitDecision::Rejected { .. }));

    // Unlike inc (sticky limit), set_if redefines the stored limit: 6 * 10 = 60.
    let rate_ten = RateLimit::try_from(10f64).unwrap();
    let _ = limiter.set_if("k", &rate_ten, RateLimitComparator::Nil, 0);

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
    let window_size_seconds = 6_u64;
    let limiter = limiter(window_size_seconds, 1000);
    let rate_limit = RateLimit::try_from(100f64).unwrap();

    // A stale bucket outside the 6s window.
    insert_series(&limiter, "k", rate_limit, &[(50, 7_000)]);

    // Eq(0) must observe the post-eviction total.
    let (new_total, old_total) = limiter.set_if("k", &rate_limit, RateLimitComparator::Eq(0), 9);
    assert_eq!((new_total, old_total), (9, 0));
}

#[test]
fn set_if_unmatched_guard_on_unknown_key_writes_no_counts() {
    let limiter = limiter(6, 1000);
    let rate_limit = RateLimit::try_from(100f64).unwrap();

    let (new_total, old_total) = limiter.set_if("k", &rate_limit, RateLimitComparator::Gt(5), 9);
    assert_eq!((new_total, old_total), (0, 0));
    assert_eq!(limiter.get("k"), 0);
    assert!(!limiter.series().contains_key("k"));
}

#[test]
fn set_if_preserve_newest_reduces_from_front_and_increases_newest() {
    let limiter = limiter(10, 1);
    let rate = RateLimit::try_from(100f64).unwrap();
    insert_series(&limiter, "k", rate, &[(4, 3_000), (5, 2_000), (6, 1_000)]);

    let result = limiter.set_if_preserve_history(
        "k",
        &rate,
        RateLimitComparator::Nil,
        8,
        HistoryPreservation::PreserveNewest,
    );
    assert_eq!(result, (8, 15));

    let series = limiter.series().get("k").unwrap();
    let counts = series
        .series
        .iter()
        .map(|bucket| bucket.count.load(std::sync::atomic::Ordering::Relaxed))
        .collect::<Vec<_>>();
    assert_eq!(counts, vec![2, 6]);
    drop(series);

    let result = limiter.set_if_preserve_history(
        "k",
        &rate,
        RateLimitComparator::Nil,
        11,
        HistoryPreservation::PreserveNewest,
    );
    assert_eq!(result, (11, 8));
    let series = limiter.series().get("k").unwrap();
    let counts = series
        .series
        .iter()
        .map(|bucket| bucket.count.load(std::sync::atomic::Ordering::Relaxed))
        .collect::<Vec<_>>();
    assert_eq!(counts, vec![2, 9]);
}

#[test]
fn set_if_preserve_oldest_reduces_from_back_and_increases_oldest() {
    let limiter = limiter(10, 1);
    let rate = RateLimit::try_from(100f64).unwrap();
    insert_series(&limiter, "k", rate, &[(4, 3_000), (5, 2_000), (6, 1_000)]);

    assert_eq!(
        limiter.set_if_preserve_history(
            "k",
            &rate,
            RateLimitComparator::Nil,
            8,
            HistoryPreservation::PreserveOldest,
        ),
        (8, 15)
    );
    let series = limiter.series().get("k").unwrap();
    let counts = series
        .series
        .iter()
        .map(|bucket| bucket.count.load(std::sync::atomic::Ordering::Relaxed))
        .collect::<Vec<_>>();
    assert_eq!(counts, vec![4, 4]);
    drop(series);

    assert_eq!(
        limiter.set_if_preserve_history(
            "k",
            &rate,
            RateLimitComparator::Nil,
            11,
            HistoryPreservation::PreserveOldest,
        ),
        (11, 8)
    );
    let series = limiter.series().get("k").unwrap();
    let counts = series
        .series
        .iter()
        .map(|bucket| bucket.count.load(std::sync::atomic::Ordering::Relaxed))
        .collect::<Vec<_>>();
    assert_eq!(counts, vec![7, 4]);
}

#[test]
fn conditional_set_guard_miss_uses_shared_dashmap_lock() {
    let limiter = Arc::new(limiter(6, 1000));
    let rate = RateLimit::try_from(100f64).unwrap();
    let _ = limiter.inc("k", &rate, 3);
    let held_read_guard = limiter.series().get("k").unwrap();
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
    worker.join().unwrap();
    assert_eq!(result.unwrap(), ((3, 3), (3, 3)));
}

#[test]
fn conditional_set_missing_zero_leaves_key_absent() {
    let limiter = limiter(6, 1000);
    let rate = RateLimit::try_from(100f64).unwrap();

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
