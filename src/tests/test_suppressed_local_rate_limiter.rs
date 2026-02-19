use std::{
    collections::VecDeque,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use crate::{
    HardLimitFactor, LocalRateLimiterOptions, RateGroupSizeMs, RateLimit, RateLimitDecision,
    SuppressedLocalRateLimiter, SuppressionFactorCacheMs, WindowSizeSeconds,
};

use crate::common::{InstantRate, RateLimitSeries};

fn limiter(
    window_size_seconds: u64,
    rate_group_size_ms: u64,
    hard_limit_factor: f64,
) -> SuppressedLocalRateLimiter {
    SuppressedLocalRateLimiter::new(LocalRateLimiterOptions {
        window_size_seconds: WindowSizeSeconds::try_from(window_size_seconds).unwrap(),
        rate_group_size_ms: RateGroupSizeMs::try_from(rate_group_size_ms).unwrap(),
        hard_limit_factor: HardLimitFactor::try_from(hard_limit_factor).unwrap(),
        suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
    })
}

fn total_for_key(limiter: &crate::AbsoluteLocalRateLimiter, key: &str) -> u64 {
    limiter
        .series()
        .get(key)
        .map(|s| s.total.load(Ordering::Relaxed))
        .unwrap_or(0)
}

fn insert_series(
    limiter: &crate::AbsoluteLocalRateLimiter,
    key: &str,
    limit: RateLimit,
    buckets: &[(u64, u64)],
) {
    let total = buckets.iter().map(|(count, _)| *count).sum::<u64>();
    let series = buckets
        .iter()
        .map(|(count, ms_ago)| InstantRate {
            count: AtomicU64::new(*count),
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
fn does_not_suppress_when_window_limit_not_exceeded() {
    let limiter = limiter(10, 1000, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    let _ = limiter.inc(key, &rate_limit, 10);
    // wait for 1s to pass
    std::thread::sleep(Duration::from_millis(1001));

    let _ = limiter.inc(key, &rate_limit, 20);

    let decision = limiter.inc(key, &rate_limit, 1);
    assert!(
        matches!(
            decision,
            RateLimitDecision::Suppressed {
                suppression_factor,
                ..
            } if suppression_factor  < 1e-12
        ) || matches!(decision, RateLimitDecision::Allowed),
        "decision: {:?}",
        decision
    );
}

#[test]
fn verify_suppression_factor_calculation_spread() {
    let limiter = limiter(10, 1000, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    // fill up in the first 3 seconds
    for _ in 0..20 {
        let _ = limiter.inc(key, &rate_limit, 1);
        std::thread::sleep(Duration::from_millis(3000 / 20));
    }

    // wait for 1.5 seconds
    std::thread::sleep(Duration::from_millis(1200));

    let expected_suppression_factor = 1f64 - (1f64 / 2.1f64);

    let decision = limiter.inc(key, &rate_limit, 1);
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
}

#[test]
fn verify_suppression_factor_calculation_last_second() {
    let limiter = limiter(10, 100, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    let _ = limiter.inc(key, &rate_limit, 10);
    // wait for 1s to pass
    std::thread::sleep(Duration::from_millis(1001));

    let _ = limiter.inc(key, &rate_limit, 20);

    let expected_suppression_factor = 1f64 - (1f64 / 21f64);

    let decision = limiter.inc(key, &rate_limit, 1);

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
}

#[test]
fn verify_hard_limit_rejects_local() {
    let limiter = limiter(10, 100, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    let _ = limiter.inc(key, &rate_limit, 100);
    // wait for 1s to pass
    std::thread::sleep(Duration::from_millis(1001));

    let _ = limiter.inc(key, &rate_limit, 20);

    let decision = limiter.inc(key, &rate_limit, 1);

    assert!(
        matches!(decision, RateLimitDecision::Rejected { .. })
            || matches!(decision, RateLimitDecision::Suppressed { suppression_factor, .. } if suppression_factor == 1.0f64),
        "decision: {:?}",
        decision
    );
}

#[test]
fn suppressed_inc_denied_returns_suppressed_and_does_not_increment_accepted() {
    let limiter = limiter(1, 1000, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    limiter.test_set_suppression_factor(key, Instant::now(), 0.25);

    let mut rng = |_p: f64| false;
    let decision = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);

    assert!(matches!(
        decision,
        RateLimitDecision::Suppressed {
            suppression_factor,
            is_allowed: false
        } if (suppression_factor - 0.25).abs() < 1e-12
    ));

    assert_eq!(total_for_key(limiter.observed_limiter(), key), 1);
    assert_eq!(total_for_key(limiter.accepted_limiter(), key), 0);
}

#[test]
fn suppressed_inc_allowed_returns_suppressed_is_allowed_true_and_increments_accepted() {
    let limiter = limiter(1, 1000, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    limiter.test_set_suppression_factor(key, Instant::now(), 0.25);

    let mut rng = |_p: f64| true;
    let decision = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);

    assert!(matches!(
        decision,
        RateLimitDecision::Suppressed {
            suppression_factor,
            is_allowed: true
        } if (suppression_factor - 0.25).abs() < 1e-12
    ));

    assert_eq!(total_for_key(limiter.observed_limiter(), key), 1);
    assert_eq!(total_for_key(limiter.accepted_limiter(), key), 1);
}

#[test]
fn hard_limit_rejection_is_returned_as_rejected_not_suppressed() {
    let limiter = limiter(1, 1000, 1f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    limiter.test_set_suppression_factor(key, Instant::now(), 0.25);
    let mut rng = |_p: f64| true;

    let d1 = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);
    assert!(matches!(
        d1,
        RateLimitDecision::Suppressed {
            is_allowed: true,
            ..
        }
    ));

    let d2 = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);
    assert!(matches!(d2, RateLimitDecision::Rejected { .. }));

    assert_eq!(total_for_key(limiter.observed_limiter(), key), 2);
    assert_eq!(total_for_key(limiter.accepted_limiter(), key), 1);
}

#[test]
fn suppression_factor_is_clamped_to_one() {
    let limiter = limiter(1, 1000, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    limiter.test_set_suppression_factor(key, Instant::now(), 2f64);

    let mut rng = |p: f64| {
        assert!((p - 0f64).abs() < 1e-12);
        false
    };

    let decision = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);
    assert!(matches!(
        decision,
        RateLimitDecision::Suppressed {
            suppression_factor,
            is_allowed: false
        } if (suppression_factor - 1.0).abs() < 1e-12
    ));
}

#[test]
fn nonpositive_suppression_factor_bypasses_suppression() {
    let limiter = limiter(1, 1000, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    limiter.test_set_suppression_factor(key, Instant::now(), -0.01);

    let mut rng = |_p: f64| panic!("rng should not be used when suppression_factor <= 0");
    let decision = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);
    assert!(matches!(decision, RateLimitDecision::Allowed));

    assert_eq!(total_for_key(limiter.observed_limiter(), key), 1);
    assert_eq!(total_for_key(limiter.accepted_limiter(), key), 1);
}

#[test]
fn suppression_factor_cache_is_recomputed_after_cache_window() {
    let limiter = limiter(1, 50, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    // Default suppression_factor_cache_ms is 100ms; make the cached value stale.
    let old_ts = Instant::now() - Duration::from_millis(101);
    limiter.test_set_suppression_factor(key, old_ts, 0.9);

    // With no accepted series, calculate_suppression_factor() persists 0.
    let mut rng = |_p: f64| panic!("rng should not be used when suppression_factor <= 0");
    let decision = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);
    assert!(matches!(decision, RateLimitDecision::Allowed));

    let (_ts, val) = limiter
        .test_get_suppression_factor(key)
        .expect("expected suppression factor to be persisted");
    assert!((val - 0f64).abs() < 1e-12);
}

#[test]
fn cleanup_removes_stale_suppression_factors_and_keeps_fresh() {
    let limiter = limiter(1, 1000, 10f64);

    limiter.test_set_suppression_factor("stale", Instant::now() - Duration::from_millis(250), 0.5);
    limiter.test_set_suppression_factor("fresh", Instant::now(), 0.25);

    limiter.cleanup(100);

    assert!(
        limiter.test_get_suppression_factor("stale").is_none(),
        "expected stale suppression factor removed"
    );
    assert!(
        limiter.test_get_suppression_factor("fresh").is_some(),
        "expected fresh suppression factor retained"
    );
}

#[test]
fn cleanup_removes_stale_keys_from_accepted_and_observed_limiters() {
    let limiter = limiter(60, 1000, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    insert_series(limiter.accepted_limiter(), key, rate_limit, &[(1, 5_000)]);
    insert_series(limiter.observed_limiter(), key, rate_limit, &[(1, 5_000)]);

    assert!(limiter.accepted_limiter().series().contains_key(key));
    assert!(limiter.observed_limiter().series().contains_key(key));

    limiter.cleanup(1_000);

    assert!(!limiter.accepted_limiter().series().contains_key(key));
    assert!(!limiter.observed_limiter().series().contains_key(key));
}

#[test]
fn cleanup_keeps_fresh_keys_in_both_limiters() {
    let limiter = limiter(60, 1000, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    insert_series(limiter.accepted_limiter(), key, rate_limit, &[(1, 10)]);
    insert_series(limiter.observed_limiter(), key, rate_limit, &[(1, 10)]);

    limiter.cleanup(10_000);

    assert!(limiter.accepted_limiter().series().contains_key(key));
    assert!(limiter.observed_limiter().series().contains_key(key));
}

#[test]
fn cleanup_drops_empty_series_entries_in_both_limiters() {
    let limiter = limiter(60, 1000, 10f64);
    let key = "empty";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    limiter
        .accepted_limiter()
        .series()
        .insert(key.to_string(), RateLimitSeries::new(rate_limit));
    limiter
        .observed_limiter()
        .series()
        .insert(key.to_string(), RateLimitSeries::new(rate_limit));

    assert!(limiter.accepted_limiter().series().contains_key(key));
    assert!(limiter.observed_limiter().series().contains_key(key));

    limiter.cleanup(10_000);

    assert!(!limiter.accepted_limiter().series().contains_key(key));
    assert!(!limiter.observed_limiter().series().contains_key(key));
}

#[test]
fn cleanup_does_not_break_next_inc_when_cache_entry_removed() {
    let limiter = limiter(1, 1000, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    limiter.test_set_suppression_factor(key, Instant::now() - Duration::from_millis(250), 0.9);
    limiter.cleanup(100);
    assert!(limiter.test_get_suppression_factor(key).is_none());

    let mut rng = |_p: f64| panic!("rng should not be used when suppression_factor <= 0");
    let decision = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);
    assert!(matches!(decision, RateLimitDecision::Allowed));

    let (_ts, val) = limiter
        .test_get_suppression_factor(key)
        .expect("expected suppression factor persisted after recompute");
    assert!((val - 0f64).abs() < 1e-12);
}

#[test]
fn cleanup_is_millisecond_granularity_for_suppression_factor() {
    let limiter = limiter(1, 1000, 10f64);
    let key = "k";

    limiter.test_set_suppression_factor(key, Instant::now() - Duration::from_millis(150), 0.5);
    limiter.cleanup(100);
    assert!(limiter.test_get_suppression_factor(key).is_none());
}
