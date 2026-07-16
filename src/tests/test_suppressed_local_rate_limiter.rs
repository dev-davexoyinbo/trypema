use std::{
    sync::{Arc, Barrier, atomic::Ordering, mpsc},
    thread,
    time::{Duration, Instant},
};

use crate::{
    HardLimitFactor, HistoryPreservation, LocalRateLimiterOptions, RateGroupSizeMs, RateLimit,
    RateLimitComparator, RateLimitDecision, SuppressedLocalRateLimiter,
    SuppressedRateLimitSnapshot, SuppressionFactorCacheMs, WindowSizeSeconds,
};

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

fn limiter_with_cache_ms(
    window_size_seconds: u64,
    rate_group_size_ms: u64,
    hard_limit_factor: f64,
    suppression_factor_cache_ms: u64,
) -> SuppressedLocalRateLimiter {
    SuppressedLocalRateLimiter::new(LocalRateLimiterOptions {
        window_size_seconds: WindowSizeSeconds::try_from(window_size_seconds).unwrap(),
        rate_group_size_ms: RateGroupSizeMs::try_from(rate_group_size_ms).unwrap(),
        hard_limit_factor: HardLimitFactor::try_from(hard_limit_factor).unwrap(),
        suppression_factor_cache_ms: SuppressionFactorCacheMs::try_from(
            suppression_factor_cache_ms,
        )
        .unwrap(),
    })
}

/// Push `key` to the soft window capacity so that subsequent `inc_with_rng` calls
/// enter the suppression-decision branch (rather than short-circuiting to `Allowed`).
///
/// The soft window limit is `window_size_seconds * rate_limit`. We need
/// `total - total_declined >= soft_window_limit`. We achieve this by recording a
/// single increment of `soft_window_limit` (all accepted, so `total_declined = 0`),
/// then overwriting the cached suppression factor with the desired test value so the
/// next call uses it exactly.
fn fill_to_soft_window_limit(
    limiter: &SuppressedLocalRateLimiter,
    key: &str,
    rate_limit: &RateLimit,
    window_size_seconds: u64,
    desired_suppression_factor: f64,
) {
    let soft_window_limit = (window_size_seconds as f64 * **rate_limit) as u64;
    // A single batch increment of exactly `soft_window_limit` puts accepted = soft_window_limit.
    let decision = limiter.inc(key, rate_limit, soft_window_limit);
    assert!(
        matches!(decision, RateLimitDecision::Allowed),
        "an increment that reaches the soft limit must be allowed: {decision:?}"
    );
    assert_eq!(limiter.get(key).total, soft_window_limit);
    let series = limiter.series().get(key).expect("series should exist");
    assert_eq!(series.total.load(Ordering::Acquire), soft_window_limit);
    assert_eq!(series.total_declined.load(Ordering::Acquire), 0);
    drop(series);

    // Overwrite the suppression factor with the desired test value (fresh TTL).
    limiter.test_set_suppression_factor(key, Instant::now(), desired_suppression_factor);
}

#[test]
fn does_not_suppress_when_soft_window_limit_not_exceeded() {
    let limiter = limiter(10, 1000, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(5f64).unwrap();
    let soft_window_limit = 50;

    let first = limiter.inc(key, &rate_limit, soft_window_limit - 1);
    assert!(
        matches!(first, RateLimitDecision::Allowed),
        "an increment that stays below the soft window limit must be allowed: {first:?}"
    );

    let boundary = limiter.inc(key, &rate_limit, 1);
    assert!(
        matches!(boundary, RateLimitDecision::Allowed),
        "an increment that reaches the soft window limit must be allowed: {boundary:?}"
    );

    assert_eq!(limiter.get(key).total, soft_window_limit);
    let series = limiter.series().get(key).expect("series should exist");
    assert_eq!(series.total_declined.load(Ordering::Acquire), 0);
}

#[test]
fn verify_suppression_factor_calculation_spread() {
    let limiter = limiter(10, 1000, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    let decision = limiter.inc(key, &rate_limit, 20);
    assert!(
        matches!(
            decision,
            RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: true,
            } if suppression_factor.abs() < 1e-12
        ),
        "the batch above soft capacity must take the suppression path: {decision:?}"
    );
    assert_eq!(limiter.get(key).total, 20);

    // Keep the bucket in the 10-second window but outside the one-second peak-rate window.
    std::thread::sleep(Duration::from_millis(1_050));

    // Under this load, perceived_rate = max(average_rate_in_window, rate_in_last_1000ms).
    // This test aims to keep perceived_rate close to 2.0 req/s.
    let expected_suppression_factor = 1f64 - (1f64 / 2.0f64);

    // Force suppression-factor recompute (ignore any cached value from earlier inc calls).
    limiter.test_set_suppression_factor(key, Instant::now() - Duration::from_millis(101), 0.0);
    let suppression_factor = limiter.get_suppression_factor(key);

    assert!(
        (suppression_factor - expected_suppression_factor).abs() < 1e-12,
        "suppression_factor: {suppression_factor:?} expected: {expected_suppression_factor:?}"
    );
}

#[test]
fn verify_suppression_factor_calculation_last_second() {
    let limiter = limiter(10, 100, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    let first = limiter.inc(key, &rate_limit, 10);
    assert!(
        matches!(first, RateLimitDecision::Allowed),
        "the batch reaching the soft limit must be allowed: {first:?}"
    );
    // wait for 1s to pass
    std::thread::sleep(Duration::from_millis(1001));

    let second = limiter.inc(key, &rate_limit, 20);
    assert!(
        matches!(
            second,
            RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: true,
            } if suppression_factor.abs() < 1e-12
        ),
        "the soft-limit boundary should activate suppression with a zero factor: {second:?}"
    );

    // After the increment, the last-second count is 20.
    let expected_suppression_factor = 1f64 - (1f64 / 20f64);

    // Force suppression-factor recompute (ignore any cached value from earlier inc calls).
    limiter.test_set_suppression_factor(key, Instant::now() - Duration::from_millis(101), 0.0);
    let suppression_factor = limiter.get_suppression_factor(key);

    assert!(
        (suppression_factor - expected_suppression_factor).abs() < 1e-12,
        "suppression_factor: {suppression_factor:?} expected: {expected_suppression_factor:?}"
    );
}

#[test]
fn reaching_shared_soft_and_hard_limit_is_allowed_then_suppresses() {
    let limiter = limiter(10, 100, 1f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    let below = limiter.inc(key, &rate_limit, 9);
    assert!(
        matches!(below, RateLimitDecision::Allowed),
        "below-limit decision: {below:?}"
    );

    let boundary = limiter.inc(key, &rate_limit, 1);
    assert!(
        matches!(boundary, RateLimitDecision::Allowed),
        "the increment reaching the shared soft/hard limit must be allowed: {boundary:?}"
    );
    assert_eq!(limiter.get(key).total, 10);
    assert!(matches!(
        limiter.test_get_suppression_factor(key),
        Some((_, factor)) if (factor - 1.0).abs() < 1e-12
    ));

    let decision = limiter.inc(key, &rate_limit, 1);

    assert!(
        matches!(
            decision,
            RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: false,
            } if (suppression_factor - 1.0).abs() < 1e-12
        ),
        "decision: {decision:?}"
    );

    let series = limiter.series().get(key).expect("series should exist");
    assert_eq!(series.total.load(Ordering::Acquire), 11);
    assert_eq!(series.total_declined.load(Ordering::Acquire), 1);
}

#[test]
fn suppressed_inc_denied_returns_suppressed_and_does_not_increment_accepted() {
    let limiter = limiter(1, 1000, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    fill_to_soft_window_limit(&limiter, key, &rate_limit, 1, 0.25);

    let mut rng = |p: f64| {
        assert!((p - 0.75).abs() < 1e-12, "p: {p:?}");
        false
    };
    let decision = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);

    assert!(
        matches!(
            decision,
            RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: false
            } if (suppression_factor - 0.25).abs() < 1e-12
        ),
        "decision: {decision:?}"
    );

    let series = limiter.series().get(key).expect("series should exist");
    assert_eq!(series.total.load(Ordering::Acquire), 6);
    assert_eq!(series.total_declined.load(Ordering::Acquire), 1);
    assert_eq!(
        series
            .total
            .load(Ordering::Acquire)
            .saturating_sub(series.total_declined.load(Ordering::Acquire)),
        5,
        "a denied increment must not increase accepted usage"
    );
}

#[test]
fn suppressed_inc_allowed_returns_suppressed_is_allowed_true_and_increments_accepted() {
    let limiter = limiter(1, 1000, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    fill_to_soft_window_limit(&limiter, key, &rate_limit, 1, 0.25);

    let mut rng = |p: f64| {
        assert!((p - 0.75).abs() < 1e-12, "p: {p:?}");
        true
    };
    let decision = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);

    assert!(
        matches!(
            decision,
            RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: true
            } if (suppression_factor - 0.25).abs() < 1e-12
        ),
        "decision: {decision:?}"
    );

    let series = limiter.series().get(key).expect("series should exist");
    assert_eq!(series.total.load(Ordering::Acquire), 6);
    assert_eq!(series.total_declined.load(Ordering::Acquire), 0);
}

#[test]
fn hard_limit_is_enforced_after_suppression_factor_cache_expires() {
    // The hard window limit = window_size_seconds * rate_limit * hard_limit_factor.
    // Setup: window=10s, rate=1 req/s, hard_limit_factor=2 => soft=10, hard=20.
    // Use a tiny cache (1ms) so the suppression factor is always recomputed.
    let limiter = limiter_with_cache_ms(10, 1000, 2f64, 1);
    let key = "k";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    // d1 reaches an accepted total of 1, below soft(10).
    let d1 = limiter.inc(key, &rate_limit, 1);
    assert!(matches!(d1, RateLimitDecision::Allowed), "d1: {d1:?}");

    // Ensure the cache expires so d2 recomputes.
    std::thread::sleep(Duration::from_millis(5));

    // d2 reaches an accepted total of 2, below soft(10).
    let d2 = limiter.inc(key, &rate_limit, 1);
    assert!(matches!(d2, RateLimitDecision::Allowed), "d2: {d2:?}");

    // Ensure the cache expires so d3 recomputes.
    std::thread::sleep(Duration::from_millis(5));

    // The batch reaches hard exactly, so it is admitted and caches a factor of 1.0.
    let d3 = limiter.inc(key, &rate_limit, 18);
    assert!(matches!(d3, RateLimitDecision::Allowed), "d3: {d3:?}");
    assert_eq!(limiter.get(key).total, 20);
    assert!(matches!(
        limiter.test_get_suppression_factor(key),
        Some((_, factor)) if (factor - 1.0).abs() < 1e-12
    ));

    thread::sleep(Duration::from_millis(5));

    let d4 = limiter.inc(key, &rate_limit, 1);
    assert!(
        matches!(
            d4,
            RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: false,
            } if (suppression_factor - 1.0).abs() < 1e-12
        ),
        "d4: {d4:?}"
    );

    assert!((limiter.get_suppression_factor(key) - 1.0).abs() < 1e-12);
    let series = limiter.series().get(key).expect("series should exist");
    assert_eq!(series.total.load(Ordering::Acquire), 21);
    assert_eq!(series.total_declined.load(Ordering::Acquire), 1);
}

#[test]
#[should_panic(
    expected = "SuppressedLocalRateLimiter::get_suppression_factor: suppression factor > 1"
)]
fn suppression_factor_gt_one_panics() {
    let limiter = limiter(1, 1000, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    fill_to_soft_window_limit(&limiter, key, &rate_limit, 1, 2f64);

    let mut rng = |_p: f64| true;
    let _ = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);
}

#[test]
#[should_panic(
    expected = "SuppressedLocalRateLimiter::get_suppression_factor: negative suppression factor"
)]
fn suppression_factor_negative_panics() {
    let limiter = limiter(1, 1000, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    fill_to_soft_window_limit(&limiter, key, &rate_limit, 1, -0.01);

    let mut rng = |_p: f64| true;
    let _ = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);
}

#[test]
fn suppression_factor_cache_is_recomputed_after_cache_window() {
    let limiter = limiter(1, 50, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    // Default suppression_factor_cache_ms is 100ms; make the cached value stale.
    let old_ts = Instant::now() - Duration::from_millis(101);
    limiter.test_set_suppression_factor(key, old_ts, 0.9);

    // get_suppression_factor triggers a recompute when the cache is stale.
    // With no series entries, calculate_suppression_factor() persists 0.
    let decision = limiter.inc(key, &rate_limit, 1);
    assert!(matches!(decision, RateLimitDecision::Allowed));
    limiter.test_set_suppression_factor(key, old_ts, 0.9);

    let val = limiter.get_suppression_factor(key);
    assert!((val - 0f64).abs() < 1e-12, "val: {val}");

    let (_ts, cached_val) = limiter
        .test_get_suppression_factor(key)
        .expect("expected suppression factor to be persisted");
    assert!((cached_val - 0f64).abs() < 1e-12);
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
fn cleanup_does_not_break_factor_recomputation_when_cache_entry_removed() {
    let limiter = limiter(1, 1000, 10f64);
    let key = "k";

    limiter.test_set_suppression_factor(key, Instant::now() - Duration::from_millis(250), 0.9);
    limiter.cleanup(100);
    assert!(limiter.test_get_suppression_factor(key).is_none());

    // After cleanup removes the cached entry, get_suppression_factor recomputes and persists.
    // With no series entries the recomputed value is 0.0.
    let val = limiter.get_suppression_factor(key);
    assert!(matches!(val, v if (v - 0f64).abs() < 1e-12));

    let (_ts, cached_val) = limiter
        .test_get_suppression_factor(key)
        .expect("expected suppression factor persisted after recompute");
    assert!((cached_val - 0f64).abs() < 1e-12);
}

#[test]
fn cleanup_removes_stale_series_and_keeps_fresh_series() {
    let limiter = limiter(6, 1, 2.0);
    let rate = RateLimit::try_from(100f64).unwrap();

    let stale = limiter.inc("stale", &rate, 3);
    assert!(matches!(stale, RateLimitDecision::Allowed));

    thread::sleep(Duration::from_millis(80));

    let fresh = limiter.inc("fresh", &rate, 4);
    assert!(matches!(fresh, RateLimitDecision::Allowed));

    limiter.cleanup(50);

    assert!(!limiter.series().contains_key("stale"));
    assert!(limiter.series().contains_key("fresh"));
    assert_eq!(limiter.get("fresh").total, 4);
}

#[test]
fn cleanup_is_millisecond_granularity_for_suppression_factor() {
    let limiter = limiter(1, 1000, 10f64);
    let key = "k";

    limiter.test_set_suppression_factor(key, Instant::now() - Duration::from_millis(150), 0.5);
    limiter.cleanup(100);
    assert!(limiter.test_get_suppression_factor(key).is_none());
}

#[test]
fn suppression_factor_zero_returns_suppressed_allowed_and_rng_not_called() {
    let limiter = limiter(1, 1000, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    fill_to_soft_window_limit(&limiter, key, &rate_limit, 1, 0.0);

    let mut rng = |_p: f64| panic!("rng must not be called when suppression_factor == 0");
    let decision = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);
    assert!(
        matches!(
            decision,
            RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: true,
            } if suppression_factor.abs() < 1e-12
        ),
        "decision: {decision:?}"
    );
}

#[test]
fn suppression_factor_one_must_not_return_allowed() {
    let limiter = limiter(1, 1000, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    fill_to_soft_window_limit(&limiter, key, &rate_limit, 1, 1.0);

    let mut rng = |_p: f64| panic!("rng must not be called when suppression_factor == 1");
    let decision = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);

    assert!(
        matches!(
            decision,
            RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: false,
            } if (suppression_factor - 1.0).abs() < 1e-12
        ),
        "decision: {decision:?}"
    );
}

#[test]
fn suppression_rng_probability_is_one_minus_suppression_factor() {
    let limiter = limiter(1, 1000, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    fill_to_soft_window_limit(&limiter, key, &rate_limit, 1, 0.25);

    let mut rng = |p: f64| {
        assert!((p - 0.75).abs() < 1e-12, "p: {p:?}");
        false
    };
    let decision = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);

    assert!(
        matches!(
            decision,
            RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: false
            } if (suppression_factor - 0.25).abs() < 1e-12
        ),
        "decision: {decision:?}"
    );
}

#[test]
fn suppression_property_values_do_not_panic_and_follow_basic_contract() {
    let limiter = limiter(1, 1000, 10f64);
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    // Sample values across the full inclusive range.
    let sfs = [0.0, 0.1, 0.25, 0.5, 0.9, 1.0];

    for sf in sfs {
        // Use a unique key per sf value to avoid accumulated-state interference.
        let key = format!("k_{sf}");
        let key = key.as_str();

        if sf == 0.0 {
            fill_to_soft_window_limit(&limiter, key, &rate_limit, 1, sf);
            let mut rng = |_p: f64| panic!("rng must not be called when suppression_factor == 0");
            let decision = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);
            assert!(
                matches!(
                    decision,
                    RateLimitDecision::Suppressed {
                        suppression_factor,
                        is_allowed: true,
                    } if suppression_factor.abs() < 1e-12
                ),
                "sf={sf} decision={decision:?}"
            );
            continue;
        }

        if sf == 1.0 {
            fill_to_soft_window_limit(&limiter, key, &rate_limit, 1, sf);
            let mut rng = |_p: f64| panic!("rng must not be called when suppression_factor == 1");
            let decision = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);
            assert!(
                matches!(
                    decision,
                    RateLimitDecision::Suppressed {
                        suppression_factor,
                        is_allowed: false,
                    } if (suppression_factor - 1.0).abs() < 1e-12
                ),
                "sf={sf} decision={decision:?}"
            );
            continue;
        }

        // For 0 < sf < 1, RNG should be consulted with p = 1 - sf.
        fill_to_soft_window_limit(&limiter, key, &rate_limit, 1, sf);
        for expected_is_allowed in [false, true] {
            // Re-inject the desired suppression factor before each call to ensure the
            // cache is still fresh (fill_past_soft_limit already set it, but the first
            // loop iteration increments the window and may cause a recompute).
            limiter.test_set_suppression_factor(key, Instant::now(), sf);

            let mut called = 0u64;
            let mut rng = |p: f64| {
                called += 1;
                assert!((p - (1.0 - sf)).abs() < 1e-12, "sf={sf} p={p:?}");
                expected_is_allowed
            };

            let decision = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);
            assert_eq!(called, 1, "sf={sf} expected 1 rng call, got {called}");

            match decision {
                RateLimitDecision::Suppressed {
                    suppression_factor,
                    is_allowed,
                } => {
                    assert!(
                        (suppression_factor - sf).abs() < 1e-12,
                        "sf={sf} got={suppression_factor:?}"
                    );
                    assert_eq!(is_allowed, expected_is_allowed, "sf={sf}");
                }
                RateLimitDecision::Allowed => {
                    panic!("sf={sf} unexpectedly returned Allowed");
                }
                RateLimitDecision::Rejected { .. } => {
                    panic!("suppressed limiter unexpectedly returned Rejected for sf={sf}");
                }
            }
        }
    }
}

#[test]
fn suppressed_is_deterministically_allowed_until_base_capacity_boundary() {
    // With window=10s, rate=1 req/s, hard_factor=2:
    //   soft_window_limit = 10 * 1 = 10
    //   hard_window_limit = 10 * 1 * 2 = 20
    let window_size_seconds = 10_u64;
    let hard_limit_factor = 2f64;
    let limiter = limiter(window_size_seconds, 1000, hard_limit_factor);

    let key = "k";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    // The forecasted accepted total includes the current increment exactly once. All ten
    // calls through the soft-window boundary must return Allowed.
    for accepted_after in 1..=10_u64 {
        let decision = limiter.inc(key, &rate_limit, 1);
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "accepted_after={accepted_after}, decision={decision:?}"
        );
    }
    assert_eq!(limiter.get(key).total, 10);

    // The next increment forecasts accepted usage above the soft window limit. Pin the factor to
    // zero so only the transition and return variant are under test.
    limiter.test_set_suppression_factor(key, Instant::now(), 0.0);
    let mut rng = |_p: f64| panic!("rng must not run for a zero suppression factor");
    let d_over = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);
    assert!(
        matches!(
            d_over,
            RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: true,
            } if suppression_factor.abs() < 1e-12
        ),
        "d_over: {d_over:?}"
    );
    assert_eq!(limiter.get(key).total, 11);
}

#[test]
fn suppressed_is_fully_denied_after_hard_limit_observed() {
    // With window=10s, rate=1 req/s, hard_factor=2:
    //   soft_window_limit = 10,  hard_window_limit = 20
    let window_size_seconds = 10_u64;
    let hard_limit_factor = 2f64;

    let limiter = limiter(window_size_seconds, 1000, hard_limit_factor);

    let key = "k_hard";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    let initial = limiter.inc(key, &rate_limit, 20);
    assert!(
        matches!(initial, RateLimitDecision::Allowed),
        "the fresh batch is evaluated against an empty window: {initial:?}"
    );
    assert_eq!(limiter.get(key).total, 20);
    assert!(matches!(
        limiter.test_get_suppression_factor(key),
        Some((_, factor)) if (factor - 1.0).abs() < 1e-12
    ));

    // The exact-hard increment cached full suppression for this immediate next call.
    let d_next = limiter.inc(key, &rate_limit, 1);
    assert!(
        matches!(
            d_next,
            RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: false,
            } if (suppression_factor - 1.0).abs() < 1e-12
        ),
        "d_next: {d_next:?}"
    );

    assert_eq!(
        limiter.get(key),
        SuppressedRateLimitSnapshot {
            total: 21,
            total_declined: 1,
            suppression_factor: 1.0,
        }
    );

    let series = limiter.series().get(key).expect("series should exist");
    assert_eq!(series.total.load(Ordering::Acquire), 21);
    assert_eq!(series.total_declined.load(Ordering::Acquire), 1);
}

#[test]
fn get_unknown_key_returns_empty_snapshot() {
    let limiter = limiter(6, 1000, 1.0);

    assert_eq!(limiter.get("k"), SuppressedRateLimitSnapshot::default());
    assert!(limiter.series().is_empty());
    assert!(limiter.test_get_suppression_factor("k").is_none());
}

#[test]
fn get_returns_observed_snapshot() {
    let limiter = limiter(6, 1000, 1.0);
    let rate_limit = RateLimit::try_from(100f64).unwrap();

    for _ in 0..3 {
        let decision = limiter.inc("k", &rate_limit, 1);
        assert!(matches!(decision, RateLimitDecision::Allowed));
    }

    assert_eq!(
        limiter.get("k"),
        SuppressedRateLimitSnapshot {
            total: 3,
            total_declined: 0,
            suppression_factor: 0.0,
        }
    );
}

#[test]
fn get_evicts_expired_buckets_and_updates_declined_total() {
    let limiter = limiter(1, 50, 1.0);
    let rate = RateLimit::try_from(10f64).unwrap();

    // Seed both keys through the public API. After the sleeps, `k` has one
    // expired bucket and one fresh declined bucket, while `k2` is expired-only.
    assert!(matches!(
        limiter.inc("k2", &rate, 10),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        limiter.inc("k2", &rate, 6),
        RateLimitDecision::Suppressed {
            is_allowed: false,
            suppression_factor,
        } if (suppression_factor - 1.0).abs() < 1e-12
    ));
    assert!(matches!(
        limiter.inc("k", &rate, 10),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        limiter.test_get_suppression_factor("k"),
        Some((_, factor)) if (factor - 1.0).abs() < 1e-12
    ));
    assert!(matches!(
        limiter.test_get_suppression_factor("k2"),
        Some((_, factor)) if (factor - 1.0).abs() < 1e-12
    ));
    thread::sleep(Duration::from_millis(300));
    assert!(matches!(
        limiter.inc("k", &rate, 7),
        RateLimitDecision::Suppressed {
            is_allowed: false,
            suppression_factor,
        } if (suppression_factor - 1.0).abs() < 1e-12
    ));
    thread::sleep(Duration::from_millis(800));

    assert_eq!(
        limiter.get("k"),
        SuppressedRateLimitSnapshot {
            total: 7,
            total_declined: 7,
            suppression_factor: 0.0,
        },
        "expired bucket must be evicted"
    );

    let series = limiter.series().get("k").expect("series should exist");
    assert_eq!(series.series.len(), 1);
    assert_eq!(series.series[0].count.load(Ordering::Acquire), 7);
    assert_eq!(series.series[0].declined.load(Ordering::Acquire), 7);
    assert_eq!(series.total.load(Ordering::Acquire), 7);
    assert_eq!(series.total_declined.load(Ordering::Acquire), 7);
    drop(series);
    assert!(matches!(
        limiter.test_get_suppression_factor("k"),
        Some((_, factor)) if factor == 0.0
    ));

    assert_eq!(limiter.get("k2"), SuppressedRateLimitSnapshot::default());

    let series = limiter.series().get("k2").expect("series should exist");
    assert!(series.series.is_empty());
    assert_eq!(series.total.load(Ordering::Acquire), 0);
    assert_eq!(series.total_declined.load(Ordering::Acquire), 0);
    drop(series);
    assert!(matches!(
        limiter.test_get_suppression_factor("k2"),
        Some((_, factor)) if factor == 0.0
    ));
}

#[test]
fn get_fresh_key_uses_shared_dashmap_lock() {
    let limiter = Arc::new(limiter(6, 1000, 1.0));
    let rate_limit = RateLimit::try_from(100f64).unwrap();
    let decision = limiter.inc("k", &rate_limit, 3);
    assert!(
        matches!(decision, RateLimitDecision::Allowed),
        "setup increment must be allowed: {decision:?}"
    );

    let held_read_guard = limiter.series().get("k").expect("series should exist");
    let barrier = Arc::new(Barrier::new(2));
    let (sender, receiver) = mpsc::channel();

    let reader = {
        let limiter = Arc::clone(&limiter);
        let barrier = Arc::clone(&barrier);

        thread::spawn(move || {
            barrier.wait();
            sender.send(limiter.get("k").total).unwrap();
        })
    };

    barrier.wait();
    let result = receiver.recv_timeout(Duration::from_secs(2));

    drop(held_read_guard);
    reader.join().expect("reader thread panicked");

    assert_eq!(
        result.expect("fresh-key get blocked on an existing shared DashMap guard"),
        3
    );
}

#[test]
fn set_if_lt_primes_empty_key_and_reprime_is_noop() {
    let limiter = limiter(6, 1000, 1.0);
    let rate_limit = RateLimit::try_from(100f64).unwrap();

    let (new_total, old_total) =
        limiter.set_if("k", &rate_limit, RateLimitComparator::Lt(100), 100);
    assert_eq!((new_total, old_total), (100, 0));

    let (new_total, old_total) =
        limiter.set_if("k", &rate_limit, RateLimitComparator::Lt(100), 100);
    assert_eq!((new_total, old_total), (100, 100));

    assert_eq!(limiter.get("k").total, 100);
}

#[test]
fn set_if_lt_with_lower_target_is_noop() {
    let limiter = limiter(6, 1000, 1.0);
    let rate_limit = RateLimit::try_from(100f64).unwrap();

    assert_eq!(
        limiter.set_if("k", &rate_limit, RateLimitComparator::Lt(100), 100),
        (100, 0)
    );

    let (new_total, old_total) = limiter.set_if("k", &rate_limit, RateLimitComparator::Lt(50), 50);
    assert_eq!((new_total, old_total), (100, 100));
    assert_eq!(limiter.get("k").total, 100);
}

#[test]
fn set_if_nil_overwrites_unconditionally_including_lowering() {
    let limiter = limiter(6, 1000, 1.0);
    let rate_limit = RateLimit::try_from(100f64).unwrap();

    assert_eq!(
        limiter.set_if("k", &rate_limit, RateLimitComparator::Nil, 100),
        (100, 0)
    );

    let (new_total, old_total) = limiter.set_if("k", &rate_limit, RateLimitComparator::Nil, 30);
    assert_eq!((new_total, old_total), (30, 100));
    assert_eq!(limiter.get("k").total, 30);
}

#[test]
fn set_if_eq_zero_sets_only_when_window_is_empty() {
    let limiter = limiter(6, 1000, 1.0);
    let rate_limit = RateLimit::try_from(100f64).unwrap();

    let (new_total, old_total) = limiter.set_if("k", &rate_limit, RateLimitComparator::Eq(0), 25);
    assert_eq!((new_total, old_total), (25, 0));

    let (new_total, old_total) = limiter.set_if("k", &rate_limit, RateLimitComparator::Eq(0), 99);
    assert_eq!((new_total, old_total), (25, 25));
}

#[test]
fn set_if_prime_below_soft_limit_allows_next_inc() {
    // window 6 * rate 5 * factor 1.0 → hard = soft = 30.
    let limiter = limiter(6, 1000, 1.0);
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    let result = limiter.set_if("k", &rate_limit, RateLimitComparator::Lt(27), 27);
    assert_eq!(result, (27, 0));

    let decision = limiter.inc("k", &rate_limit, 1);
    assert!(
        matches!(decision, RateLimitDecision::Allowed),
        "decision: {decision:?}"
    );
}

#[test]
fn set_if_prime_at_hard_limit_declines_next_inc() {
    let limiter = limiter(6, 1000, 1.0);
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    let result = limiter.set_if("k", &rate_limit, RateLimitComparator::Lt(30), 30);
    assert_eq!(result, (30, 0));

    let decision = limiter.inc("k", &rate_limit, 1);
    assert!(
        matches!(
            decision,
            RateLimitDecision::Suppressed {
                is_allowed: false,
                suppression_factor,
            } if (suppression_factor - 1.0).abs() < 1e-12
        ),
        "decision: {decision:?}"
    );
}

#[test]
fn set_if_zero_count_resets_declines_and_suppression_state() {
    let limiter = limiter(6, 1000, 1.0);
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    // Saturate the window and record some declines.
    assert_eq!(
        limiter.set_if("k", &rate_limit, RateLimitComparator::Nil, 30),
        (30, 0)
    );
    for _ in 0..3 {
        let decision = limiter.inc("k", &rate_limit, 1);
        assert!(
            matches!(
                decision,
                RateLimitDecision::Suppressed {
                    is_allowed: false,
                    ..
                }
            ),
            "decision: {decision:?}"
        );
    }
    let series = limiter.series().get("k").expect("series should exist");
    assert_eq!(series.total.load(Ordering::Acquire), 33);
    assert_eq!(series.total_declined.load(Ordering::Acquire), 3);
    drop(series);
    assert_eq!(limiter.get_suppression_factor("k"), 1.0);
    assert!(limiter.test_get_suppression_factor("k").is_some());

    // Clearing the window removes all per-key state, including the cached factor.
    let (new_total, old_total) = limiter.set_if("k", &rate_limit, RateLimitComparator::Nil, 0);
    assert_eq!((new_total, old_total), (0, 33));
    assert!(!limiter.series().contains_key("k"));
    assert!(limiter.test_get_suppression_factor("k").is_none());
    assert_eq!(limiter.get("k").total, 0);

    let decision = limiter.inc("k", &rate_limit, 1);
    assert!(
        matches!(decision, RateLimitDecision::Allowed),
        "decision: {decision:?}"
    );
    assert_eq!(limiter.get("k").total, 1);
}

#[test]
fn set_if_replacement_resets_declined_history_and_cached_factor() {
    let limiter = limiter(6, 1000, 1.0);
    let rate = RateLimit::try_from(5f64).unwrap();

    assert_eq!(
        limiter.set_if("k", &rate, RateLimitComparator::Nil, 30),
        (30, 0)
    );
    let denied = limiter.inc("k", &rate, 3);
    assert!(matches!(
        denied,
        RateLimitDecision::Suppressed {
            suppression_factor,
            is_allowed: false,
        } if (suppression_factor - 1.0).abs() < 1e-12
    ));
    assert!(limiter.test_get_suppression_factor("k").is_some());

    assert_eq!(
        limiter.set_if("k", &rate, RateLimitComparator::Nil, 5),
        (5, 33)
    );
    let series = limiter.series().get("k").expect("series should exist");
    assert_eq!(series.series.len(), 1);
    assert_eq!(series.series[0].count.load(Ordering::Acquire), 5);
    assert_eq!(series.series[0].declined.load(Ordering::Acquire), 0);
    assert_eq!(series.total.load(Ordering::Acquire), 5);
    assert_eq!(series.total_declined.load(Ordering::Acquire), 0);
    drop(series);
    assert!(limiter.test_get_suppression_factor("k").is_none());
}

#[test]
fn set_if_redefines_sticky_hard_window_limit() {
    // Original hard window limit: 6 * 1 * 1.0 = 6.
    let limiter = limiter(6, 1000, 1.0);
    let rate_one = RateLimit::try_from(1f64).unwrap();

    assert_eq!(
        limiter.set_if("k", &rate_one, RateLimitComparator::Nil, 6),
        (6, 0)
    );
    {
        let series = limiter.series().get("k").expect("series should exist");
        assert!((series.hard_window_limit - 6.0).abs() < f64::EPSILON);
    }
    let decision = limiter.inc("k", &rate_one, 1);
    assert!(
        matches!(
            decision,
            RateLimitDecision::Suppressed {
                is_allowed: false,
                ..
            }
        ),
        "decision: {decision:?}"
    );

    // set_if with a higher rate redefines the stored limit (6 * 10 * 1.0 = 60),
    // unlike inc where the limit is sticky.
    let rate_ten = RateLimit::try_from(10f64).unwrap();
    assert_eq!(
        limiter.set_if("k", &rate_ten, RateLimitComparator::Nil, 1),
        (1, 7)
    );
    {
        let series = limiter.series().get("k").expect("series should exist");
        assert!((series.hard_window_limit - 60.0).abs() < f64::EPSILON);
    }

    for i in 0..12_u64 {
        let decision = limiter.inc("k", &rate_one, 1);
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "i: {i}, decision: {decision:?}"
        );
    }
}

#[test]
fn set_if_preserve_history_scales_partial_declines_proportionally() {
    let limiter = limiter(10, 50, 1.0);
    let rate = RateLimit::try_from(2f64).unwrap();

    assert!(matches!(
        limiter.inc("k", &rate, 4),
        RateLimitDecision::Allowed
    ));
    thread::sleep(Duration::from_millis(60));
    assert!(matches!(
        limiter.inc("k", &rate, 16),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        limiter.inc("k", &rate, 7),
        RateLimitDecision::Suppressed {
            is_allowed: false,
            suppression_factor,
        } if (suppression_factor - 1.0).abs() < 1e-12
    ));

    {
        let series = limiter.series().get("k").expect("series should exist");
        let counts = series
            .series
            .iter()
            .map(|bucket| bucket.count.load(Ordering::Acquire))
            .collect::<Vec<_>>();

        let declined = series
            .series
            .iter()
            .map(|bucket| bucket.declined.load(Ordering::Acquire))
            .collect::<Vec<_>>();

        assert_eq!(counts, vec![4, 23], "wrong count edge for");
        assert_eq!(declined, vec![0, 7], "wrong declined for");

        assert_eq!(series.total.load(Ordering::Acquire), 27, "wrong total ");
        assert_eq!(
            series.total_declined.load(Ordering::Acquire),
            7,
            "wrong declined total"
        );
    }

    assert_eq!(
        limiter.set_if_preserve_history(
            "k",
            &rate,
            RateLimitComparator::Nil,
            10,
            HistoryPreservation::PreserveNewest,
        ),
        (10, 27),
        "wrong result for newest"
    );

    let series = limiter.series().get("k").unwrap();
    assert_eq!(series.series.len(), 1, "wrong series length");
    assert_eq!(
        series.series[0].count.load(Ordering::Acquire),
        10,
        "wrong count"
    );
    assert_eq!(
        series.series[0].declined.load(Ordering::Acquire),
        3,
        "wrong declined"
    );
    assert_eq!(series.total.load(Ordering::Acquire), 10, "wrong total");
    assert_eq!(
        series.total_declined.load(Ordering::Acquire),
        3,
        "wrong declined total"
    );
}

#[test]
fn set_if_preserve_oldest_keeps_oldest_suppressed_history() {
    let limiter = limiter(10, 50, 1.0);
    let rate = RateLimit::try_from(2f64).unwrap();

    assert!(matches!(
        limiter.inc("k", &rate, 20),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        limiter.inc("k", &rate, 12),
        RateLimitDecision::Suppressed {
            is_allowed: false,
            suppression_factor,
        } if (suppression_factor - 1.0).abs() < 1e-12
    ));
    thread::sleep(Duration::from_millis(60));
    assert!(matches!(
        limiter.inc("k", &rate, 6),
        RateLimitDecision::Suppressed {
            is_allowed: false,
            suppression_factor,
        } if (suppression_factor - 1.0).abs() < 1e-12
    ));

    {
        let series = limiter.series().get("k").expect("series should exist");
        let counts = series
            .series
            .iter()
            .map(|bucket| bucket.count.load(Ordering::Acquire))
            .collect::<Vec<_>>();
        let declined = series
            .series
            .iter()
            .map(|bucket| bucket.declined.load(Ordering::Acquire))
            .collect::<Vec<_>>();
        assert_eq!(counts, vec![32, 6]);
        assert_eq!(declined, vec![12, 6]);
        assert_eq!(series.total.load(Ordering::Acquire), 38);
        assert_eq!(series.total_declined.load(Ordering::Acquire), 18);
    }

    assert_eq!(
        limiter.set_if_preserve_history(
            "k",
            &rate,
            RateLimitComparator::Nil,
            7,
            HistoryPreservation::PreserveOldest,
        ),
        (7, 38)
    );
    let series = limiter.series().get("k").unwrap();
    let counts = series
        .series
        .iter()
        .map(|bucket| bucket.count.load(Ordering::Acquire))
        .collect::<Vec<_>>();
    assert_eq!(counts, vec![7]);
    assert_eq!(series.series[0].declined.load(Ordering::Acquire), 2);
    assert_eq!(series.total.load(Ordering::Acquire), 7);
    assert_eq!(series.total_declined.load(Ordering::Acquire), 2);
}

#[test]
fn set_if_preserve_history_increases_the_selected_edge_bucket() {
    let limiter = limiter(10, 50, 1.5);
    let rate = RateLimit::try_from(100f64).unwrap();

    for key in ["newest", "oldest"] {
        let decision = limiter.inc(key, &rate, 4);
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "first {key} increment must be allowed: {decision:?}"
        );
    }
    thread::sleep(Duration::from_millis(60));
    for key in ["newest", "oldest"] {
        let decision = limiter.inc(key, &rate, 4);
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "second {key} increment must be allowed: {decision:?}"
        );
        limiter.test_set_suppression_factor(key, Instant::now(), 0.25);
    }

    assert_eq!(
        limiter.set_if_preserve_history(
            "newest",
            &rate,
            RateLimitComparator::Nil,
            10,
            HistoryPreservation::PreserveNewest,
        ),
        (10, 8)
    );
    assert_eq!(
        limiter.set_if_preserve_history(
            "oldest",
            &rate,
            RateLimitComparator::Nil,
            10,
            HistoryPreservation::PreserveOldest,
        ),
        (10, 8)
    );

    for (key, expected_counts) in [("newest", vec![4, 6]), ("oldest", vec![6, 4])] {
        let series = limiter.series().get(key).expect("series should exist");
        let counts = series
            .series
            .iter()
            .map(|bucket| bucket.count.load(Ordering::Acquire))
            .collect::<Vec<_>>();
        let declined = series
            .series
            .iter()
            .map(|bucket| bucket.declined.load(Ordering::Acquire))
            .collect::<Vec<_>>();
        assert_eq!(counts, expected_counts, "wrong retained edge for {key}");
        assert_eq!(declined, vec![0, 0]);
        assert_eq!(series.total.load(Ordering::Acquire), 10);
        assert_eq!(series.total_declined.load(Ordering::Acquire), 0);
        drop(series);
        assert!(
            limiter.test_get_suppression_factor(key).is_none(),
            "a changed history must invalidate the {key} suppression cache"
        );
    }
}

#[test]
fn set_if_preserve_history_creates_missing_positive_targets() {
    let limiter = limiter(6, 1000, 1.5);
    let rate = RateLimit::try_from(10f64).unwrap();

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

        let series = limiter.series().get(key).expect("series should exist");
        assert_eq!(series.series.len(), 1);
        assert_eq!(series.series[0].count.load(Ordering::Acquire), 9);
        assert_eq!(series.series[0].declined.load(Ordering::Acquire), 0);
        assert_eq!(series.total.load(Ordering::Acquire), 9);
        assert_eq!(series.total_declined.load(Ordering::Acquire), 0);
        assert!((series.hard_window_limit - 90.0).abs() < f64::EPSILON);
    }
}

#[test]
fn set_if_preserve_history_prunes_expired_buckets_before_matching_update() {
    let limiter = limiter(1, 50, 2.0);
    let rate = RateLimit::try_from(100f64).unwrap();

    for key in ["newest", "oldest"] {
        let decision = limiter.inc(key, &rate, 4);
        assert!(matches!(decision, RateLimitDecision::Allowed));
    }
    thread::sleep(Duration::from_millis(300));
    for key in ["newest", "oldest"] {
        let decision = limiter.inc(key, &rate, 5);
        assert!(matches!(decision, RateLimitDecision::Allowed));
        limiter.test_set_suppression_factor(key, Instant::now(), 0.25);
    }
    thread::sleep(Duration::from_millis(800));

    for (key, preservation) in [
        ("newest", HistoryPreservation::PreserveNewest),
        ("oldest", HistoryPreservation::PreserveOldest),
    ] {
        assert_eq!(
            limiter.set_if_preserve_history(key, &rate, RateLimitComparator::Nil, 3, preservation,),
            (3, 5)
        );

        let series = limiter.series().get(key).expect("series should exist");
        assert_eq!(series.series.len(), 1);
        assert_eq!(series.series[0].count.load(Ordering::Acquire), 3);
        assert_eq!(series.series[0].declined.load(Ordering::Acquire), 0);
        assert_eq!(series.total.load(Ordering::Acquire), 3);
        assert_eq!(series.total_declined.load(Ordering::Acquire), 0);
        drop(series);
        assert!(limiter.test_get_suppression_factor(key).is_none());
    }
}

#[test]
fn unchanged_preserve_history_uses_shared_lock_and_keeps_cached_factor() {
    let limiter = Arc::new(limiter(6, 1000, 1.5));
    let rate = RateLimit::try_from(100f64).unwrap();
    let setup = limiter.inc("k", &rate, 3);
    assert!(matches!(setup, RateLimitDecision::Allowed));

    let cached_at = Instant::now();
    limiter.test_set_suppression_factor("k", cached_at, 0.25);
    let held_read_guard = limiter.series().get("k").expect("series should exist");
    let barrier = Arc::new(Barrier::new(2));
    let (sender, receiver) = mpsc::channel();

    let worker = {
        let limiter = Arc::clone(&limiter);
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            barrier.wait();
            let result = limiter.set_if_preserve_history(
                "k",
                &rate,
                RateLimitComparator::Nil,
                3,
                HistoryPreservation::PreserveNewest,
            );
            sender.send(result).unwrap();
        })
    };

    barrier.wait();
    let result = receiver.recv_timeout(Duration::from_secs(2));
    drop(held_read_guard);
    worker.join().expect("worker thread panicked");

    assert_eq!(
        result.expect("unchanged preservation blocked on an existing shared DashMap guard"),
        (3, 3)
    );
    assert_eq!(
        limiter.test_get_suppression_factor("k"),
        Some((cached_at, 0.25)),
        "an unchanged preservation must keep the valid suppression cache"
    );
}

#[test]
fn unchanged_total_with_a_new_limit_updates_limit_and_invalidates_cache() {
    let limiter = limiter(6, 1000, 1.5);
    let original_rate = RateLimit::try_from(1f64).unwrap();
    let new_rate = RateLimit::try_from(10f64).unwrap();

    let setup = limiter.inc("k", &original_rate, 3);
    assert!(matches!(setup, RateLimitDecision::Allowed));
    limiter.test_set_suppression_factor("k", Instant::now(), 0.25);

    assert_eq!(
        limiter.set_if_preserve_history(
            "k",
            &new_rate,
            RateLimitComparator::Nil,
            3,
            HistoryPreservation::PreserveOldest,
        ),
        (3, 3)
    );

    let series = limiter.series().get("k").expect("series should exist");
    assert_eq!(series.series.len(), 1);
    assert_eq!(series.series[0].count.load(Ordering::Acquire), 3);
    assert_eq!(series.series[0].declined.load(Ordering::Acquire), 0);
    assert!((series.hard_window_limit - 90.0).abs() < f64::EPSILON);
    drop(series);
    assert!(limiter.test_get_suppression_factor("k").is_none());
}

#[test]
fn inc_keeps_the_first_hard_window_limit_for_the_key() {
    let limiter = limiter(6, 1000, 1.5);
    let strict_rate = RateLimit::try_from(1f64).unwrap();
    let loose_rate = RateLimit::try_from(100f64).unwrap();

    let first = limiter.inc("k", &strict_rate, 1);
    assert!(matches!(first, RateLimitDecision::Allowed));
    {
        let series = limiter.series().get("k").expect("series should exist");
        assert!((series.hard_window_limit - 9.0).abs() < f64::EPSILON);
    }

    let reaches_hard = limiter.inc("k", &loose_rate, 8);
    assert!(
        matches!(reaches_hard, RateLimitDecision::Allowed),
        "the original hard limit should be reached exactly: {reaches_hard:?}"
    );
    {
        let series = limiter.series().get("k").expect("series should exist");
        assert!((series.hard_window_limit - 9.0).abs() < f64::EPSILON);
    }

    let next = limiter.inc("k", &loose_rate, 1);
    assert!(matches!(
        next,
        RateLimitDecision::Suppressed {
            suppression_factor,
            is_allowed: false,
        } if (suppression_factor - 1.0).abs() < 1e-12
    ));
    let series = limiter.series().get("k").expect("series should exist");
    assert_eq!(series.total.load(Ordering::Acquire), 10);
    assert_eq!(series.total_declined.load(Ordering::Acquire), 1);
}

#[test]
fn conditional_set_guard_miss_uses_shared_dashmap_lock() {
    let limiter = Arc::new(limiter(6, 1000, 1.5));
    let rate = RateLimit::try_from(100f64).unwrap();
    let setup = limiter.inc("k", &rate, 3);
    assert!(
        matches!(setup, RateLimitDecision::Allowed),
        "setup increment must be allowed: {setup:?}"
    );
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
fn conditional_set_guard_miss_leaves_expired_history_untouched() {
    let limiter = limiter(1, 1, 2.0);
    let rate = RateLimit::try_from(100f64).unwrap();
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

    let cached_at = Instant::now();
    limiter.test_set_suppression_factor("k", cached_at, 0.25);

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
            HistoryPreservation::PreserveOldest,
        ),
        (5, 5)
    );

    let series = limiter.series().get("k").expect("series should remain");
    assert_eq!(series.series.len(), 2, "miss must not prune history");
    assert_eq!(
        series.total.load(Ordering::Acquire),
        9,
        "miss must not rewrite stored counters"
    );
    drop(series);
    assert_eq!(
        limiter.test_get_suppression_factor("k"),
        Some((cached_at, 0.25)),
        "a comparator miss must not invalidate the suppression cache"
    );
}

#[test]
fn conditional_set_missing_zero_leaves_suppressed_key_absent() {
    let limiter = limiter(6, 1000, 1.5);
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

#[test]
fn conditional_set_present_zero_removes_suppressed_key_completely() {
    let limiter = limiter(6, 1000, 1.5);
    let rate = RateLimit::try_from(100f64).unwrap();

    for (key, preserve) in [("replace", false), ("preserve", true)] {
        assert_eq!(
            limiter.set_if(key, &rate, RateLimitComparator::Nil, 10),
            (10, 0)
        );
        let _ = limiter.get_suppression_factor(key);
        assert!(limiter.series().contains_key(key));
        assert!(limiter.test_get_suppression_factor(key).is_some());

        let result = if preserve {
            limiter.set_if_preserve_history(
                key,
                &rate,
                RateLimitComparator::Nil,
                0,
                HistoryPreservation::PreserveOldest,
            )
        } else {
            limiter.set_if(key, &rate, RateLimitComparator::Nil, 0)
        };

        assert_eq!(result, (0, 10));
        assert!(!limiter.series().contains_key(key));
        assert!(limiter.test_get_suppression_factor(key).is_none());
        assert_eq!(limiter.get(key).total, 0);
    }
}
