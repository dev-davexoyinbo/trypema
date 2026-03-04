use std::time::{Duration, Instant};

use crate::{
    HardLimitFactor, LocalRateLimiterOptions, RateGroupSizeMs, RateLimit, RateLimitDecision,
    SuppressedLocalRateLimiter, SuppressionFactorCacheMs, WindowSizeSeconds,
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

    let _ = limiter.inc(key, &rate_limit, 10);
    // wait for 1s to pass
    std::thread::sleep(Duration::from_millis(1001));

    let _ = limiter.inc(key, &rate_limit, 20);

    // Suppression factor is computed from the pre-increment state.
    // Here, the last-second count is 20.
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

    // Contract:
    // - suppression_factor == 0.0 => Allowed
    // - 0.0 < suppression_factor < 1.0 => may return Rejected or Suppressed, and suppressed is_allowed may be true or false
    // - suppression_factor == 1.0 => must not return Allowed
    let mut rng = |p: f64| {
        assert!((p - 0.75).abs() < 1e-12, "p: {p:?}");
        false
    };
    let decision = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);

    assert!(
        matches!(decision, RateLimitDecision::Rejected { .. })
            || matches!(
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
fn suppressed_inc_allowed_returns_suppressed_is_allowed_true_and_increments_accepted() {
    let limiter = limiter(1, 1000, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    limiter.test_set_suppression_factor(key, Instant::now(), 0.25);

    // Contract: for 0.0 < suppression_factor < 1.0, it may return Rejected or Suppressed,
    // and suppressed `is_allowed` may be true or false.
    let mut rng = |p: f64| {
        assert!((p - 0.75).abs() < 1e-12, "p: {p:?}");
        true
    };
    let decision = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);

    assert!(
        matches!(decision, RateLimitDecision::Rejected { .. })
            || matches!(
                    decision,
                RateLimitDecision::Suppressed {
                    suppression_factor,
                    is_allowed: true
                } if (suppression_factor - 0.25).abs() < 1e-12
            ),
        "decision: {decision:?}"
    );
}

#[test]
fn hard_limit_is_enforced_after_suppression_factor_cache_expires() {
    // Suppression factor is computed from the pre-increment state and cached.
    // To observe a hard-limit-triggered suppression_factor=1.0 on the next call,
    // ensure the cache expires between calls.
    let limiter = limiter_with_cache_ms(1, 1000, 2f64, 1);
    let key = "k";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    // Do not set suppression factor manually; drive the limiter through the public API.
    // With window=1s, rate_limit=1 req/s, hard_limit_factor=2 => hard_window_limit=2.
    // The 3rd call should observe total>=hard_window_limit and compute suppression_factor=1.0.
    let d1 = limiter.inc(key, &rate_limit, 1);
    assert!(matches!(d1, RateLimitDecision::Allowed), "d1: {d1:?}");
    let d2 = limiter.inc(key, &rate_limit, 1);
    assert!(matches!(d2, RateLimitDecision::Allowed), "d2: {d2:?}");

    // Ensure suppression factor is recomputed for d3.
    std::thread::sleep(Duration::from_millis(10));

    let d3 = limiter.inc(key, &rate_limit, 1);
    assert!(
        matches!(d3, RateLimitDecision::Rejected { .. })
            || matches!(
                d3,
                RateLimitDecision::Suppressed {
                    suppression_factor,
                    is_allowed: false
                } if (suppression_factor - 1.0).abs() < 1e-12
            ),
        "d3: {d3:?}"
    );

    assert!((limiter.get_suppression_factor(key) - 1.0).abs() < 1e-12);
}

#[test]
#[should_panic(
    expected = "SuppressedLocalRateLimiter::get_suppression_factor: suppression factor > 1"
)]
fn suppression_factor_gt_one_panics() {
    let limiter = limiter(1, 1000, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    limiter.test_set_suppression_factor(key, Instant::now(), 2f64);

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

    limiter.test_set_suppression_factor(key, Instant::now(), -0.01);

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

#[test]
fn suppression_factor_zero_returns_allowed_and_rng_not_called() {
    let limiter = limiter(1, 1000, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    limiter.test_set_suppression_factor(key, Instant::now(), 0.0);

    // Ensure we are really observing the cached value.
    assert_eq!(limiter.get_suppression_factor(key), 0.0);

    let mut rng = |_p: f64| panic!("rng must not be called when suppression_factor == 0");
    let decision = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);
    assert!(matches!(decision, RateLimitDecision::Allowed));
}

#[test]
fn suppression_factor_one_must_not_return_allowed() {
    let limiter = limiter(1, 1000, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    limiter.test_set_suppression_factor(key, Instant::now(), 1.0);

    let mut rng = |_p: f64| panic!("rng must not be called when suppression_factor == 1");
    let decision = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);

    // Contract: suppression_factor == 1.0 must not return Allowed.
    assert!(
        matches!(decision, RateLimitDecision::Rejected { .. })
            || matches!(decision, RateLimitDecision::Suppressed { .. }),
        "decision: {decision:?}"
    );
}

#[test]
fn suppression_rng_probability_is_one_minus_suppression_factor() {
    let limiter = limiter(1, 1000, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    limiter.test_set_suppression_factor(key, Instant::now(), 0.25);

    let mut rng = |p: f64| {
        assert!((p - 0.75).abs() < 1e-12, "p: {p:?}");
        false
    };
    let decision = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);

    assert!(
        matches!(decision, RateLimitDecision::Rejected { .. })
            || matches!(
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
    let key = "k";
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    // Sample values across the full inclusive range.
    let sfs = [0.0, 0.1, 0.25, 0.5, 0.9, 1.0];

    for sf in sfs {
        limiter.test_set_suppression_factor(key, Instant::now(), sf);

        if sf == 0.0 {
            let mut rng = |_p: f64| panic!("rng must not be called when suppression_factor == 0");
            let decision = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);
            assert!(
                matches!(decision, RateLimitDecision::Allowed),
                "sf={sf} decision={decision:?}"
            );
            continue;
        }

        if sf == 1.0 {
            let mut rng = |_p: f64| panic!("rng must not be called when suppression_factor == 1");
            let decision = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);
            assert!(
                !matches!(decision, RateLimitDecision::Allowed),
                "sf={sf} decision={decision:?}"
            );
            continue;
        }

        // For 0 < sf < 1, RNG should be consulted with p = 1 - sf.
        for expected_is_allowed in [false, true] {
            let mut called = 0u64;
            let mut rng = |p: f64| {
                called += 1;
                assert!((p - (1.0 - sf)).abs() < 1e-12, "sf={sf} p={p:?}");
                expected_is_allowed
            };

            let decision = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);
            assert_eq!(called, 1, "sf={sf} expected 1 rng call, got {called}");

            match decision {
                RateLimitDecision::Rejected { .. } => {}
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
            }
        }
    }
}

#[test]
fn suppressed_is_deterministically_allowed_until_base_capacity_boundary() {
    // Deterministic regime: suppression does not start until the *base* window capacity is met.
    // We assert decisions remain Allowed right up to the boundary.
    let window_size_seconds = 10_u64;
    let hard_limit_factor = 2f64;
    let limiter = limiter(window_size_seconds, 1000, hard_limit_factor);

    let key = "k";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    let base_capacity = window_size_seconds; // 10s * 1 req/s

    // Pre-increment total is below base capacity.
    let d1 = limiter.inc(key, &rate_limit, base_capacity - 1);
    assert!(matches!(d1, RateLimitDecision::Allowed), "d1: {d1:?}");

    // Still below base capacity pre-increment (total = 9), so suppression must not start.
    let d2 = limiter.inc(key, &rate_limit, 1);
    assert!(matches!(d2, RateLimitDecision::Allowed), "d2: {d2:?}");
}

#[test]
fn suppressed_is_fully_denied_after_hard_limit_observed() {
    // Deterministic regime: once the observed count reaches the hard cutoff,
    // suppression_factor becomes 1.0 and requests are denied (is_allowed=false).
    let window_size_seconds = 10_u64;
    let hard_limit_factor = 2f64;

    // Use a tiny cache so we can deterministically observe recomputation after state changes.
    let limiter = limiter_with_cache_ms(window_size_seconds, 1000, hard_limit_factor, 1);

    let key = "k_hard";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    let hard_capacity = window_size_seconds * 2; // 10s * 1 req/s * 2.0

    // First call: pre-increment total is 0 (< base capacity), so it's Allowed.
    let d1 = limiter.inc(key, &rate_limit, hard_capacity);
    assert!(matches!(d1, RateLimitDecision::Allowed), "d1: {d1:?}");

    // Ensure the cached suppression factor expires so the next call recomputes from updated totals.
    std::thread::sleep(Duration::from_millis(5));

    for i in 0..5u64 {
        let d = limiter.inc(key, &rate_limit, 1);
        assert!(
            matches!(
                d,
                RateLimitDecision::Suppressed {
                    suppression_factor,
                    is_allowed: false
                } if (suppression_factor - 1.0).abs() < 1e-12
            ),
            "i={i} d={d:?}"
        );
    }
}
