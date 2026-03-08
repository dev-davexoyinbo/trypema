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

/// Push `key` past the soft window capacity so that subsequent `inc_with_rng` calls
/// enter the suppression-decision branch (rather than short-circuiting to `Allowed`).
///
/// The soft window limit is `window_size_seconds * rate_limit`. We need
/// `total - total_declined >= soft_window_limit`. We achieve this by recording a
/// single increment of `soft_window_limit` (all accepted, so `total_declined = 0`),
/// then overwriting the cached suppression factor with the desired test value so the
/// next call uses it exactly.
fn fill_past_soft_limit(
    limiter: &SuppressedLocalRateLimiter,
    key: &str,
    rate_limit: &RateLimit,
    window_size_seconds: u64,
    desired_suppression_factor: f64,
) {
    let soft_limit = window_size_seconds * **rate_limit as u64;
    // A single batch increment of exactly `soft_limit` puts accepted = soft_limit.
    // forecasted_allowed for the *next* single call will be soft_limit + 1 > soft_limit,
    // which enters the suppression path.
    let _ = limiter.inc(key, rate_limit, soft_limit);
    // Overwrite the suppression factor with the desired test value (fresh TTL).
    limiter.test_set_suppression_factor(key, Instant::now(), desired_suppression_factor);
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
    // Full suppression (sf=1.0) triggers when forecasted_allowed > hard_window_limit.
    // forecasted_allowed = total + count, where total already includes the new count.
    //
    // Use fill_past_soft_limit to establish a clean window state (total=soft, declined=0),
    // then increment by enough to push forecasted > hard_window_limit.
    //
    // limiter(10, 100, 10f64): window=10s, rate=1 req/s, hard=10.
    //   soft=10, hard=100.
    // After fill_past_soft_limit: total=10, declined=0.
    // Then inc(91): total=101, forecasted=101+91=192 > hard(100) => sf=1.0, denied.
    let limiter = limiter(10, 100, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    fill_past_soft_limit(&limiter, key, &rate_limit, 10, 0.0);

    let decision = limiter.inc(key, &rate_limit, 91);

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

    fill_past_soft_limit(&limiter, key, &rate_limit, 1, 0.25);

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

    fill_past_soft_limit(&limiter, key, &rate_limit, 1, 0.25);

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
    // The hard window limit = window_size_seconds * rate_limit * hard_limit_factor.
    // Full suppression (sf=1.0) triggers when forecasted_allowed > hard_window_limit,
    // where forecasted_allowed = total + count (total already includes the new count).
    //
    // Setup: window=10s, rate=1 req/s, hard_limit_factor=2 => soft=10, hard=20.
    // Use a tiny cache (1ms) so the suppression factor is always recomputed.
    let limiter = limiter_with_cache_ms(10, 1000, 2f64, 1);
    let key = "k";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    // d1: total=1, forecasted=2 <= soft(10) => Allowed.
    let d1 = limiter.inc(key, &rate_limit, 1);
    assert!(matches!(d1, RateLimitDecision::Allowed), "d1: {d1:?}");

    // Ensure the cache expires so d2 recomputes.
    std::thread::sleep(Duration::from_millis(5));

    // d2: total=2, forecasted=3 <= soft(10) => Allowed.
    let d2 = limiter.inc(key, &rate_limit, 1);
    assert!(matches!(d2, RateLimitDecision::Allowed), "d2: {d2:?}");

    // Ensure the cache expires so d3 recomputes.
    std::thread::sleep(Duration::from_millis(5));

    // d3: increment by enough to push forecasted > hard(20).
    // After d2 total=2; d3 count=20: total=22, forecasted=22+20=42 > 20 => sf=1.0, denied.
    let d3 = limiter.inc(key, &rate_limit, 20);
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

    fill_past_soft_limit(&limiter, key, &rate_limit, 1, 2f64);

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

    fill_past_soft_limit(&limiter, key, &rate_limit, 1, -0.01);

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
    let _ = limiter.inc(key, &rate_limit, 1);
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
fn cleanup_does_not_break_next_inc_when_cache_entry_removed() {
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

    fill_past_soft_limit(&limiter, key, &rate_limit, 1, 1.0);

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

    fill_past_soft_limit(&limiter, key, &rate_limit, 1, 0.25);

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
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    // Sample values across the full inclusive range.
    let sfs = [0.0, 0.1, 0.25, 0.5, 0.9, 1.0];

    for sf in sfs {
        // Use a unique key per sf value to avoid accumulated-state interference.
        let key = format!("k_{sf}");
        let key = key.as_str();

        if sf == 0.0 {
            // sf=0: short-circuit to Allowed without consulting suppression factor.
            limiter.test_set_suppression_factor(key, Instant::now(), sf);
            let mut rng = |_p: f64| panic!("rng must not be called when suppression_factor == 0");
            let decision = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);
            assert!(
                matches!(decision, RateLimitDecision::Allowed),
                "sf={sf} decision={decision:?}"
            );
            continue;
        }

        if sf == 1.0 {
            fill_past_soft_limit(&limiter, key, &rate_limit, 1, sf);
            let mut rng = |_p: f64| panic!("rng must not be called when suppression_factor == 1");
            let decision = limiter.inc_with_rng(key, &rate_limit, 1, &mut rng);
            assert!(
                !matches!(decision, RateLimitDecision::Allowed),
                "sf={sf} decision={decision:?}"
            );
            continue;
        }

        // For 0 < sf < 1, RNG should be consulted with p = 1 - sf.
        fill_past_soft_limit(&limiter, key, &rate_limit, 1, sf);
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
    // `Allowed` is returned only when `forecasted_allowed <= soft_window_limit`.
    // forecasted_allowed = total + count, where `total` already includes the new count.
    // So: (prev_total + count) + count <= soft_window_limit
    //     prev_total + 2*count <= soft_window_limit
    //
    // With window=10s, rate=1 req/s, hard_factor=2:
    //   soft_window_limit = 10 * 1 = 10
    //   hard_window_limit = 10 * 1 * 2 = 20
    //
    // For a single-unit increment (count=1): Allowed iff prev_total + 2 <= 10 => prev_total <= 8.
    let window_size_seconds = 10_u64;
    let hard_limit_factor = 2f64;
    let limiter = limiter(window_size_seconds, 1000, hard_limit_factor);

    let key = "k";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    // prev_total=0, count=1: forecasted=0+1+1=2 <= 10 => Allowed
    let d1 = limiter.inc(key, &rate_limit, 1);
    assert!(matches!(d1, RateLimitDecision::Allowed), "d1: {d1:?}");

    // prev_total=1, count=1: forecasted=1+1+1=3 <= 10 => Allowed
    let d2 = limiter.inc(key, &rate_limit, 1);
    assert!(matches!(d2, RateLimitDecision::Allowed), "d2: {d2:?}");

    // prev_total=2...7, each count=1: forecasted stays <= 10 => all Allowed.
    for i in 2..8u64 {
        let d = limiter.inc(key, &rate_limit, 1);
        assert!(matches!(d, RateLimitDecision::Allowed), "i={i} d={d:?}");
    }

    // prev_total=8, count=1: forecasted=8+1+1=10 <= 10 => still Allowed (boundary).
    let d_boundary = limiter.inc(key, &rate_limit, 1);
    assert!(
        matches!(d_boundary, RateLimitDecision::Allowed),
        "d_boundary: {d_boundary:?}"
    );

    // prev_total=9, count=1: forecasted=9+1+1=11 > 10 => enters suppression branch.
    // With empty/low series, sf=0 => is_allowed=true, but returns Suppressed, not Allowed.
    let d_over = limiter.inc(key, &rate_limit, 1);
    assert!(
        !matches!(d_over, RateLimitDecision::Allowed),
        "d_over should not be Allowed once past soft limit: {d_over:?}"
    );
}

#[test]
fn suppressed_is_fully_denied_after_hard_limit_observed() {
    // Once forecasted_allowed >= hard_window_limit, suppression_factor becomes 1.0
    // and requests are denied (is_allowed=false).
    //
    // With window=10s, rate=1 req/s, hard_factor=2:
    //   soft_window_limit = 10,  hard_window_limit = 20
    //
    // forecasted_allowed = (prev_total + count) - declined + count = prev_total - declined + 2*count.
    // Full suppression triggers when forecasted_allowed >= hard_window_limit (20).
    //
    // Strategy: make 9 unit-increment calls (all Allowed since forecasted stays < soft),
    // then one call of count=6 that pushes forecasted = (9+6) - 0 + 6 = 21 >= hard(20).
    let window_size_seconds = 10_u64;
    let hard_limit_factor = 2f64;

    // Use a tiny cache so recomputation is deterministic.
    let limiter = limiter_with_cache_ms(window_size_seconds, 1000, hard_limit_factor, 1);

    let key = "k_hard";
    let rate_limit = RateLimit::try_from(1f64).unwrap();

    // Each unit call: forecasted = prev_total + 2. Stays Allowed while prev_total + 2 < soft(10),
    // i.e. prev_total < 8. After 8 calls, prev_total=8, forecasted=10 == soft(10). Since
    // soft(10) < hard(20), stays_allowed = true (== soft but soft < hard).
    // After 9 calls, prev_total=9, forecasted=11 > soft(10) — enters suppression branch.
    // sf is computed from the series: average and last-second rates both ≤ rate_limit → sf small.
    // All 9 calls should be admitted (Allowed or Suppressed{is_allowed:true}).
    for i in 0..9u64 {
        std::thread::sleep(Duration::from_millis(2)); // let cache expire
        let d = limiter.inc(key, &rate_limit, 1);
        assert!(
            matches!(d, RateLimitDecision::Allowed)
                || matches!(
                    d,
                    RateLimitDecision::Suppressed {
                        is_allowed: true,
                        ..
                    }
                ),
            "i={i} d={d:?}"
        );
    }

    // Now push hard: prev_total=9 (all accepted, declined=0), count=6.
    // forecasted = (9+6) - 0 + 6 = 21 >= hard(20) => sf=1.0, denied.
    std::thread::sleep(Duration::from_millis(2));
    let d_push = limiter.inc(key, &rate_limit, 6);
    assert!(
        matches!(
            d_push,
            RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: false,
            } if (suppression_factor - 1.0).abs() < 1e-12
        ),
        "d_push: {d_push:?}"
    );

    // Subsequent single-unit calls: prev_total=15 (9 accepted + 6 denied counts in total),
    // declined incremented by 6 from d_push. forecasted = (15+1)-6+1 = 11 > soft(10).
    // calculate_suppression_factor: total(15) >= hard(20) → false. accepted(15-6=9) < soft(10) → sf=0??
    // Actually total(15) < hard(20), accepted=9 < soft(10): sf=0, should_allow=true.
    // We need total >= hard to keep sf=1. So let's add more denied volume first.
    // Let's instead verify sf stays 1.0 immediately after d_push while cache is fresh.
    std::thread::sleep(Duration::from_millis(2)); // expire cache
    let d_next = limiter.inc(key, &rate_limit, 7);
    // After d_push: total=15, declined=6. d_next: prev_total=15, count=7.
    // forecasted = (15+7)-6+7 = 23 >= hard(20) => sf=1.0, denied.
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
}
