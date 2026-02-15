use std::time::{Duration, Instant};

use crate::{
    HardLimitFactor, LocalRateLimiterOptions, RateGroupSizeMs, RateLimit, RateLimitDecision,
    SuppressedLocalRateLimiter, WindowSizeSeconds,
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
    })
}

fn total_for_key(limiter: &crate::AbsoluteLocalRateLimiter, key: &str) -> u64 {
    limiter
        .series()
        .get(key)
        .map(|s| s.total.load(std::sync::atomic::Ordering::Relaxed))
        .unwrap_or(0)
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
fn suppression_factor_cache_is_recomputed_after_group_window() {
    let limiter = limiter(1, 50, 10f64);
    let key = "k";
    let rate_limit = RateLimit::try_from(5f64).unwrap();

    let old_ts = Instant::now() - Duration::from_millis(51);
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
