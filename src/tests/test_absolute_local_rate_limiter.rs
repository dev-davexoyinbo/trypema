use std::{sync::Arc, thread, time::Duration};

use crate::common::{
    HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision, WindowSizeSeconds,
};
use crate::{AbsoluteLocalRateLimiter, LocalRateLimiterOptions};

fn limiter(window_size_seconds: u64, rate_group_size_ms: u64) -> AbsoluteLocalRateLimiter {
    AbsoluteLocalRateLimiter::new(LocalRateLimiterOptions {
        window_size_seconds: WindowSizeSeconds::try_from(window_size_seconds).unwrap(),
        rate_group_size_ms: RateGroupSizeMs::try_from(rate_group_size_ms).unwrap(),
        hard_limit_factor: HardLimitFactor::default(),
    })
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

    // is_allowed eviction uses `elapsed().as_secs()`; for a 1s window that effectively
    // means waiting until elapsed is >= 2s.
    thread::sleep(Duration::from_millis(2200));

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

    // Eviction uses `elapsed().as_secs()`; for a 2s window that effectively
    // means waiting until elapsed is >= 3s.
    thread::sleep(Duration::from_millis(3200));

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
    // - the oldest bucket is evicted (elapsed >= 3s)
    // - the newer bucket is still in-window (elapsed < 3s, i.e. as_secs() == 2)
    thread::sleep(Duration::from_millis(2750));

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

    // Keep the first bucket below the limit so the second increment is applied.
    limiter.inc(key, &rate_limit, 1);

    // Ensure the second increment is beyond the grouping threshold.
    // Use a large enough gap that there is a window where the first bucket is evictable
    // (as_secs() > window) while the second is still in-window.
    thread::sleep(Duration::from_millis(1100));

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

        let first_elapsed_s = series
            .series
            .front()
            .expect("expected first bucket")
            .timestamp
            .elapsed()
            .as_secs();
        let second_elapsed_s = series
            .series
            .back()
            .expect("expected second bucket")
            .timestamp
            .elapsed()
            .as_secs();
        drop(series);

        if first_elapsed_s > 1 && second_elapsed_s <= 1 {
            break;
        }

        if start.elapsed() > Duration::from_secs(5) {
            panic!(
                "timed out waiting for bucket ages; first_elapsed_s={first_elapsed_s} second_elapsed_s={second_elapsed_s}"
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
