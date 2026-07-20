use std::{env, time::Duration};

use super::runtime;

use crate::common::SuppressionFactorCachePeriod;
use crate::hybrid::SyncInterval;
use crate::{
    BucketSize, HardLimitFactor, HistoryPreservation, LocalRateLimiterOptions, RateLimit,
    RateLimitComparator, RateLimitDecision, RateLimiter, RateLimiterOptions, RedisKey,
    RedisRateLimiterOptions, SuppressedRateLimitSnapshot, WindowSize,
};

fn redis_url() -> String {
    env::var("REDIS_URL").unwrap_or_else(|_| {
        panic!(
            "REDIS_URL env var must be set for Redis-backed tests (e.g. REDIS_URL=redis://127.0.0.1:16379/)"
        )
    })
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
    window_size: u64,
    bucket_size: u64,
    hard_limit_factor: f64,
) -> std::sync::Arc<RateLimiter> {
    build_limiter_with_cache_ms(
        url,
        window_size,
        bucket_size,
        hard_limit_factor,
        *SuppressionFactorCachePeriod::default(),
    )
    .await
}

async fn build_limiter_with_cache_ms(
    url: &str,
    window_size: u64,
    bucket_size: u64,
    hard_limit_factor: f64,
    suppression_factor_cache_period: u64,
) -> std::sync::Arc<RateLimiter> {
    let client = redis::Client::open(url).unwrap();
    let cm = client.get_connection_manager().await.unwrap();
    let prefix = unique_prefix();

    let options = RateLimiterOptions {
        local: LocalRateLimiterOptions {
            window_size: WindowSize::seconds(window_size).unwrap(),
            bucket_size: BucketSize::milliseconds(bucket_size).unwrap(),
            hard_limit_factor: HardLimitFactor::try_from(hard_limit_factor).unwrap(),
            suppression_factor_cache_period: SuppressionFactorCachePeriod::milliseconds(
                suppression_factor_cache_period,
            )
            .unwrap(),
        },
        redis: RedisRateLimiterOptions {
            connection_manager: cm,
            prefix: Some(prefix.clone()),
            window_size: WindowSize::seconds(window_size).unwrap(),
            bucket_size: BucketSize::milliseconds(bucket_size).unwrap(),
            hard_limit_factor: HardLimitFactor::try_from(hard_limit_factor).unwrap(),
            suppression_factor_cache_period: SuppressionFactorCachePeriod::milliseconds(
                suppression_factor_cache_period,
            )
            .unwrap(),
            sync_interval: SyncInterval::default(),
        },
    };

    std::sync::Arc::new(RateLimiter::new(options))
}

#[test]
fn get_suppression_factor_fresh_key_returns_zero() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_cache_ms(&url, 10, 100, 2f64, 500).await;
        let k = key("k");

        // With no usage, suppression factor resolves to 0.
        let sf = rl
            .redis()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();
        assert!((sf - 0.0).abs() < 1e-12, "sf: {sf}");
    })
}

#[test]
fn get_suppression_factor_computed_uses_last_second_peak_rate_at_threshold_boundary() {
    let url = redis_url();

    runtime::block_on(async {
        // window_size=10s, hard_limit_factor=2 => hard_window_limit=20, soft_window_limit=10.
        // Under the new semantics, accepted == soft with soft < hard returns sf=0 (no suppression
        // yet). We must drive accepted *past* soft (to 11) so the ramp zone is entered and
        // perceived_rate uses last-second peak instead of average.
        let cache_ms = 50_u64;
        let rl = build_limiter_with_cache_ms(&url, 10, 100, 2f64, cache_ms).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(1f64).unwrap();

        // Seed accepted usage through the public conditional-set API. This creates one current
        // bucket with no declines and no cached factor, so the read below must calculate the
        // factor from exactly 11 accepted requests.
        assert_eq!(
            rl.redis()
                .suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Nil, 11)
                .await
                .unwrap(),
            (11, 0)
        );

        let sf = rl
            .redis()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();

        // accepted(11) > soft(10) and total(11) < hard(20): enter ramp zone.
        // rate_in_last_1s = 11, average_rate = 11/10 = 1.1.
        // perceived_rate = max(1.1, 11) = 11.
        // rate_limit = hard_window_limit / window_size / hard_limit_factor = 20 / 10 / 2 = 1.
        // sf = 1 - (1 / 11)
        let expected = 1.0_f64 - (1.0_f64 / 11.0_f64);
        assert!(
            (sf - expected).abs() < 1e-12,
            "sf: {sf}, expected: {expected}"
        );
    });
}

#[test]
fn get_suppression_factor_evicts_out_of_window_usage_and_resets_admission() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size = 1_u64;
        let cache_ms = 50_u64;
        let rl = build_limiter_with_cache_ms(&url, window_size, 1000, 2f64, cache_ms).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(1f64).unwrap();

        // hard_window_limit = window_size * rate_limit * hard_limit_factor = 1 * 1 * 2 = 2.
        // We record 2 calls in the window, then wait for the full window to pass. If eviction
        // does not occur, the next increment would see total_count >= hard_window_limit and go to full
        // suppression (sf=1.0). If eviction occurs, the next increment is Allowed.
        let d1 = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 2)
            .await
            .unwrap();

        assert!(matches!(d1, RateLimitDecision::Allowed), "d1: {:?}", d1);

        std::thread::sleep(Duration::from_millis(window_size * 1000 + 50));
        std::thread::sleep(Duration::from_millis(cache_ms + 25));

        let sf = rl
            .redis()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();
        assert!((sf - 0.0).abs() < 1e-12, "sf: {sf}");

        let d2 = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(matches!(d2, RateLimitDecision::Allowed), "d2: {:?}", d2);
    });
}

#[test]
fn verify_suppression_factor_calculation_spread_redis() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 10, 100, 10f64).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(1f64).unwrap();

        // Seed one public-API bucket containing 20 accepted requests. Waiting until that bucket is
        // outside the last-second peak interval leaves an average accepted rate of 2 requests/s.
        assert_eq!(
            rl.redis()
                .suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Nil, 20)
                .await
                .unwrap(),
            (20, 0)
        );

        // wait for 1.5 seconds
        runtime::async_sleep(Duration::from_millis(1200)).await;

        let expected_suppression_factor = 1f64 - (1f64 / 2f64);

        let decision = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();

        assert!(
            matches!(
                decision,
                RateLimitDecision::Suppressed {
                    suppression_factor,
                    ..
                } if (suppression_factor - expected_suppression_factor).abs() < 1e-12
            ),
            "decision: {:?}",
            decision
        );
    });
}

#[test]
fn verify_suppression_factor_calculation_last_second_redis() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 10, 100, 10f64).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(1f64).unwrap();

        let first = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 10)
            .await
            .unwrap();
        assert!(
            matches!(first, RateLimitDecision::Allowed),
            "first: {first:?}"
        );

        // wait for 1s to pass
        runtime::async_sleep(Duration::from_millis(1001)).await;

        let second = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 20)
            .await
            .unwrap();
        assert!(
            matches!(
                second,
                RateLimitDecision::Suppressed {
                    suppression_factor,
                    is_allowed: true,
                } if suppression_factor.abs() < 1e-12
            ),
            "second: {second:?}"
        );

        // Allow time for the suppression_factor to expire
        runtime::async_sleep(Duration::from_millis(101)).await;

        let expected_suppression_factor = 1f64 - (1f64 / 20f64);

        let decision = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();

        assert!(
            matches!(
                decision,
                RateLimitDecision::Suppressed {
                    suppression_factor,
                    ..
                } if (suppression_factor - expected_suppression_factor).abs() < 1e-12
            ),
            "decision: {:?}, expected sf: {expected_suppression_factor}",
            decision
        );
    });
}

#[test]
fn verify_hard_limit_rejects() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 10, 100, 10f64).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(1f64).unwrap();

        let reaches_hard = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 100)
            .await
            .unwrap();
        assert!(
            matches!(reaches_hard, RateLimitDecision::Allowed),
            "reaches_hard: {reaches_hard:?}"
        );

        let decision = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();

        assert!(
            matches!(
                decision,
                RateLimitDecision::Suppressed {
                    suppression_factor,
                    is_allowed: false,
                } if suppression_factor == 1.0f64
            ),
            "decision: {:?}",
            decision
        );

        assert_eq!(
            rl.redis().suppressed().get(&k).await.unwrap(),
            SuppressedRateLimitSnapshot {
                total: 101,
                total_declined: 1,
                suppression_factor: 1.0,
            }
        );
    });
}

#[test]
fn public_suppressed_decisions_never_return_absolute_rejection_metadata() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 1, 1_000, 1.0).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(5f64).unwrap();

        for observed_after in 1..=10_u64 {
            let decision = rl
                .redis()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();

            match decision {
                RateLimitDecision::Allowed if observed_after <= 5 => {}
                RateLimitDecision::Suppressed {
                    suppression_factor: 1.0,
                    is_allowed: false,
                } if observed_after > 5 => {}
                RateLimitDecision::Rejected { .. } => {
                    panic!("suppressed strategy returned absolute rejection metadata: {decision:?}")
                }
                _ => {
                    panic!("unexpected decision after observation {observed_after}: {decision:?}")
                }
            }
        }

        assert_eq!(
            rl.redis().suppressed().get(&k).await.unwrap(),
            SuppressedRateLimitSnapshot {
                total: 10,
                total_declined: 5,
                suppression_factor: 1.0,
            }
        );
    });
}

#[test]
fn suppressed_is_deterministically_allowed_until_base_capacity_boundary_redis() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size = 10_u64;
        let hard_limit_factor = 2f64;
        let rl = build_limiter(&url, window_size, 1000, hard_limit_factor).await;

        let k = key("k_base");
        let rate_limit = RateLimit::per_second(1f64).unwrap();

        // Base capacity = 10s * 1 req/s = 10.
        let base_capacity = window_size;

        // The projected accepted total includes the current increment exactly once.
        let d1 = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, base_capacity - 1)
            .await
            .unwrap();
        assert!(matches!(d1, RateLimitDecision::Allowed), "d1: {d1:?}");

        // Landing exactly on the soft boundary is still deterministically allowed.
        let d2 = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(matches!(d2, RateLimitDecision::Allowed), "d2: {d2:?}");
    });
}

#[test]
fn suppressed_fractional_hard_limit_preserves_local_soft_and_hard_boundaries_redis() {
    let url = redis_url();

    runtime::block_on(async {
        // raw soft window = 6 * 0.5 = 3; raw hard window = 3 * 1.5 = 4.5.
        // Like suppressed-local, operational capacities are soft=3 and hard=4.
        let rl = build_limiter(&url, 6, 1000, 1.5).await;
        let k = key("k_fractional_boundaries");
        let rate_limit = RateLimit::per_second(0.5).unwrap();

        for accepted_after in 1..=4_u64 {
            let decision = rl
                .redis()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            assert!(
                matches!(decision, RateLimitDecision::Allowed),
                "accepted_after={accepted_after}, decision={decision:?}"
            );
        }

        let over_hard = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(
            matches!(
                over_hard,
                RateLimitDecision::Suppressed {
                    suppression_factor,
                    is_allowed: false,
                } if (suppression_factor - 1.0).abs() < 1e-12
            ),
            "over_hard={over_hard:?}"
        );
        assert_eq!(
            rl.redis().suppressed().get(&k).await.unwrap(),
            SuppressedRateLimitSnapshot {
                total: 5,
                total_declined: 1,
                suppression_factor: 1.0,
            }
        );
    });
}

#[test]
fn suppressed_is_fully_denied_after_hard_limit_observed_redis() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size = 10_u64;
        let hard_limit_factor = 2f64;

        let cache_ms = 60_000_u64;
        let rl =
            build_limiter_with_cache_ms(&url, window_size, 1000, hard_limit_factor, cache_ms).await;

        let k = key("k_hard");
        let rate_limit = RateLimit::per_second(1f64).unwrap();

        let hard_capacity = window_size * 2;

        // The increment that lands exactly on the hard limit is admitted and caches factor 1.
        let d1 = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, hard_capacity)
            .await
            .unwrap();
        assert!(matches!(d1, RateLimitDecision::Allowed), "d1: {d1:?}");

        let factor = rl
            .redis()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();
        assert!((factor - 1.0).abs() < 1e-12, "factor: {factor}");

        // The cached exact-hard factor makes every immediate subsequent call fully suppressed.
        for i in 0..5u64 {
            let d = rl
                .redis()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();

            assert!(
                matches!(
                    d,
                    RateLimitDecision::Suppressed {
                        suppression_factor,
                        is_allowed: false,
                    } if (suppression_factor - 1.0).abs() < 1e-12
                ),
                "i={i} d={d:?}"
            );
        }
    });
}

/// After the window elapses, previously committed usage must be evicted from Redis so that
/// a fresh burst is admitted at the full rate again.
///
/// This test catches the bug where `read_state` passes `window_size_ms` to the Lua script
/// instead of `window_size`. With the wrong value the eviction threshold is pushed
/// ~16 minutes into the past, old buckets are never removed, `total_count` accumulates
/// indefinitely, and suppression stays at 1.0 even after the window has expired.
#[test]
fn suppressed_redis_window_eviction_allows_fresh_burst_after_expiry() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size = 1_u64;
        // hard_limit_factor=1.0 so hard_window_limit == soft_window_limit == window capacity.
        let hard_limit_factor = 1.0_f64;
        let cache_ms = 5_u64;

        let rate_limit = RateLimit::per_second(5f64).unwrap();
        // hard_window_limit = 1s * 5 req/s * 1.0 = 5
        let hard_window_limit = (window_size as f64 * *rate_limit * hard_limit_factor) as u64;

        let rl = build_limiter_with_cache_ms(&url, window_size, 1_000, hard_limit_factor, cache_ms)
            .await;

        let k = key("k_evict");

        // Drive observed count to the hard limit in one call.
        let d1 = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, hard_window_limit)
            .await
            .unwrap();
        assert!(matches!(d1, RateLimitDecision::Allowed), "d1: {d1:?}");

        // Let the suppression_factor cache expire so the next read recomputes from Redis.
        runtime::async_sleep(Duration::from_millis(cache_ms + 50)).await;

        // Confirm full suppression before the window expires.
        let sf_before = rl
            .redis()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();
        assert!(
            (sf_before - 1.0).abs() < 1e-12,
            "expected sf=1.0 before window expiry, got {sf_before}"
        );

        // Wait for the full window to expire, then let the suppression cache expire again.
        std::thread::sleep(Duration::from_millis(window_size * 1_000 + 50));
        runtime::async_sleep(Duration::from_millis(cache_ms + 50)).await;

        // After the window has expired, eviction must have cleared the old buckets.
        let sf_after = rl
            .redis()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();
        assert!(
            (sf_after - 0.0).abs() < 1e-12,
            "expected sf=0.0 after window expiry but got {sf_after} — \
             old buckets were not evicted (window_size_ms passed instead of window_size?)"
        );

        // A new request must be admitted.
        let d2 = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(
            matches!(d2, RateLimitDecision::Allowed),
            "expected Allowed after window expiry, got {d2:?}"
        );
    });
}

/// Run for three consecutive windows at well above the rate limit and assert that the total
/// admitted volume across all windows is close to `rate * num_windows`.
///
/// If window eviction is broken, `total_count` accumulates and the hard limit is hit after
/// the first window — every subsequent request is suppressed, giving total_allowed ≈
/// hard_window_limit instead of ≈ rate * num_windows.
#[test]
fn suppressed_redis_throughput_over_multiple_windows_stays_at_rate_limit() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size = 1_u64;
        let hard_limit_factor = 1.5_f64;
        let cache_ms = 5_u64;
        let num_windows = 3_u64;

        let rate_limit = RateLimit::per_second(10f64).unwrap();
        // soft_window_limit = 10, hard_window_limit = 15
        let soft_window_limit = (window_size as f64 * *rate_limit) as u64;
        let hard_window_limit = (soft_window_limit as f64 * hard_limit_factor) as u64;

        let rl =
            build_limiter_with_cache_ms(&url, window_size, 100, hard_limit_factor, cache_ms).await;

        let k = key("k_multi");
        let mut total_allowed: u64 = 0;

        for _window in 0..num_windows {
            // Hammer at 10× the rate limit to ensure we hit the ceiling each window.
            let burst = soft_window_limit * 10;
            for _ in 0..burst {
                let d = rl
                    .redis()
                    .suppressed()
                    .inc(&k, &rate_limit, 1)
                    .await
                    .unwrap();
                match d {
                    RateLimitDecision::Allowed => total_allowed += 1,
                    RateLimitDecision::Suppressed { is_allowed, .. } => {
                        if is_allowed {
                            total_allowed += 1;
                        }
                    }
                    RateLimitDecision::Rejected { .. } => {
                        panic!("suppressed strategy must never return Rejected")
                    }
                }
            }

            // Wait for the window to expire and the suppression cache to clear.
            std::thread::sleep(Duration::from_millis(window_size * 1_000 + 50));
            runtime::async_sleep(Duration::from_millis(cache_ms + 50)).await;
        }

        // Over num_windows windows the total must be at least soft_window_limit * num_windows.
        // If eviction is broken, total_allowed ≈ hard_window_limit (15) instead of ≈ 30+.
        let expected_min = soft_window_limit * num_windows;
        assert!(
            total_allowed >= expected_min,
            "total_allowed={total_allowed} but expected >= {expected_min} over {num_windows} windows \
             (hard_window_limit={hard_window_limit}) — window eviction is likely broken"
        );
    });
}

#[test]
fn get_returns_empty_snapshot_for_untouched_key() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 6, 1000, 1.0).await;

        let snapshot = rl.redis().suppressed().get(&key("k")).await.unwrap();
        assert_eq!(snapshot, SuppressedRateLimitSnapshot::default());
    });
}

#[test]
fn get_returns_observed_snapshot() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 6, 1000, 1.0).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(100f64).unwrap();

        for _ in 0..3 {
            let d = rl
                .redis()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");
        }

        let snapshot = rl.redis().suppressed().get(&k).await.unwrap();
        assert_eq!(
            snapshot,
            SuppressedRateLimitSnapshot {
                total: 3,
                total_declined: 0,
                suppression_factor: 0.0,
            }
        );
    });
}

#[test]
fn inc_uses_the_first_rate_limit_for_existing_keys() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 1, 1000, 1.0).await;
        let low_then_high = key("low-then-high");
        let low_rate = RateLimit::per_second(2f64).unwrap();
        let high_rate = RateLimit::per_second(10f64).unwrap();

        let decision = rl
            .redis()
            .suppressed()
            .inc(&low_then_high, &low_rate, 2)
            .await
            .unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "the first increment should fill the original hard capacity: {decision:?}"
        );
        let decision = rl
            .redis()
            .suppressed()
            .inc(&low_then_high, &high_rate, 1)
            .await
            .unwrap();
        assert!(
            matches!(
                decision,
                RateLimitDecision::Suppressed {
                    suppression_factor: 1.0,
                    is_allowed: false,
                }
            ),
            "a later larger rate must not increase the sticky hard capacity: {decision:?}"
        );
        let snapshot = rl.redis().suppressed().get(&low_then_high).await.unwrap();
        assert_eq!((snapshot.total, snapshot.total_declined), (3, 1));

        let high_then_low = key("high-then-low");
        let decision = rl
            .redis()
            .suppressed()
            .inc(&high_then_low, &high_rate, 8)
            .await
            .unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "the first increment should establish the larger hard capacity: {decision:?}"
        );
        let decision = rl
            .redis()
            .suppressed()
            .inc(&high_then_low, &low_rate, 2)
            .await
            .unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "a later smaller rate must not reduce the sticky hard capacity: {decision:?}"
        );
        let snapshot = rl.redis().suppressed().get(&high_then_low).await.unwrap();
        assert_eq!((snapshot.total, snapshot.total_declined), (10, 0));
    });
}

#[test]
fn set_if_lt_primes_empty_key_and_reprime_is_noop() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 6, 1000, 1.0).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(100f64).unwrap();

        let (new_total, old_total) = rl
            .redis()
            .suppressed()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(100), 100)
            .await
            .unwrap();
        assert_eq!((new_total, old_total), (100, 0));

        let (new_total, old_total) = rl
            .redis()
            .suppressed()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(100), 100)
            .await
            .unwrap();
        assert_eq!((new_total, old_total), (100, 100));

        assert_eq!(rl.redis().suppressed().get(&k).await.unwrap().total, 100);
    });
}

#[test]
fn set_if_lt_with_lower_target_is_noop() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 6, 1000, 1.0).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(100f64).unwrap();

        assert_eq!(
            rl.redis()
                .suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Lt(100), 100)
                .await
                .unwrap(),
            (100, 0)
        );

        let (new_total, old_total) = rl
            .redis()
            .suppressed()
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
        let rl = build_limiter(&url, 6, 1000, 1.0).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(100f64).unwrap();

        assert_eq!(
            rl.redis()
                .suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Nil, 100)
                .await
                .unwrap(),
            (100, 0)
        );

        let (new_total, old_total) = rl
            .redis()
            .suppressed()
            .set_if(&k, &rate_limit, RateLimitComparator::Nil, 30)
            .await
            .unwrap();
        assert_eq!((new_total, old_total), (30, 100));
        assert_eq!(rl.redis().suppressed().get(&k).await.unwrap().total, 30);
    });
}

#[test]
fn set_if_eq_zero_sets_only_when_window_is_empty() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 6, 1000, 1.0).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(100f64).unwrap();

        let (new_total, old_total) = rl
            .redis()
            .suppressed()
            .set_if(&k, &rate_limit, RateLimitComparator::Eq(0), 25)
            .await
            .unwrap();
        assert_eq!((new_total, old_total), (25, 0));

        let (new_total, old_total) = rl
            .redis()
            .suppressed()
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
        let rl = build_limiter(&url, 6, 1000, 1.0).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(100f64).unwrap();

        assert_eq!(
            rl.redis()
                .suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Nil, 10)
                .await
                .unwrap(),
            (10, 0)
        );
        assert_eq!(
            rl.redis()
                .suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Gt(5), 3)
                .await
                .unwrap(),
            (3, 10)
        );
        assert_eq!(
            rl.redis()
                .suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Ne(3), 7)
                .await
                .unwrap(),
            (3, 3)
        );
        assert_eq!(
            rl.redis()
                .suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Ne(5), 7)
                .await
                .unwrap(),
            (7, 3)
        );
        assert_eq!(rl.redis().suppressed().get(&k).await.unwrap().total, 7);
    });
}

#[test]
fn set_if_preserve_history_creates_missing_positive_keys_in_both_directions() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 6, 1000, 1.0).await;
        let rate_limit = RateLimit::per_second(10f64).unwrap();

        for (name, preservation) in [
            ("newest", HistoryPreservation::PreserveNewest),
            ("oldest", HistoryPreservation::PreserveOldest),
        ] {
            let k = key(name);
            assert_eq!(
                rl.redis()
                    .suppressed()
                    .set_if_preserve_history(
                        &k,
                        &rate_limit,
                        RateLimitComparator::Eq(0),
                        5,
                        preservation,
                    )
                    .await
                    .unwrap(),
                (5, 0)
            );
            let snapshot = rl.redis().suppressed().get(&k).await.unwrap();
            assert_eq!((snapshot.total, snapshot.total_declined), (5, 0));
        }
    });
}

#[test]
fn set_if_preserve_history_redefines_limit_when_total_is_unchanged() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 1, 1000, 1.0).await;
        let k = key("k");
        let initial_rate = RateLimit::per_second(10f64).unwrap();
        let replacement_rate = RateLimit::per_second(6f64).unwrap();

        let decision = rl
            .redis()
            .suppressed()
            .inc(&k, &initial_rate, 5)
            .await
            .unwrap();
        assert!(matches!(decision, RateLimitDecision::Allowed));
        assert_eq!(
            rl.redis()
                .suppressed()
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

        let decision = rl
            .redis()
            .suppressed()
            .inc(&k, &initial_rate, 1)
            .await
            .unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "one unit should remain under the redefined hard capacity: {decision:?}"
        );
        let decision = rl
            .redis()
            .suppressed()
            .inc(&k, &initial_rate, 1)
            .await
            .unwrap();
        assert!(
            matches!(
                decision,
                RateLimitDecision::Suppressed {
                    suppression_factor: 1.0,
                    is_allowed: false,
                }
            ),
            "the unchanged-target update must redefine the hard capacity: {decision:?}"
        );
    });
}

#[test]
fn set_if_prime_below_soft_limit_allows_next_inc() {
    let url = redis_url();

    runtime::block_on(async {
        // window 6 * rate 5 * factor 1.0 → hard = soft = 30.
        let rl = build_limiter(&url, 6, 1000, 1.0).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(5f64).unwrap();

        let (new_total, _) = rl
            .redis()
            .suppressed()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(27), 27)
            .await
            .unwrap();
        assert_eq!(new_total, 27);

        let d = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");
    });
}

#[test]
fn set_if_prime_at_hard_limit_declines_next_inc() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 6, 1000, 1.0).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(5f64).unwrap();

        let (new_total, _) = rl
            .redis()
            .suppressed()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(30), 30)
            .await
            .unwrap();
        assert_eq!(new_total, 30);

        let d = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(
            matches!(
                d,
                RateLimitDecision::Suppressed {
                    is_allowed: false,
                    suppression_factor,
                } if (suppression_factor - 1.0).abs() < 1e-12
            ),
            "d: {d:?}"
        );
    });
}

#[test]
fn set_if_zero_count_resets_declines_and_suppression_state() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 6, 1000, 1.0).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(5f64).unwrap();

        // Saturate the window and record some declines.
        assert_eq!(
            rl.redis()
                .suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Nil, 30)
                .await
                .unwrap(),
            (30, 0)
        );
        for _ in 0..3 {
            let d = rl
                .redis()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            assert!(
                matches!(
                    d,
                    RateLimitDecision::Suppressed {
                        is_allowed: false,
                        ..
                    }
                ),
                "d: {d:?}"
            );
        }

        // Clear the window: declines and the cached factor must be reset too,
        // so admission resumes immediately.
        let (new_total, _) = rl
            .redis()
            .suppressed()
            .set_if(&k, &rate_limit, RateLimitComparator::Nil, 0)
            .await
            .unwrap();
        assert_eq!(new_total, 0);

        let d = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");
        assert_eq!(rl.redis().suppressed().get(&k).await.unwrap().total, 1);
    });
}
