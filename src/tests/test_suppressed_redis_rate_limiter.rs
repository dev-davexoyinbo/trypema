use std::{
    env,
    sync::{Arc, Barrier, mpsc},
    thread,
    time::Duration,
};

use super::runtime;

use crate::common::SuppressionFactorCachePeriod;
use crate::{
    BucketSize, HardLimitFactor, HistoryPreservation, RateLimit, RateLimitComparator,
    RateLimitDecision, RateLimiterBuilder, SuppressedRateLimitSnapshot, WindowSize,
    redis::{RedisKey, RedisRateLimiterProvider},
};

fn redis_url() -> String {
    env::var("REDIS_URL").unwrap_or_else(|_| {
        panic!(
            "REDIS_URL env var must be set for Redis-backed tests (e.g. REDIS_URL=redis://127.0.0.1:16379/)"
        )
    })
}

#[test]
fn rate_and_key_lifecycle_preserves_snapshot_and_resets_state() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 6, 1_000, 2.0).await;
        let first = key("first");
        let second = key("second");
        let missing = key("missing");
        let initial_rate = RateLimit::per_second_or_panic(2.5);
        let lower_rate = RateLimit::per_second_or_panic(0.5);

        assert!(matches!(
            rl.suppressed().inc(&first, &initial_rate, 4).await.unwrap(),
            RateLimitDecision::Allowed
        ));
        let before = rl.suppressed().get(&first).await.unwrap();
        assert_eq!(
            rl.suppressed()
                .set_rate_limit(&missing, &lower_rate)
                .await
                .unwrap(),
            None
        );
        let zero = key("zero");
        assert!(matches!(
            rl.suppressed().inc(&zero, &initial_rate, 0).await.unwrap(),
            RateLimitDecision::Allowed
        ));
        assert_eq!(rl.suppressed().delete(&zero).await.unwrap(), Some(0));
        assert_eq!(
            rl.suppressed()
                .set_rate_limit(&first, &lower_rate)
                .await
                .unwrap(),
            Some(initial_rate)
        );
        let after = rl.suppressed().get(&first).await.unwrap();
        assert_eq!(after.total, before.total);
        assert_eq!(after.total_declined, before.total_declined);

        assert_eq!(rl.suppressed().delete(&first).await.unwrap(), Some(4));
        assert_eq!(rl.suppressed().delete(&first).await.unwrap(), None);
        assert_eq!(rl.suppressed().get(&first).await.unwrap().total, 0);

        assert!(matches!(
            rl.suppressed().inc(&first, &initial_rate, 1).await.unwrap(),
            RateLimitDecision::Allowed
        ));
        assert!(matches!(
            rl.suppressed()
                .inc(&second, &initial_rate, 1)
                .await
                .unwrap(),
            RateLimitDecision::Allowed
        ));

        rl.suppressed().clear().await.unwrap();
        assert_eq!(rl.suppressed().get(&first).await.unwrap().total, 0);
        assert_eq!(rl.suppressed().get(&second).await.unwrap().total, 0);
        rl.suppressed().clear().await.unwrap();

        let unbounded = key("unbounded");
        assert!(matches!(
            rl.suppressed()
                .inc(&unbounded, &RateLimit::max(), 1)
                .await
                .unwrap(),
            RateLimitDecision::Allowed
        ));
        assert_eq!(
            rl.suppressed()
                .set_rate_limit(&unbounded, &initial_rate)
                .await
                .unwrap(),
            Some(RateLimit::max())
        );
    });
}

#[test]
fn delete_returns_zero_for_expired_suppressed_redis_state() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 1, 1_000, 2.0).await;
        let key = key("expired");
        let rate_limit = RateLimit::per_second_or_panic(1.0);

        assert!(matches!(
            rl.suppressed().inc(&key, &rate_limit, 1).await.unwrap(),
            RateLimitDecision::Allowed
        ));
        runtime::async_sleep(Duration::from_millis(1_050)).await;

        assert_eq!(rl.suppressed().delete(&key).await.unwrap(), Some(0));
    });
}

#[test]
fn multi_instance_threaded_lifecycle_cutovers_preserve_exact_snapshot() {
    const INSTANCES: usize = 4;
    const INITIAL_INCREMENTS: u64 = 32;
    const AFTER_RAISE_INCREMENTS: u64 = 8;
    const RECREATED_INCREMENTS: u64 = 4;
    const CLEAR_INCREMENTS: u64 = 2;

    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let administrator = build_limiter_with_prefix(&url, 60, 1_000, 1.0, prefix.clone()).await;
        let high_rate = RateLimit::per_second_or_panic(1_000.0);
        let low_rate = RateLimit::per_second_or_panic(1.0);
        let lifecycle_key = key("threaded_lifecycle");
        let clear_even = key("threaded_clear_even");
        let clear_odd = key("threaded_clear_odd");
        let barrier = Arc::new(Barrier::new(INSTANCES + 1));
        let (deleted_tx, deleted_rx) = mpsc::channel();
        let mut workers = Vec::with_capacity(INSTANCES);

        for worker_index in 0..INSTANCES {
            let url = url.clone();
            let prefix = prefix.clone();
            let lifecycle_key = lifecycle_key.clone();
            let clear_even = clear_even.clone();
            let clear_odd = clear_odd.clone();
            let barrier = Arc::clone(&barrier);
            let deleted_tx = deleted_tx.clone();

            workers.push(thread::spawn(move || {
                runtime::block_on(async move {
                    let limiter = build_limiter_with_prefix(&url, 60, 1_000, 1.0, prefix).await;

                    barrier.wait();
                    for _ in 0..INITIAL_INCREMENTS {
                        let decision = limiter
                            .suppressed()
                            .inc(&lifecycle_key, &high_rate, 1)
                            .await
                            .unwrap();
                        assert!(matches!(decision, RateLimitDecision::Allowed));
                    }
                    barrier.wait();

                    barrier.wait();
                    let decision = limiter
                        .suppressed()
                        .inc(&lifecycle_key, &high_rate, 1)
                        .await
                        .unwrap();
                    assert!(matches!(
                        decision,
                        RateLimitDecision::Suppressed {
                            is_allowed: false,
                            ..
                        }
                    ));
                    barrier.wait();

                    barrier.wait();
                    for _ in 0..AFTER_RAISE_INCREMENTS {
                        let decision = limiter
                            .suppressed()
                            .inc(&lifecycle_key, &high_rate, 1)
                            .await
                            .unwrap();
                        assert!(matches!(decision, RateLimitDecision::Allowed));
                    }
                    barrier.wait();

                    barrier.wait();
                    for _ in 0..RECREATED_INCREMENTS {
                        let decision = limiter
                            .suppressed()
                            .inc(&lifecycle_key, &high_rate, 1)
                            .await
                            .unwrap();
                        assert!(matches!(decision, RateLimitDecision::Allowed));
                    }
                    deleted_tx
                        .send(
                            limiter
                                .suppressed()
                                .delete(&lifecycle_key)
                                .await
                                .unwrap()
                                .unwrap_or(0),
                        )
                        .unwrap();
                    barrier.wait();

                    barrier.wait();
                    let clear_key = if worker_index % 2 == 0 {
                        &clear_even
                    } else {
                        &clear_odd
                    };
                    for _ in 0..CLEAR_INCREMENTS {
                        let decision = limiter
                            .suppressed()
                            .inc(clear_key, &high_rate, 1)
                            .await
                            .unwrap();
                        assert!(matches!(decision, RateLimitDecision::Allowed));
                    }
                    barrier.wait();

                    barrier.wait();
                    for _ in 0..CLEAR_INCREMENTS {
                        let decision = limiter
                            .suppressed()
                            .inc(clear_key, &high_rate, 1)
                            .await
                            .unwrap();
                        assert!(matches!(decision, RateLimitDecision::Allowed));
                    }
                    barrier.wait();
                });
            }));
        }
        drop(deleted_tx);

        barrier.wait();
        barrier.wait();
        let initial_total = INSTANCES as u64 * INITIAL_INCREMENTS;
        assert_eq!(
            administrator
                .suppressed()
                .get(&lifecycle_key)
                .await
                .unwrap(),
            SuppressedRateLimitSnapshot {
                total: initial_total,
                total_declined: 0,
                suppression_factor: 0.0,
            }
        );
        assert_eq!(
            administrator
                .suppressed()
                .set_rate_limit(&lifecycle_key, &low_rate)
                .await
                .unwrap(),
            Some(high_rate)
        );

        barrier.wait();
        barrier.wait();
        let declined_total = INSTANCES as u64;
        let snapshot = administrator
            .suppressed()
            .get(&lifecycle_key)
            .await
            .unwrap();
        assert_eq!(snapshot.total, initial_total + declined_total);
        assert_eq!(snapshot.total_declined, declined_total);
        assert_eq!(snapshot.total - snapshot.total_declined, initial_total);
        assert_eq!(
            administrator
                .suppressed()
                .set_rate_limit(&lifecycle_key, &high_rate)
                .await
                .unwrap(),
            Some(low_rate)
        );

        barrier.wait();
        barrier.wait();
        let accepted_before_delete = initial_total + INSTANCES as u64 * AFTER_RAISE_INCREMENTS;
        let snapshot = administrator
            .suppressed()
            .get(&lifecycle_key)
            .await
            .unwrap();
        assert_eq!(snapshot.total, accepted_before_delete + declined_total);
        assert_eq!(snapshot.total_declined, declined_total);
        assert_eq!(
            administrator
                .suppressed()
                .delete(&lifecycle_key)
                .await
                .unwrap(),
            Some(snapshot.total - snapshot.total_declined)
        );
        assert_eq!(
            administrator
                .suppressed()
                .delete(&lifecycle_key)
                .await
                .unwrap(),
            None
        );

        barrier.wait();
        barrier.wait();
        let deleted_total = (0..INSTANCES)
            .map(|_| deleted_rx.recv().unwrap())
            .sum::<u64>();
        assert_eq!(deleted_total, INSTANCES as u64 * RECREATED_INCREMENTS);
        let observer = build_limiter_with_prefix(&url, 60, 1_000, 1.0, prefix.clone()).await;
        assert_eq!(
            observer
                .suppressed()
                .get(&lifecycle_key)
                .await
                .unwrap()
                .total,
            0
        );

        assert_eq!(
            administrator
                .suppressed()
                .set_if(&clear_even, &high_rate, RateLimitComparator::Always, 10)
                .await
                .unwrap(),
            (10, 0)
        );
        assert_eq!(
            administrator
                .suppressed()
                .set_if(&clear_odd, &high_rate, RateLimitComparator::Always, 10)
                .await
                .unwrap(),
            (10, 0)
        );
        barrier.wait();
        barrier.wait();
        let per_key_concurrent_total = INSTANCES as u64 / 2 * CLEAR_INCREMENTS;
        assert_eq!(
            administrator
                .suppressed()
                .get(&clear_even)
                .await
                .unwrap()
                .total,
            10 + per_key_concurrent_total
        );
        assert_eq!(
            administrator
                .suppressed()
                .get(&clear_odd)
                .await
                .unwrap()
                .total,
            10 + per_key_concurrent_total
        );
        administrator.suppressed().clear().await.unwrap();

        barrier.wait();
        barrier.wait();
        let observer = build_limiter_with_prefix(&url, 60, 1_000, 1.0, prefix).await;
        assert_eq!(
            observer.suppressed().get(&clear_even).await.unwrap().total,
            per_key_concurrent_total
        );
        assert_eq!(
            observer.suppressed().get(&clear_odd).await.unwrap().total,
            per_key_concurrent_total
        );
        administrator.suppressed().clear().await.unwrap();
        administrator.suppressed().clear().await.unwrap();
        assert_eq!(
            observer.suppressed().get(&clear_even).await.unwrap().total,
            0
        );
        assert_eq!(
            observer.suppressed().get(&clear_odd).await.unwrap().total,
            0
        );

        for worker in workers {
            worker.join().expect("Redis lifecycle worker panicked");
        }
    });
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
) -> std::sync::Arc<RedisRateLimiterProvider> {
    build_limiter_with_prefix_and_cache_ms(
        url,
        window_size,
        bucket_size,
        hard_limit_factor,
        SuppressionFactorCachePeriod::default().as_milliseconds(),
        unique_prefix(),
    )
    .await
}

async fn build_limiter_with_prefix(
    url: &str,
    window_size: u64,
    bucket_size: u64,
    hard_limit_factor: f64,
    prefix: RedisKey,
) -> Arc<RedisRateLimiterProvider> {
    build_limiter_with_prefix_and_cache_ms(
        url,
        window_size,
        bucket_size,
        hard_limit_factor,
        SuppressionFactorCachePeriod::default().as_milliseconds(),
        prefix,
    )
    .await
}

async fn build_limiter_with_cache_ms(
    url: &str,
    window_size: u64,
    bucket_size: u64,
    hard_limit_factor: f64,
    suppression_factor_cache_period: u64,
) -> std::sync::Arc<RedisRateLimiterProvider> {
    build_limiter_with_prefix_and_cache_ms(
        url,
        window_size,
        bucket_size,
        hard_limit_factor,
        suppression_factor_cache_period,
        unique_prefix(),
    )
    .await
}

async fn build_limiter_with_prefix_and_cache_ms(
    url: &str,
    window_size: u64,
    bucket_size: u64,
    hard_limit_factor: f64,
    suppression_factor_cache_period: u64,
    prefix: RedisKey,
) -> Arc<RedisRateLimiterProvider> {
    let client = redis::Client::open(url).unwrap();
    let cm = client.get_connection_manager().await.unwrap();

    RedisRateLimiterProvider::builder(cm)
        .prefix(prefix)
        .window_size(WindowSize::seconds(window_size).unwrap())
        .bucket_size(BucketSize::milliseconds(bucket_size).unwrap())
        .hard_limit_factor(HardLimitFactor::try_from(hard_limit_factor).unwrap())
        .suppression_factor_cache_period(
            SuppressionFactorCachePeriod::milliseconds(suppression_factor_cache_period).unwrap(),
        )
        .cleanup_enabled(false)
        .build()
        .unwrap()
}

#[test]
fn get_suppression_factor_fresh_key_returns_zero() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_cache_ms(&url, 10, 100, 2f64, 500).await;
        let k = key("k");

        // With no usage, suppression factor resolves to 0.
        let sf = rl.suppressed().get_suppression_factor(&k).await.unwrap();
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
            rl.suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Always, 11)
                .await
                .unwrap(),
            (11, 0)
        );

        let sf = rl.suppressed().get_suppression_factor(&k).await.unwrap();

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
        let d1 = rl.suppressed().inc(&k, &rate_limit, 2).await.unwrap();

        assert!(matches!(d1, RateLimitDecision::Allowed), "d1: {:?}", d1);

        std::thread::sleep(Duration::from_millis(window_size * 1000 + 50));
        std::thread::sleep(Duration::from_millis(cache_ms + 25));

        let sf = rl.suppressed().get_suppression_factor(&k).await.unwrap();
        assert!((sf - 0.0).abs() < 1e-12, "sf: {sf}");

        let d2 = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();
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
            rl.suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Always, 20)
                .await
                .unwrap(),
            (20, 0)
        );

        // wait for 1.5 seconds
        runtime::async_sleep(Duration::from_millis(1200)).await;

        let expected_suppression_factor = 1f64 - (1f64 / 2f64);

        let decision = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();

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

        let first = rl.suppressed().inc(&k, &rate_limit, 10).await.unwrap();
        assert!(
            matches!(first, RateLimitDecision::Allowed),
            "first: {first:?}"
        );

        // wait for 1s to pass
        runtime::async_sleep(Duration::from_millis(1001)).await;

        let second = rl.suppressed().inc(&k, &rate_limit, 20).await.unwrap();
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

        let decision = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();

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

        let reaches_hard = rl.suppressed().inc(&k, &rate_limit, 100).await.unwrap();
        assert!(
            matches!(reaches_hard, RateLimitDecision::Allowed),
            "reaches_hard: {reaches_hard:?}"
        );

        let decision = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();

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
            rl.suppressed().get(&k).await.unwrap(),
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
            let decision = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();

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
            rl.suppressed().get(&k).await.unwrap(),
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
            .suppressed()
            .inc(&k, &rate_limit, base_capacity - 1)
            .await
            .unwrap();
        assert!(matches!(d1, RateLimitDecision::Allowed), "d1: {d1:?}");

        // Landing exactly on the soft boundary is still deterministically allowed.
        let d2 = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();
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
            let decision = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();
            assert!(
                matches!(decision, RateLimitDecision::Allowed),
                "accepted_after={accepted_after}, decision={decision:?}"
            );
        }

        let over_hard = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();
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
            rl.suppressed().get(&k).await.unwrap(),
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
            .suppressed()
            .inc(&k, &rate_limit, hard_capacity)
            .await
            .unwrap();
        assert!(matches!(d1, RateLimitDecision::Allowed), "d1: {d1:?}");

        let factor = rl.suppressed().get_suppression_factor(&k).await.unwrap();
        assert!((factor - 1.0).abs() < 1e-12, "factor: {factor}");

        // The cached exact-hard factor makes every immediate subsequent call fully suppressed.
        for i in 0..5u64 {
            let d = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();

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
        let hard_window_limit =
            (window_size as f64 * rate_limit.as_per_second() * hard_limit_factor) as u64;

        let rl = build_limiter_with_cache_ms(&url, window_size, 1_000, hard_limit_factor, cache_ms)
            .await;

        let k = key("k_evict");

        // Drive observed count to the hard limit in one call.
        let d1 = rl
            .suppressed()
            .inc(&k, &rate_limit, hard_window_limit)
            .await
            .unwrap();
        assert!(matches!(d1, RateLimitDecision::Allowed), "d1: {d1:?}");

        // Let the suppression_factor cache expire so the next read recomputes from Redis.
        runtime::async_sleep(Duration::from_millis(cache_ms + 50)).await;

        // Confirm full suppression before the window expires.
        let sf_before = rl.suppressed().get_suppression_factor(&k).await.unwrap();
        assert!(
            (sf_before - 1.0).abs() < 1e-12,
            "expected sf=1.0 before window expiry, got {sf_before}"
        );

        // Wait for the full window to expire, then let the suppression cache expire again.
        std::thread::sleep(Duration::from_millis(window_size * 1_000 + 50));
        runtime::async_sleep(Duration::from_millis(cache_ms + 50)).await;

        // After the window has expired, eviction must have cleared the old buckets.
        let sf_after = rl.suppressed().get_suppression_factor(&k).await.unwrap();
        assert!(
            (sf_after - 0.0).abs() < 1e-12,
            "expected sf=0.0 after window expiry but got {sf_after} — \
             old buckets were not evicted (window_size_ms passed instead of window_size?)"
        );

        // A new request must be admitted.
        let d2 = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();
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
        let soft_window_limit = (window_size as f64 * rate_limit.as_per_second()) as u64;
        let hard_window_limit = (soft_window_limit as f64 * hard_limit_factor) as u64;

        let rl =
            build_limiter_with_cache_ms(&url, window_size, 100, hard_limit_factor, cache_ms).await;

        let k = key("k_multi");
        let mut total_allowed: u64 = 0;

        for _window in 0..num_windows {
            // Hammer at 10× the rate limit to ensure we hit the ceiling each window.
            let burst = soft_window_limit * 10;
            for _ in 0..burst {
                let d = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();
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

        let snapshot = rl.suppressed().get(&key("k")).await.unwrap();
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
            let d = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();
            assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");
        }

        let snapshot = rl.suppressed().get(&k).await.unwrap();
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
            .suppressed()
            .inc(&low_then_high, &low_rate, 2)
            .await
            .unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "the first increment should fill the original hard capacity: {decision:?}"
        );
        let decision = rl
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
        let snapshot = rl.suppressed().get(&low_then_high).await.unwrap();
        assert_eq!((snapshot.total, snapshot.total_declined), (3, 1));

        let high_then_low = key("high-then-low");
        let decision = rl
            .suppressed()
            .inc(&high_then_low, &high_rate, 8)
            .await
            .unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "the first increment should establish the larger hard capacity: {decision:?}"
        );
        let decision = rl
            .suppressed()
            .inc(&high_then_low, &low_rate, 2)
            .await
            .unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "a later smaller rate must not reduce the sticky hard capacity: {decision:?}"
        );
        let snapshot = rl.suppressed().get(&high_then_low).await.unwrap();
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

        let outcome = rl
            .suppressed()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(100), 100)
            .await
            .unwrap();
        let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
        assert_eq!((new_total, old_total), (100, 0));

        let outcome = rl
            .suppressed()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(100), 100)
            .await
            .unwrap();
        let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
        assert_eq!((new_total, old_total), (100, 100));

        assert_eq!(rl.suppressed().get(&k).await.unwrap().total, 100);
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
            rl.suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Lt(100), 100)
                .await
                .unwrap(),
            (100, 0)
        );

        let outcome = rl
            .suppressed()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(50), 50)
            .await
            .unwrap();
        let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
        assert_eq!((new_total, old_total), (100, 100));
    });
}

#[test]
fn set_if_always_overwrites_unconditionally_including_lowering() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 6, 1000, 1.0).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(100f64).unwrap();

        assert_eq!(
            rl.suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Always, 100)
                .await
                .unwrap(),
            (100, 0)
        );

        let outcome = rl
            .suppressed()
            .set_if(&k, &rate_limit, RateLimitComparator::Always, 30)
            .await
            .unwrap();
        let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
        assert_eq!((new_total, old_total), (30, 100));
        assert_eq!(rl.suppressed().get(&k).await.unwrap().total, 30);
    });
}

#[test]
fn set_if_eq_zero_sets_only_when_window_is_empty() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter(&url, 6, 1000, 1.0).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(100f64).unwrap();

        let outcome = rl
            .suppressed()
            .set_if(&k, &rate_limit, RateLimitComparator::Eq(0), 25)
            .await
            .unwrap();
        let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
        assert_eq!((new_total, old_total), (25, 0));

        let outcome = rl
            .suppressed()
            .set_if(&k, &rate_limit, RateLimitComparator::Eq(0), 99)
            .await
            .unwrap();
        let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
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
            rl.suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Always, 10)
                .await
                .unwrap(),
            (10, 0)
        );
        assert_eq!(
            rl.suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Gt(5), 3)
                .await
                .unwrap(),
            (3, 10)
        );
        assert_eq!(
            rl.suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Ne(3), 7)
                .await
                .unwrap(),
            (3, 3)
        );
        assert_eq!(
            rl.suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Ne(5), 7)
                .await
                .unwrap(),
            (7, 3)
        );
        assert_eq!(rl.suppressed().get(&k).await.unwrap().total, 7);
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
                rl.suppressed()
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
            let snapshot = rl.suppressed().get(&k).await.unwrap();
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

        let decision = rl.suppressed().inc(&k, &initial_rate, 5).await.unwrap();
        assert!(matches!(decision, RateLimitDecision::Allowed));
        assert_eq!(
            rl.suppressed()
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

        let decision = rl.suppressed().inc(&k, &initial_rate, 1).await.unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "one unit should remain under the redefined hard capacity: {decision:?}"
        );
        let decision = rl.suppressed().inc(&k, &initial_rate, 1).await.unwrap();
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

        let outcome = rl
            .suppressed()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(27), 27)
            .await
            .unwrap();
        let new_total = outcome.current_total;
        assert_eq!(new_total, 27);

        let d = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();
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

        let outcome = rl
            .suppressed()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(30), 30)
            .await
            .unwrap();
        let new_total = outcome.current_total;
        assert_eq!(new_total, 30);

        let d = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();
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
            rl.suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Always, 30)
                .await
                .unwrap(),
            (30, 0)
        );
        for _ in 0..3 {
            let d = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();
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
        let outcome = rl
            .suppressed()
            .set_if(&k, &rate_limit, RateLimitComparator::Always, 0)
            .await
            .unwrap();
        let new_total = outcome.current_total;
        assert_eq!(new_total, 0);

        let d = rl.suppressed().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");
        assert_eq!(rl.suppressed().get(&k).await.unwrap().total, 1);
    });
}
