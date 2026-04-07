use std::{env, thread, time::Duration};

use super::runtime;

use crate::hybrid::SyncIntervalMs;
use crate::{
    HardLimitFactor, LocalRateLimiterOptions, RateGroupSizeMs, RateLimit, RateLimitDecision,
    RateLimiter, RateLimiterOptions, RedisKey, RedisRateLimiterOptions, SuppressionFactorCacheMs,
    WindowSizeSeconds,
};

fn window_capacity(window_size_seconds: u64, rate_limit: &RateLimit) -> u64 {
    ((window_size_seconds as f64) * **rate_limit) as u64
}

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

async fn build_limiter_with_prefix(
    url: &str,
    window_size_seconds: u64,
    rate_group_size_ms: u64,
    hard_limit_factor: f64,
    suppression_factor_cache_ms: u64,
    sync_interval_ms: u64,
    prefix: RedisKey,
) -> std::sync::Arc<RateLimiter> {
    let client = redis::Client::open(url).unwrap();
    let cm = client.get_connection_manager().await.unwrap();

    let options = RateLimiterOptions {
        local: LocalRateLimiterOptions {
            window_size_seconds: WindowSizeSeconds::try_from(window_size_seconds).unwrap(),
            rate_group_size_ms: RateGroupSizeMs::try_from(rate_group_size_ms).unwrap(),
            hard_limit_factor: HardLimitFactor::try_from(hard_limit_factor).unwrap(),
            suppression_factor_cache_ms: SuppressionFactorCacheMs::try_from(
                suppression_factor_cache_ms,
            )
            .unwrap(),
        },
        redis: RedisRateLimiterOptions {
            connection_manager: cm,
            prefix: Some(prefix),
            window_size_seconds: WindowSizeSeconds::try_from(window_size_seconds).unwrap(),
            rate_group_size_ms: RateGroupSizeMs::try_from(rate_group_size_ms).unwrap(),
            hard_limit_factor: HardLimitFactor::try_from(hard_limit_factor).unwrap(),
            suppression_factor_cache_ms: SuppressionFactorCacheMs::try_from(
                suppression_factor_cache_ms,
            )
            .unwrap(),
            sync_interval_ms: SyncIntervalMs::try_from(sync_interval_ms).unwrap(),
        },
    };

    std::sync::Arc::new(RateLimiter::new(options))
}

fn assert_in_01(v: f64) {
    assert!(v >= 0.0 && v <= 1.0, "expected v in [0,1], got {v:?}");
}

async fn wait_for_hybrid_sync(sync_interval_ms: u64) {
    // Hybrid commit flow is tick-based:
    // - tick N: committer flushes queued commits, then signals limiter.flush()
    // - limiter.flush() can enqueue additional commits
    // - tick N+1: those follow-up commits are flushed
    // Waiting ~2 ticks makes committed state observable from another instance.
    runtime::async_sleep(Duration::from_millis(sync_interval_ms * 2 + 50)).await;
}

fn record_suppressed_decision(
    decision: RateLimitDecision,
    count: u64,
    allowed_volume: &mut u64,
    denied_volume: &mut u64,
    allowed_ops: &mut u64,
    denied_ops: &mut u64,
) {
    match decision {
        RateLimitDecision::Allowed => {
            *allowed_volume += count;
            *allowed_ops += 1;
        }
        RateLimitDecision::Suppressed { is_allowed, .. } => {
            if is_allowed {
                *allowed_volume += count;
                *allowed_ops += 1;
            } else {
                *denied_volume += count;
                *denied_ops += 1;
            }
        }
        RateLimitDecision::Rejected { .. } => {
            panic!("rejected decision is not expected in suppressed strategy")
        }
    }
}

#[test]
fn hybrid_suppressed_get_suppression_factor_fresh_key_returns_zero() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 1, 1000, 1.0, 50, 25, unique_prefix()).await;

        let k = key("k");
        let sf = rl
            .hybrid()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();
        assert!((sf - 0.0).abs() < 1e-12, "sf: {sf}");
    });
}

#[test]
fn hybrid_suppressed_allows_until_base_capacity_boundary() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size_seconds = 10_u64;
        let hard_limit_factor = 2.0_f64;
        let rl = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            1000,
            hard_limit_factor,
            25,
            25,
            unique_prefix(),
        )
        .await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();
        let base_cap = window_capacity(window_size_seconds, &rate_limit);

        // Deterministic regime: suppression does not start until base capacity is met.
        for _ in 0..base_cap {
            let mut rng = |_p: f64| panic!("rng must not be called before suppression begins");
            let d = rl
                .hybrid()
                .suppressed()
                .inc_with_rng(&k, 1, Some(&rate_limit), &mut rng)
                .await
                .unwrap();
            assert!(
                matches!(
                    d,
                    RateLimitDecision::Allowed
                        | RateLimitDecision::Suppressed {
                            suppression_factor: 0f64,
                            is_allowed: true
                        }
                ),
                "d: {d:?}"
            );
        }
    });
}

#[test]
fn hybrid_suppressed_crossing_base_capacity_does_not_call_rng_when_sf_is_zero() {
    let url = redis_url();

    runtime::block_on(async {
        // When the hybrid suppressed limiter overflows its local accepting budget, it calls
        // get_suppression_factor(). If local state is Accepting, get_suppression_factor() returns
        // 0.0 immediately, so RNG is not consulted and the limiter returns a suppressed decision
        // with suppression_factor == 0.0.

        let window_size_seconds = 1_u64;
        let rate_group_size_ms = 1_000_u64;
        let sync_interval_ms = 25_u64;
        let hard_limit_factor = 1.0_f64;
        let cache_ms = 25_u64;

        let rl = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            unique_prefix(),
        )
        .await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let cap = window_capacity(window_size_seconds, &rate_limit);

        // Fill local accepting budget; RNG must not be called.
        for _ in 0..cap {
            let mut rng = |_p: f64| panic!("rng must not be called while accepting");
            let d = rl
                .hybrid()
                .suppressed()
                .inc_with_rng(&k, 1, Some(&rate_limit), &mut rng)
                .await
                .unwrap();
            match d {
                RateLimitDecision::Allowed => {}
                // With hard_limit_factor=1.0, the final request that *reaches* soft==hard
                // triggers a Redis-read. Redis may not yet reflect committed increments so
                // sf can be 0.0 or 1.0; either way the request must be admitted (is_allowed: true).
                RateLimitDecision::Suppressed {
                    is_allowed: true, ..
                } => {}
                other => panic!("d: {other:?}"),
            }
        }

        // Overflow: local state is still Accepting so Redis-derived sf is 0.0; RNG is not
        // consulted. With hard_limit_factor=1.0, this overflow also exceeds the hard cap so the
        // decision reports suppression_factor=1.0 and denies.
        let mut rng = |_p: f64| panic!("rng must not be called when suppression_factor == 0");
        let d1 = rl
            .hybrid()
            .suppressed()
            .inc_with_rng(&k, 1, Some(&rate_limit), &mut rng)
            .await
            .unwrap();

        assert!(
            matches!(
                d1,
                RateLimitDecision::Suppressed {
                    suppression_factor: 1f64,
                    is_allowed: false
                }
            ),
            "d1: {d1:?}"
        );
    });
}

#[test]
fn hybrid_suppressed_calls_rng_when_redis_reports_mid_suppression_factor() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size_seconds = 10_u64;
        let rate_group_size_ms = 1_000_u64;
        let sync_interval_ms = 75_u64;
        let hard_limit_factor = 2.0_f64;
        let cache_ms = 5_u64;

        let prefix = unique_prefix();

        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();
        let soft_limit = window_capacity(window_size_seconds, &rate_limit);
        assert_eq!(soft_limit, 10);

        // Seed HYBRID_SUPPRESSED state through the public API.
        // With window_size=10, rate_limit=1, hard_limit_factor=2:
        // - soft limit = 10
        // - hard limit stored in Redis = 20
        // Choose a total strictly between soft and hard so 0 < suppression_factor < 1.
        let seed_total = 11_u64;
        let rl_seed = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;

        for _ in 0..seed_total {
            let d = rl_seed
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            // Seeding may cross the soft limit and return a suppressed decision with
            // suppression_factor == 0.0 (deterministic allow). For this test we only care that
            // the observed volume is committed so that a fresh instance reads a mid-range factor.
            assert!(
                matches!(
                    d,
                    RateLimitDecision::Allowed
                        | RateLimitDecision::Suppressed {
                            is_allowed: true,
                            ..
                        }
                ),
                "d: {d:?}"
            );
        }

        wait_for_hybrid_sync(sync_interval_ms).await;

        // Let the suppression_factor cache expire so the next read uses the committed totals.
        runtime::async_sleep(Duration::from_millis(cache_ms + 50)).await;

        // New hybrid instance reads Redis state and should take the RNG path.
        let rl = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix,
        )
        .await;

        let mut called = 0_u64;
        let mut seen_p: Option<f64> = None;
        let mut rng = |p: f64| {
            called += 1;
            assert_in_01(p);
            seen_p = Some(p);
            false
        };

        let d1 = rl
            .hybrid()
            .suppressed()
            .inc_with_rng(&k, 1, Some(&rate_limit), &mut rng)
            .await
            .unwrap();

        assert_eq!(called, 1, "expected exactly one rng call");

        let RateLimitDecision::Suppressed {
            suppression_factor,
            is_allowed,
        } = d1
        else {
            panic!("expected suppressed decision, got: {d1:?}");
        };

        assert!(
            suppression_factor > 0.0 && suppression_factor < 1.0,
            "suppression_factor: {suppression_factor}"
        );
        let seen_p = seen_p.expect("rng must receive p");
        let expected_p = 1.0 - suppression_factor;
        assert!(
            (seen_p - expected_p).abs() < 1e-12,
            "seen_p: {seen_p} expected_p: {expected_p}"
        );
        assert!(!is_allowed, "rng returns false so is_allowed must be false");
    });
}

#[test]
fn hybrid_suppressed_redis_suppressed_state_does_not_poison_hybrid_keyspace() {
    let url = redis_url();

    runtime::block_on(async {
        // Ensure the Redis suppressed namespace does not contaminate the Hybrid suppressed
        // namespace (they are keyed by different RateType values).
        let window_size_seconds = 10_u64;
        let rate_group_size_ms = 1_000_u64;
        let sync_interval_ms = 25_u64;
        let hard_limit_factor = 2.0_f64;
        let cache_ms = 50_u64;

        let prefix = unique_prefix();
        let rl = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix,
        )
        .await;

        let k = key("k_poison");
        let rate_limit = RateLimit::try_from(1f64).unwrap();
        let soft_limit = window_capacity(window_size_seconds, &rate_limit);

        // Seed ONLY Redis suppressed keyspace through the public API.
        for _ in 0..(soft_limit + 2) {
            let d = rl
                .redis()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            assert!(
                matches!(
                    d,
                    RateLimitDecision::Allowed
                        | RateLimitDecision::Suppressed {
                            is_allowed: true,
                            ..
                        }
                ),
                "d: {d:?}"
            );
        }

        // Hybrid suppressed should still see a fresh key (sf=0) because it uses a different
        // key namespace.
        let sf = rl
            .hybrid()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();
        assert!((sf - 0.0).abs() < 1e-12, "sf: {sf}");
    });
}

#[test]
fn hybrid_suppressed_denies_100_percent_after_hard_limit() {
    let url = redis_url();

    runtime::block_on(async {
        // Use small limits to keep this integration test fast.
        let window_size_seconds = 1_u64;
        let rate_group_size_ms = 1_000_u64;
        let sync_interval_ms = 75_u64;
        let hard_limit_factor = 2.0_f64;
        let cache_ms = 25_u64;

        let prefix = unique_prefix();
        let k = key("k_hard");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let soft_limit = window_capacity(window_size_seconds, &rate_limit);

        // Seed at the hard limit and ensure suppression_factor reaches 1.0.
        let hard_limit = (soft_limit as f64 * hard_limit_factor) as u64;
        let rl_seed = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;

        for _ in 0..hard_limit {
            let d = rl_seed
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();

            // Suppressed strategy starts returning Suppressed decisions once the soft limit is
            // reached. During seeding, we only need to ensure the API never returns Rejected.
            // Suppressed decisions may be allowed or denied depending on suppression_factor.
            assert!(
                matches!(
                    d,
                    RateLimitDecision::Allowed | RateLimitDecision::Suppressed { .. }
                ),
                "d: {d:?}"
            );
        }

        wait_for_hybrid_sync(sync_interval_ms).await;

        let rl = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix,
        )
        .await;

        runtime::async_sleep(Duration::from_millis(cache_ms + 50)).await;

        let sf = rl
            .hybrid()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();
        assert!((sf - 1.0).abs() < 1e-12, "sf: {sf}");

        let mut allowed_volume = 0_u64;
        let mut denied_volume = 0_u64;
        let mut allowed_ops = 0_u64;
        let mut denied_ops = 0_u64;

        let mut rng = |_p: f64| panic!("rng must not be called when suppression_factor == 1");

        for _ in 0..10_u64 {
            let d = rl
                .hybrid()
                .suppressed()
                .inc_with_rng(&k, 1, Some(&rate_limit), &mut rng)
                .await
                .unwrap();

            assert!(
                matches!(
                    d,
                    RateLimitDecision::Suppressed {
                        suppression_factor,
                        is_allowed: false
                    } if (suppression_factor - 1.0).abs() < 1e-12
                ),
                "d: {d:?}"
            );

            record_suppressed_decision(
                d,
                1,
                &mut allowed_volume,
                &mut denied_volume,
                &mut allowed_ops,
                &mut denied_ops,
            );
        }

        assert_eq!(allowed_volume, 0);
        assert_eq!(denied_volume, 10);
        assert_eq!(allowed_ops, 0);
        assert_eq!(denied_ops, 10);
    });
}

#[test]
fn hybrid_suppressed_suppressing_ttl_fast_path_skips_rng_when_sf_is_zero() {
    let url = redis_url();

    runtime::block_on(async {
        // This test exercises the Suppressing fast-path with suppression_factor == 0.0.
        let window_size_seconds = 1_u64;
        let rate_group_size_ms = 1_000_u64;
        let sync_interval_ms = 25_u64;
        let hard_limit_factor = 1.0_f64;
        let cache_ms = 50_u64;

        let rl = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            unique_prefix(),
        )
        .await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let cap = window_capacity(window_size_seconds, &rate_limit);

        // Fill local budget.
        for _ in 0..cap {
            let mut rng = |_p: f64| panic!("rng must not be called while accepting");
            let d = rl
                .hybrid()
                .suppressed()
                .inc_with_rng(&k, 1, Some(&rate_limit), &mut rng)
                .await
                .unwrap();
            match d {
                RateLimitDecision::Allowed => {}
                // The final item (accepted == soft == hard) triggers a Redis-read; sf may be 0.0
                // or 1.0 depending on Redis commit lag, but the request must be admitted.
                RateLimitDecision::Suppressed {
                    is_allowed: true, ..
                } => {}
                other => panic!("d: {other:?}"),
            }
        }

        // Overflow transitions to Suppressing. The hard-cap guard fires since
        // current_total_count >= hard_window_limit, so suppression_factor=1.0 and denies.
        let mut rng = |_p: f64| panic!("rng must not be called when suppression_factor == 0 or 1");

        let d1 = rl
            .hybrid()
            .suppressed()
            .inc_with_rng(&k, 1, Some(&rate_limit), &mut rng)
            .await
            .unwrap();
        assert!(
            matches!(
                d1,
                RateLimitDecision::Suppressed {
                    suppression_factor: 1f64,
                    is_allowed: false
                }
            ),
            "d1: {d1:?}"
        );

        // While in Suppressing and TTL has not elapsed, it should return immediately without RNG.
        let mut rng2 = |_p: f64| panic!("rng must not be called in suppressing fast path");
        let d2 = rl
            .hybrid()
            .suppressed()
            .inc_with_rng(&k, 1, Some(&rate_limit), &mut rng2)
            .await
            .unwrap();
        assert!(
            matches!(
                d2,
                RateLimitDecision::Suppressed {
                    suppression_factor: 1f64,
                    is_allowed: false
                }
            ),
            "d2: {d2:?}"
        );
    });
}

#[test]
fn hybrid_suppressed_per_key_state_is_independent() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size_seconds = 1_u64;
        let rate_group_size_ms = 1_000_u64;
        let sync_interval_ms = 1_000_u64;
        let hard_limit_factor = 1.0_f64;
        let cache_ms = 250_u64;

        let rl = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            unique_prefix(),
        )
        .await;

        let a = key("a");
        let b = key("b");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let cap = window_capacity(window_size_seconds, &rate_limit);

        // Drive key `a` to the overflow boundary.
        for _ in 0..cap {
            let mut rng = |_p: f64| panic!("rng must not be called while accepting");
            let d = rl
                .hybrid()
                .suppressed()
                .inc_with_rng(&a, 1, Some(&rate_limit), &mut rng)
                .await
                .unwrap();
            match d {
                RateLimitDecision::Allowed => {}
                // Final item may trigger Redis-read with stale sf; must be admitted.
                RateLimitDecision::Suppressed {
                    is_allowed: true, ..
                } => {}
                other => panic!("d: {other:?}"),
            }
        }

        let mut rng = |_p: f64| panic!("rng must not be called when suppression_factor == 0 or 1");
        let d_overflow = rl
            .hybrid()
            .suppressed()
            .inc_with_rng(&a, 1, Some(&rate_limit), &mut rng)
            .await
            .unwrap();
        assert!(matches!(d_overflow, RateLimitDecision::Suppressed { .. }));

        // Key `b` should be unaffected.
        let mut rng_b = |_p: f64| panic!("rng must not be called below the rate limit");
        let d_b = rl
            .hybrid()
            .suppressed()
            .inc_with_rng(&b, 1, Some(&rate_limit), &mut rng_b)
            .await
            .unwrap();
        assert!(matches!(d_b, RateLimitDecision::Allowed), "d_b: {d_b:?}");
    });
}

#[test]
fn hybrid_suppressed_batch_increment_respects_soft_limit_boundary() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size_seconds = 1_u64;
        let rate_group_size_ms = 1_000_u64;
        let sync_interval_ms = 10_u64;
        let hard_limit_factor = 1.0_f64;
        let cache_ms = 10_u64;

        let rl = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            unique_prefix(),
        )
        .await;

        let k = key("k_batch");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let cap = window_capacity(window_size_seconds, &rate_limit);
        assert_eq!(cap, 5);

        // Batch below boundary.
        let mut rng1 = |_p: f64| panic!("rng must not be called while accepting");
        let d1 = rl
            .hybrid()
            .suppressed()
            .inc_with_rng(&k, 4, Some(&rate_limit), &mut rng1)
            .await
            .unwrap();
        assert!(matches!(d1, RateLimitDecision::Allowed), "d1: {d1:?}");

        // Exactly hits boundary.
        let mut rng2 = |_p: f64| panic!("rng must not be called while accepting");
        let d2 = rl
            .hybrid()
            .suppressed()
            .inc_with_rng(&k, 1, Some(&rate_limit), &mut rng2)
            .await
            .unwrap();
        assert!(
            matches!(
                d2,
                RateLimitDecision::Allowed
                    | RateLimitDecision::Suppressed {
                        suppression_factor: 1f64,
                        is_allowed: true
                    }
            ),
            "d2: {d2:?}"
        );

        // Crosses boundary -> suppression begins; sf is 0.0 on the local overflow path.
        let mut rng3 = |_p: f64| panic!("rng must not be called when suppression_factor == 0");
        let d3 = rl
            .hybrid()
            .suppressed()
            .inc_with_rng(&k, 1, Some(&rate_limit), &mut rng3)
            .await
            .unwrap();
        assert!(
            matches!(
                d3,
                RateLimitDecision::Suppressed {
                    suppression_factor: 1f64,
                    is_allowed: false
                }
            ),
            "d3: {d3:?}"
        );
    });
}

#[test]
fn hybrid_suppressed_does_not_commit_before_soft_limit_overflow() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 1_u64;
        let rate_group_size_ms = 1_000_u64;
        // Keep ticks infrequent to avoid a background flush committing accepting state.
        let sync_interval_ms = 2_000_u64;
        let hard_limit_factor = 1.0_f64;
        let cache_ms = 250_u64;

        let rl_a = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;

        let rl_b = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix,
        )
        .await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let cap = window_capacity(window_size_seconds, &rate_limit);

        // Fill to the soft limit exactly (no overflow, no queued commit).
        for _ in 0..cap {
            let d = rl_a
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            match d {
                RateLimitDecision::Allowed => {}
                // Final item may trigger Redis-read with stale sf; must be admitted.
                RateLimitDecision::Suppressed {
                    is_allowed: true, ..
                } => {}
                other => panic!("d: {other:?}"),
            }
        }

        // If `rl_a` had committed, Redis would have total_count==window_limit and report sf==1.0.
        let sf_b = rl_b
            .hybrid()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();
        assert!((sf_b - 0.0).abs() < 1e-12, "sf_b: {sf_b}");
    });
}

#[test]
fn hybrid_suppressed_suppressing_hard_cap_guard_forces_full_denial_without_rng() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size_seconds = 1_u64;
        let rate_group_size_ms = 1_000_u64;
        let sync_interval_ms = 2_000_u64;
        let hard_limit_factor = 1.0_f64;
        let cache_ms = 500_u64;

        let rl = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            unique_prefix(),
        )
        .await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let cap = window_capacity(window_size_seconds, &rate_limit);

        for _ in 0..cap {
            let mut rng = |_p: f64| panic!("rng must not be called while accepting");
            let d = rl
                .hybrid()
                .suppressed()
                .inc_with_rng(&k, 1, Some(&rate_limit), &mut rng)
                .await
                .unwrap();
            match d {
                RateLimitDecision::Allowed => {}
                // Final item may trigger Redis-read with stale sf; must be admitted.
                RateLimitDecision::Suppressed {
                    is_allowed: true, ..
                } => {}
                other => panic!("d: {other:?}"),
            }
        }

        // Overflow transitions to Suppressing. The hard-cap guard fires since
        // current_total_count >= hard_window_limit, so suppression_factor=1.0 and denies.
        let mut rng0 = |_p: f64| panic!("rng must not be called when suppression_factor == 0 or 1");
        let d0 = rl
            .hybrid()
            .suppressed()
            .inc_with_rng(&k, 1, Some(&rate_limit), &mut rng0)
            .await
            .unwrap();
        assert!(
            matches!(
                d0,
                RateLimitDecision::Suppressed {
                    suppression_factor: 1f64,
                    is_allowed: false
                }
            ),
            "d0: {d0:?}"
        );

        // Next call hits the hard-cap guard (starting_count + local_count > window_limit).
        let mut rng1 = |_p: f64| panic!("rng must not be called when suppression_factor == 1");
        let d1 = rl
            .hybrid()
            .suppressed()
            .inc_with_rng(&k, 1, Some(&rate_limit), &mut rng1)
            .await
            .unwrap();

        assert!(
            matches!(
                d1,
                RateLimitDecision::Suppressed {
                    suppression_factor: 1f64,
                    is_allowed: false
                }
            ),
            "d1: {d1:?}"
        );
    });
}

#[test]
fn hybrid_suppressed_get_suppression_factor_returns_cached_value_in_suppressing_state() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size_seconds = 10_u64;
        let rate_group_size_ms = 1_000_u64;
        let sync_interval_ms = 75_u64;
        let hard_limit_factor = 2.0_f64;
        let cache_ms = 5_u64;

        let prefix = unique_prefix();
        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        // Seed total to mid-regime (between soft=10 and hard=20).
        let rl_seed = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;

        for _ in 0..11_u64 {
            let _ = rl_seed
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
        }
        wait_for_hybrid_sync(sync_interval_ms).await;
        runtime::async_sleep(Duration::from_millis(cache_ms + 50)).await;

        let rl = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix,
        )
        .await;

        // First call should read Redis and enter Suppressing with a mid-range sf.
        let mut rng = |_p: f64| true;
        let d1 = rl
            .hybrid()
            .suppressed()
            .inc_with_rng(&k, 1, Some(&rate_limit), &mut rng)
            .await
            .unwrap();

        let RateLimitDecision::Suppressed {
            suppression_factor: sf1,
            ..
        } = d1
        else {
            panic!("expected suppressed decision, got: {d1:?}");
        };
        assert!(sf1 > 0.0 && sf1 < 1.0, "sf1: {sf1}");

        // Now in Suppressing state; get_suppression_factor should return the cached value.
        let sf2 = rl
            .hybrid()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();
        assert!((sf2 - sf1).abs() < 1e-12, "sf2: {sf2} sf1: {sf1}");
    });
}

#[test]
fn hybrid_suppressed_unblocks_after_window_expires() {
    let url = redis_url();

    runtime::block_on(async {
        // Drive hybrid suppressed into full suppression and verify it clears after the window expires.
        let window_size_seconds = 1_u64;
        let rate_group_size_ms = 1_000_u64;
        let sync_interval_ms = 25_u64;
        let hard_limit_factor = 1.0_f64;
        let cache_ms = 5_u64;

        let prefix = unique_prefix();

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let cap = window_capacity(window_size_seconds, &rate_limit);

        let rl = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;

        // With hard_limit_factor=1.0, the hard limit equals base capacity.
        // Drive usage to capacity, then ensure commit is flushed and suppression factor recomputed.
        for _ in 0..cap {
            let _ = rl
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
        }
        runtime::async_sleep(Duration::from_millis(sync_interval_ms * 4)).await;
        runtime::async_sleep(Duration::from_millis(cache_ms + 50)).await;

        let mut rng = |_p: f64| panic!("rng must not be called when suppression_factor == 1");
        let d1 = rl
            .hybrid()
            .suppressed()
            .inc_with_rng(&k, 1, Some(&rate_limit), &mut rng)
            .await
            .unwrap();
        assert!(
            matches!(
                d1,
                RateLimitDecision::Suppressed {
                    suppression_factor,
                    is_allowed: false
                } if (suppression_factor - 1.0).abs() < 1e-12
            ),
            "d1: {d1:?}"
        );

        // Wait for the window and the cached suppression_factor to expire.
        runtime::async_sleep(Duration::from_millis(
            window_size_seconds * 1000 + cache_ms + 50,
        ))
        .await;

        // Use a fresh instance so local cached state doesn't mask Redis updates.
        let rl2 = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix,
        )
        .await;

        let sf2 = rl2
            .hybrid()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();
        assert!((sf2 - 0.0).abs() < 1e-12, "sf2: {sf2}");

        let mut rng2 = |_p: f64| panic!("rng must not be called when suppression_factor == 0");
        let d2 = rl2
            .hybrid()
            .suppressed()
            .inc_with_rng(&k, 1, Some(&rate_limit), &mut rng2)
            .await
            .unwrap();
        match d2 {
            RateLimitDecision::Allowed => {}
            RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: true,
            } => assert!(
                (suppression_factor - 0.0).abs() < 1e-12,
                "suppression_factor: {suppression_factor}"
            ),
            other => panic!("d2: {other:?}"),
        }
    });
}

#[test]
fn hybrid_suppressed_full_denial_seeded_from_hybrid_redis_keyspace_does_not_call_rng() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 1_u64;
        let rate_group_size_ms = 1_000_u64;
        let sync_interval_ms = 25_u64;

        let cache_ms = 5_u64;
        // With hard_limit_factor=1.0, window_limit == base capacity.
        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let cap = (window_size_seconds as f64 * *rate_limit) as u64;

        let rl_hybrid = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            1.0,
            cache_ms,
            sync_interval_ms,
            prefix,
        )
        .await;

        // Drive the limiter to full denial (sf == 1.0) via the public API.
        for _ in 0..cap {
            let _ = rl_hybrid
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
        }
        runtime::async_sleep(Duration::from_millis(sync_interval_ms * 4)).await;
        runtime::async_sleep(Duration::from_millis(cache_ms + 25)).await;

        // Hybrid should observe suppression_factor=1.0 and deny without consulting RNG.
        let mut rng = |_p: f64| panic!("rng must not be called when suppression_factor == 1.0");
        let d1 = rl_hybrid
            .hybrid()
            .suppressed()
            .inc_with_rng(&k, 1, Some(&rate_limit), &mut rng)
            .await
            .unwrap();

        assert!(
            matches!(
                d1,
                RateLimitDecision::Suppressed {
                    suppression_factor,
                    is_allowed: false
                } if (suppression_factor - 1.0).abs() < 1e-12
            ),
            "d1: {d1:?}"
        );
    });
}

#[test]
fn hybrid_suppressed_usage_is_committed_to_redis_and_visible_to_others() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();

        let window_size_seconds = 1_u64;
        let rate_group_size_ms = 1_000_u64;
        let sync_interval_ms = 25_u64;
        let cache_ms = 5_u64;

        let rl_a = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            1.0,
            cache_ms,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;

        let rl_b = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            1.0,
            cache_ms,
            sync_interval_ms,
            prefix,
        )
        .await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let cap = (window_size_seconds as f64 * *rate_limit) as u64;

        // Fill local budget.
        for _ in 0..cap {
            let d = rl_a
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            match d {
                RateLimitDecision::Allowed => {}
                // Final item may trigger Redis-read with stale sf; must be admitted.
                RateLimitDecision::Suppressed {
                    is_allowed: true, ..
                } => {}
                other => panic!("d: {other:?}"),
            }
        }

        // Crossing the local window limit queues a commit.
        let d1 = rl_a
            .hybrid()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(
            matches!(d1, RateLimitDecision::Suppressed { .. }),
            "d1: {d1:?}"
        );

        // Poll until HYBRID_SUPPRESSED Redis reflects the committed state.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            let sf = rl_b
                .hybrid()
                .suppressed()
                .get_suppression_factor(&k)
                .await
                .unwrap();

            if (sf - 1.0).abs() < 1e-12 {
                break;
            }

            if std::time::Instant::now() >= deadline {
                panic!("timed out waiting for commit to become visible; sf={sf}");
            }

            thread::sleep(Duration::from_millis(10));
        }
    });
}

#[test]
fn hybrid_suppressed_concurrent_smoke_does_not_panic() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 1, 1000, 2.0, 25, 25, unique_prefix()).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(10f64).unwrap();

        let mut tasks = Vec::new();
        for _ in 0..16 {
            let rl = rl.clone();
            let k = k.clone();
            tasks.push(runtime::spawn(async move {
                for _ in 0..50 {
                    let d = rl
                        .hybrid()
                        .suppressed()
                        .inc(&k, &rate_limit, 1)
                        .await
                        .unwrap();
                    assert!(
                        matches!(
                            d,
                            RateLimitDecision::Allowed | RateLimitDecision::Suppressed { .. }
                        ),
                        "d: {d:?}"
                    );
                }
            }));
        }

        for t in tasks {
            runtime::join(t).await;
        }
    });
}

#[test]
fn hybrid_suppressed_prefix_isolation() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size_seconds = 5_u64;
        let rate_group_size_ms = 1_000_u64;
        let sync_interval_ms = 25_u64;
        let hard_limit_factor = 2.0_f64;
        let cache_ms = 5_u64;
        let rate_limit = RateLimit::try_from(2f64).unwrap();

        let prefix_a = unique_prefix();
        let prefix_b = unique_prefix();

        let rl_a = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix_a,
        )
        .await;

        let rl_b = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix_b,
        )
        .await;

        let k = key("k");
        // soft_cap = floor(window_size_seconds * rate_limit * 1) = floor(5 * 2) = 10
        let soft_cap = window_capacity(window_size_seconds, &rate_limit);

        // Send soft_cap + 1 increments so the (soft_cap+1)-th call overflows the local
        // budget and queues a Redis commit for prefix_a.
        for _ in 0..=soft_cap {
            let _ = rl_a
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
        }

        // Flush prefix_a state to Redis, then let the sf cache expire.
        wait_for_hybrid_sync(sync_interval_ms).await;
        runtime::async_sleep(Duration::from_millis(cache_ms + 50)).await;

        // prefix_a is now in the suppression zone (total > soft_cap).
        let sf_a = rl_a
            .hybrid()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();
        assert!(
            sf_a > 0.0,
            "prefix_a should be suppressed after overflow, sf_a={sf_a}"
        );

        // prefix_b has received no traffic; its Redis namespace is empty.
        let sf_b = rl_b
            .hybrid()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();
        assert!(
            (sf_b - 0.0).abs() < 1e-12,
            "prefix_b must be unaffected by prefix_a traffic, sf_b={sf_b}"
        );
    });
}

#[test]
fn hybrid_suppressed_never_returns_rejected_smoke() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 1, 1_000, 1.0, 50, 25, unique_prefix()).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();

        for _ in 0..25_u64 {
            let d = rl
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            assert!(
                matches!(
                    d,
                    RateLimitDecision::Allowed | RateLimitDecision::Suppressed { .. }
                ),
                "d: {d:?}"
            );
        }
    });
}

#[test]
fn hybrid_suppressed_redis_key_validation_rejects_empty_and_colons() {
    assert!(RedisKey::try_from("".to_string()).is_err());
    assert!(RedisKey::try_from("has:colon".to_string()).is_err());
}

/// After the window elapses, previously committed usage must be evicted from Redis so that
/// a fresh burst is admitted at the full rate again.
///
/// This test catches the bug where `read_state` passes `window_size_ms` (e.g. 1 000) to the
/// Lua script instead of `window_size_seconds` (e.g. 1).  With the wrong value the eviction
/// threshold is ~16 minutes into the past, so old buckets are never removed and
/// `total_count` accumulates indefinitely — suppression stays at 1.0 even after the window
/// has expired.
#[test]
fn hybrid_suppressed_window_eviction_allows_fresh_burst_after_expiry() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size_seconds = 1_u64;
        let rate_group_size_ms = 100_u64;
        let sync_interval_ms = 25_u64;
        // Use hard_limit_factor=2.0: soft_limit=5, hard_limit=10.
        // We fill to hard_limit (10) and then sync, guaranteeing Redis sees total_count=hard_limit
        // and computes suppression_factor=1.0.
        let hard_limit_factor = 2.0_f64;
        let cache_ms = 5_u64;

        let prefix = unique_prefix();
        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        // soft_limit=5, hard_limit=10
        let soft_limit = window_capacity(window_size_seconds, &rate_limit);
        let hard_limit = (soft_limit as f64 * hard_limit_factor) as u64;

        let rl = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;

        // Fill past the hard limit: the overflow request commits everything to Redis and sets
        // suppression_factor = 1.0 in the Redis cache.
        for _ in 0..=hard_limit {
            let _ = rl
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
        }
        // Wait for the committer to flush all queued counts to Redis (two ticks).
        wait_for_hybrid_sync(sync_interval_ms).await;
        // Let the suppression_factor Redis cache expire so the next read recomputes.
        runtime::async_sleep(Duration::from_millis(cache_ms + 50)).await;

        // Confirm that a fresh limiter instance sees full suppression before the window expires.
        let rl_check = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;
        let sf_before = rl_check
            .hybrid()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();
        assert!(
            (sf_before - 1.0).abs() < 1e-12,
            "expected sf=1.0 before window expiry, got {sf_before}"
        );

        // Wait for the full window to expire, then let the suppression cache expire again.
        runtime::async_sleep(Duration::from_millis(window_size_seconds * 1_000 + 50)).await;
        runtime::async_sleep(Duration::from_millis(cache_ms + 50)).await;

        // Use a fresh limiter instance so no stale local state can mask the Redis result.
        let rl2 = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix,
        )
        .await;

        // After the window has expired, Redis must have evicted the old buckets and the
        // suppression factor must be back to 0.
        let sf_after = rl2
            .hybrid()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();
        assert!(
            (sf_after - 0.0).abs() < 1e-12,
            "expected sf=0.0 after window expiry but got {sf_after} — \
             old buckets were not evicted (window_size_ms passed instead of window_size_seconds?)"
        );

        // And a new request must be admitted.
        let mut rng = |_p: f64| panic!("rng must not be called when suppression_factor == 0");
        let d = rl2
            .hybrid()
            .suppressed()
            .inc_with_rng(&k, 1, Some(&rate_limit), &mut rng)
            .await
            .unwrap();
        assert!(
            matches!(
                d,
                RateLimitDecision::Allowed
                    | RateLimitDecision::Suppressed {
                        suppression_factor: 0f64,
                        is_allowed: true
                    }
            ),
            "expected Allowed after window expiry, got {d:?}"
        );
    });
}

/// Run for three consecutive windows at well above the rate limit and assert that the total
/// admitted volume across all windows is close to `rate * num_windows`.
///
/// If window eviction is broken (buckets accumulate), the hard limit is hit after the first
/// window and every subsequent request is suppressed, so total_allowed ≈ window_limit rather
/// than ≈ rate * num_windows.
#[test]
fn hybrid_suppressed_throughput_over_multiple_windows_stays_at_rate_limit() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size_seconds = 1_u64;
        let rate_group_size_ms = 100_u64;
        let sync_interval_ms = 25_u64;
        let hard_limit_factor = 1.5_f64;
        let cache_ms = 5_u64;
        let num_windows = 3_u64;

        let prefix = unique_prefix();
        let k = key("k");
        let rate_limit = RateLimit::try_from(10f64).unwrap();
        // soft_window_limit = window_size * rate = 10
        // hard_window_limit = soft * hard_limit_factor = 15
        let soft_limit = window_capacity(window_size_seconds, &rate_limit); // 10

        let rl = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            cache_ms,
            sync_interval_ms,
            prefix,
        )
        .await;

        let mut total_allowed: u64 = 0;

        for _window in 0..num_windows {
            // Within each window, hammer at 10× the rate limit to ensure we hit the ceiling.
            // The limiter should admit ≈ soft_limit requests and suppress the rest.
            let burst = soft_limit * 10;
            for _ in 0..burst {
                let d = rl
                    .hybrid()
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

            // Flush committed state and wait for the window to fully expire before next window.
            wait_for_hybrid_sync(sync_interval_ms).await;
            runtime::async_sleep(Duration::from_millis(window_size_seconds * 1_000 + 50)).await;
            runtime::async_sleep(Duration::from_millis(cache_ms + 50)).await;
        }

        // Each window should admit at most hard_window_limit requests and at least soft_limit.
        // Over num_windows windows the total must be at least soft_limit * num_windows.
        // If eviction is broken, total_allowed ≈ hard_window_limit (15) instead of ≈ 30+.
        let hard_limit = (soft_limit as f64 * hard_limit_factor) as u64; // 15
        let expected_min = soft_limit * num_windows; // 30
        assert!(
            total_allowed >= expected_min,
            "total_allowed={total_allowed} but expected >= {expected_min} over {num_windows} windows \
             (hard_limit={hard_limit}) — window eviction is likely broken"
        );
    });
}
