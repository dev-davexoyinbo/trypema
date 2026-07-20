use std::{env, time::Duration};

use super::runtime;

use crate::hybrid::SyncIntervalMs;
use crate::{
    HardLimitFactor, HistoryPreservation, LocalRateLimiterOptions, RateGroupSizeMs, RateLimit,
    RateLimitComparator, RateLimitDecision, RateLimiter, RateLimiterOptions, RedisKey,
    RedisRateLimiterOptions, SuppressedRateLimitSnapshot, SuppressionFactorCacheMs,
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
        for accepted_after in 1..=base_cap {
            let mut rng = |_p: f64| panic!("rng must not be called before suppression begins");
            let d = rl
                .hybrid()
                .suppressed()
                .inc_with_rng(&k, 1, Some(&rate_limit), &mut rng)
                .await
                .unwrap();
            assert!(
                matches!(d, RateLimitDecision::Allowed),
                "accepted_after={accepted_after}, d={d:?}"
            );
        }
    });
}

#[test]
fn hybrid_suppressed_fractional_hard_limit_preserves_local_soft_and_hard_boundaries() {
    let url = redis_url();

    runtime::block_on(async {
        // raw soft window = 6 * 0.5 = 3; raw hard window = 3 * 1.5 = 4.5.
        // Like suppressed-local, operational capacities are soft=3 and hard=4.
        let rl = build_limiter_with_prefix(&url, 6, 1000, 1.5, 60_000, 25, unique_prefix()).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(0.5).unwrap();

        for accepted_after in 1..=4_u64 {
            let mut rng = |_p: f64| panic!("rng must not run through the hard boundary");
            let decision = rl
                .hybrid()
                .suppressed()
                .inc_with_rng(&k, 1, Some(&rate_limit), &mut rng)
                .await
                .unwrap();
            assert!(
                matches!(decision, RateLimitDecision::Allowed),
                "accepted_after={accepted_after}, decision={decision:?}"
            );
        }

        let mut rng = |_p: f64| panic!("rng must not run for a cached factor of one");
        let over_hard = rl
            .hybrid()
            .suppressed()
            .inc_with_rng(&k, 1, Some(&rate_limit), &mut rng)
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
            rl.hybrid().suppressed().get(&k).await.unwrap(),
            SuppressedRateLimitSnapshot {
                total: 5,
                total_declined: 1,
                suppression_factor: 1.0,
            }
        );
    });
}

#[test]
fn hybrid_suppressed_shared_soft_hard_boundary_is_allowed_then_fully_suppressed() {
    let url = redis_url();

    runtime::block_on(async {
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

        // Every request through the exact shared soft/hard boundary is allowed. The boundary
        // request also transitions local state to a cached factor of one.
        for accepted_after in 1..=cap {
            let mut rng = |_p: f64| panic!("rng must not be called while accepting");
            let d = rl
                .hybrid()
                .suppressed()
                .inc_with_rng(&k, 1, Some(&rate_limit), &mut rng)
                .await
                .unwrap();
            assert!(
                matches!(d, RateLimitDecision::Allowed),
                "accepted_after={accepted_after}, d={d:?}"
            );
        }

        let factor = rl
            .hybrid()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();
        assert!((factor - 1.0).abs() < 1e-12, "factor: {factor}");

        // Crossing the now-cached hard boundary is fully suppressed without consulting RNG.
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
        let cache_ms = 60_000_u64;

        let prefix = unique_prefix();

        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();
        let soft_window_limit = window_capacity(window_size_seconds, &rate_limit);
        assert_eq!(soft_window_limit, 10);

        // Seed HYBRID_SUPPRESSED state through the public API.
        // With window_size=10, rate_limit=1, hard_limit_factor=2:
        // - soft limit = 10
        // - hard limit stored in Redis = 20
        // Choose a total strictly between soft and hard so 0 < suppression_factor < 1.
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

        // Seed one exact committed bucket through the public hybrid API. Conditional replacement
        // leaves the factor cache absent, forcing the fresh instance below to calculate it.
        assert_eq!(
            rl_seed
                .hybrid()
                .suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Nil, 11)
                .await
                .unwrap(),
            (11, 0)
        );

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
        let soft_window_limit = window_capacity(window_size_seconds, &rate_limit);

        // Seed ONLY Redis suppressed keyspace through the public API.
        assert_eq!(
            rl.redis()
                .suppressed()
                .set_if(
                    &k,
                    &rate_limit,
                    RateLimitComparator::Nil,
                    soft_window_limit + 2,
                )
                .await
                .unwrap(),
            (soft_window_limit + 2, 0)
        );

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
        let sync_interval_ms = 2_000_u64;
        let hard_limit_factor = 2.0_f64;
        let cache_ms = 60_000_u64;

        let k = key("k_hard");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let soft_window_limit = window_capacity(window_size_seconds, &rate_limit);

        let hard_window_limit = (soft_window_limit as f64 * hard_limit_factor) as u64;
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

        let mut boundary_rng = |_p: f64| panic!("rng must not run at the exact hard boundary");
        let reaches_hard = rl
            .hybrid()
            .suppressed()
            .inc_with_rng(&k, hard_window_limit, Some(&rate_limit), &mut boundary_rng)
            .await
            .unwrap();
        assert!(
            matches!(reaches_hard, RateLimitDecision::Allowed),
            "reaches_hard: {reaches_hard:?}"
        );

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
fn hybrid_suppressed_suppressing_ttl_fast_path_skips_rng_when_sf_is_one() {
    let url = redis_url();

    runtime::block_on(async {
        // This test exercises the Suppressing fast path after the shared soft/hard boundary
        // establishes suppression_factor == 1.0.
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

        // Fill through the exact boundary. Every request must be an exact Allowed variant.
        for accepted_after in 1..=cap {
            let mut rng = |_p: f64| panic!("rng must not be called while accepting");
            let d = rl
                .hybrid()
                .suppressed()
                .inc_with_rng(&k, 1, Some(&rate_limit), &mut rng)
                .await
                .unwrap();
            assert!(
                matches!(d, RateLimitDecision::Allowed),
                "accepted_after={accepted_after}, d={d:?}"
            );
        }

        // Crossing the cached hard boundary denies with factor one.
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

        // While in Suppressing and TTL has not elapsed, return immediately without RNG.
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

        // Drive key `a` to the exact shared soft/hard boundary.
        for accepted_after in 1..=cap {
            let mut rng = |_p: f64| panic!("rng must not be called while accepting");
            let d = rl
                .hybrid()
                .suppressed()
                .inc_with_rng(&a, 1, Some(&rate_limit), &mut rng)
                .await
                .unwrap();
            assert!(
                matches!(d, RateLimitDecision::Allowed),
                "accepted_after={accepted_after}, d={d:?}"
            );
        }

        let mut rng = |_p: f64| panic!("rng must not be called when suppression_factor == 0 or 1");
        let d_overflow = rl
            .hybrid()
            .suppressed()
            .inc_with_rng(&a, 1, Some(&rate_limit), &mut rng)
            .await
            .unwrap();
        assert!(
            matches!(
                d_overflow,
                RateLimitDecision::Suppressed {
                    suppression_factor: 1f64,
                    is_allowed: false,
                }
            ),
            "d_overflow: {d_overflow:?}"
        );

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
        assert!(matches!(d2, RateLimitDecision::Allowed), "d2: {d2:?}");

        // Crosses the shared soft/hard boundary, so the cached factor is one.
        let mut rng3 = |_p: f64| panic!("rng must not be called when suppression_factor == 1");
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
        for accepted_after in 1..=cap {
            let d = rl_a
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            assert!(
                matches!(d, RateLimitDecision::Allowed),
                "accepted_after={accepted_after}, d={d:?}"
            );
        }

        // If `rl_a` had committed, Redis would have total_count==hard_window_limit and report sf==1.0.
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

        for accepted_after in 1..=cap {
            let mut rng = |_p: f64| panic!("rng must not be called while accepting");
            let d = rl
                .hybrid()
                .suppressed()
                .inc_with_rng(&k, 1, Some(&rate_limit), &mut rng)
                .await
                .unwrap();
            assert!(
                matches!(d, RateLimitDecision::Allowed),
                "accepted_after={accepted_after}, d={d:?}"
            );
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

        // Next call hits the hard-cap guard (starting_count + local_count > hard_window_limit).
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
        let cache_ms = 60_000_u64;

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

        assert_eq!(
            rl_seed
                .hybrid()
                .suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Nil, 11)
                .await
                .unwrap(),
            (11, 0)
        );

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
        for accepted_after in 1..=cap {
            let decision = rl
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            assert!(
                matches!(decision, RateLimitDecision::Allowed),
                "accepted_after={accepted_after}, decision={decision:?}"
            );
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
        assert!(matches!(d2, RateLimitDecision::Allowed), "d2: {d2:?}");
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
        // With hard_limit_factor=1.0, hard_window_limit == base capacity.
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
        let reaches_hard = rl_hybrid
            .hybrid()
            .suppressed()
            .inc(&k, &rate_limit, cap)
            .await
            .unwrap();
        assert!(
            matches!(reaches_hard, RateLimitDecision::Allowed),
            "reaches_hard: {reaches_hard:?}"
        );
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
fn hybrid_suppressed_refresh_commit_is_visible_to_other_instances() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();

        // Long window prevents unrelated parallel test load from expiring committed state while
        // this test polls another instance. Fractional rate keeps setup capacity small.
        let window_size_seconds = 60_u64;
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
        let rate_limit = RateLimit::try_from(0.1f64).unwrap();
        let cap = (window_size_seconds as f64 * *rate_limit) as u64;

        // Fill local budget.
        for accepted_after in 1..=cap {
            let d = rl_a
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            assert!(
                matches!(d, RateLimitDecision::Allowed),
                "accepted_after={accepted_after}, d={d:?}"
            );
        }

        // The hard-boundary refresh commits the previously pending local count. The boundary
        // request and this overflow remain local until a later refresh or background flush.
        let d1 = rl_a
            .hybrid()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(
            matches!(
                d1,
                RateLimitDecision::Suppressed {
                    suppression_factor: 1f64,
                    is_allowed: false,
                }
            ),
            "d1: {d1:?}"
        );

        assert_eq!(
            rl_b.hybrid().suppressed().get(&k).await.unwrap(),
            SuppressedRateLimitSnapshot {
                total: cap - 1,
                total_declined: 0,
                suppression_factor: 0.0,
            }
        );
    });
}

#[test]
fn hybrid_suppressed_concurrent_increments_preserve_exact_total() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 60, 1_000, 2.0, 25, 25, unique_prefix()).await;

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

        let snapshot = rl.hybrid().suppressed().get(&k).await.unwrap();
        assert_eq!(snapshot.total, 16 * 50);
        assert!(snapshot.total_declined <= snapshot.total);
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

        assert_eq!(
            rl_a.hybrid()
                .suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Nil, soft_cap + 1,)
                .await
                .unwrap(),
            (soft_cap + 1, 0)
        );

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
fn hybrid_suppressed_public_decisions_never_return_absolute_rejection_metadata() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 1, 1_000, 1.0, 50, 25, unique_prefix()).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();

        for observed_after in 1..=25_u64 {
            let decision = rl
                .hybrid()
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
            rl.hybrid().suppressed().get(&k).await.unwrap(),
            SuppressedRateLimitSnapshot {
                total: 25,
                total_declined: 20,
                suppression_factor: 1.0,
            }
        );
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
        // Use hard_limit_factor=2.0: soft_window_limit=5, hard_window_limit=10.
        // We fill to hard_window_limit (10) and then sync, guaranteeing Redis sees total_count=hard_window_limit
        // and computes suppression_factor=1.0.
        let hard_limit_factor = 2.0_f64;
        let cache_ms = 5_u64;

        let prefix = unique_prefix();
        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        // soft_window_limit=5, hard_window_limit=10
        let soft_window_limit = window_capacity(window_size_seconds, &rate_limit);
        let hard_window_limit = (soft_window_limit as f64 * hard_limit_factor) as u64;

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

        let reaches_hard = rl
            .hybrid()
            .suppressed()
            .inc(&k, &rate_limit, hard_window_limit)
            .await
            .unwrap();
        assert!(
            matches!(reaches_hard, RateLimitDecision::Allowed),
            "reaches_hard: {reaches_hard:?}"
        );

        let declined = rl
            .hybrid()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(
            matches!(
                declined,
                RateLimitDecision::Suppressed {
                    suppression_factor: 1f64,
                    is_allowed: false,
                }
            ),
            "declined: {declined:?}"
        );
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
            matches!(d, RateLimitDecision::Allowed),
            "expected Allowed after window expiry, got {d:?}"
        );
    });
}

/// Run for three consecutive windows at well above the rate limit and assert that the total
/// admitted volume across all windows is close to `rate * num_windows`.
///
/// If window eviction is broken (buckets accumulate), the hard limit is hit after the first
/// window and every subsequent request is suppressed, so total_allowed ≈ hard_window_limit rather
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
        let soft_window_limit = window_capacity(window_size_seconds, &rate_limit); // 10

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
            // The limiter should admit ≈ soft_window_limit requests and suppress the rest.
            let burst = soft_window_limit * 10;
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

        // Each window should admit at most hard_window_limit requests and at least soft_window_limit.
        // Over num_windows windows the total must be at least soft_window_limit * num_windows.
        // If eviction is broken, total_allowed ≈ hard_window_limit (15) instead of ≈ 30+.
        let hard_window_limit = (soft_window_limit as f64 * hard_limit_factor) as u64; // 15
        let expected_min = soft_window_limit * num_windows; // 30
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
        let rl = build_limiter_with_prefix(&url, 6, 1000, 1.0, 100, 25, unique_prefix()).await;

        let k = key("k");
        let snapshot = rl.hybrid().suppressed().get(&k).await.unwrap();
        assert_eq!(snapshot, SuppressedRateLimitSnapshot::default());
        assert_eq!(rl.hybrid().suppressed().local_state_count(), 0);
    });
}

#[test]
fn cleanup_keeps_suppressing_state_while_cache_ttl_is_live() {
    let url = redis_url();

    runtime::block_on(async {
        let cache_ttl_ms = 500_u64;
        let stale_after_ms = 50_u64;
        let rl =
            build_limiter_with_prefix(&url, 6, 1_000, 1.0, cache_ttl_ms, 2_000, unique_prefix())
                .await;
        let k = key("k");
        let rate = RateLimit::try_from(5f64).unwrap();

        assert_eq!(
            rl.hybrid()
                .suppressed()
                .set_if(&k, &rate, RateLimitComparator::Nil, 30)
                .await
                .unwrap(),
            (30, 0)
        );
        assert!(matches!(
            rl.hybrid().suppressed().inc(&k, &rate, 1).await.unwrap(),
            RateLimitDecision::Suppressed {
                is_allowed: false,
                ..
            }
        ));
        assert_eq!(rl.hybrid().suppressed().local_state_count(), 1);

        runtime::async_sleep(Duration::from_millis(100)).await;
        rl.hybrid()
            .suppressed()
            .cleanup(stale_after_ms)
            .await
            .unwrap();
        assert_eq!(
            rl.hybrid().suppressed().local_state_count(),
            1,
            "live suppression cache must outlive the shorter stale horizon"
        );

        runtime::async_sleep(Duration::from_millis(cache_ttl_ms)).await;
        rl.hybrid()
            .suppressed()
            .cleanup(stale_after_ms)
            .await
            .unwrap();
        assert_eq!(rl.hybrid().suppressed().local_state_count(), 0);
    });
}

#[test]
fn cleanup_keeps_suppressing_state_until_stale_horizon_after_cache_expiry() {
    let url = redis_url();

    runtime::block_on(async {
        let cache_ttl_ms = 500_u64;
        let stale_after_ms = 300_u64;
        let rl =
            build_limiter_with_prefix(&url, 6, 1_000, 1.0, cache_ttl_ms, 2_000, unique_prefix())
                .await;
        let k = key("k");
        let rate = RateLimit::try_from(5f64).unwrap();

        assert_eq!(
            rl.hybrid()
                .suppressed()
                .set_if(&k, &rate, RateLimitComparator::Nil, 30)
                .await
                .unwrap(),
            (30, 0)
        );
        assert!(matches!(
            rl.hybrid().suppressed().inc(&k, &rate, 1).await.unwrap(),
            RateLimitDecision::Suppressed {
                is_allowed: false,
                ..
            }
        ));
        assert_eq!(rl.hybrid().suppressed().local_state_count(), 1);

        runtime::async_sleep(Duration::from_millis(cache_ttl_ms + 100)).await;
        rl.hybrid()
            .suppressed()
            .cleanup(stale_after_ms)
            .await
            .unwrap();
        assert_eq!(
            rl.hybrid().suppressed().local_state_count(),
            1,
            "the stale horizon must begin after the suppression cache expires"
        );

        runtime::async_sleep(Duration::from_millis(stale_after_ms)).await;
        rl.hybrid()
            .suppressed()
            .cleanup(stale_after_ms)
            .await
            .unwrap();
        assert_eq!(rl.hybrid().suppressed().local_state_count(), 0);
    });
}

#[test]
fn get_does_not_resurrect_an_expired_local_baseline() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 1, 100, 2.0, 25, 2_000, unique_prefix()).await;
        let k = key("k");
        let rate = RateLimit::try_from(10f64).unwrap();

        assert_eq!(
            rl.hybrid()
                .suppressed()
                .set_if(&k, &rate, RateLimitComparator::Nil, 5)
                .await
                .unwrap(),
            (5, 0)
        );
        assert_eq!(rl.hybrid().suppressed().get(&k).await.unwrap().total, 5);

        runtime::async_sleep(Duration::from_millis(1_100)).await;

        assert_eq!(
            rl.hybrid().suppressed().get(&k).await.unwrap(),
            SuppressedRateLimitSnapshot::default(),
            "expired Redis history must not be restored from the local committed baseline"
        );
    });
}

#[test]
fn get_inferred_refreshes_once_then_uses_local_snapshot() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let writer =
            build_limiter_with_prefix(&url, 6, 1_000, 1.5, 100, 2_000, prefix.clone()).await;
        let reader = build_limiter_with_prefix(&url, 6, 1_000, 1.5, 100, 2_000, prefix).await;
        let k = key("k");
        let rate = RateLimit::try_from(100f64).unwrap();

        assert_eq!(
            writer
                .hybrid()
                .suppressed()
                .set_if(&k, &rate, RateLimitComparator::Nil, 5)
                .await
                .unwrap(),
            (5, 0)
        );
        assert_eq!(
            reader
                .hybrid()
                .suppressed()
                .get_inferred(&k)
                .await
                .unwrap()
                .total,
            5
        );

        assert_eq!(
            writer
                .hybrid()
                .suppressed()
                .set_if(&k, &rate, RateLimitComparator::Nil, 9)
                .await
                .unwrap(),
            (9, 5)
        );

        assert_eq!(
            reader
                .hybrid()
                .suppressed()
                .get_inferred(&k)
                .await
                .unwrap()
                .total,
            5,
            "usable local state must stay on the inference fast path"
        );
        assert_eq!(reader.hybrid().suppressed().get(&k).await.unwrap().total, 9);
    });
}

#[test]
fn get_does_not_hide_newer_redis_suppression_with_local_accepting_state() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let writer =
            build_limiter_with_prefix(&url, 6, 1_000, 1.0, 100, 2_000, prefix.clone()).await;
        let reader = build_limiter_with_prefix(&url, 6, 1_000, 1.0, 100, 2_000, prefix).await;
        let k = key("k");
        let rate = RateLimit::try_from(1f64).unwrap();

        assert_eq!(
            writer
                .hybrid()
                .suppressed()
                .set_if(&k, &rate, RateLimitComparator::Nil, 5)
                .await
                .unwrap(),
            (5, 0)
        );
        assert_eq!(
            reader
                .hybrid()
                .suppressed()
                .get_inferred(&k)
                .await
                .unwrap()
                .total,
            5
        );

        assert_eq!(
            writer
                .hybrid()
                .suppressed()
                .set_if(&k, &rate, RateLimitComparator::Nil, 6)
                .await
                .unwrap(),
            (6, 5)
        );

        assert_eq!(
            reader.hybrid().suppressed().get_inferred(&k).await.unwrap(),
            SuppressedRateLimitSnapshot {
                total: 5,
                total_declined: 0,
                suppression_factor: 0.0,
            }
        );
        assert_eq!(
            reader.hybrid().suppressed().get(&k).await.unwrap(),
            SuppressedRateLimitSnapshot {
                total: 6,
                total_declined: 0,
                suppression_factor: 1.0,
            }
        );
    });
}

#[test]
fn get_snapshot_includes_local_pending_increments() {
    let url = redis_url();

    runtime::block_on(async {
        // Very slow sync tick so pending local increments are not flushed in the background.
        let rl = build_limiter_with_prefix(&url, 6, 1000, 1.0, 100, 2_000, unique_prefix()).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(100f64).unwrap();

        for _ in 0..3 {
            let d = rl
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");
        }

        let snapshot = rl.hybrid().suppressed().get(&k).await.unwrap();
        assert_eq!(
            snapshot,
            SuppressedRateLimitSnapshot {
                total: 3,
                total_declined: 0,
                suppression_factor: 0.0,
            }
        );

        assert_eq!(
            rl.hybrid().suppressed().get_inferred(&k).await.unwrap(),
            snapshot
        );
    });
}

#[test]
fn get_methods_keep_the_exact_snapshot_during_background_sync() {
    let url = redis_url();

    runtime::block_on(async {
        let sync_interval_ms = 500_u64;
        let rl =
            build_limiter_with_prefix(&url, 60, 1_000, 1.5, 100, sync_interval_ms, unique_prefix())
                .await;
        let k = key("k");
        let rate = RateLimit::try_from(100f64).unwrap();

        assert!(matches!(
            rl.hybrid().suppressed().inc(&k, &rate, 3).await.unwrap(),
            RateLimitDecision::Allowed
        ));

        runtime::async_sleep(Duration::from_millis(sync_interval_ms + 100)).await;

        let expected = SuppressedRateLimitSnapshot {
            total: 3,
            total_declined: 0,
            suppression_factor: 0.0,
        };
        assert_eq!(
            rl.hybrid().suppressed().get_inferred(&k).await.unwrap(),
            expected
        );
        assert_eq!(rl.hybrid().suppressed().get(&k).await.unwrap(), expected);
    });
}

#[test]
fn set_if_lt_primes_empty_key_and_reprime_is_noop() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 6, 1000, 1.0, 100, 25, unique_prefix()).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(100f64).unwrap();

        let (new_total, old_total) = rl
            .hybrid()
            .suppressed()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(100), 100)
            .await
            .unwrap();
        assert_eq!((new_total, old_total), (100, 0));

        let (new_total, old_total) = rl
            .hybrid()
            .suppressed()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(100), 100)
            .await
            .unwrap();
        assert_eq!((new_total, old_total), (100, 100));

        assert_eq!(rl.hybrid().suppressed().get(&k).await.unwrap().total, 100);
    });
}

#[test]
fn set_if_folds_pending_local_increments_before_comparing() {
    let url = redis_url();

    runtime::block_on(async {
        // Very slow sync tick: the 5 increments stay local until set_if folds them.
        let rl = build_limiter_with_prefix(&url, 6, 1000, 1.0, 100, 2_000, unique_prefix()).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(100f64).unwrap();

        for _ in 0..5 {
            let d = rl
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");
        }

        // The pending 5 are included in the Redis comparison overlay, so Lt(3)
        // sees 5 and the write is skipped — increments are never lost.
        let (new_total, old_total) = rl
            .hybrid()
            .suppressed()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(3), 3)
            .await
            .unwrap();
        assert_eq!((new_total, old_total), (5, 5));

        assert_eq!(rl.hybrid().suppressed().get(&k).await.unwrap().total, 5);
    });
}

#[test]
fn set_if_preserves_declined_increment_racing_after_pending_snapshot() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 6, 1000, 1.0, 100, 2_000, unique_prefix()).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();

        assert_eq!(
            rl.hybrid()
                .suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Nil, 30)
                .await
                .unwrap(),
            (30, 0)
        );
        assert!(matches!(
            rl.hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap(),
            RateLimitDecision::Suppressed {
                is_allowed: false,
                ..
            }
        ));

        let hook = rl.hybrid().suppressed().install_set_if_test_hook().unwrap();
        let set_limiter = std::sync::Arc::clone(&rl);
        let set_key = k.clone();
        let set_rate_limit = rate_limit;
        let set_task = runtime::spawn(async move {
            set_limiter
                .hybrid()
                .suppressed()
                .set_if(&set_key, &set_rate_limit, RateLimitComparator::Nil, 10)
                .await
        });

        hook.snapshot_taken.notified().await;
        let racing_decision = rl.hybrid().suppressed().inc(&k, &rate_limit, 2).await;
        hook.resume.notify_one();

        assert!(matches!(
            racing_decision.unwrap(),
            RateLimitDecision::Suppressed {
                is_allowed: false,
                ..
            }
        ));
        assert_eq!(runtime::join(set_task).await.unwrap(), (10, 31));
        assert_eq!(
            rl.hybrid().suppressed().get(&k).await.unwrap(),
            SuppressedRateLimitSnapshot {
                total: 12,
                total_declined: 2,
                suppression_factor: 0.0,
            }
        );
    });
}

#[test]
fn set_if_prime_below_soft_limit_allows_next_inc() {
    let url = redis_url();

    runtime::block_on(async {
        // window 6 * rate 5 * factor 1.0 → hard = soft = 30.
        let rl = build_limiter_with_prefix(&url, 6, 1000, 1.0, 100, 25, unique_prefix()).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();

        let (new_total, _) = rl
            .hybrid()
            .suppressed()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(27), 27)
            .await
            .unwrap();
        assert_eq!(new_total, 27);

        let d = rl
            .hybrid()
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
        let rl = build_limiter_with_prefix(&url, 6, 1000, 1.0, 100, 25, unique_prefix()).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();

        let (new_total, _) = rl
            .hybrid()
            .suppressed()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(30), 30)
            .await
            .unwrap();
        assert_eq!(new_total, 30);

        let d = rl
            .hybrid()
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

        assert_eq!(
            rl.hybrid().suppressed().get(&k).await.unwrap(),
            SuppressedRateLimitSnapshot {
                total: 31,
                total_declined: 1,
                suppression_factor: 1.0,
            }
        );
    });
}

#[test]
fn set_if_zero_count_reopens_admission() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 6, 1000, 1.0, 100, 25, unique_prefix()).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();

        assert_eq!(
            rl.hybrid()
                .suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Nil, 30)
                .await
                .unwrap(),
            (30, 0)
        );

        let d = rl
            .hybrid()
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

        // Clearing the window resets counts, declines, and the cached factor:
        // admission resumes immediately on the same instance.
        let (new_total, old_total) = rl
            .hybrid()
            .suppressed()
            .set_if(&k, &rate_limit, RateLimitComparator::Nil, 0)
            .await
            .unwrap();
        assert_eq!((new_total, old_total), (0, 31));

        let d = rl
            .hybrid()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");
    });
}

#[test]
fn conditional_set_zero_handles_missing_and_present_suppressed_keys() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 6, 1000, 1.5, 100, 25, unique_prefix()).await;
        let rate = RateLimit::try_from(100f64).unwrap();

        for (key_name, preservation) in [
            ("replace", None),
            ("preserve", Some(HistoryPreservation::PreserveOldest)),
        ] {
            let k = key(key_name);
            let missing_result = match preservation {
                Some(preservation) => rl
                    .hybrid()
                    .suppressed()
                    .set_if_preserve_history(&k, &rate, RateLimitComparator::Eq(0), 0, preservation)
                    .await
                    .unwrap(),
                None => rl
                    .hybrid()
                    .suppressed()
                    .set_if(&k, &rate, RateLimitComparator::Eq(0), 0)
                    .await
                    .unwrap(),
            };
            assert_eq!(missing_result, (0, 0));
            assert_eq!(rl.hybrid().suppressed().get(&k).await.unwrap().total, 0);

            assert_eq!(
                rl.hybrid()
                    .suppressed()
                    .set_if(&k, &rate, RateLimitComparator::Nil, 9)
                    .await
                    .unwrap(),
                (9, 0)
            );

            let present_result = match preservation {
                Some(preservation) => rl
                    .hybrid()
                    .suppressed()
                    .set_if_preserve_history(&k, &rate, RateLimitComparator::Nil, 0, preservation)
                    .await
                    .unwrap(),
                None => rl
                    .hybrid()
                    .suppressed()
                    .set_if(&k, &rate, RateLimitComparator::Nil, 0)
                    .await
                    .unwrap(),
            };
            assert_eq!(present_result, (0, 9));
            assert_eq!(rl.hybrid().suppressed().get(&k).await.unwrap().total, 0);
        }
    });
}

#[test]
fn set_if_and_get_do_not_cross_provider_keyspaces() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 6, 1000, 1.0, 100, 25, unique_prefix()).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(100f64).unwrap();

        // Write through the hybrid suppressed provider only.
        assert_eq!(
            rl.hybrid()
                .suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Nil, 40)
                .await
                .unwrap(),
            (40, 0)
        );

        // The pure Redis suppressed provider (same prefix) uses a separate keyspace.
        assert_eq!(rl.redis().suppressed().get(&k).await.unwrap().total, 0);
        assert_eq!(rl.hybrid().suppressed().get(&k).await.unwrap().total, 40);

        // And the reverse.
        assert_eq!(
            rl.redis()
                .suppressed()
                .set_if(&k, &rate_limit, RateLimitComparator::Nil, 7)
                .await
                .unwrap(),
            (7, 0)
        );
        assert_eq!(rl.hybrid().suppressed().get(&k).await.unwrap().total, 40);
        assert_eq!(rl.redis().suppressed().get(&k).await.unwrap().total, 7);
    });
}

#[test]
fn set_if_preserve_history_includes_suppressed_pending_and_noop_keeps_it() {
    let url = redis_url();
    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 60, 1000, 1.5, 100, 2_000, unique_prefix()).await;
        let k = key("k");
        let rate = RateLimit::try_from(100f64).unwrap();

        for _ in 0..4 {
            assert!(matches!(
                rl.hybrid().suppressed().inc(&k, &rate, 1).await.unwrap(),
                RateLimitDecision::Allowed
            ));
        }

        assert_eq!(
            rl.hybrid()
                .suppressed()
                .set_if_preserve_history(
                    &k,
                    &rate,
                    RateLimitComparator::Eq(99),
                    10,
                    HistoryPreservation::PreserveNewest,
                )
                .await
                .unwrap(),
            (4, 4)
        );
        assert_eq!(rl.hybrid().suppressed().get(&k).await.unwrap().total, 4);

        assert_eq!(
            rl.hybrid()
                .suppressed()
                .set_if_preserve_history(
                    &k,
                    &rate,
                    RateLimitComparator::Nil,
                    10,
                    HistoryPreservation::PreserveNewest,
                )
                .await
                .unwrap(),
            (10, 4)
        );
        assert_eq!(rl.hybrid().suppressed().get(&k).await.unwrap().total, 10);
    });
}
