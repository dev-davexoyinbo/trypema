use std::{env, future::Future, thread, time::Duration};

use crate::common::SuppressionFactorCacheMs;
use crate::hybrid::SyncIntervalMs;
use crate::{
    HardLimitFactor, LocalRateLimiterOptions, RateGroupSizeMs, RateLimit, RateLimitDecision,
    RateLimiter, RateLimiterOptions, RedisKey, RedisRateLimiterOptions, WindowSizeSeconds,
};

fn window_capacity(window_size_seconds: u64, rate_limit: &RateLimit) -> u64 {
    ((window_size_seconds as f64) * **rate_limit) as u64
}

#[cfg(feature = "redis-tokio")]
fn block_on<F, T>(f: F) -> T
where
    F: Future<Output = T>,
{
    tokio::runtime::Runtime::new().unwrap().block_on(f)
}

#[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
fn block_on<F, T>(f: F) -> T
where
    F: Future<Output = T>,
{
    smol::block_on(f)
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
    sync_interval_ms: u64,
    prefix: RedisKey,
) -> std::sync::Arc<RateLimiter> {
    let client = redis::Client::open(url).unwrap();
    let cm = client.get_connection_manager().await.unwrap();

    let options = RateLimiterOptions {
        local: LocalRateLimiterOptions {
            window_size_seconds: WindowSizeSeconds::try_from(window_size_seconds).unwrap(),
            rate_group_size_ms: RateGroupSizeMs::try_from(rate_group_size_ms).unwrap(),
            hard_limit_factor: HardLimitFactor::default(),
            suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
        },
        redis: RedisRateLimiterOptions {
            connection_manager: cm,
            prefix: Some(prefix),
            window_size_seconds: WindowSizeSeconds::try_from(window_size_seconds).unwrap(),
            rate_group_size_ms: RateGroupSizeMs::try_from(rate_group_size_ms).unwrap(),
            hard_limit_factor: HardLimitFactor::default(),
            suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
            sync_interval_ms: SyncIntervalMs::try_from(sync_interval_ms).unwrap(),
        },
    };

    std::sync::Arc::new(RateLimiter::new(options))
}

#[test]
fn hybrid_allows_until_capacity_then_rejects() {
    let url = redis_url();

    block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 1_u64;
        let rate_group_size_ms = 1_000_u64;
        let sync_interval_ms = 25_u64;

        let rl = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            sync_interval_ms,
            prefix,
        )
        .await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let cap = window_capacity(window_size_seconds, &rate_limit);

        for _ in 0..cap {
            let d = rl
                .hybrid()
                .absolute()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");
        }

        let d = rl
            .hybrid()
            .absolute()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(matches!(d, RateLimitDecision::Rejected { .. }), "d: {d:?}");
    });
}

#[test]
fn hybrid_absolute_never_returns_suppressed() {
    let url = redis_url();

    block_on(async {
        let rl = build_limiter_with_prefix(&url, 1, 1000, 25, unique_prefix()).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(50f64).unwrap();

        for _ in 0..200_u64 {
            let d = rl
                .hybrid()
                .absolute()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            if matches!(d, RateLimitDecision::Suppressed { .. }) {
                panic!("suppressed decision is not expected in hybrid absolute: {d:?}");
            }
        }
    });
}

#[test]
fn hybrid_absolute_retry_after_is_bounded_by_min_sync_or_group_when_ttl_unknown() {
    let url = redis_url();

    block_on(async {
        let prefix = unique_prefix();
        let window_size_seconds = 1_u64;
        let rate_group_size_ms = 1_000_u64;
        let sync_interval_ms = 25_u64;

        let rl = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            sync_interval_ms,
            prefix,
        )
        .await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let cap = window_capacity(window_size_seconds, &rate_limit);

        for _ in 0..cap {
            let _ = rl
                .hybrid()
                .absolute()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
        }

        let d = rl
            .hybrid()
            .absolute()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        let RateLimitDecision::Rejected { retry_after_ms, .. } = d else {
            panic!("expected rejected decision, got: {d:?}");
        };

        let bound_ms = sync_interval_ms.min(rate_group_size_ms) as u128;
        assert!(
            retry_after_ms <= bound_ms,
            "retry_after_ms={retry_after_ms} bound_ms={bound_ms}"
        );
        assert!(retry_after_ms > 0, "retry_after_ms should be > 0");
    });
}

#[test]
fn hybrid_usage_is_committed_to_redis_on_flush_and_then_visible_to_others() {
    let url = redis_url();

    block_on(async {
        // Use a shared prefix so both instances address the same Redis keys.
        let prefix = unique_prefix();

        let window_size_seconds = 1_u64;
        let rate_group_size_ms = 1_000_u64;
        let sync_interval_ms = 25_u64;

        let rl_a = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;
        let rl_b = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            sync_interval_ms,
            prefix,
        )
        .await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();

        // Fill the hybrid local accept budget (does not write to Redis yet).
        for _ in 0..5_u64 {
            let d = rl_a
                .hybrid()
                .absolute()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");
        }

        // Other instances checking Redis directly should still see no limit state.
        let d0 = rl_b.redis().absolute().is_allowed(&k).await.unwrap();
        assert!(matches!(d0, RateLimitDecision::Allowed), "d0: {d0:?}");

        // Crossing the accept limit queues a commit (and rejects).
        let d1 = rl_a
            .hybrid()
            .absolute()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(
            matches!(d1, RateLimitDecision::Rejected { .. }),
            "d1: {d1:?}"
        );

        // Wait for the committer to flush the queued commit to Redis.
        // We poll because timing depends on the interval tick schedule.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            let d2 = rl_b.redis().absolute().is_allowed(&k).await.unwrap();
            if matches!(d2, RateLimitDecision::Rejected { .. }) {
                break;
            }

            if std::time::Instant::now() >= deadline {
                panic!("timed out waiting for commit to become visible; last decision: {d2:?}");
            }

            thread::sleep(Duration::from_millis(10));
        }
    });
}

#[test]
fn hybrid_absolute_does_not_touch_redis_until_commit() {
    let url = redis_url();

    block_on(async {
        let prefix = unique_prefix();

        let window_size_seconds = 1_u64;
        let rate_group_size_ms = 1_000_u64;
        let sync_interval_ms = 25_u64;

        let rl_a = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;
        let rl_b = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            sync_interval_ms,
            prefix,
        )
        .await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let cap = window_capacity(window_size_seconds, &rate_limit);

        // Fill local accept budget; no commit should have happened yet.
        for _ in 0..cap {
            let d = rl_a
                .hybrid()
                .absolute()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");
        }

        // Redis should still have no state for this key.
        let d0 = rl_b.redis().absolute().is_allowed(&k).await.unwrap();
        assert!(matches!(d0, RateLimitDecision::Allowed), "d0: {d0:?}");
    });
}

#[test]
fn hybrid_absolute_unblocks_after_window_expires() {
    let url = redis_url();

    block_on(async {
        let prefix = unique_prefix();

        let window_size_seconds = 1_u64;
        let rate_group_size_ms = 1_000_u64;
        let sync_interval_ms = 25_u64;

        let rl = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            sync_interval_ms,
            prefix,
        )
        .await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let cap = window_capacity(window_size_seconds, &rate_limit);

        for _ in 0..cap {
            let _ = rl
                .hybrid()
                .absolute()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
        }

        let d = rl
            .hybrid()
            .absolute()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(matches!(d, RateLimitDecision::Rejected { .. }), "d: {d:?}");

        // Wait for the full window to expire. We poll to avoid flakiness.
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            if std::time::Instant::now() >= deadline {
                panic!("timed out waiting for window to expire");
            }

            thread::sleep(Duration::from_millis(25));
            let d2 = rl
                .hybrid()
                .absolute()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            if matches!(d2, RateLimitDecision::Allowed) {
                break;
            }
        }
    });
}

#[test]
fn hybrid_absolute_per_key_state_is_independent() {
    let url = redis_url();

    block_on(async {
        let rl = build_limiter_with_prefix(&url, 1, 1000, 25, unique_prefix()).await;

        let a = key("a");
        let b = key("b");
        let rate_limit = RateLimit::try_from(2f64).unwrap();
        let cap = window_capacity(1, &rate_limit);

        for _ in 0..cap {
            let _ = rl
                .hybrid()
                .absolute()
                .inc(&a, &rate_limit, 1)
                .await
                .unwrap();
        }

        let da = rl
            .hybrid()
            .absolute()
            .inc(&a, &rate_limit, 1)
            .await
            .unwrap();
        assert!(
            matches!(da, RateLimitDecision::Rejected { .. }),
            "da: {da:?}"
        );

        let db = rl
            .hybrid()
            .absolute()
            .inc(&b, &rate_limit, 1)
            .await
            .unwrap();
        assert!(matches!(db, RateLimitDecision::Allowed), "db: {db:?}");
    });
}

#[test]
fn hybrid_absolute_prefix_isolation() {
    let url = redis_url();

    block_on(async {
        let window_size_seconds = 1_u64;
        let rate_group_size_ms = 1_000_u64;
        let sync_interval_ms = 25_u64;

        let rl_a = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            sync_interval_ms,
            unique_prefix(),
        )
        .await;
        let rl_b = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            sync_interval_ms,
            unique_prefix(),
        )
        .await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();
        let cap = window_capacity(window_size_seconds, &rate_limit);

        for _ in 0..cap {
            let _ = rl_a
                .hybrid()
                .absolute()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
        }
        let d = rl_a
            .hybrid()
            .absolute()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(matches!(d, RateLimitDecision::Rejected { .. }), "d: {d:?}");

        let d_other = rl_b
            .hybrid()
            .absolute()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(
            matches!(d_other, RateLimitDecision::Allowed),
            "d_other: {d_other:?}"
        );
    });
}

#[test]
fn hybrid_absolute_remaining_after_waiting_reflects_oldest_bucket_when_seeded_from_redis() {
    let url = redis_url();

    block_on(async {
        let prefix = unique_prefix();

        let window_size_seconds = 6_u64;
        let rate_group_size_ms = 300_u64;
        let sync_interval_ms = 25_u64;

        // Seed Redis with a single merged bucket: 2 + 4 within the same group.
        let rl_seed = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        rl_seed
            .redis()
            .absolute()
            .inc(&k, &rate_limit, 2)
            .await
            .unwrap();
        thread::sleep(Duration::from_millis(50));
        rl_seed
            .redis()
            .absolute()
            .inc(&k, &rate_limit, 4)
            .await
            .unwrap();

        // Create a hybrid limiter that will read the current Redis state.
        let rl = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            sync_interval_ms,
            prefix,
        )
        .await;

        // Load Redis state into the hybrid cache without adding local usage.
        let _ = rl
            .hybrid()
            .absolute()
            .inc(&k, &rate_limit, 0)
            .await
            .unwrap();

        // At capacity (6 * 1 = 6). Next increment should be rejected and remaining_after_waiting
        // should reflect the oldest bucket count (which is the full 6 when merged).
        let d = rl
            .hybrid()
            .absolute()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        let RateLimitDecision::Rejected {
            remaining_after_waiting,
            ..
        } = d
        else {
            panic!("expected rejected decision, got: {d:?}");
        };
        assert_eq!(remaining_after_waiting, 6, "d: {d:?}");
    });
}

#[test]
fn hybrid_absolute_remaining_after_waiting_differs_when_buckets_are_separate() {
    let url = redis_url();

    block_on(async {
        let prefix = unique_prefix();

        let window_size_seconds = 6_u64;
        let rate_group_size_ms = 300_u64;
        let sync_interval_ms = 25_u64;

        // Seed Redis with two separate buckets: sleep > group size.
        let rl_seed = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            sync_interval_ms,
            prefix.clone(),
        )
        .await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        rl_seed
            .redis()
            .absolute()
            .inc(&k, &rate_limit, 2)
            .await
            .unwrap();
        thread::sleep(Duration::from_millis(350));
        rl_seed
            .redis()
            .absolute()
            .inc(&k, &rate_limit, 4)
            .await
            .unwrap();

        let rl = build_limiter_with_prefix(
            &url,
            window_size_seconds,
            rate_group_size_ms,
            sync_interval_ms,
            prefix,
        )
        .await;

        let _ = rl
            .hybrid()
            .absolute()
            .inc(&k, &rate_limit, 0)
            .await
            .unwrap();

        let d = rl
            .hybrid()
            .absolute()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        let RateLimitDecision::Rejected {
            remaining_after_waiting,
            ..
        } = d
        else {
            panic!("expected rejected decision, got: {d:?}");
        };

        // Oldest bucket is the first bucket with count=2.
        assert_eq!(remaining_after_waiting, 2, "d: {d:?}");
    });
}

#[cfg(feature = "redis-tokio")]
#[test]
fn hybrid_absolute_concurrent_tokio_smoke_does_not_panic() {
    let url = redis_url();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let rl = build_limiter_with_prefix(&url, 1, 1000, 25, unique_prefix()).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(10f64).unwrap();

        let mut tasks = Vec::new();
        for _ in 0..16 {
            let rl = rl.clone();
            let k = k.clone();
            let rate_limit = rate_limit.clone();
            tasks.push(tokio::spawn(async move {
                for _ in 0..50 {
                    let d = rl
                        .hybrid()
                        .absolute()
                        .inc(&k, &rate_limit, 1)
                        .await
                        .unwrap();
                    if matches!(d, RateLimitDecision::Suppressed { .. }) {
                        panic!("unexpected suppressed decision in hybrid absolute");
                    }
                }
            }));
        }

        for t in tasks {
            t.await.unwrap();
        }
    });
}

#[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
#[test]
fn hybrid_absolute_concurrent_smol_smoke_does_not_panic() {
    let url = redis_url();

    smol::block_on(async {
        let rl = build_limiter_with_prefix(&url, 1, 1000, 25, unique_prefix()).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(10f64).unwrap();

        let mut tasks = Vec::new();
        for _ in 0..16 {
            let rl = rl.clone();
            let k = k.clone();
            let rate_limit = rate_limit.clone();
            tasks.push(smol::spawn(async move {
                for _ in 0..50 {
                    let d = rl
                        .hybrid()
                        .absolute()
                        .inc(&k, &rate_limit, 1)
                        .await
                        .unwrap();
                    if matches!(d, RateLimitDecision::Suppressed { .. }) {
                        panic!("unexpected suppressed decision in hybrid absolute");
                    }
                }
            }));
        }

        for t in tasks {
            t.await;
        }
    });
}
