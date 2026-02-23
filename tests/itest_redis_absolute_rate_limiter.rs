#![cfg(any(feature = "redis-tokio", feature = "redis-smol"))]

use std::{
    env,
    future::Future,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use trypema::local::LocalRateLimiterOptions;
use trypema::redis::{RedisKey, RedisRateLimiterOptions};
use trypema::{
    HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision, RateLimiter,
    RateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds,
};

fn redis_url() -> Option<String> {
    env::var("REDIS_URL").ok()
}

fn unique_prefix() -> RedisKey {
    let n: u64 = rand::random();
    RedisKey::try_from(format!("trypema_test_{n}")).unwrap()
}

fn key(s: &str) -> RedisKey {
    RedisKey::try_from(s.to_string()).unwrap()
}

async fn eventually<T, F, Fut>(timeout: Duration, poll: Duration, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let start = Instant::now();
    loop {
        if let Some(val) = f().await {
            return val;
        }
        if start.elapsed() >= timeout {
            panic!("condition not met within {timeout:?}");
        }
        thread::sleep(poll);
    }
}

async fn build_rate_limiter(
    url: &str,
    window_size_seconds: u64,
    rate_group_size_ms: u128,
) -> Arc<RateLimiter> {
    let client = redis::Client::open(url).unwrap();
    let connection_manager = client.get_connection_manager().await.unwrap();
    let prefix = unique_prefix();

    Arc::new(RateLimiter::new(RateLimiterOptions {
        local: LocalRateLimiterOptions {
            window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
            rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
            hard_limit_factor: HardLimitFactor::default(),
            suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
        },
        redis: RedisRateLimiterOptions {
            connection_manager,
            prefix: Some(prefix),
            window_size_seconds: WindowSizeSeconds::try_from(window_size_seconds).unwrap(),
            rate_group_size_ms: RateGroupSizeMs::try_from(rate_group_size_ms).unwrap(),
            hard_limit_factor: HardLimitFactor::default(),
            suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
        },
    }))
}

async fn saturate_to_rejected(rl: &Arc<RateLimiter>, k: &RedisKey, rate: &RateLimit, count: u64) {
    let d0 = rl.redis().absolute().inc(k, rate, count).await.unwrap();
    assert!(matches!(d0, RateLimitDecision::Allowed));

    eventually(
        Duration::from_secs(2),
        Duration::from_millis(10),
        || async {
            let d = rl.redis().absolute().is_allowed(k).await.unwrap();
            matches!(d, RateLimitDecision::Rejected { .. }).then_some(())
        },
    )
    .await;
}

#[test]
fn rejects_at_exact_window_limit() {
    let Some(url) = redis_url() else {
        return;
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let rl = build_rate_limiter(&url, 1, 1000).await;
        let k = key("k");
        let rate = RateLimit::try_from(2f64).unwrap();

        // capacity = 1s * 2/s = 2
        saturate_to_rejected(&rl, &k, &rate, 2).await;

        let d = rl.redis().absolute().inc(&k, &rate, 1).await.unwrap();
        assert!(matches!(d, RateLimitDecision::Rejected { .. }));
    });
}

#[test]
fn per_key_state_is_independent() {
    let Some(url) = redis_url() else {
        return;
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let rl = build_rate_limiter(&url, 1, 1000).await;

        let a = key("a");
        let b = key("b");
        let rate = RateLimit::try_from(2f64).unwrap();

        saturate_to_rejected(&rl, &a, &rate, 2).await;

        let db = rl.redis().absolute().inc(&b, &rate, 1).await.unwrap();
        assert!(matches!(db, RateLimitDecision::Allowed));
    });
}

#[test]
fn unblocks_after_window_expires() {
    let Some(url) = redis_url() else {
        return;
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let rl = build_rate_limiter(&url, 1, 1000).await;
        let k = key("k");
        let rate = RateLimit::try_from(3f64).unwrap();

        // capacity = 3
        saturate_to_rejected(&rl, &k, &rate, 3).await;

        thread::sleep(Duration::from_millis(1100));

        eventually(
            Duration::from_secs(2),
            Duration::from_millis(10),
            || async {
                let d = rl.redis().absolute().is_allowed(&k).await.unwrap();
                matches!(d, RateLimitDecision::Allowed).then_some(())
            },
        )
        .await;
    });
}

#[test]
fn rejected_includes_retry_after_and_remaining_after_waiting() {
    let Some(url) = redis_url() else {
        return;
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // capacity = 6s * 1/s = 6
        let rl = build_rate_limiter(&url, 6, 200).await;
        let k = key("k");
        let rate = RateLimit::try_from(1f64).unwrap();

        let d1 = rl.redis().absolute().inc(&k, &rate, 2).await.unwrap();
        assert!(matches!(d1, RateLimitDecision::Allowed));
        thread::sleep(Duration::from_millis(250));
        let d2 = rl.redis().absolute().inc(&k, &rate, 4).await.unwrap();
        assert!(matches!(d2, RateLimitDecision::Allowed));

        let decision = eventually(
            Duration::from_secs(2),
            Duration::from_millis(10),
            || async {
                let d = rl.redis().absolute().is_allowed(&k).await.unwrap();
                match d {
                    RateLimitDecision::Rejected { .. } => Some(d),
                    _ => None,
                }
            },
        )
        .await;

        let RateLimitDecision::Rejected {
            window_size_seconds,
            retry_after_ms,
            remaining_after_waiting,
        } = decision
        else {
            unreachable!();
        };

        assert_eq!(window_size_seconds, 6);
        assert!(retry_after_ms > 0);
        assert!(retry_after_ms <= 6000);
        assert_eq!(remaining_after_waiting, 2);
    });
}

#[test]
fn rate_grouping_merges_within_group_observable_via_remaining_after_waiting() {
    let Some(url) = redis_url() else {
        return;
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // window=6s, rate=1/s => capacity=6
        // group size=200ms: increments within 200ms should coalesce.
        let rl = build_rate_limiter(&url, 6, 200).await;
        let k = key("k");
        let rate = RateLimit::try_from(1f64).unwrap();

        let d1 = rl.redis().absolute().inc(&k, &rate, 1).await.unwrap();
        assert!(matches!(d1, RateLimitDecision::Allowed));
        thread::sleep(Duration::from_millis(50));
        let d2 = rl.redis().absolute().inc(&k, &rate, 1).await.unwrap();
        assert!(matches!(d2, RateLimitDecision::Allowed));

        thread::sleep(Duration::from_millis(250));
        let d3 = rl.redis().absolute().inc(&k, &rate, 4).await.unwrap();
        assert!(matches!(d3, RateLimitDecision::Allowed));

        let decision = eventually(
            Duration::from_secs(2),
            Duration::from_millis(10),
            || async {
                let d = rl.redis().absolute().is_allowed(&k).await.unwrap();
                match d {
                    RateLimitDecision::Rejected { .. } => Some(d),
                    _ => None,
                }
            },
        )
        .await;

        let RateLimitDecision::Rejected {
            remaining_after_waiting,
            ..
        } = decision
        else {
            unreachable!();
        };

        // The oldest bucket should be the coalesced 2-count bucket.
        assert_eq!(remaining_after_waiting, 2);
    });
}

#[test]
fn rate_grouping_separates_beyond_group_observable_via_remaining_after_waiting() {
    let Some(url) = redis_url() else {
        return;
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // window=6s, rate=1/s => capacity=6
        // group size=200ms: increments beyond 200ms should not coalesce.
        let rl = build_rate_limiter(&url, 6, 200).await;
        let k = key("k");
        let rate = RateLimit::try_from(1f64).unwrap();

        let d1 = rl.redis().absolute().inc(&k, &rate, 1).await.unwrap();
        assert!(matches!(d1, RateLimitDecision::Allowed));
        thread::sleep(Duration::from_millis(250));
        let d2 = rl.redis().absolute().inc(&k, &rate, 1).await.unwrap();
        assert!(matches!(d2, RateLimitDecision::Allowed));

        thread::sleep(Duration::from_millis(250));
        let d3 = rl.redis().absolute().inc(&k, &rate, 4).await.unwrap();
        assert!(matches!(d3, RateLimitDecision::Allowed));

        let decision = eventually(
            Duration::from_secs(2),
            Duration::from_millis(10),
            || async {
                let d = rl.redis().absolute().is_allowed(&k).await.unwrap();
                match d {
                    RateLimitDecision::Rejected { .. } => Some(d),
                    _ => None,
                }
            },
        )
        .await;

        let RateLimitDecision::Rejected {
            remaining_after_waiting,
            ..
        } = decision
        else {
            unreachable!();
        };

        assert_eq!(remaining_after_waiting, 1);
    });
}

#[test]
fn cleanup_loop_removes_stale_entities_without_waiting_for_window_expiry() {
    let Some(url) = redis_url() else {
        return;
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Use a long window so "Allowed" after cleanup can't be explained by natural expiry.
        let rl = build_rate_limiter(&url, 30, 1000).await;
        let k = key("k");
        let rate = RateLimit::try_from(1f64).unwrap();

        // capacity = 30
        saturate_to_rejected(&rl, &k, &rate, 30).await;

        rl.run_cleanup_loop_with_config(50, 50);

        eventually(
            Duration::from_secs(2),
            Duration::from_millis(20),
            || async {
                let d = rl.redis().absolute().is_allowed(&k).await.unwrap();
                matches!(d, RateLimitDecision::Allowed).then_some(())
            },
        )
        .await;

        rl.stop_cleanup_loop();
    });
}

#[test]
fn stop_cleanup_loop_prevents_redis_cleanup() {
    let Some(url) = redis_url() else {
        return;
    };

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let rl = build_rate_limiter(&url, 30, 1000).await;
        let k = key("k");
        let rate = RateLimit::try_from(1f64).unwrap();

        // capacity = 30
        saturate_to_rejected(&rl, &k, &rate, 30).await;

        // Start loop with an interval, then stop immediately.
        rl.run_cleanup_loop_with_config(50, 200);
        rl.stop_cleanup_loop();
        rl.stop_cleanup_loop();

        thread::sleep(Duration::from_millis(320));

        let d = rl.redis().absolute().is_allowed(&k).await.unwrap();
        assert!(matches!(d, RateLimitDecision::Rejected { .. }));
    });
}
