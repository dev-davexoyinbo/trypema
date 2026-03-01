use std::{env, future::Future, thread, time::Duration};

use crate::common::SuppressionFactorCacheMs;
use crate::hybrid::SyncIntervalMs;
use crate::{
    HardLimitFactor, LocalRateLimiterOptions, RateGroupSizeMs, RateLimit, RateLimitDecision,
    RateLimiter, RateLimiterOptions, RedisKey, RedisRateLimiterOptions, WindowSizeSeconds,
};

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
fn hybrid_usage_is_committed_to_redis_on_flush_and_then_visible_to_others() {
    let Some(url) = redis_url() else { return };

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
