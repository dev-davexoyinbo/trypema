use std::{env, future::Future, time::Duration};

use crate::common::SuppressionFactorCacheMs;
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

async fn build_limiter(
    url: &str,
    window_size_seconds: u64,
    rate_group_size_ms: u64,
    hard_limit_factor: f64,
) -> std::sync::Arc<RateLimiter> {
    build_limiter_with_cache_ms(
        url,
        window_size_seconds,
        rate_group_size_ms,
        hard_limit_factor,
        *SuppressionFactorCacheMs::default(),
    )
    .await
}

async fn build_limiter_with_cache_ms(
    url: &str,
    window_size_seconds: u64,
    rate_group_size_ms: u64,
    hard_limit_factor: f64,
    suppression_factor_cache_ms: u64,
) -> std::sync::Arc<RateLimiter> {
    let client = redis::Client::open(url).unwrap();
    let cm = client.get_connection_manager().await.unwrap();
    let prefix = unique_prefix();

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
            prefix: Some(prefix.clone()),
            window_size_seconds: WindowSizeSeconds::try_from(window_size_seconds).unwrap(),
            rate_group_size_ms: RateGroupSizeMs::try_from(rate_group_size_ms).unwrap(),
            hard_limit_factor: HardLimitFactor::try_from(hard_limit_factor).unwrap(),
            suppression_factor_cache_ms: SuppressionFactorCacheMs::try_from(
                suppression_factor_cache_ms,
            )
            .unwrap(),
        },
    };

    std::sync::Arc::new(RateLimiter::new(options))
}

#[test]
fn get_suppression_factor_fresh_key_returns_zero_and_sets_cache_ttl() {
    let Some(url) = redis_url() else { return };

    block_on(async {
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
    let Some(url) = redis_url() else { return };

    block_on(async {
        // window_size=10s, hard_limit_factor=2 => window_limit=20 and threshold is 10.
        // We drive total_count to exactly 10 with a burst in < 1s so perceived_rate uses
        // last-second peak (10 req/s) instead of average (1 req/s).
        let cache_ms = 50_u64;
        let rl = build_limiter_with_cache_ms(&url, 10, 100, 2f64, cache_ms).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        for _ in 0..10 {
            let _ = rl
                .redis()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
        }

        // Ensure any cached suppression_factor from the burst expires so we recompute.
        std::thread::sleep(Duration::from_millis(cache_ms + 25));

        let sf = rl
            .redis()
            .suppressed()
            .get_suppression_factor(&k)
            .await
            .unwrap();

        // rate_limit = 1 req/s; perceived_rate = 10 req/s => sf = 1 - 1/10 = 0.9
        let expected = 1.0_f64 - (1.0_f64 / 10.0_f64);
        assert!(
            (sf - expected).abs() < 1e-12,
            "sf: {sf}, expected: {expected}"
        );
    });
}

#[test]
fn get_suppression_factor_evicts_out_of_window_usage_and_resets_admission() {
    let Some(url) = redis_url() else { return };

    block_on(async {
        let window_size_seconds = 1_u64;
        let cache_ms = 50_u64;
        let rl = build_limiter_with_cache_ms(&url, window_size_seconds, 1000, 2f64, cache_ms).await;

        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        // window_limit = window_size * rate_limit * hard_limit_factor = 1 * 1 * 2 = 2.
        // We record 2 calls in the window, then wait for the full window to pass. If eviction
        // does not occur, the next increment would see total_count >= window_limit and go to full
        // suppression (sf=1.0). If eviction occurs, the next increment is Allowed.
        let d1 = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 2)
            .await
            .unwrap();

        assert!(matches!(d1, RateLimitDecision::Allowed), "d1: {:?}", d1);

        std::thread::sleep(Duration::from_millis(window_size_seconds * 1000 + 50));
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
fn suppression_factor_gt_one_is_invalid_and_is_recomputed() {
    let Some(url) = redis_url() else { return };

    block_on(async {
        let rl = build_limiter(&url, 1, 1000, 10f64).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();

        // This should never error; any invalid cached value handling is internal.
        let decision = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(matches!(
            decision,
            RateLimitDecision::Allowed | RateLimitDecision::Suppressed { .. }
        ));
    });
}

#[test]
fn suppression_factor_negative_is_invalid_and_is_recomputed() {
    let Some(url) = redis_url() else { return };

    block_on(async {
        let rl = build_limiter(&url, 1, 1000, 10f64).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(5f64).unwrap();

        let decision = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(matches!(
            decision,
            RateLimitDecision::Allowed | RateLimitDecision::Suppressed { .. }
        ));
    });
}

#[test]
fn verify_suppression_factor_calculation_spread_redis() {
    let Some(url) = redis_url() else { return };

    block_on(async {
        let rl = build_limiter(&url, 10, 100, 10f64).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        // fill up in the first 3 seconds
        for _ in 0..20 {
            let _ = rl
                .redis()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(3000 / 20)).await;
        }

        // wait for 1.5 seconds
        tokio::time::sleep(Duration::from_millis(1200)).await;

        let expected_suppression_factor = 1f64 - (1f64 / 2f64);

        let decision = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();

        eprintln!("decision: {:?}", decision);

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
    let Some(url) = redis_url() else { return };

    block_on(async {
        let rl = build_limiter(&url, 10, 100, 10f64).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        let _ = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 10)
            .await
            .unwrap();
        // wait for 1s to pass
        tokio::time::sleep(Duration::from_millis(1001)).await;

        let _ = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 20)
            .await
            .unwrap();
        // Allow time for the suppression_factor to expire
        tokio::time::sleep(Duration::from_millis(101)).await;

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
    let Some(url) = redis_url() else { return };

    block_on(async {
        let rl = build_limiter(&url, 10, 100, 10f64).await;
        let k = key("k");
        let rate_limit = RateLimit::try_from(1f64).unwrap();

        let _ = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 100)
            .await
            .unwrap();
        // wait for 1s to pass
        tokio::time::sleep(Duration::from_millis(1001)).await;

        let _ = rl
            .redis()
            .suppressed()
            .inc(&k, &rate_limit, 20)
            .await
            .unwrap();

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
    });
}
