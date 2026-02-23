use std::{sync::Arc, time::Duration};

use crate::{
    HardLimitFactor, LocalRateLimiterOptions, RateGroupSizeMs, RateLimit, RateLimiter,
    RateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds,
};

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
#[test]
fn test_local_cleanup_loop_runs() {
    // Create a rate limiter with local support only
    let options = RateLimiterOptions {
        local: LocalRateLimiterOptions {
            window_size_seconds: WindowSizeSeconds::try_from(1).unwrap(),
            rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
            hard_limit_factor: HardLimitFactor::default(),
            suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
        },
    };

    let rl = Arc::new(RateLimiter::new(options));

    // Add some entries
    let rate_limit = RateLimit::try_from(10.0).unwrap();
    rl.local().absolute().inc("key1", &rate_limit, 1);
    rl.local().absolute().inc("key2", &rate_limit, 1);
    rl.local().absolute().inc("key3", &rate_limit, 1);

    // Verify entries exist
    assert_eq!(rl.local().absolute().series().len(), 3);

    // Start cleanup loop with aggressive timing (100ms stale, 50ms interval)
    rl.run_cleanup_loop_with_config(100, 50);

    // Wait for entries to become stale and be cleaned up
    std::thread::sleep(Duration::from_millis(200));

    // Entries should be cleaned up now
    assert_eq!(rl.local().absolute().series().len(), 0);
}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
#[test]
fn test_cleanup_loop_keeps_active_entries() {
    let options = RateLimiterOptions {
        local: LocalRateLimiterOptions {
            window_size_seconds: WindowSizeSeconds::try_from(1).unwrap(),
            rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
            hard_limit_factor: HardLimitFactor::default(),
            suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
        },
    };

    let rl = Arc::new(RateLimiter::new(options));

    // Add initial entries
    let rate_limit = RateLimit::try_from(10.0).unwrap();
    rl.local().absolute().inc("key1", &rate_limit, 1);

    // Start cleanup with 500ms stale threshold, 100ms interval
    rl.run_cleanup_loop_with_config(500, 100);

    // Keep the key active by updating it periodically
    for _ in 0..5 {
        std::thread::sleep(Duration::from_millis(100));
        rl.local().absolute().inc("key1", &rate_limit, 1);
    }

    // Key should still exist since it's been kept active
    assert_eq!(rl.local().absolute().series().len(), 1);
}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
#[test]
fn test_stop_cleanup_loop_prevents_future_cleanup() {
    let options = RateLimiterOptions {
        local: LocalRateLimiterOptions {
            window_size_seconds: WindowSizeSeconds::try_from(1).unwrap(),
            rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
            hard_limit_factor: HardLimitFactor::default(),
            suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
        },
    };

    let rl = Arc::new(RateLimiter::new(options));

    let rate_limit = RateLimit::try_from(10.0).unwrap();
    rl.local().absolute().inc("key1", &rate_limit, 1);
    assert_eq!(rl.local().absolute().series().len(), 1);

    // Start the loop with a stale threshold the key will only exceed later.
    // The cleanup thread runs immediately on start, so ensure we stop before the key is stale.
    rl.run_cleanup_loop_with_config(100, 80);
    std::thread::sleep(Duration::from_millis(20));

    // Idempotent stop
    rl.stop_cleanup_loop();
    rl.stop_cleanup_loop();

    // If the loop were still running, a later tick would observe the key as stale and remove it.
    std::thread::sleep(Duration::from_millis(220));
    assert_eq!(rl.local().absolute().series().len(), 1);
}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
#[test]
fn test_run_cleanup_loop_with_config_is_idempotent() {
    let options = RateLimiterOptions {
        local: LocalRateLimiterOptions {
            window_size_seconds: WindowSizeSeconds::try_from(1).unwrap(),
            rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
            hard_limit_factor: HardLimitFactor::default(),
            suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
        },
    };

    let rl = Arc::new(RateLimiter::new(options));

    let rate_limit = RateLimit::try_from(10.0).unwrap();
    rl.local().absolute().inc("key1", &rate_limit, 1);
    assert_eq!(rl.local().absolute().series().len(), 1);

    // Start with a long stale threshold.
    rl.run_cleanup_loop_with_config(5_000, 50);

    // Second call should be a no-op (no reconfiguration / no second loop).
    rl.run_cleanup_loop_with_config(10, 50);

    // Wait long enough that the key would be stale under the second configuration.
    std::thread::sleep(Duration::from_millis(120));
    assert_eq!(rl.local().absolute().series().len(), 1);

    rl.stop_cleanup_loop();
}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
#[test]
fn test_stop_then_restart_cleanup_loop_works() {
    let options = RateLimiterOptions {
        local: LocalRateLimiterOptions {
            window_size_seconds: WindowSizeSeconds::try_from(1).unwrap(),
            rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
            hard_limit_factor: HardLimitFactor::default(),
            suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
        },
    };

    let rl = Arc::new(RateLimiter::new(options));

    let rate_limit = RateLimit::try_from(10.0).unwrap();
    rl.local().absolute().inc("key1", &rate_limit, 1);
    assert_eq!(rl.local().absolute().series().len(), 1);

    rl.run_cleanup_loop_with_config(100, 80);
    std::thread::sleep(Duration::from_millis(20));
    rl.stop_cleanup_loop();
    std::thread::sleep(Duration::from_millis(220));
    assert_eq!(rl.local().absolute().series().len(), 1);

    // Restart: now the key is stale and should be removed by the first cleanup.
    rl.run_cleanup_loop_with_config(100, 80);
    std::thread::sleep(Duration::from_millis(120));
    assert_eq!(rl.local().absolute().series().len(), 0);
}

#[cfg(feature = "redis-tokio")]
#[test]
fn test_redis_cleanup_loop_with_tokio() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let url = std::env::var("REDIS_URL").unwrap_or_else(|_| {
            eprintln!("skipping: REDIS_URL not set");
            String::new()
        });
        if url.is_empty() {
            return;
        }

        let client = redis::Client::open(url).unwrap();
        let connection_manager = client.get_connection_manager().await.unwrap();

        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let prefix = crate::RedisKey::try_from(format!("test_cleanup_{unique}")).unwrap();

        let options = RateLimiterOptions {
            local: LocalRateLimiterOptions {
                window_size_seconds: WindowSizeSeconds::try_from(1).unwrap(),
                rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
                hard_limit_factor: HardLimitFactor::default(),
                suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
            },
            redis: crate::RedisRateLimiterOptions {
                connection_manager,
                prefix: Some(prefix),
                // Use a long window so Allowed-after-cleanup can't be natural expiry.
                window_size_seconds: WindowSizeSeconds::try_from(30).unwrap(),
                rate_group_size_ms: RateGroupSizeMs::try_from(1000).unwrap(),
                hard_limit_factor: HardLimitFactor::default(),
                suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
            },
        };

        let rl = Arc::new(RateLimiter::new(options));

        let rate_limit = RateLimit::try_from(1.0).unwrap();
        let key = crate::RedisKey::try_from(format!("test_key_{unique}")).unwrap();

        // capacity = 30 (30s * 1/s)
        let d0 = rl.redis().absolute().inc(&key, &rate_limit, 30).await.unwrap();
        assert!(matches!(d0, crate::RateLimitDecision::Allowed));

        // Wait until state is observed as rejected (commit has applied).
        let start = std::time::Instant::now();
        loop {
            let d = rl.redis().absolute().is_allowed(&key).await.unwrap();
            if matches!(d, crate::RateLimitDecision::Rejected { .. }) {
                break;
            }
            if start.elapsed() > Duration::from_secs(2) {
                panic!("expected key to become rejected after saturating");
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        // Start cleanup loop and wait for it to delete stale state.
        rl.run_cleanup_loop_with_config(50, 50);

        let start = std::time::Instant::now();
        loop {
            let d = rl.redis().absolute().is_allowed(&key).await.unwrap();
            if matches!(d, crate::RateLimitDecision::Allowed) {
                break;
            }
            if start.elapsed() > Duration::from_secs(2) {
                panic!("expected cleanup loop to make key allowed");
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        rl.stop_cleanup_loop();
    });
}

#[cfg(feature = "redis-tokio")]
#[test]
fn test_redis_stop_cleanup_loop_prevents_cleanup() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let url = std::env::var("REDIS_URL").unwrap_or_else(|_| {
            eprintln!("skipping: REDIS_URL not set");
            String::new()
        });
        if url.is_empty() {
            return;
        }

        let client = redis::Client::open(url).unwrap();
        let connection_manager = client.get_connection_manager().await.unwrap();

        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let prefix = crate::RedisKey::try_from(format!("test_stop_cleanup_{}", unique)).unwrap();

        let options = RateLimiterOptions {
            local: LocalRateLimiterOptions {
                window_size_seconds: WindowSizeSeconds::try_from(1).unwrap(),
                rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
                hard_limit_factor: HardLimitFactor::default(),
                suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
            },
            redis: crate::RedisRateLimiterOptions {
                connection_manager: connection_manager.clone(),
                prefix: Some(prefix.clone()),
                window_size_seconds: WindowSizeSeconds::try_from(1).unwrap(),
                rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
                hard_limit_factor: HardLimitFactor::default(),
                suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
            },
        };

        let rl = Arc::new(RateLimiter::new(options));

        // Use a long window so Allowed can't be natural expiry.
        let rate_limit = RateLimit::try_from(1.0).unwrap();
        let key = crate::RedisKey::try_from(format!("test_key_{}", unique)).unwrap();

        // capacity = 1s * 1/s = 1
        let d0 = rl.redis().absolute().inc(&key, &rate_limit, 1).await.unwrap();
        assert!(matches!(d0, crate::RateLimitDecision::Allowed));

        // Wait until the key is observed as rejected (commit has applied).
        let start = std::time::Instant::now();
        loop {
            let d = rl.redis().absolute().is_allowed(&key).await.unwrap();
            if matches!(d, crate::RateLimitDecision::Rejected { .. }) {
                break;
            }
            if start.elapsed() > Duration::from_secs(2) {
                panic!("expected key to become rejected after saturating");
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        // Start loop with a long interval (first Redis cleanup would occur after the first tick).
        rl.run_cleanup_loop_with_config(50, 200);

        // Stop immediately (idempotent).
        rl.stop_cleanup_loop();
        rl.stop_cleanup_loop();

        // Wait longer than the interval; if cleanup ran, it would delete state and allow the key.
        std::thread::sleep(Duration::from_millis(320));

        let d = rl.redis().absolute().is_allowed(&key).await.unwrap();
        assert!(matches!(d, crate::RateLimitDecision::Rejected { .. }));
    });
}
