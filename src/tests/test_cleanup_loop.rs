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
    use crate::RedisKey;

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Setup Redis connection
        let client = redis::Client::open(
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string()),
        )
        .unwrap();
        let connection_manager = client.get_connection_manager().await.unwrap();

        let options = RateLimiterOptions {
            local: LocalRateLimiterOptions {
                window_size_seconds: WindowSizeSeconds::try_from(1).unwrap(),
                rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
                hard_limit_factor: HardLimitFactor::default(),
                suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
            },
            redis: crate::RedisRateLimiterOptions {
                connection_manager,
                prefix: Some(RedisKey::try_from("test_cleanup".to_string()).unwrap()),
                window_size_seconds: WindowSizeSeconds::try_from(1).unwrap(),
                rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
                hard_limit_factor: HardLimitFactor::default(),
                suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
            },
        };

        let rl = Arc::new(RateLimiter::new(options));

        // Add some Redis entries
        let rate_limit = RateLimit::try_from(10.0).unwrap();
        let key = RedisKey::try_from("test_key".to_string()).unwrap();
        let _ = rl.redis().absolute().inc(&key, &rate_limit, 1).await;

        // Start cleanup loop (it will detect the Tokio runtime)
        rl.run_cleanup_loop_with_config(100, 50);

        // Wait a bit for cleanup to run
        tokio::time::sleep(Duration::from_millis(200)).await;

        // The key should be cleaned up after becoming stale
        // (We can't easily verify Redis state without adding more test infrastructure,
        // but at least verify the loop runs without panicking)
    });
}

#[cfg(feature = "redis-tokio")]
#[test]
fn test_redis_stop_cleanup_loop_prevents_cleanup() {
    use redis::AsyncCommands;

    use crate::{RedisKey, RedisKeyGenerator, common::RateType};

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let client = redis::Client::open(
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string()),
        )
        .unwrap();
        let connection_manager = client.get_connection_manager().await.unwrap();

        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let prefix = RedisKey::try_from(format!("test_stop_cleanup_{}", unique)).unwrap();

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

        let rate_limit = RateLimit::try_from(10.0).unwrap();
        let key = RedisKey::try_from(format!("test_key_{}", unique)).unwrap();
        let _ = rl.redis().absolute().inc(&key, &rate_limit, 1).await;

        // Sanity: ensure the keys exist before cleanup might run.
        let kg = RedisKeyGenerator::new(prefix.clone(), RateType::Absolute);
        let mut conn = connection_manager.clone();
        let h_exists: bool = conn.exists(kg.get_hash_key(&key)).await.unwrap();
        assert!(h_exists, "expected hash key to exist before cleanup");

        // Start loop with a long interval (first Redis cleanup would occur after the first tick).
        rl.run_cleanup_loop_with_config(50, 200);

        // Stop immediately (idempotent).
        rl.stop_cleanup_loop();
        rl.stop_cleanup_loop();

        // Wait longer than the interval; if cleanup ran, it would delete the keys.
        tokio::time::sleep(Duration::from_millis(320)).await;

        let mut conn = connection_manager.clone();
        let h_exists: bool = conn.exists(kg.get_hash_key(&key)).await.unwrap();
        let a_exists: bool = conn.exists(kg.get_active_keys(&key)).await.unwrap();
        let w_exists: bool = conn.exists(kg.get_window_limit_key(&key)).await.unwrap();
        let t_exists: bool = conn.exists(kg.get_total_count_key(&key)).await.unwrap();
        assert!(
            h_exists && a_exists && w_exists && t_exists,
            "expected keys to remain after stopping cleanup loop"
        );

        let ae_key = kg.get_active_entities_key();
        let score: Option<f64> = conn.zscore(&ae_key, &**key).await.unwrap();
        assert!(score.is_some(), "expected active_entities entry to remain");
    });
}
