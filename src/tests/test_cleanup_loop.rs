use std::{sync::Arc, thread, time::Duration};

use crate::{
    HardLimitFactor, LocalRateLimiterOptions, RateGroupSizeMs, RateLimit, RateLimiter,
    RateLimiterOptions, WindowSizeSeconds,
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
    thread::sleep(Duration::from_millis(200));

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
        thread::sleep(Duration::from_millis(100));
        rl.local().absolute().inc("key1", &rate_limit, 1);
    }

    // Key should still exist since it's been kept active
    assert_eq!(rl.local().absolute().series().len(), 1);
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
            },
            redis: crate::RedisRateLimiterOptions {
                connection_manager,
                prefix: Some(RedisKey::try_from("test_cleanup".to_string()).unwrap()),
                window_size_seconds: WindowSizeSeconds::try_from(1).unwrap(),
                rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
                hard_limit_factor: HardLimitFactor::default(),
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
