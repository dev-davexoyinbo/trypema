use std::{sync::Arc, time::Duration};

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
use crate::common::RateType;
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
use crate::hybrid::SyncIntervalMs;
use crate::{
    HardLimitFactor, LocalRateLimiterOptions, RateGroupSizeMs, RateLimit, RateLimiter,
    RateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds,
};

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
use redis::AsyncCommands;

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
use super::runtime;

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
use super::common::key_gen;

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

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn test_redis_cleanup_loop_runs() {
    use crate::RedisKey;

    runtime::block_on(async {
        let Ok(url) = std::env::var("REDIS_URL") else {
            return;
        };

        let client = redis::Client::open(url).unwrap();
        let connection_manager = match client.get_connection_manager().await {
            Ok(cm) => cm,
            Err(_) => return,
        };

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
                sync_interval_ms: SyncIntervalMs::default(),
            },
        };

        let rl = Arc::new(RateLimiter::new(options));

        let rate_limit = RateLimit::try_from(10.0).unwrap();
        let key = RedisKey::try_from("test_key".to_string()).unwrap();
        let _ = rl.redis().absolute().inc(&key, &rate_limit, 1).await;

        rl.run_cleanup_loop_with_config(100, 50);
        runtime::async_sleep(Duration::from_millis(200)).await;
    });
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn test_redis_stop_cleanup_loop_prevents_cleanup() {
    use crate::{RateLimitDecision, RedisKey};

    runtime::block_on(async {
        let Ok(url) = std::env::var("REDIS_URL") else {
            return;
        };
        let client = redis::Client::open(url).unwrap();
        let connection_manager = match client.get_connection_manager().await {
            Ok(cm) => cm,
            Err(_) => return,
        };

        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let prefix = RedisKey::try_from(format!("test_stop_cleanup_{unique}")).unwrap();

        let window_size_seconds = WindowSizeSeconds::try_from(5).unwrap();
        let rate_group_size_ms = RateGroupSizeMs::try_from(100).unwrap();

        let options = RateLimiterOptions {
            local: LocalRateLimiterOptions {
                window_size_seconds,
                rate_group_size_ms,
                hard_limit_factor: HardLimitFactor::default(),
                suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
            },
            redis: crate::RedisRateLimiterOptions {
                connection_manager: connection_manager.clone(),
                prefix: Some(prefix.clone()),
                window_size_seconds,
                rate_group_size_ms,
                hard_limit_factor: HardLimitFactor::default(),
                suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
                sync_interval_ms: SyncIntervalMs::default(),
            },
        };

        let rl = Arc::new(RateLimiter::new(options));

        let rate_limit = RateLimit::try_from(1.0).unwrap();
        let key = RedisKey::try_from(format!("test_key_{unique}")).unwrap();

        // window_size=5s, rate=1/s => capacity=5.
        let decision = rl
            .redis()
            .absolute()
            .inc(&key, &rate_limit, 5)
            .await
            .unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "check 1 decision={decision:?}"
        );

        let d0 = rl.redis().absolute().is_allowed(&key).await.unwrap();
        assert!(matches!(d0, RateLimitDecision::Rejected { .. }));

        // Start loop with a long interval (first Redis cleanup would occur after the first tick).
        rl.run_cleanup_loop_with_config(50, 200);

        // Stop immediately (idempotent).
        rl.stop_cleanup_loop();
        rl.stop_cleanup_loop();

        // Wait longer than the interval; if cleanup ran, it would delete the keys.
        runtime::async_sleep(Duration::from_millis(320)).await;

        // Still within the 5s window, so we should remain rejected if cleanup did not run.
        let d1 = rl.redis().absolute().is_allowed(&key).await.unwrap();
        assert!(matches!(d1, RateLimitDecision::Rejected { .. }));
    });
}

// ---------------------------------------------------------------------------
// Hybrid cleanup loop tests
// ---------------------------------------------------------------------------

/// The cleanup loop must remove stale hybrid absolute Redis keys automatically.
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn test_hybrid_absolute_cleanup_loop_removes_stale_redis_keys() {
    use crate::{RateLimitDecision, RedisKey};

    runtime::block_on(async {
        let Ok(url) = std::env::var("REDIS_URL") else {
            return;
        };
        let client = redis::Client::open(url.clone()).unwrap();
        let connection_manager = match client.get_connection_manager().await {
            Ok(cm) => cm,
            Err(_) => return,
        };

        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let prefix = RedisKey::try_from(format!("test_hybrid_abs_cleanup_{unique}")).unwrap();
        let sync_interval_ms = 25_u64;

        let options = RateLimiterOptions {
            local: LocalRateLimiterOptions {
                window_size_seconds: WindowSizeSeconds::try_from(5).unwrap(),
                rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
                hard_limit_factor: HardLimitFactor::default(),
                suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
            },
            redis: crate::RedisRateLimiterOptions {
                connection_manager,
                prefix: Some(prefix.clone()),
                window_size_seconds: WindowSizeSeconds::try_from(5).unwrap(),
                rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
                hard_limit_factor: HardLimitFactor::default(),
                suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
                sync_interval_ms: SyncIntervalMs::try_from(sync_interval_ms).unwrap(),
            },
        };

        let rl = Arc::new(RateLimiter::new(options));

        let rate_limit = RateLimit::try_from(2.0).unwrap();
        let k = RedisKey::try_from(format!("k_{unique}")).unwrap();
        let cap = 5_u64 * 2; // window=5s, rate=2/s => cap=10

        // Overflow to trigger a Redis commit.
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
        assert!(
            matches!(d, RateLimitDecision::Rejected { .. }),
            "expected Rejected after overflow: {d:?}"
        );

        runtime::async_sleep(Duration::from_millis(sync_interval_ms * 2 + 50)).await;

        let kg = key_gen(&prefix, RateType::HybridAbsolute);
        let total_key = kg.get_total_count_key(&k);
        let active_entities_key = kg.get_active_entities_key();

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let exists: bool = conn.exists(&total_key).await.unwrap();
        assert!(
            exists,
            "hybrid absolute total key must exist before cleanup loop fires"
        );

        rl.run_cleanup_loop_with_config(100, 50);
        runtime::async_sleep(Duration::from_millis(350)).await;
        rl.stop_cleanup_loop();

        for entity_key in kg.get_all_entity_keys(&k) {
            let exists: bool = conn.exists(&entity_key).await.unwrap();
            assert!(!exists, "key {entity_key} must be absent after cleanup loop");
        }

        let score: Option<f64> = conn.zscore(&active_entities_key, k.as_str()).await.unwrap();
        assert!(
            score.is_none(),
            "entity must be removed from active_entities after cleanup loop"
        );
    });
}

/// The cleanup loop must leave active hybrid absolute entities untouched.
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn test_hybrid_absolute_cleanup_loop_keeps_active_entity() {
    use crate::{RateLimitDecision, RedisKey};

    runtime::block_on(async {
        let Ok(url) = std::env::var("REDIS_URL") else {
            return;
        };
        let client = redis::Client::open(url.clone()).unwrap();
        let connection_manager = match client.get_connection_manager().await {
            Ok(cm) => cm,
            Err(_) => return,
        };

        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let prefix = RedisKey::try_from(format!("test_hybrid_abs_active_{unique}")).unwrap();
        let sync_interval_ms = 25_u64;

        let options = RateLimiterOptions {
            local: LocalRateLimiterOptions {
                window_size_seconds: WindowSizeSeconds::try_from(5).unwrap(),
                rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
                hard_limit_factor: HardLimitFactor::default(),
                suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
            },
            redis: crate::RedisRateLimiterOptions {
                connection_manager,
                prefix: Some(prefix.clone()),
                window_size_seconds: WindowSizeSeconds::try_from(5).unwrap(),
                rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
                hard_limit_factor: HardLimitFactor::default(),
                suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
                sync_interval_ms: SyncIntervalMs::try_from(sync_interval_ms).unwrap(),
            },
        };

        let rl = Arc::new(RateLimiter::new(options));

        let rate_limit = RateLimit::try_from(2.0).unwrap();
        let k = RedisKey::try_from(format!("k_{unique}")).unwrap();
        let cap = 5_u64 * 2;

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
        assert!(
            matches!(d, RateLimitDecision::Rejected { .. }),
            "expected Rejected: {d:?}"
        );
        runtime::async_sleep(Duration::from_millis(sync_interval_ms * 2 + 50)).await;

        let total_key = key_gen(&prefix, RateType::HybridAbsolute).get_total_count_key(&k);

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let exists: bool = conn.exists(&total_key).await.unwrap();
        assert!(exists, "total key must exist before cleanup loop");

        // stale_after_ms = 5000 — entity is recent, loop interval = 100ms.
        rl.run_cleanup_loop_with_config(5_000, 100);
        runtime::async_sleep(Duration::from_millis(400)).await;
        rl.stop_cleanup_loop();

        let exists_after: bool = conn.exists(&total_key).await.unwrap();
        assert!(
            exists_after,
            "active entity must NOT be removed by cleanup loop"
        );
    });
}

/// The cleanup loop must remove stale hybrid suppressed Redis keys automatically.
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn test_hybrid_suppressed_cleanup_loop_removes_stale_redis_keys() {
    use crate::RedisKey;

    runtime::block_on(async {
        let Ok(url) = std::env::var("REDIS_URL") else {
            return;
        };
        let client = redis::Client::open(url.clone()).unwrap();
        let connection_manager = client
            .get_connection_manager()
            .await
            .expect("connection manager should be present");

        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let prefix = RedisKey::try_from(format!("test_hybrid_sup_cleanup_{unique}")).unwrap();
        let sync_interval_ms = 25_u64;

        let options = RateLimiterOptions {
            local: LocalRateLimiterOptions {
                window_size_seconds: WindowSizeSeconds::try_from(5).unwrap(),
                rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
                hard_limit_factor: HardLimitFactor::try_from(2.0).unwrap(),
                suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
            },
            redis: crate::RedisRateLimiterOptions {
                connection_manager,
                prefix: Some(prefix.clone()),
                window_size_seconds: WindowSizeSeconds::try_from(5).unwrap(),
                rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
                hard_limit_factor: HardLimitFactor::try_from(2.0).unwrap(),
                suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
                sync_interval_ms: SyncIntervalMs::try_from(sync_interval_ms).unwrap(),
            },
        };

        let rl = Arc::new(RateLimiter::new(options));

        let rate_limit = RateLimit::try_from(2.0).unwrap();
        let k = RedisKey::try_from(format!("k_{unique}")).unwrap();
        let soft_cap = 5_u64 * 2; // window=5s, rate=2/s => soft_cap=10

        // Overflow past soft limit to trigger a commit.
        for _ in 0..=soft_cap {
            let _ = rl
                .hybrid()
                .suppressed()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
        }
        runtime::async_sleep(Duration::from_millis(sync_interval_ms * 2 + 50)).await;

        let kg = key_gen(&prefix, RateType::HybridSuppressed);
        let total_key = kg.get_total_count_key(&k);
        let active_entities_key = kg.get_active_entities_key();

        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let exists: bool = conn.exists(&total_key).await.unwrap();
        assert!(
            exists,
            "hybrid suppressed total key must exist before cleanup loop fires"
        );

        rl.run_cleanup_loop_with_config(100, 50);
        runtime::async_sleep(Duration::from_millis(350)).await;
        rl.stop_cleanup_loop();

        for entity_key in kg.get_all_entity_keys(&k) {
            let exists: bool = conn.exists(&entity_key).await.unwrap();
            assert!(!exists, "key {entity_key} must be absent after cleanup loop");
        }

        let score: Option<f64> = conn.zscore(&active_entities_key, k.as_str()).await.unwrap();
        assert!(
            score.is_none(),
            "entity must be removed from active_entities after cleanup loop"
        );
    });
}

/// End-to-end: the cleanup loop cleans a stale hybrid absolute entity, and the next request
/// for that entity is allowed (in-memory state was cleared alongside Redis state).
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn test_hybrid_cleanup_loop_fresh_requests_allowed_after_cleanup() {
    use crate::{RateLimitDecision, RedisKey};

    runtime::block_on(async {
        let Ok(url) = std::env::var("REDIS_URL") else {
            return;
        };
        let client = redis::Client::open(url.clone()).unwrap();
        let connection_manager = match client.get_connection_manager().await {
            Ok(cm) => cm,
            Err(_) => return,
        };

        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let prefix = RedisKey::try_from(format!("test_hybrid_loop_fresh_{unique}")).unwrap();
        let sync_interval_ms = 25_u64;

        let options = RateLimiterOptions {
            local: LocalRateLimiterOptions {
                window_size_seconds: WindowSizeSeconds::try_from(5).unwrap(),
                rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
                hard_limit_factor: HardLimitFactor::default(),
                suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
            },
            redis: crate::RedisRateLimiterOptions {
                connection_manager,
                prefix: Some(prefix.clone()),
                window_size_seconds: WindowSizeSeconds::try_from(5).unwrap(),
                rate_group_size_ms: RateGroupSizeMs::try_from(100).unwrap(),
                hard_limit_factor: HardLimitFactor::default(),
                suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
                sync_interval_ms: SyncIntervalMs::try_from(sync_interval_ms).unwrap(),
            },
        };

        let rl = Arc::new(RateLimiter::new(options));

        let rate_limit = RateLimit::try_from(2.0).unwrap();
        let k = RedisKey::try_from(format!("k_{unique}")).unwrap();
        let cap = 5_u64 * 2;

        // Overflow — entity ends up in Rejecting state.
        for _ in 0..cap {
            let _ = rl
                .hybrid()
                .absolute()
                .inc(&k, &rate_limit, 1)
                .await
                .unwrap();
        }
        let rejected = rl
            .hybrid()
            .absolute()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(
            matches!(rejected, RateLimitDecision::Rejected { .. }),
            "expected Rejected after overflow: {rejected:?}"
        );
        runtime::async_sleep(Duration::from_millis(sync_interval_ms * 2 + 50)).await;

        // Start cleanup loop with stale_after_ms=150, interval=75.
        // The entity should be cleaned after roughly 150 + 75 ms from loop start.
        rl.run_cleanup_loop_with_config(150, 75);
        runtime::async_sleep(Duration::from_millis(400)).await;
        rl.stop_cleanup_loop();

        let total_key = key_gen(&prefix, RateType::HybridAbsolute).get_total_count_key(&k);
        let mut conn = redis::Client::open(url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let exists_after: bool = conn.exists(&total_key).await.unwrap();
        assert!(
            !exists_after,
            "hybrid absolute total key must be deleted after cleanup loop"
        );

        // Next request must be allowed — stale in-memory Rejecting state must have been cleared.
        let decision = rl
            .hybrid()
            .absolute()
            .inc(&k, &rate_limit, 1)
            .await
            .unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Allowed),
            "expected Allowed after cleanup loop cleared state, got {decision:?}"
        );
    });
}
