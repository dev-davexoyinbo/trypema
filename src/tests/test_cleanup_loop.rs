use std::time::Duration;

use crate::{RateLimit, RateLimiterBuilder, WindowSize, local::LocalRateLimiterProvider};

#[test]
fn local_cleanup_removes_stale_entries_and_keeps_active_entries() {
    let provider = LocalRateLimiterProvider::builder()
        .window_size(WindowSize::seconds_or_panic(1))
        .stale_after(Duration::from_millis(100))
        .cleanup_interval(Duration::from_millis(40))
        .build()
        .unwrap();
    let rate = RateLimit::per_second_or_panic(100.0);

    provider.absolute().inc("stale", &rate, 1);
    provider.absolute().inc("active", &rate, 1);
    for _ in 0..4 {
        std::thread::sleep(Duration::from_millis(40));
        provider.absolute().inc("active", &rate, 1);
    }
    std::thread::sleep(Duration::from_millis(60));

    assert!(provider.absolute().series().get("stale").is_none());
    assert!(provider.absolute().series().get("active").is_some());
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn redis_and_hybrid_cleanup_start_by_default() {
    use redis::AsyncCommands;

    use crate::{
        BucketSize,
        common::RateType,
        hybrid::{HybridRateLimiterProvider, SyncInterval},
        redis::RedisRateLimiterProvider,
    };

    super::runtime::block_on(async {
        let connection_manager = super::common::connection_manager().await;
        let rate = RateLimit::per_second_or_panic(100.0);

        let redis_prefix = super::common::unique_prefix();
        let redis = RedisRateLimiterProvider::builder(connection_manager.clone())
            .prefix(redis_prefix.clone())
            .window_size(WindowSize::seconds_or_panic(60))
            .stale_after(Duration::from_millis(50))
            .cleanup_interval(Duration::from_millis(200))
            .build()
            .unwrap();
        let redis_key = super::common::key("redis_cleanup");
        redis.absolute().inc(&redis_key, &rate, 1).await.unwrap();

        let hybrid_prefix = super::common::unique_prefix();
        let hybrid = HybridRateLimiterProvider::builder(connection_manager.clone())
            .prefix(hybrid_prefix.clone())
            .window_size(WindowSize::seconds_or_panic(60))
            .bucket_size(BucketSize::milliseconds_or_panic(5))
            .sync_interval(SyncInterval::milliseconds_or_panic(5))
            .stale_after(Duration::from_millis(50))
            .cleanup_interval(Duration::from_millis(200))
            .build()
            .unwrap();
        let hybrid_key = super::common::key("hybrid_cleanup");
        hybrid.absolute().inc(&hybrid_key, &rate, 1).await.unwrap();

        // This is after one cleanup interval but before two; Smol must not discard its first tick.
        super::runtime::async_sleep(Duration::from_millis(325)).await;

        for (prefix, rate_type, key) in [
            (&redis_prefix, RateType::Absolute, &redis_key),
            (&hybrid_prefix, RateType::HybridAbsolute, &hybrid_key),
        ] {
            let key_generator = super::common::key_gen(prefix, rate_type);
            let mut connection = connection_manager.clone();
            for entity_key in key_generator.get_all_entity_keys(key) {
                let exists: bool = connection.exists(entity_key).await.unwrap();
                assert!(!exists, "default cleanup left stale state for {key:?}");
            }
            let score: Option<f64> = connection
                .zscore(key_generator.get_active_entities_key(), key.as_str())
                .await
                .unwrap();
            assert!(score.is_none(), "default cleanup left active membership");
        }
    });
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn redis_and_hybrid_cleanup_support_opt_out_stop_and_restart() {
    use redis::AsyncCommands;

    use crate::{
        BucketSize,
        common::RateType,
        hybrid::{HybridRateLimiterProvider, SyncInterval},
        redis::RedisRateLimiterProvider,
    };

    super::runtime::block_on(async {
        let connection_manager = super::common::connection_manager().await;
        let rate = RateLimit::per_second_or_panic(100.0);

        let redis_prefix = super::common::unique_prefix();
        let redis = RedisRateLimiterProvider::builder(connection_manager.clone())
            .prefix(redis_prefix.clone())
            .window_size(WindowSize::seconds_or_panic(60))
            .stale_after(Duration::from_millis(100))
            .cleanup_interval(Duration::from_millis(40))
            .cleanup_enabled(false)
            .build()
            .unwrap();
        let redis_key = super::common::key("redis_lifecycle");
        redis.absolute().inc(&redis_key, &rate, 1).await.unwrap();

        let hybrid_prefix = super::common::unique_prefix();
        let hybrid = HybridRateLimiterProvider::builder(connection_manager.clone())
            .prefix(hybrid_prefix.clone())
            .window_size(WindowSize::seconds_or_panic(60))
            .bucket_size(BucketSize::milliseconds_or_panic(5))
            .sync_interval(SyncInterval::milliseconds_or_panic(5))
            .stale_after(Duration::from_millis(100))
            .cleanup_interval(Duration::from_millis(40))
            .cleanup_enabled(false)
            .build()
            .unwrap();
        let hybrid_key = super::common::key("hybrid_lifecycle");
        hybrid.absolute().inc(&hybrid_key, &rate, 1).await.unwrap();

        super::runtime::async_sleep(Duration::from_millis(220)).await;
        for (prefix, rate_type, key) in [
            (&redis_prefix, RateType::Absolute, &redis_key),
            (&hybrid_prefix, RateType::HybridAbsolute, &hybrid_key),
        ] {
            let key_generator = super::common::key_gen(prefix, rate_type);
            let mut connection = connection_manager.clone();
            let exists: bool = connection
                .exists(key_generator.get_total_count_key(key))
                .await
                .unwrap();
            assert!(exists, "cleanup opt-out lost state for {key:?}");
        }

        redis.start_cleanup_loop();
        redis.start_cleanup_loop();
        hybrid.start_cleanup_loop();
        hybrid.start_cleanup_loop();
        super::runtime::async_sleep(Duration::from_millis(220)).await;

        for (prefix, rate_type, key) in [
            (&redis_prefix, RateType::Absolute, &redis_key),
            (&hybrid_prefix, RateType::HybridAbsolute, &hybrid_key),
        ] {
            let key_generator = super::common::key_gen(prefix, rate_type);
            let mut connection = connection_manager.clone();
            let exists: bool = connection
                .exists(key_generator.get_total_count_key(key))
                .await
                .unwrap();
            assert!(!exists, "started cleanup retained stale state for {key:?}");
        }

        redis.stop_cleanup_loop();
        redis.stop_cleanup_loop();
        hybrid.stop_cleanup_loop();
        hybrid.stop_cleanup_loop();

        let redis_key = super::common::key("redis_stopped");
        let hybrid_key = super::common::key("hybrid_stopped");
        redis.absolute().inc(&redis_key, &rate, 1).await.unwrap();
        hybrid.absolute().inc(&hybrid_key, &rate, 1).await.unwrap();
        super::runtime::async_sleep(Duration::from_millis(220)).await;

        for (prefix, rate_type, key) in [
            (&redis_prefix, RateType::Absolute, &redis_key),
            (&hybrid_prefix, RateType::HybridAbsolute, &hybrid_key),
        ] {
            let key_generator = super::common::key_gen(prefix, rate_type);
            let mut connection = connection_manager.clone();
            let exists: bool = connection
                .exists(key_generator.get_total_count_key(key))
                .await
                .unwrap();
            assert!(exists, "stopped cleanup removed state for {key:?}");
        }

        redis.start_cleanup_loop();
        redis.stop_cleanup_loop();
        redis.start_cleanup_loop();
        hybrid.start_cleanup_loop();
        hybrid.stop_cleanup_loop();
        hybrid.start_cleanup_loop();
        super::runtime::async_sleep(Duration::from_millis(220)).await;

        for (prefix, rate_type, key) in [
            (&redis_prefix, RateType::Absolute, &redis_key),
            (&hybrid_prefix, RateType::HybridAbsolute, &hybrid_key),
        ] {
            let key_generator = super::common::key_gen(prefix, rate_type);
            let mut connection = connection_manager.clone();
            let exists: bool = connection
                .exists(key_generator.get_total_count_key(key))
                .await
                .unwrap();
            assert!(
                !exists,
                "restarted cleanup retained stale state for {key:?}"
            );
        }
    });
}
