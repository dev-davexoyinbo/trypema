//! Tests for `RateLimiterBuilder` when it builds one limiter without a provider.

use std::{sync::Arc, time::Duration};

use crate::{
    BucketSize, HardLimitFactor, RateLimit, RateLimitDecision, RateLimiterBuilder, TrypemaError,
    WindowSize,
    local::{AbsoluteLocalRateLimiter, LocalRateLimiterProvider, SuppressedLocalRateLimiter},
};

fn invalid_bucket_size() -> TrypemaError {
    TrypemaError::InvalidBucketSize(
        "bucket size must be less than or equal to window size".to_string(),
    )
}

#[test]
fn absolute_local_builder_defaults_build() {
    let limiter: Arc<AbsoluteLocalRateLimiter> = RateLimiterBuilder::local().build().unwrap();
    let rate = RateLimit::per_second_or_panic(1.0);

    assert!(matches!(
        limiter.inc("key", &rate, 1),
        RateLimitDecision::Allowed
    ));
    assert_eq!(limiter.series().get("key").unwrap().window_limit, 10.0);
    assert_eq!(Arc::strong_count(&limiter), 1);
}

#[test]
fn absolute_local_builder_passes_window_and_bucket_size() {
    let limiter: Arc<AbsoluteLocalRateLimiter> = RateLimiterBuilder::local()
        .window_size(WindowSize::seconds_or_panic(2))
        .bucket_size(BucketSize::seconds_or_panic(1))
        .hard_limit_factor(HardLimitFactor::new_or_panic(3.0))
        .disable_cleanup()
        .build()
        .unwrap();
    let rate = RateLimit::per_second_or_panic(1.0);

    assert!(matches!(
        limiter.inc("key", &rate, 1),
        RateLimitDecision::Allowed
    ));
    // The gap is longer than the default 100 ms bucket and shorter than the configured 1 s.
    std::thread::sleep(Duration::from_millis(150));
    assert!(matches!(
        limiter.inc("key", &rate, 1),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        limiter.inc("key", &rate, 1),
        RateLimitDecision::Rejected { .. }
    ));

    let series = limiter.series().get("key").unwrap();
    assert_eq!(series.window_limit, 2.0);
    assert_eq!(series.buckets.len(), 1);
}

#[test]
fn suppressed_local_builder_defaults_build() {
    let limiter: Arc<SuppressedLocalRateLimiter> = RateLimiterBuilder::local().build().unwrap();
    let rate = RateLimit::per_second_or_panic(1.0);

    assert!(matches!(
        limiter.inc("key", &rate, 1),
        RateLimitDecision::Allowed
    ));
    assert_eq!(limiter.series().get("key").unwrap().hard_window_limit, 10.0);
}

#[test]
fn suppressed_local_builder_passes_window_bucket_and_hard_limit_factor() {
    let limiter: Arc<SuppressedLocalRateLimiter> = RateLimiterBuilder::local()
        .window_size(WindowSize::seconds_or_panic(2))
        .bucket_size(BucketSize::seconds_or_panic(1))
        .hard_limit_factor(HardLimitFactor::new_or_panic(1.5))
        .disable_cleanup()
        .build()
        .unwrap();
    let rate = RateLimit::per_second_or_panic(10.0);

    assert!(matches!(
        limiter.inc("key", &rate, 1),
        RateLimitDecision::Allowed
    ));
    std::thread::sleep(Duration::from_millis(150));
    assert!(matches!(
        limiter.inc("key", &rate, 1),
        RateLimitDecision::Allowed
    ));

    let series = limiter.series().get("key").unwrap();
    assert_eq!(series.hard_window_limit, 30.0);
    assert_eq!(series.buckets.len(), 1);
}

#[test]
fn local_limiter_builders_reject_bucket_larger_than_window() {
    for result in [
        RateLimiterBuilder::local()
            .bucket_size(BucketSize::milliseconds_or_panic(1_001))
            .window_size(WindowSize::seconds_or_panic(1))
            .build::<AbsoluteLocalRateLimiter>()
            .map(|_| ()),
        SuppressedLocalRateLimiter::builder()
            .window_size(WindowSize::seconds_or_panic(1))
            .bucket_size(BucketSize::milliseconds_or_panic(1_001))
            .build()
            .map(|_| ()),
    ] {
        assert_eq!(result.unwrap_err(), invalid_bucket_size());
    }
}

#[test]
fn absolute_local_builder_cleanup_enabled_and_disabled() {
    let rate = RateLimit::per_second_or_panic(100.0);
    let build = |enabled: bool| {
        AbsoluteLocalRateLimiter::builder()
            .stale_after(Duration::from_millis(40))
            .cleanup_interval(Duration::from_millis(20))
            .cleanup_enabled(enabled)
            .build()
            .unwrap()
    };
    let enabled = build(true);
    let disabled = build(false);

    assert!(matches!(
        enabled.inc("key", &rate, 1),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        disabled.inc("key", &rate, 1),
        RateLimitDecision::Allowed
    ));
    std::thread::sleep(Duration::from_millis(150));

    assert_eq!(enabled.series().len(), 0);
    assert_eq!(disabled.series().len(), 1);
}

#[test]
fn suppressed_local_builder_cleanup_enabled_and_disabled() {
    let rate = RateLimit::per_second_or_panic(100.0);
    let build = |enabled: bool| {
        SuppressedLocalRateLimiter::builder()
            .stale_after(Duration::from_millis(40))
            .cleanup_interval(Duration::from_millis(20))
            .cleanup_enabled(enabled)
            .build()
            .unwrap()
    };
    let enabled = build(true);
    let disabled = build(false);

    assert!(matches!(
        enabled.inc("key", &rate, 1),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        disabled.inc("key", &rate, 1),
        RateLimitDecision::Allowed
    ));
    std::thread::sleep(Duration::from_millis(150));

    assert_eq!(enabled.series().len(), 0);
    assert_eq!(disabled.series().len(), 1);
}

#[test]
fn local_limiter_cleanup_can_be_started_stopped_and_restarted() {
    let limiter: Arc<AbsoluteLocalRateLimiter> = RateLimiterBuilder::local()
        .stale_after(Duration::from_millis(40))
        .cleanup_interval(Duration::from_millis(20))
        .disable_cleanup()
        .build()
        .unwrap();
    let rate = RateLimit::per_second_or_panic(100.0);

    assert!(matches!(
        limiter.inc("key", &rate, 1),
        RateLimitDecision::Allowed
    ));
    limiter.start_cleanup_loop();
    limiter.start_cleanup_loop();
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(limiter.series().len(), 0);

    assert!(matches!(
        limiter.inc("key", &rate, 1),
        RateLimitDecision::Allowed
    ));
    limiter.stop_cleanup_loop();
    limiter.stop_cleanup_loop();
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(limiter.series().len(), 1);

    limiter.start_cleanup_loop();
    limiter.stop_cleanup_loop();
    limiter.start_cleanup_loop();
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(limiter.series().len(), 0);
}

#[test]
fn provider_limiter_stop_does_not_stop_provider_cleanup() {
    let provider = LocalRateLimiterProvider::builder()
        .stale_after(Duration::from_millis(40))
        .cleanup_interval(Duration::from_millis(20))
        .build()
        .unwrap();
    let rate = RateLimit::per_second_or_panic(100.0);

    // The limiter of a provider has no running loop of its own; the provider loop cleans it.
    provider.absolute().stop_cleanup_loop();
    assert!(matches!(
        provider.absolute().inc("key", &rate, 1),
        RateLimitDecision::Allowed
    ));
    std::thread::sleep(Duration::from_millis(150));

    assert_eq!(provider.absolute().series().len(), 0);
}

#[test]
fn dropping_last_local_limiter_arc_frees_limiter_with_cleanup_running() {
    let limiter: Arc<AbsoluteLocalRateLimiter> = RateLimiterBuilder::local()
        .cleanup_interval(Duration::from_millis(20))
        .build()
        .unwrap();
    let weak = Arc::downgrade(&limiter);

    drop(limiter);
    // The loop holds a strong reference only during a cleanup pass.
    std::thread::sleep(Duration::from_millis(60));

    assert!(weak.upgrade().is_none());
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
mod redis_backed {
    use std::{sync::Arc, time::Duration};

    use redis::AsyncCommands;

    use super::super::{
        common::{
            assert_inside_allowed_lag, connection_manager, key, key_gen, sleep_past_allowed_lag,
            unique_prefix, wait_for_hybrid_sync,
        },
        runtime,
    };
    use super::invalid_bucket_size;
    use crate::{
        BucketSize, HardLimitFactor, RateLimit, RateLimitComparator, RateLimitDecision,
        RateLimiterBuilder, WindowSize,
        common::RateType,
        hybrid::{
            AbsoluteHybridRateLimiter, AllowedLag, HybridRateLimiterProvider,
            SuppressedHybridRateLimiter, SyncInterval,
        },
        redis::{
            AbsoluteRedisRateLimiter, ConnectionManager, RedisKey, RedisRateLimiterProvider,
            SuppressedRedisRateLimiter,
        },
    };

    async fn window_limit(
        connection: &mut ConnectionManager,
        prefix: &RedisKey,
        rate_type: RateType,
        key: &RedisKey,
    ) -> Option<f64> {
        connection
            .get(key_gen(prefix, rate_type).get_window_limit_key(key))
            .await
            .unwrap()
    }

    async fn bucket_count(
        connection: &mut ConnectionManager,
        prefix: &RedisKey,
        rate_type: RateType,
        key: &RedisKey,
    ) -> u64 {
        connection
            .hlen(key_gen(prefix, rate_type).get_hash_key(key))
            .await
            .unwrap()
    }

    async fn total_exists(
        connection: &mut ConnectionManager,
        prefix: &RedisKey,
        rate_type: RateType,
        key: &RedisKey,
    ) -> bool {
        connection
            .exists(key_gen(prefix, rate_type).get_total_count_key(key))
            .await
            .unwrap()
    }

    #[test]
    fn absolute_redis_builder_defaults_build() {
        runtime::block_on(async {
            let limiter: Arc<AbsoluteRedisRateLimiter> =
                RateLimiterBuilder::redis(connection_manager().await)
                    .build()
                    .unwrap();

            assert_eq!(Arc::strong_count(&limiter), 1);
        });
    }

    #[test]
    fn absolute_redis_builder_passes_prefix_window_and_bucket_size() {
        runtime::block_on(async {
            let mut connection = connection_manager().await;
            let prefix = unique_prefix();
            let limiter: Arc<AbsoluteRedisRateLimiter> =
                RateLimiterBuilder::redis(connection.clone())
                    .prefix(prefix.clone())
                    .window_size(WindowSize::seconds_or_panic(2))
                    .bucket_size(BucketSize::seconds_or_panic(1))
                    .disable_cleanup()
                    .build()
                    .unwrap();
            let k = key("settings");
            let rate = RateLimit::per_second_or_panic(10.0);

            assert!(matches!(
                limiter.inc(&k, &rate, 1).await.unwrap(),
                RateLimitDecision::Allowed
            ));
            runtime::async_sleep(Duration::from_millis(150)).await;
            assert!(matches!(
                limiter.inc(&k, &rate, 1).await.unwrap(),
                RateLimitDecision::Allowed
            ));

            assert_eq!(
                window_limit(&mut connection, &prefix, RateType::Absolute, &k).await,
                Some(20.0)
            );
            assert_eq!(
                bucket_count(&mut connection, &prefix, RateType::Absolute, &k).await,
                1
            );
        });
    }

    #[test]
    fn suppressed_redis_builder_defaults_build() {
        runtime::block_on(async {
            let limiter: Arc<SuppressedRedisRateLimiter> =
                RateLimiterBuilder::redis(connection_manager().await)
                    .build()
                    .unwrap();

            assert_eq!(Arc::strong_count(&limiter), 1);
        });
    }

    #[test]
    fn suppressed_redis_builder_passes_prefix_window_bucket_and_hard_limit_factor() {
        runtime::block_on(async {
            let mut connection = connection_manager().await;
            let prefix = unique_prefix();
            let limiter: Arc<SuppressedRedisRateLimiter> =
                RateLimiterBuilder::redis(connection.clone())
                    .prefix(prefix.clone())
                    .window_size(WindowSize::seconds_or_panic(2))
                    .bucket_size(BucketSize::seconds_or_panic(1))
                    .hard_limit_factor(HardLimitFactor::new_or_panic(1.5))
                    .disable_cleanup()
                    .build()
                    .unwrap();
            let k = key("settings");
            let rate = RateLimit::per_second_or_panic(10.0);

            assert!(matches!(
                limiter.inc(&k, &rate, 1).await.unwrap(),
                RateLimitDecision::Allowed
            ));
            runtime::async_sleep(Duration::from_millis(150)).await;
            assert!(matches!(
                limiter.inc(&k, &rate, 1).await.unwrap(),
                RateLimitDecision::Allowed
            ));

            assert_eq!(
                window_limit(&mut connection, &prefix, RateType::Suppressed, &k).await,
                Some(30.0)
            );
            assert_eq!(
                bucket_count(&mut connection, &prefix, RateType::Suppressed, &k).await,
                1
            );
        });
    }

    #[test]
    fn redis_and_hybrid_limiter_builders_reject_bucket_larger_than_window() {
        runtime::block_on(async {
            let connection = connection_manager().await;
            let bucket = BucketSize::milliseconds_or_panic(1_001);
            let window = WindowSize::seconds_or_panic(1);

            for result in [
                RateLimiterBuilder::redis(connection.clone())
                    .bucket_size(bucket)
                    .window_size(window)
                    .build::<AbsoluteRedisRateLimiter>()
                    .map(|_| ()),
                SuppressedRedisRateLimiter::builder(connection.clone())
                    .bucket_size(bucket)
                    .window_size(window)
                    .build()
                    .map(|_| ()),
                RateLimiterBuilder::hybrid(connection.clone())
                    .bucket_size(bucket)
                    .window_size(window)
                    .build::<AbsoluteHybridRateLimiter>()
                    .map(|_| ()),
                SuppressedHybridRateLimiter::builder(connection)
                    .window_size(window)
                    .bucket_size(bucket)
                    .build()
                    .map(|_| ()),
            ] {
                assert_eq!(result.unwrap_err(), invalid_bucket_size());
            }
        });
    }

    #[test]
    fn redis_limiter_builders_cleanup_enabled_and_disabled() {
        runtime::block_on(async {
            let mut connection = connection_manager().await;
            let rate = RateLimit::per_second_or_panic(100.0);
            let k = key("cleanup");
            let mut cases = Vec::new();

            for enabled in [true, false] {
                let prefix = unique_prefix();
                let absolute: Arc<AbsoluteRedisRateLimiter> =
                    RateLimiterBuilder::redis(connection.clone())
                        .prefix(prefix.clone())
                        .window_size(WindowSize::seconds_or_panic(60))
                        .stale_after(Duration::from_millis(100))
                        .cleanup_interval(Duration::from_millis(40))
                        .cleanup_enabled(enabled)
                        .build()
                        .unwrap();
                let suppressed: Arc<SuppressedRedisRateLimiter> =
                    RateLimiterBuilder::redis(connection.clone())
                        .prefix(prefix.clone())
                        .window_size(WindowSize::seconds_or_panic(60))
                        .stale_after(Duration::from_millis(100))
                        .cleanup_interval(Duration::from_millis(40))
                        .cleanup_enabled(enabled)
                        .build()
                        .unwrap();
                assert!(matches!(
                    absolute.inc(&k, &rate, 1).await.unwrap(),
                    RateLimitDecision::Allowed
                ));
                assert!(matches!(
                    suppressed.inc(&k, &rate, 1).await.unwrap(),
                    RateLimitDecision::Allowed
                ));
                cases.push((enabled, prefix, absolute, suppressed));
            }

            runtime::async_sleep(Duration::from_millis(220)).await;

            for (enabled, prefix, _absolute, _suppressed) in &cases {
                for rate_type in [RateType::Absolute, RateType::Suppressed] {
                    assert_eq!(
                        total_exists(&mut connection, prefix, rate_type, &k).await,
                        !enabled,
                        "cleanup enabled {enabled}, rate type {rate_type}"
                    );
                }
            }
        });
    }

    #[test]
    fn redis_limiter_cleanup_can_be_started_and_stopped() {
        runtime::block_on(async {
            let mut connection = connection_manager().await;
            let prefix = unique_prefix();
            let limiter: Arc<AbsoluteRedisRateLimiter> =
                RateLimiterBuilder::redis(connection.clone())
                    .prefix(prefix.clone())
                    .window_size(WindowSize::seconds_or_panic(60))
                    .stale_after(Duration::from_millis(100))
                    .cleanup_interval(Duration::from_millis(40))
                    .disable_cleanup()
                    .build()
                    .unwrap();
            let rate = RateLimit::per_second_or_panic(100.0);
            let started = key("started");
            let stopped = key("stopped");

            limiter.inc(&started, &rate, 1).await.unwrap();
            limiter.start_cleanup_loop();
            limiter.start_cleanup_loop();
            runtime::async_sleep(Duration::from_millis(220)).await;
            assert!(!total_exists(&mut connection, &prefix, RateType::Absolute, &started).await);

            limiter.stop_cleanup_loop();
            limiter.stop_cleanup_loop();
            limiter.inc(&stopped, &rate, 1).await.unwrap();
            runtime::async_sleep(Duration::from_millis(220)).await;
            assert!(total_exists(&mut connection, &prefix, RateType::Absolute, &stopped).await);
        });
    }

    #[test]
    fn absolute_redis_limiter_and_provider_share_keys() {
        runtime::block_on(async {
            let connection = connection_manager().await;
            let prefix = unique_prefix();
            let limiter: Arc<AbsoluteRedisRateLimiter> =
                RateLimiterBuilder::redis(connection.clone())
                    .prefix(prefix.clone())
                    .disable_cleanup()
                    .build()
                    .unwrap();
            let provider = RedisRateLimiterProvider::builder(connection)
                .prefix(prefix)
                .disable_cleanup()
                .build()
                .unwrap();
            let k = key("shared");
            let rate = RateLimit::per_second_or_panic(10.0);

            assert!(matches!(
                limiter.inc(&k, &rate, 3).await.unwrap(),
                RateLimitDecision::Allowed
            ));
            assert_eq!(provider.absolute().get(&k).await.unwrap(), 3);

            assert!(matches!(
                provider.absolute().inc(&k, &rate, 2).await.unwrap(),
                RateLimitDecision::Allowed
            ));
            assert_eq!(limiter.get(&k).await.unwrap(), 5);
        });
    }

    #[test]
    fn absolute_hybrid_builder_defaults_build() {
        runtime::block_on(async {
            let limiter: Arc<AbsoluteHybridRateLimiter> =
                RateLimiterBuilder::hybrid(connection_manager().await)
                    .build()
                    .unwrap();

            assert_eq!(Arc::strong_count(&limiter), 1);
            assert_eq!(limiter.allowed_lag, Duration::from_secs(1));
            assert!(!limiter.allowed_lag_applies_to_rejections);
        });
    }

    #[test]
    fn absolute_hybrid_builder_passes_settings() {
        runtime::block_on(async {
            let mut connection = connection_manager().await;
            let prefix = unique_prefix();
            let limiter: Arc<AbsoluteHybridRateLimiter> =
                RateLimiterBuilder::hybrid(connection.clone())
                    .prefix(prefix.clone())
                    .window_size(WindowSize::seconds_or_panic(2))
                    .bucket_size(BucketSize::seconds_or_panic(1))
                    .sync_interval(SyncInterval::milliseconds_or_panic(1_000))
                    .allowed_lag(AllowedLag::milliseconds_or_panic(250))
                    .allowed_lag_applies_to_rejections(true)
                    .disable_cleanup()
                    .build()
                    .unwrap();
            let k = key("settings");
            let rate = RateLimit::per_second_or_panic(10.0);

            assert_eq!(limiter.allowed_lag, Duration::from_millis(250));
            assert!(limiter.allowed_lag_applies_to_rejections);

            assert!(matches!(
                limiter.inc(&k, &rate, 1).await.unwrap(),
                RateLimitDecision::Allowed
            ));
            runtime::async_sleep(Duration::from_millis(150)).await;
            assert!(matches!(
                limiter.inc(&k, &rate, 1).await.unwrap(),
                RateLimitDecision::Allowed
            ));
            assert!(
                !total_exists(&mut connection, &prefix, RateType::HybridAbsolute, &k).await,
                "a 1 s sync interval must not flush after 150 ms"
            );

            wait_for_hybrid_sync(1_000).await;
            assert_eq!(
                window_limit(&mut connection, &prefix, RateType::HybridAbsolute, &k).await,
                Some(20.0)
            );
            assert_eq!(
                bucket_count(&mut connection, &prefix, RateType::HybridAbsolute, &k).await,
                1
            );
        });
    }

    #[test]
    fn suppressed_hybrid_builder_defaults_build() {
        runtime::block_on(async {
            let limiter: Arc<SuppressedHybridRateLimiter> =
                RateLimiterBuilder::hybrid(connection_manager().await)
                    .build()
                    .unwrap();

            assert_eq!(Arc::strong_count(&limiter), 1);
            assert_eq!(limiter.allowed_lag, Duration::from_secs(1));
        });
    }

    #[test]
    fn suppressed_hybrid_builder_passes_settings() {
        runtime::block_on(async {
            let mut connection = connection_manager().await;
            let prefix = unique_prefix();
            let limiter: Arc<SuppressedHybridRateLimiter> =
                RateLimiterBuilder::hybrid(connection.clone())
                    .prefix(prefix.clone())
                    .window_size(WindowSize::seconds_or_panic(2))
                    .bucket_size(BucketSize::seconds_or_panic(1))
                    .hard_limit_factor(HardLimitFactor::new_or_panic(1.5))
                    .sync_interval(SyncInterval::milliseconds_or_panic(1_000))
                    .allowed_lag(AllowedLag::milliseconds_or_panic(250))
                    .disable_cleanup()
                    .build()
                    .unwrap();
            let k = key("settings");
            let rate = RateLimit::per_second_or_panic(10.0);

            assert_eq!(limiter.allowed_lag, Duration::from_millis(250));

            assert!(matches!(
                limiter.inc(&k, &rate, 1).await.unwrap(),
                RateLimitDecision::Allowed
            ));
            runtime::async_sleep(Duration::from_millis(150)).await;
            assert!(matches!(
                limiter.inc(&k, &rate, 1).await.unwrap(),
                RateLimitDecision::Allowed
            ));
            assert!(
                !total_exists(&mut connection, &prefix, RateType::HybridSuppressed, &k).await,
                "a 1 s sync interval must not flush after 150 ms"
            );

            wait_for_hybrid_sync(1_000).await;
            assert_eq!(
                window_limit(&mut connection, &prefix, RateType::HybridSuppressed, &k).await,
                Some(30.0)
            );
            assert_eq!(
                bucket_count(&mut connection, &prefix, RateType::HybridSuppressed, &k).await,
                1
            );
        });
    }

    #[test]
    fn hybrid_limiter_builders_cleanup_enabled_and_disabled() {
        runtime::block_on(async {
            let mut connection = connection_manager().await;
            let rate = RateLimit::per_second_or_panic(100.0);
            let k = key("cleanup");
            let mut cases = Vec::new();

            for enabled in [true, false] {
                let prefix = unique_prefix();
                let absolute: Arc<AbsoluteHybridRateLimiter> =
                    RateLimiterBuilder::hybrid(connection.clone())
                        .prefix(prefix.clone())
                        .window_size(WindowSize::seconds_or_panic(60))
                        .bucket_size(BucketSize::milliseconds_or_panic(5))
                        .sync_interval(SyncInterval::milliseconds_or_panic(5))
                        .stale_after(Duration::from_millis(100))
                        .cleanup_interval(Duration::from_millis(40))
                        .cleanup_enabled(enabled)
                        .build()
                        .unwrap();
                let suppressed: Arc<SuppressedHybridRateLimiter> =
                    RateLimiterBuilder::hybrid(connection.clone())
                        .prefix(prefix.clone())
                        .window_size(WindowSize::seconds_or_panic(60))
                        .bucket_size(BucketSize::milliseconds_or_panic(5))
                        .sync_interval(SyncInterval::milliseconds_or_panic(5))
                        .stale_after(Duration::from_millis(100))
                        .cleanup_interval(Duration::from_millis(40))
                        .cleanup_enabled(enabled)
                        .build()
                        .unwrap();
                assert!(matches!(
                    absolute.inc(&k, &rate, 1).await.unwrap(),
                    RateLimitDecision::Allowed
                ));
                assert!(matches!(
                    suppressed.inc(&k, &rate, 1).await.unwrap(),
                    RateLimitDecision::Allowed
                ));
                cases.push((enabled, prefix, absolute, suppressed));
            }

            runtime::async_sleep(Duration::from_millis(300)).await;

            for (enabled, prefix, absolute, suppressed) in &cases {
                for rate_type in [RateType::HybridAbsolute, RateType::HybridSuppressed] {
                    assert_eq!(
                        total_exists(&mut connection, prefix, rate_type, &k).await,
                        !enabled,
                        "cleanup enabled {enabled}, rate type {rate_type}"
                    );
                }
                let expected_local_count = if *enabled { 0 } else { 1 };
                assert_eq!(absolute.local_state_count(), expected_local_count);
                assert_eq!(suppressed.local_state_count(), expected_local_count);
            }
        });
    }

    #[test]
    fn absolute_hybrid_limiter_and_provider_share_keys() {
        runtime::block_on(async {
            let connection = connection_manager().await;
            let prefix = unique_prefix();
            let limiter: Arc<AbsoluteHybridRateLimiter> =
                RateLimiterBuilder::hybrid(connection.clone())
                    .prefix(prefix.clone())
                    .sync_interval(SyncInterval::milliseconds_or_panic(5))
                    .disable_cleanup()
                    .build()
                    .unwrap();
            let provider = HybridRateLimiterProvider::builder(connection)
                .prefix(prefix)
                .sync_interval(SyncInterval::milliseconds_or_panic(5))
                .disable_cleanup()
                .build()
                .unwrap();
            let k = key("shared");
            let rate = RateLimit::per_second_or_panic(10.0);

            assert!(matches!(
                limiter.inc(&k, &rate, 3).await.unwrap(),
                RateLimitDecision::Allowed
            ));
            wait_for_hybrid_sync(5).await;
            assert_eq!(provider.absolute().get(&k).await.unwrap(), 3);

            assert!(matches!(
                provider.absolute().inc(&k, &rate, 2).await.unwrap(),
                RateLimitDecision::Allowed
            ));
            wait_for_hybrid_sync(5).await;
            assert_eq!(limiter.get(&k).await.unwrap(), 5);
        });
    }

    #[test]
    fn absolute_hybrid_limiter_inc_flushes_to_redis() {
        runtime::block_on(async {
            let mut connection = connection_manager().await;
            let prefix = unique_prefix();
            let limiter: Arc<AbsoluteHybridRateLimiter> =
                RateLimiterBuilder::hybrid(connection.clone())
                    .prefix(prefix.clone())
                    .sync_interval(SyncInterval::milliseconds_or_panic(5))
                    .disable_cleanup()
                    .build()
                    .unwrap();
            let k = key("flush");
            let rate = RateLimit::per_second_or_panic(10.0);

            assert!(matches!(
                limiter.inc(&k, &rate, 2).await.unwrap(),
                RateLimitDecision::Allowed
            ));
            wait_for_hybrid_sync(5).await;

            let total: Option<u64> = connection
                .get(key_gen(&prefix, RateType::HybridAbsolute).get_total_count_key(&k))
                .await
                .unwrap();
            assert_eq!(total, Some(2));
        });
    }

    #[test]
    fn absolute_hybrid_limiter_rejection_lag_reads_redis_again_after_lag() {
        runtime::block_on(async {
            let connection = connection_manager().await;
            let prefix = unique_prefix();
            let allowed_lag_ms = 500_u64;
            // The 30-second window keeps the retry TTL far beyond the allowed lag.
            let reader: Arc<AbsoluteHybridRateLimiter> =
                RateLimiterBuilder::hybrid(connection.clone())
                    .prefix(prefix.clone())
                    .window_size(WindowSize::seconds_or_panic(30))
                    .bucket_size(BucketSize::seconds_or_panic(1))
                    .allowed_lag(AllowedLag::milliseconds_or_panic(allowed_lag_ms))
                    .allowed_lag_applies_to_rejections(true)
                    .disable_cleanup()
                    .build()
                    .unwrap();
            let writer = HybridRateLimiterProvider::builder(connection)
                .prefix(prefix)
                .window_size(WindowSize::seconds_or_panic(30))
                .bucket_size(BucketSize::seconds_or_panic(1))
                .disable_cleanup()
                .build()
                .unwrap();
            let k = key("k");
            let rate = RateLimit::per_second_or_panic(1.0);

            let outcome = writer
                .absolute()
                .set_if(&k, &rate, RateLimitComparator::Always, 30)
                .await
                .unwrap();
            assert!(outcome.matched);
            let synced_before = std::time::Instant::now();
            assert!(matches!(
                reader.is_allowed(&k).await.unwrap(),
                RateLimitDecision::Rejected { .. }
            ));
            let synced_after = std::time::Instant::now();

            assert_eq!(writer.absolute().delete(&k).await.unwrap(), Some(30));
            let decision = reader.is_allowed(&k).await.unwrap();
            assert_inside_allowed_lag(synced_before, allowed_lag_ms);
            assert!(
                matches!(decision, RateLimitDecision::Rejected { .. }),
                "a check inside the allowed lag answers from the rejection cache"
            );

            sleep_past_allowed_lag(synced_after, allowed_lag_ms).await;
            assert!(
                matches!(
                    reader.is_allowed(&k).await.unwrap(),
                    RateLimitDecision::Allowed
                ),
                "a check after the allowed lag must read the deleted key from Redis"
            );
        });
    }

    #[test]
    fn dropping_last_hybrid_limiter_arc_frees_limiter_with_workers_running() {
        runtime::block_on(async {
            let limiter: Arc<AbsoluteHybridRateLimiter> =
                RateLimiterBuilder::hybrid(connection_manager().await)
                    .prefix(unique_prefix())
                    .sync_interval(SyncInterval::milliseconds_or_panic(5))
                    .cleanup_interval(Duration::from_millis(20))
                    .build()
                    .unwrap();
            let rate = RateLimit::per_second_or_panic(10.0);
            assert!(matches!(
                limiter.inc(&key("drop"), &rate, 1).await.unwrap(),
                RateLimitDecision::Allowed
            ));
            let weak = Arc::downgrade(&limiter);

            drop(limiter);
            // Workers hold a strong reference only while they run one pass.
            runtime::async_sleep(Duration::from_millis(100)).await;

            assert_eq!(weak.strong_count(), 0);
        });
    }

    /// Watches the background work of one hybrid limiter or provider through the Redis
    /// connection it gets and, on Tokio, through the number of tasks alive on the runtime.
    struct WorkerProbe {
        observer: ConnectionManager,
        client_name: String,
        #[cfg(feature = "redis-tokio")]
        baseline_task_count: usize,
    }

    impl WorkerProbe {
        /// Return the probe and the dedicated connection to build the limiter from.
        async fn new() -> (Self, ConnectionManager) {
            let observer = connection_manager().await;
            #[cfg(feature = "redis-tokio")]
            let baseline_task_count = tokio::runtime::Handle::current()
                .metrics()
                .num_alive_tasks();

            let mut connection = connection_manager().await;
            let client_name = format!("trypema_worker_probe_{}", rand::random::<u64>());
            let () = redis::cmd("CLIENT")
                .arg("SETNAME")
                .arg(&client_name)
                .query_async(&mut connection)
                .await
                .unwrap();

            let probe = Self {
                observer,
                client_name,
                #[cfg(feature = "redis-tokio")]
                baseline_task_count,
            };
            assert!(probe.is_client_connected().await);
            (probe, connection)
        }

        async fn is_client_connected(&self) -> bool {
            let clients: String = redis::cmd("CLIENT")
                .arg("LIST")
                .query_async(&mut self.observer.clone())
                .await
                .unwrap();
            clients.contains(&format!(" name={} ", self.client_name))
        }

        /// Fail unless every worker ends within the bound after the last `Arc` drops, and
        /// nothing under `prefix` changes in Redis after that.
        async fn assert_workers_end(&self, prefix: &RedisKey) {
            let bound = Duration::from_secs(3);
            let deadline = std::time::Instant::now() + bound;

            // The connection closes only when no worker and no limiter part holds it.
            while self.is_client_connected().await {
                assert!(
                    std::time::Instant::now() < deadline,
                    "a worker still holds the Redis connection {bound:?} after the drop"
                );
                runtime::async_sleep(Duration::from_millis(10)).await;
            }

            #[cfg(feature = "redis-tokio")]
            loop {
                let alive = tokio::runtime::Handle::current()
                    .metrics()
                    .num_alive_tasks();
                if alive <= self.baseline_task_count {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "{} tasks are alive {bound:?} after the drop, {} before the build",
                    alive,
                    self.baseline_task_count
                );
                runtime::async_sleep(Duration::from_millis(10)).await;
            }

            let mut observer = self.observer.clone();
            let pattern = format!("{}*", prefix.as_str());
            let mut keys_at_end: Vec<String> = observer.keys(&pattern).await.unwrap();
            keys_at_end.sort();
            let mut values_at_end = Vec::new();
            for redis_key in &keys_at_end {
                let value: Vec<u8> = redis::cmd("DUMP")
                    .arg(redis_key)
                    .query_async(&mut observer)
                    .await
                    .unwrap();
                values_at_end.push(value);
            }

            // Many sync and cleanup intervals of the dropped limiter.
            runtime::async_sleep(Duration::from_millis(200)).await;

            let mut keys_later: Vec<String> = observer.keys(&pattern).await.unwrap();
            keys_later.sort();
            assert_eq!(
                keys_later, keys_at_end,
                "Redis keys changed after the workers ended"
            );
            for (redis_key, value_at_end) in keys_at_end.iter().zip(&values_at_end) {
                let value: Vec<u8> = redis::cmd("DUMP")
                    .arg(redis_key)
                    .query_async(&mut observer)
                    .await
                    .unwrap();
                assert_eq!(
                    &value, value_at_end,
                    "{redis_key} changed after the workers ended"
                );
            }
        }
    }

    #[test]
    fn dropping_last_absolute_hybrid_limiter_arc_ends_every_worker() {
        runtime::block_on(async {
            let (probe, connection) = WorkerProbe::new().await;
            let prefix = unique_prefix();
            let limiter: Arc<AbsoluteHybridRateLimiter> = RateLimiterBuilder::hybrid(connection)
                .prefix(prefix.clone())
                .sync_interval(SyncInterval::milliseconds_or_panic(5))
                .stale_after(Duration::from_millis(50))
                .cleanup_interval(Duration::from_millis(20))
                .build()
                .unwrap();
            let rate = RateLimit::per_second_or_panic(10.0);
            assert!(matches!(
                limiter.inc(&key("workers"), &rate, 1).await.unwrap(),
                RateLimitDecision::Allowed
            ));
            wait_for_hybrid_sync(5).await;

            drop(limiter);

            probe.assert_workers_end(&prefix).await;
        });
    }

    #[test]
    fn dropping_last_suppressed_hybrid_limiter_arc_ends_every_worker() {
        runtime::block_on(async {
            let (probe, connection) = WorkerProbe::new().await;
            let prefix = unique_prefix();
            let limiter: Arc<SuppressedHybridRateLimiter> = RateLimiterBuilder::hybrid(connection)
                .prefix(prefix.clone())
                .sync_interval(SyncInterval::milliseconds_or_panic(5))
                .stale_after(Duration::from_millis(50))
                .cleanup_interval(Duration::from_millis(20))
                .build()
                .unwrap();
            let rate = RateLimit::per_second_or_panic(10.0);
            assert!(matches!(
                limiter.inc(&key("workers"), &rate, 1).await.unwrap(),
                RateLimitDecision::Allowed
            ));
            wait_for_hybrid_sync(5).await;

            drop(limiter);

            probe.assert_workers_end(&prefix).await;
        });
    }

    #[test]
    fn dropping_last_hybrid_provider_arc_ends_every_worker() {
        runtime::block_on(async {
            let (probe, connection) = WorkerProbe::new().await;
            let prefix = unique_prefix();
            let provider: Arc<HybridRateLimiterProvider> = RateLimiterBuilder::hybrid(connection)
                .prefix(prefix.clone())
                .sync_interval(SyncInterval::milliseconds_or_panic(5))
                .stale_after(Duration::from_millis(50))
                .cleanup_interval(Duration::from_millis(20))
                .build()
                .unwrap();
            let rate = RateLimit::per_second_or_panic(10.0);
            assert!(matches!(
                provider
                    .absolute()
                    .inc(&key("workers"), &rate, 1)
                    .await
                    .unwrap(),
                RateLimitDecision::Allowed
            ));
            assert!(matches!(
                provider
                    .suppressed()
                    .inc(&key("workers"), &rate, 1)
                    .await
                    .unwrap(),
                RateLimitDecision::Allowed
            ));
            wait_for_hybrid_sync(5).await;

            drop(provider);

            probe.assert_workers_end(&prefix).await;
        });
    }

    #[test]
    fn dropping_last_redis_limiter_arc_ends_its_cleanup_task() {
        runtime::block_on(async {
            let (probe, connection) = WorkerProbe::new().await;
            let prefix = unique_prefix();
            let limiter: Arc<AbsoluteRedisRateLimiter> = RateLimiterBuilder::redis(connection)
                .prefix(prefix.clone())
                .stale_after(Duration::from_millis(50))
                .cleanup_interval(Duration::from_millis(20))
                .build()
                .unwrap();
            let rate = RateLimit::per_second_or_panic(10.0);
            assert!(matches!(
                limiter.inc(&key("workers"), &rate, 1).await.unwrap(),
                RateLimitDecision::Allowed
            ));

            drop(limiter);

            probe.assert_workers_end(&prefix).await;
        });
    }
}
