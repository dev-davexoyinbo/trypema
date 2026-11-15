use std::{sync::Arc, time::Duration};

use crate::{
    BucketSize, HardLimitFactor, RateLimit, RateLimitComparator, RateLimitDecision,
    RateLimiterBuilder, SuppressionFactorCachePeriod, TrypemaError, WindowSize,
    local::{
        AbsoluteLocalRateLimiter, LocalBackend, LocalRateLimiterProvider,
        SuppressedLocalRateLimiter,
    },
};

#[test]
fn local_builder_builds_provider_by_annotation_and_returns_arc() {
    let provider: Arc<LocalRateLimiterProvider> = RateLimiterBuilder::local()
        .window_size(WindowSize::seconds_or_panic(1))
        .bucket_size(BucketSize::milliseconds_or_panic(10))
        .hard_limit_factor(HardLimitFactor::new_or_panic(1.5))
        .suppression_factor_cache_period(SuppressionFactorCachePeriod::milliseconds_or_panic(25))
        .disable_cleanup()
        .build()
        .unwrap();

    let clone = Arc::clone(&provider);
    assert!(Arc::ptr_eq(&provider, &clone));

    let rate = RateLimit::per_second_or_panic(1.0);
    assert!(matches!(
        provider.absolute().inc("key", &rate, 1),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        provider.absolute().inc("key", &rate, 1),
        RateLimitDecision::Rejected { .. }
    ));
}

#[test]
fn local_builder_builds_provider_and_both_limiters_from_one_chain() {
    let builder: RateLimiterBuilder<LocalBackend> = RateLimiterBuilder::local()
        .window_size(WindowSize::seconds_or_panic(2))
        .hard_limit_factor(HardLimitFactor::new_or_panic(1.5))
        .disable_cleanup();

    // The local builder is `Copy`, so one chain builds every target.
    let provider: Arc<LocalRateLimiterProvider> = builder.build().unwrap();
    let absolute: Arc<AbsoluteLocalRateLimiter> = builder.build().unwrap();
    let suppressed = builder.build::<SuppressedLocalRateLimiter>().unwrap();
    let rate = RateLimit::per_second_or_panic(10.0);

    assert!(matches!(
        absolute.inc("key", &rate, 1),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        suppressed.inc("key", &rate, 1),
        RateLimitDecision::Allowed
    ));
    assert_eq!(absolute.series().get("key").unwrap().window_limit, 20.0);
    assert_eq!(
        suppressed.series().get("key").unwrap().hard_window_limit,
        30.0
    );

    // Each build makes its own state.
    assert_eq!(provider.absolute().get("key"), 0);
    assert_eq!(provider.suppressed().get("key").total, 0);
}

#[test]
fn shortcut_builders_build_their_own_type_without_annotation() {
    let provider = LocalRateLimiterProvider::builder()
        .disable_cleanup()
        .build()
        .unwrap();
    let absolute = AbsoluteLocalRateLimiter::builder()
        .disable_cleanup()
        .build()
        .unwrap();
    let suppressed = SuppressedLocalRateLimiter::builder()
        .disable_cleanup()
        .build()
        .unwrap();
    let rate = RateLimit::per_second_or_panic(10.0);

    let _: &Arc<LocalRateLimiterProvider> = &provider;
    assert!(matches!(
        provider.absolute().inc("key", &rate, 1),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        absolute.inc("key", &rate, 1),
        RateLimitDecision::Allowed
    ));
    assert!(matches!(
        suppressed.inc("key", &rate, 1),
        RateLimitDecision::Allowed
    ));
}

#[test]
fn shortcut_builder_keeps_settings_and_validation() {
    let shortcut = LocalRateLimiterProvider::builder()
        .window_size(WindowSize::seconds_or_panic(1))
        .bucket_size(BucketSize::milliseconds_or_panic(1_001))
        .disable_cleanup();

    assert_eq!(
        shortcut.build().unwrap_err(),
        TrypemaError::InvalidBucketSize(
            "bucket size must be less than or equal to window size".to_string()
        )
    );
}

#[test]
fn conditional_outcome_distinguishes_match_noop_and_miss() {
    let provider = LocalRateLimiterProvider::builder()
        .cleanup_enabled(false)
        .build()
        .unwrap();
    let rate = RateLimit::per_second_or_panic(100.0);

    let changed = provider
        .absolute()
        .set_if("key", &rate, RateLimitComparator::Always, 5);
    assert!(changed.matched);
    assert_eq!((changed.previous_total, changed.current_total), (0, 5));

    let noop = provider
        .absolute()
        .set_if("key", &rate, RateLimitComparator::Eq(5), 5);
    assert!(noop.matched);
    assert_eq!((noop.previous_total, noop.current_total), (5, 5));

    let miss = provider
        .absolute()
        .set_if("key", &rate, RateLimitComparator::Eq(99), 10);
    assert!(!miss.matched);
    assert_eq!((miss.previous_total, miss.current_total), (5, 5));
}

#[test]
fn cleanup_durations_are_validated() {
    assert!(matches!(
        LocalRateLimiterProvider::builder()
            .stale_after(Duration::ZERO)
            .build(),
        Err(TrypemaError::InvalidCleanupConfiguration(_))
    ));
    assert!(matches!(
        LocalRateLimiterProvider::builder()
            .cleanup_interval(Duration::ZERO)
            .build(),
        Err(TrypemaError::InvalidCleanupConfiguration(_))
    ));
    assert!(matches!(
        LocalRateLimiterProvider::builder()
            .cleanup_interval(Duration::from_nanos(1))
            .build(),
        Err(TrypemaError::InvalidCleanupConfiguration(_))
    ));

    let overflowing = Duration::from_secs(u64::MAX);
    assert!(matches!(
        LocalRateLimiterProvider::builder()
            .stale_after(overflowing)
            .build(),
        Err(TrypemaError::InvalidCleanupConfiguration(_))
    ));
}

#[test]
fn bucket_size_must_not_exceed_window_size() {
    LocalRateLimiterProvider::builder()
        .window_size(WindowSize::seconds_or_panic(1))
        .bucket_size(BucketSize::milliseconds_or_panic(999))
        .cleanup_enabled(false)
        .build()
        .unwrap();

    LocalRateLimiterProvider::builder()
        .window_size(WindowSize::seconds_or_panic(1))
        .bucket_size(BucketSize::seconds_or_panic(1))
        .cleanup_enabled(false)
        .build()
        .unwrap();

    for builder in [
        LocalRateLimiterProvider::builder()
            .window_size(WindowSize::seconds_or_panic(1))
            .bucket_size(BucketSize::milliseconds_or_panic(1_001)),
        LocalRateLimiterProvider::builder()
            .bucket_size(BucketSize::milliseconds_or_panic(1_001))
            .window_size(WindowSize::seconds_or_panic(1)),
    ] {
        let error = builder
            .cleanup_interval(Duration::ZERO)
            .build()
            .unwrap_err();
        assert_eq!(
            error,
            TrypemaError::InvalidBucketSize(
                "bucket size must be less than or equal to window size".to_string()
            )
        );
    }
}

#[test]
fn cleanup_can_be_disabled_started_stopped_and_restarted() {
    let provider = LocalRateLimiterProvider::builder()
        .stale_after(Duration::from_millis(40))
        .cleanup_interval(Duration::from_millis(20))
        .cleanup_enabled(false)
        .build()
        .unwrap();
    let rate = RateLimit::per_second_or_panic(100.0);

    assert!(matches!(
        provider.absolute().inc("key", &rate, 1),
        RateLimitDecision::Allowed
    ));
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(provider.absolute().series().len(), 1);

    provider.start_cleanup_loop();
    provider.start_cleanup_loop();
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(provider.absolute().series().len(), 0);

    assert!(matches!(
        provider.absolute().inc("key", &rate, 1),
        RateLimitDecision::Allowed
    ));
    provider.stop_cleanup_loop();
    provider.stop_cleanup_loop();
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(provider.absolute().series().len(), 1);

    provider.start_cleanup_loop();
    provider.stop_cleanup_loop();
    provider.start_cleanup_loop();
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(provider.absolute().series().len(), 0);
}

#[test]
fn cleanup_convenience_methods_follow_builder_order() {
    let provider = LocalRateLimiterProvider::builder()
        .stale_after(Duration::from_millis(40))
        .cleanup_interval(Duration::from_millis(20))
        .disable_cleanup()
        .enable_cleanup()
        .build()
        .unwrap();
    let rate = RateLimit::per_second_or_panic(100.0);

    assert!(matches!(
        provider.absolute().inc("key", &rate, 1),
        RateLimitDecision::Allowed
    ));
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(provider.absolute().series().len(), 0);
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn redis_and_hybrid_builders_support_shared_and_specific_methods() {
    use crate::{
        hybrid::{HybridRateLimiterProvider, SyncInterval},
        redis::RedisRateLimiterProvider,
    };

    super::runtime::block_on(async {
        let connection_manager: crate::redis::ConnectionManager =
            super::common::connection_manager().await;
        assert!(matches!(
            RedisRateLimiterProvider::builder(connection_manager.clone())
                .cleanup_interval(Duration::ZERO)
                .build(),
            Err(TrypemaError::InvalidCleanupConfiguration(_))
        ));
        assert!(matches!(
            HybridRateLimiterProvider::builder(connection_manager.clone())
                .stale_after(Duration::from_secs(u64::MAX))
                .build(),
            Err(TrypemaError::InvalidCleanupConfiguration(_))
        ));
        assert!(matches!(
            RedisRateLimiterProvider::builder(connection_manager.clone())
                .window_size(WindowSize::seconds_or_panic(1))
                .bucket_size(BucketSize::milliseconds_or_panic(1_001))
                .cleanup_enabled(false)
                .build(),
            Err(TrypemaError::InvalidBucketSize(_))
        ));
        assert!(matches!(
            HybridRateLimiterProvider::builder(connection_manager.clone())
                .bucket_size(BucketSize::milliseconds_or_panic(1_001))
                .window_size(WindowSize::seconds_or_panic(1))
                .cleanup_enabled(false)
                .build(),
            Err(TrypemaError::InvalidBucketSize(_))
        ));

        let prefix = super::common::unique_prefix();
        let redis: Arc<RedisRateLimiterProvider> =
            RateLimiterBuilder::redis(connection_manager.clone())
                .window_size(WindowSize::seconds_or_panic(1))
                .prefix(prefix)
                .disable_cleanup()
                .build()
                .unwrap();
        assert_eq!(Arc::strong_count(&redis), 1);
        let key = super::common::key("redis_builder_outcome");
        let rate = RateLimit::per_second_or_panic(100.0);
        let outcome = redis
            .absolute()
            .set_if(&key, &rate, RateLimitComparator::Eq(0), 0)
            .await
            .unwrap();
        assert!(outcome.matched);
        assert_eq!((outcome.previous_total, outcome.current_total), (0, 0));

        let prefix = super::common::unique_prefix();
        let hybrid: Arc<HybridRateLimiterProvider> = RateLimiterBuilder::hybrid(connection_manager)
            .bucket_size(BucketSize::milliseconds_or_panic(10))
            .prefix(prefix)
            .sync_interval(SyncInterval::milliseconds_or_panic(5))
            .disable_cleanup()
            .build()
            .unwrap();
        assert_eq!(Arc::strong_count(&hybrid), 1);
        let key = super::common::key("hybrid_builder_outcome");
        let outcome = hybrid
            .absolute()
            .set_if(&key, &rate, RateLimitComparator::Eq(1), 0)
            .await
            .unwrap();
        assert!(!outcome.matched);
        assert_eq!((outcome.previous_total, outcome.current_total), (0, 0));
    });
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn hybrid_builder_passes_allowed_lag_to_both_strategies() {
    use crate::hybrid::{AllowedLag, HybridRateLimiterProvider};

    super::runtime::block_on(async {
        let connection_manager: crate::redis::ConnectionManager =
            super::common::connection_manager().await;

        let default_lag = HybridRateLimiterProvider::builder(connection_manager.clone())
            .prefix(super::common::unique_prefix())
            .disable_cleanup()
            .build()
            .unwrap();
        assert_eq!(default_lag.absolute().allowed_lag, Duration::from_secs(1));
        assert_eq!(default_lag.suppressed().allowed_lag, Duration::from_secs(1));
        assert!(!default_lag.absolute().allowed_lag_applies_to_rejections);

        let custom_lag = HybridRateLimiterProvider::builder(connection_manager)
            .prefix(super::common::unique_prefix())
            .allowed_lag(AllowedLag::milliseconds_or_panic(250))
            .disable_cleanup()
            .build()
            .unwrap();
        assert_eq!(
            custom_lag.absolute().allowed_lag,
            Duration::from_millis(250)
        );
        assert_eq!(
            custom_lag.suppressed().allowed_lag,
            Duration::from_millis(250)
        );
        assert!(!custom_lag.absolute().allowed_lag_applies_to_rejections);

        let rejection_lag =
            HybridRateLimiterProvider::builder(super::common::connection_manager().await)
                .prefix(super::common::unique_prefix())
                .allowed_lag_applies_to_rejections(true)
                .disable_cleanup()
                .build()
                .unwrap();
        assert!(rejection_lag.absolute().allowed_lag_applies_to_rejections);
    });
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn redis_builder_builds_provider_and_both_limiters_from_one_chain() {
    use crate::redis::{
        AbsoluteRedisRateLimiter, RedisBackend, RedisRateLimiterProvider,
        SuppressedRedisRateLimiter,
    };

    super::runtime::block_on(async {
        let builder: RateLimiterBuilder<RedisBackend> =
            RateLimiterBuilder::redis(super::common::connection_manager().await)
                .prefix(super::common::unique_prefix())
                .disable_cleanup();

        // The Redis builder is `Clone`; a clone keeps the prefix, so all three share keys.
        let provider: Arc<RedisRateLimiterProvider> = builder.clone().build().unwrap();
        let absolute: Arc<AbsoluteRedisRateLimiter> = builder.clone().build().unwrap();
        let suppressed: Arc<SuppressedRedisRateLimiter> = builder.build().unwrap();
        let key = super::common::key("one_chain");
        let rate = RateLimit::per_second_or_panic(10.0);

        assert!(matches!(
            absolute.inc(&key, &rate, 2).await.unwrap(),
            RateLimitDecision::Allowed
        ));
        assert!(matches!(
            suppressed.inc(&key, &rate, 3).await.unwrap(),
            RateLimitDecision::Allowed
        ));
        assert_eq!(provider.absolute().get(&key).await.unwrap(), 2);
        assert_eq!(provider.suppressed().get(&key).await.unwrap().total, 3);
    });
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn hybrid_builder_builds_provider_and_both_limiters_from_one_chain() {
    use crate::hybrid::{
        AbsoluteHybridRateLimiter, AllowedLag, HybridBackend, HybridRateLimiterProvider,
        SuppressedHybridRateLimiter,
    };

    super::runtime::block_on(async {
        let builder: RateLimiterBuilder<HybridBackend> =
            RateLimiterBuilder::hybrid(super::common::connection_manager().await)
                .prefix(super::common::unique_prefix())
                .allowed_lag(AllowedLag::milliseconds_or_panic(250))
                .allowed_lag_applies_to_rejections(true)
                .disable_cleanup();

        let provider: Arc<HybridRateLimiterProvider> = builder.clone().build().unwrap();
        let absolute: Arc<AbsoluteHybridRateLimiter> = builder.clone().build().unwrap();
        let suppressed: Arc<SuppressedHybridRateLimiter> = builder.build().unwrap();

        for allowed_lag in [
            provider.absolute().allowed_lag,
            provider.suppressed().allowed_lag,
            absolute.allowed_lag,
            suppressed.allowed_lag,
        ] {
            assert_eq!(allowed_lag, Duration::from_millis(250));
        }
        assert!(provider.absolute().allowed_lag_applies_to_rejections);
        assert!(absolute.allowed_lag_applies_to_rejections);
    });
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn redis_and_hybrid_shortcuts_build_their_own_type_without_annotation() {
    use crate::{
        hybrid::{
            AbsoluteHybridRateLimiter, HybridRateLimiterProvider, SuppressedHybridRateLimiter,
        },
        redis::{AbsoluteRedisRateLimiter, RedisRateLimiterProvider, SuppressedRedisRateLimiter},
    };

    super::runtime::block_on(async {
        let connection_manager = super::common::connection_manager().await;

        let redis = RedisRateLimiterProvider::builder(connection_manager.clone())
            .disable_cleanup()
            .build()
            .unwrap();
        let absolute_redis = AbsoluteRedisRateLimiter::builder(connection_manager.clone())
            .disable_cleanup()
            .build()
            .unwrap();
        let suppressed_redis = SuppressedRedisRateLimiter::builder(connection_manager.clone())
            .disable_cleanup()
            .build()
            .unwrap();
        let hybrid = HybridRateLimiterProvider::builder(connection_manager.clone())
            .disable_cleanup()
            .build()
            .unwrap();
        let absolute_hybrid = AbsoluteHybridRateLimiter::builder(connection_manager.clone())
            .disable_cleanup()
            .build()
            .unwrap();
        let suppressed_hybrid = SuppressedHybridRateLimiter::builder(connection_manager)
            .disable_cleanup()
            .build()
            .unwrap();

        let _: &Arc<RedisRateLimiterProvider> = &redis;
        let _: &Arc<AbsoluteRedisRateLimiter> = &absolute_redis;
        let _: &Arc<SuppressedRedisRateLimiter> = &suppressed_redis;
        let _: &Arc<HybridRateLimiterProvider> = &hybrid;
        let _: &Arc<AbsoluteHybridRateLimiter> = &absolute_hybrid;
        let _: &Arc<SuppressedHybridRateLimiter> = &suppressed_hybrid;
        assert_eq!(Arc::strong_count(&redis), 1);
        assert_eq!(Arc::strong_count(&absolute_hybrid), 1);
    });
}

#[test]
fn local_targets_return_default_window_size_when_none_is_set() {
    let builder = RateLimiterBuilder::local().disable_cleanup();

    let provider: Arc<LocalRateLimiterProvider> = builder.build().unwrap();
    let absolute: Arc<AbsoluteLocalRateLimiter> = builder.build().unwrap();
    let suppressed: Arc<SuppressedLocalRateLimiter> = builder.build().unwrap();

    assert_eq!(provider.window_size(), WindowSize::default());
    assert_eq!(absolute.window_size(), WindowSize::default());
    assert_eq!(suppressed.window_size(), WindowSize::default());
}

#[test]
fn local_targets_return_the_set_window_size() {
    // 90 seconds is not a whole number of minutes, so a value rounded through a larger unit fails.
    let window_size = WindowSize::seconds_or_panic(90);
    let builder = RateLimiterBuilder::local()
        .window_size(window_size)
        .disable_cleanup();

    let provider: Arc<LocalRateLimiterProvider> = builder.build().unwrap();
    let absolute: Arc<AbsoluteLocalRateLimiter> = builder.build().unwrap();
    let suppressed: Arc<SuppressedLocalRateLimiter> = builder.build().unwrap();

    assert_eq!(provider.window_size(), window_size);
    assert_eq!(absolute.window_size(), window_size);
    assert_eq!(suppressed.window_size(), window_size);
}

#[test]
fn local_provider_and_its_limiters_return_the_same_window_size() {
    let provider = LocalRateLimiterProvider::builder()
        .window_size(WindowSize::seconds_or_panic(90))
        .disable_cleanup()
        .build()
        .unwrap();

    assert_eq!(provider.absolute().window_size(), provider.window_size());
    assert_eq!(provider.suppressed().window_size(), provider.window_size());
}

#[test]
fn local_shortcut_and_annotation_limiters_return_the_same_window_size() {
    let window_size = WindowSize::seconds_or_panic(90);

    let absolute_shortcut = AbsoluteLocalRateLimiter::builder()
        .window_size(window_size)
        .disable_cleanup()
        .build()
        .unwrap();
    let absolute_annotation: Arc<AbsoluteLocalRateLimiter> = RateLimiterBuilder::local()
        .window_size(window_size)
        .disable_cleanup()
        .build()
        .unwrap();
    let suppressed_shortcut = SuppressedLocalRateLimiter::builder()
        .window_size(window_size)
        .disable_cleanup()
        .build()
        .unwrap();
    let suppressed_annotation: Arc<SuppressedLocalRateLimiter> = RateLimiterBuilder::local()
        .window_size(window_size)
        .disable_cleanup()
        .build()
        .unwrap();

    assert_eq!(
        absolute_shortcut.window_size(),
        absolute_annotation.window_size()
    );
    assert_eq!(
        suppressed_shortcut.window_size(),
        suppressed_annotation.window_size()
    );
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn redis_targets_return_default_window_size_when_none_is_set() {
    use crate::redis::{
        AbsoluteRedisRateLimiter, RedisBackend, RedisRateLimiterProvider,
        SuppressedRedisRateLimiter,
    };

    super::runtime::block_on(async {
        let builder: RateLimiterBuilder<RedisBackend> =
            RateLimiterBuilder::redis(super::common::connection_manager().await)
                .prefix(super::common::unique_prefix())
                .disable_cleanup();

        let provider: Arc<RedisRateLimiterProvider> = builder.clone().build().unwrap();
        let absolute: Arc<AbsoluteRedisRateLimiter> = builder.clone().build().unwrap();
        let suppressed: Arc<SuppressedRedisRateLimiter> = builder.build().unwrap();

        assert_eq!(provider.window_size(), WindowSize::default());
        assert_eq!(absolute.window_size(), WindowSize::default());
        assert_eq!(suppressed.window_size(), WindowSize::default());
    });
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn redis_targets_return_the_set_window_size() {
    use crate::redis::{
        AbsoluteRedisRateLimiter, RedisBackend, RedisRateLimiterProvider,
        SuppressedRedisRateLimiter,
    };

    super::runtime::block_on(async {
        // 90 seconds is not a whole number of minutes, so a value rounded through a larger unit
        // fails.
        let window_size = WindowSize::seconds_or_panic(90);
        let builder: RateLimiterBuilder<RedisBackend> =
            RateLimiterBuilder::redis(super::common::connection_manager().await)
                .prefix(super::common::unique_prefix())
                .window_size(window_size)
                .disable_cleanup();

        let provider: Arc<RedisRateLimiterProvider> = builder.clone().build().unwrap();
        let absolute: Arc<AbsoluteRedisRateLimiter> = builder.clone().build().unwrap();
        let suppressed: Arc<SuppressedRedisRateLimiter> = builder.build().unwrap();

        assert_eq!(provider.window_size(), window_size);
        assert_eq!(absolute.window_size(), window_size);
        assert_eq!(suppressed.window_size(), window_size);
    });
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn redis_provider_and_its_limiters_return_the_same_window_size() {
    use crate::redis::RedisRateLimiterProvider;

    super::runtime::block_on(async {
        let provider = RedisRateLimiterProvider::builder(super::common::connection_manager().await)
            .prefix(super::common::unique_prefix())
            .window_size(WindowSize::seconds_or_panic(90))
            .disable_cleanup()
            .build()
            .unwrap();

        assert_eq!(provider.absolute().window_size(), provider.window_size());
        assert_eq!(provider.suppressed().window_size(), provider.window_size());
    });
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn redis_shortcut_and_annotation_limiters_return_the_same_window_size() {
    use crate::redis::{AbsoluteRedisRateLimiter, SuppressedRedisRateLimiter};

    super::runtime::block_on(async {
        let connection_manager = super::common::connection_manager().await;
        let window_size = WindowSize::seconds_or_panic(90);

        let absolute_shortcut = AbsoluteRedisRateLimiter::builder(connection_manager.clone())
            .prefix(super::common::unique_prefix())
            .window_size(window_size)
            .disable_cleanup()
            .build()
            .unwrap();
        let absolute_annotation: Arc<AbsoluteRedisRateLimiter> =
            RateLimiterBuilder::redis(connection_manager.clone())
                .prefix(super::common::unique_prefix())
                .window_size(window_size)
                .disable_cleanup()
                .build()
                .unwrap();
        let suppressed_shortcut = SuppressedRedisRateLimiter::builder(connection_manager.clone())
            .prefix(super::common::unique_prefix())
            .window_size(window_size)
            .disable_cleanup()
            .build()
            .unwrap();
        let suppressed_annotation: Arc<SuppressedRedisRateLimiter> =
            RateLimiterBuilder::redis(connection_manager)
                .prefix(super::common::unique_prefix())
                .window_size(window_size)
                .disable_cleanup()
                .build()
                .unwrap();

        assert_eq!(
            absolute_shortcut.window_size(),
            absolute_annotation.window_size()
        );
        assert_eq!(
            suppressed_shortcut.window_size(),
            suppressed_annotation.window_size()
        );
    });
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn hybrid_targets_return_default_window_size_when_none_is_set() {
    use crate::hybrid::{
        AbsoluteHybridRateLimiter, HybridBackend, HybridRateLimiterProvider,
        SuppressedHybridRateLimiter,
    };

    super::runtime::block_on(async {
        let builder: RateLimiterBuilder<HybridBackend> =
            RateLimiterBuilder::hybrid(super::common::connection_manager().await)
                .prefix(super::common::unique_prefix())
                .disable_cleanup();

        let provider: Arc<HybridRateLimiterProvider> = builder.clone().build().unwrap();
        let absolute: Arc<AbsoluteHybridRateLimiter> = builder.clone().build().unwrap();
        let suppressed: Arc<SuppressedHybridRateLimiter> = builder.build().unwrap();

        assert_eq!(provider.window_size(), WindowSize::default());
        assert_eq!(absolute.window_size(), WindowSize::default());
        assert_eq!(suppressed.window_size(), WindowSize::default());
    });
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn hybrid_targets_return_the_set_window_size() {
    use crate::hybrid::{
        AbsoluteHybridRateLimiter, HybridBackend, HybridRateLimiterProvider,
        SuppressedHybridRateLimiter,
    };

    super::runtime::block_on(async {
        // 90 seconds is not a whole number of minutes, so a value rounded through a larger unit
        // fails.
        let window_size = WindowSize::seconds_or_panic(90);
        let builder: RateLimiterBuilder<HybridBackend> =
            RateLimiterBuilder::hybrid(super::common::connection_manager().await)
                .prefix(super::common::unique_prefix())
                .window_size(window_size)
                .disable_cleanup();

        let provider: Arc<HybridRateLimiterProvider> = builder.clone().build().unwrap();
        let absolute: Arc<AbsoluteHybridRateLimiter> = builder.clone().build().unwrap();
        let suppressed: Arc<SuppressedHybridRateLimiter> = builder.build().unwrap();

        assert_eq!(provider.window_size(), window_size);
        assert_eq!(absolute.window_size(), window_size);
        assert_eq!(suppressed.window_size(), window_size);
    });
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn hybrid_provider_and_its_limiters_return_the_same_window_size() {
    use crate::hybrid::HybridRateLimiterProvider;

    super::runtime::block_on(async {
        let provider =
            HybridRateLimiterProvider::builder(super::common::connection_manager().await)
                .prefix(super::common::unique_prefix())
                .window_size(WindowSize::seconds_or_panic(90))
                .disable_cleanup()
                .build()
                .unwrap();

        assert_eq!(provider.absolute().window_size(), provider.window_size());
        assert_eq!(provider.suppressed().window_size(), provider.window_size());
    });
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn hybrid_shortcut_and_annotation_limiters_return_the_same_window_size() {
    use crate::hybrid::{AbsoluteHybridRateLimiter, SuppressedHybridRateLimiter};

    super::runtime::block_on(async {
        let connection_manager = super::common::connection_manager().await;
        let window_size = WindowSize::seconds_or_panic(90);

        let absolute_shortcut = AbsoluteHybridRateLimiter::builder(connection_manager.clone())
            .prefix(super::common::unique_prefix())
            .window_size(window_size)
            .disable_cleanup()
            .build()
            .unwrap();
        let absolute_annotation: Arc<AbsoluteHybridRateLimiter> =
            RateLimiterBuilder::hybrid(connection_manager.clone())
                .prefix(super::common::unique_prefix())
                .window_size(window_size)
                .disable_cleanup()
                .build()
                .unwrap();
        let suppressed_shortcut = SuppressedHybridRateLimiter::builder(connection_manager.clone())
            .prefix(super::common::unique_prefix())
            .window_size(window_size)
            .disable_cleanup()
            .build()
            .unwrap();
        let suppressed_annotation: Arc<SuppressedHybridRateLimiter> =
            RateLimiterBuilder::hybrid(connection_manager)
                .prefix(super::common::unique_prefix())
                .window_size(window_size)
                .disable_cleanup()
                .build()
                .unwrap();

        assert_eq!(
            absolute_shortcut.window_size(),
            absolute_annotation.window_size()
        );
        assert_eq!(
            suppressed_shortcut.window_size(),
            suppressed_annotation.window_size()
        );
    });
}
