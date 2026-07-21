use std::{sync::Arc, time::Duration};

use crate::{
    BucketSize, HardLimitFactor, RateLimit, RateLimitComparator, RateLimitDecision,
    RateLimiterBuilder, SuppressionFactorCachePeriod, TrypemaError, WindowSize,
    local::LocalRateLimiterProvider,
};

fn build_provider<B>(builder: B) -> Result<Arc<B::Provider>, TrypemaError>
where
    B: RateLimiterBuilder,
{
    builder.build()
}

#[test]
fn local_builder_implements_shared_trait_and_returns_arc() {
    let provider: Arc<LocalRateLimiterProvider> = build_provider(
        LocalRateLimiterProvider::builder()
            .window_size(WindowSize::seconds_or_panic(1))
            .bucket_size(BucketSize::milliseconds_or_panic(10))
            .hard_limit_factor(HardLimitFactor::new_or_panic(1.5))
            .suppression_factor_cache_period(SuppressionFactorCachePeriod::milliseconds_or_panic(
                25,
            ))
            .cleanup_enabled(false),
    )
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
        let redis = build_provider(
            RedisRateLimiterProvider::builder(connection_manager.clone())
                .window_size(WindowSize::seconds_or_panic(1))
                .prefix(prefix)
                .cleanup_enabled(false),
        )
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
        let hybrid = build_provider(
            HybridRateLimiterProvider::builder(connection_manager)
                .bucket_size(BucketSize::milliseconds_or_panic(10))
                .prefix(prefix)
                .sync_interval(SyncInterval::milliseconds_or_panic(5))
                .cleanup_enabled(false),
        )
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
