use std::time::Duration;

use crate::{
    BucketSize, HardLimitFactor, RateLimit, RateLimitComparator, SuppressionFactorCachePeriod,
    TrypemaError, WindowSize, common::duration_from_milliseconds,
};

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
use crate::hybrid::SyncInterval;

type RateConstructor = fn(f64) -> Result<RateLimit, TrypemaError>;
type PanicRateConstructor = fn(f64) -> RateLimit;
type RateGetter = fn(RateLimit) -> f64;
type WindowConstructor = fn(u64) -> Result<WindowSize, TrypemaError>;
type PanicWindowConstructor = fn(u64) -> WindowSize;
type BucketConstructor = fn(u64) -> Result<BucketSize, TrypemaError>;
type PanicBucketConstructor = fn(u64) -> BucketSize;
type CacheConstructor = fn(u64) -> Result<SuppressionFactorCachePeriod, TrypemaError>;
type PanicCacheConstructor = fn(u64) -> SuppressionFactorCachePeriod;

#[test]
fn rate_limit_period_helpers_convert_both_directions() {
    let cases: [(RateConstructor, PanicRateConstructor, RateGetter, f64); 6] = [
        (
            RateLimit::per_second,
            RateLimit::per_second_or_panic,
            RateLimit::as_per_second,
            1.0,
        ),
        (
            RateLimit::per_minute,
            RateLimit::per_minute_or_panic,
            RateLimit::as_per_minute,
            60.0,
        ),
        (
            RateLimit::per_hour,
            RateLimit::per_hour_or_panic,
            RateLimit::as_per_hour,
            3_600.0,
        ),
        (
            RateLimit::per_day,
            RateLimit::per_day_or_panic,
            RateLimit::as_per_day,
            86_400.0,
        ),
        (
            RateLimit::per_week,
            RateLimit::per_week_or_panic,
            RateLimit::as_per_week,
            604_800.0,
        ),
        (
            RateLimit::per_month,
            RateLimit::per_month_or_panic,
            RateLimit::as_per_month,
            2_592_000.0,
        ),
    ];

    for (constructor, panic_constructor, getter, value) in cases {
        let rate = constructor(value).unwrap();
        assert_eq!(rate.as_per_second(), 1.0);
        assert_eq!(getter(rate), value);

        let rate = panic_constructor(value);
        assert_eq!(rate.as_per_second(), 1.0);
        assert_eq!(getter(rate), value);
        assert!(std::panic::catch_unwind(|| panic_constructor(0.0)).is_err());
    }

    assert!(matches!(
        RateLimit::per_second(0.0),
        Err(TrypemaError::InvalidRateLimit(_))
    ));
    assert!(matches!(
        RateLimit::per_second(-1.0),
        Err(TrypemaError::InvalidRateLimit(_))
    ));
    assert!(RateLimit::max().as_per_second() > 0.0);
    assert!(RateLimit::per_second(f64::NAN).is_err());
    assert!(RateLimit::per_second(f64::INFINITY).is_err());
    assert!(RateLimit::per_month(f64::from_bits(1)).is_err());
}

#[test]
fn semantic_rate_and_window_units_preserve_window_capacity() {
    let per_second = RateLimit::per_second_or_panic(1.0);
    let per_minute = RateLimit::per_minute_or_panic(60.0);
    let seconds = WindowSize::seconds_or_panic(60);
    let minutes = WindowSize::minutes_or_panic(1);

    let per_second_capacity = (seconds.as_seconds() as f64 * per_second.as_per_second()) as u64;
    let per_minute_capacity = (minutes.as_seconds() as f64 * per_minute.as_per_second()) as u64;

    assert_eq!(per_second_capacity, 60);
    assert_eq!(per_minute_capacity, per_second_capacity);
}

#[test]
fn window_size_helpers_store_seconds() {
    let cases: [(WindowConstructor, PanicWindowConstructor, u64, u64); 6] = [
        (WindowSize::seconds, WindowSize::seconds_or_panic, 1, 1),
        (WindowSize::minutes, WindowSize::minutes_or_panic, 1, 60),
        (WindowSize::hours, WindowSize::hours_or_panic, 1, 3_600),
        (WindowSize::days, WindowSize::days_or_panic, 1, 86_400),
        (WindowSize::weeks, WindowSize::weeks_or_panic, 1, 604_800),
        (
            WindowSize::months,
            WindowSize::months_or_panic,
            1,
            2_592_000,
        ),
    ];

    for (constructor, panic_constructor, value, expected) in cases {
        assert_eq!(constructor(value).unwrap().as_seconds(), expected);
        assert_eq!(panic_constructor(value).as_seconds(), expected);
        assert_eq!(
            constructor(value).unwrap().as_milliseconds(),
            u128::from(expected) * 1_000
        );
        assert!(std::panic::catch_unwind(|| panic_constructor(0)).is_err());
    }

    assert!(matches!(
        WindowSize::seconds(0),
        Err(TrypemaError::InvalidWindowSize(_))
    ));
    assert!(matches!(
        WindowSize::minutes(u64::MAX),
        Err(TrypemaError::InvalidWindowSize(_))
    ));
    assert_eq!(
        WindowSize::seconds_or_panic(u64::MAX).as_milliseconds(),
        u128::from(u64::MAX) * 1_000
    );
}

#[test]
fn window_size_getters_convert_from_seconds() {
    assert_eq!(WindowSize::seconds_or_panic(90).as_minutes(), 1.5);
    assert_eq!(WindowSize::minutes_or_panic(90).as_hours(), 1.5);
    assert_eq!(WindowSize::hours_or_panic(36).as_days(), 1.5);
    assert_eq!(WindowSize::days_or_panic(21).as_weeks(), 3.0);
    assert_eq!(WindowSize::days_or_panic(45).as_months(), 1.5);
}

#[test]
fn millisecond_duration_conversion_preserves_full_window_range() {
    assert_eq!(
        duration_from_milliseconds(1_001),
        Duration::new(1, 1_000_000)
    );
    assert_eq!(
        duration_from_milliseconds(u128::from(u64::MAX) * 1_000),
        Duration::from_secs(u64::MAX)
    );
    assert_eq!(duration_from_milliseconds(u128::MAX), Duration::MAX);
}

#[test]
fn bucket_size_helpers_store_milliseconds() {
    let cases: [(BucketConstructor, PanicBucketConstructor, u64, u64); 7] = [
        (
            BucketSize::milliseconds,
            BucketSize::milliseconds_or_panic,
            1,
            1,
        ),
        (BucketSize::seconds, BucketSize::seconds_or_panic, 1, 1_000),
        (BucketSize::minutes, BucketSize::minutes_or_panic, 1, 60_000),
        (BucketSize::hours, BucketSize::hours_or_panic, 1, 3_600_000),
        (BucketSize::days, BucketSize::days_or_panic, 1, 86_400_000),
        (
            BucketSize::weeks,
            BucketSize::weeks_or_panic,
            1,
            604_800_000,
        ),
        (
            BucketSize::months,
            BucketSize::months_or_panic,
            1,
            2_592_000_000,
        ),
    ];

    for (constructor, panic_constructor, value, expected) in cases {
        assert_eq!(constructor(value).unwrap().as_milliseconds(), expected);
        assert_eq!(panic_constructor(value).as_milliseconds(), expected);
        assert!(std::panic::catch_unwind(|| panic_constructor(0)).is_err());
    }

    assert!(matches!(
        BucketSize::milliseconds(0),
        Err(TrypemaError::InvalidBucketSize(_))
    ));
    assert!(matches!(
        BucketSize::seconds(u64::MAX),
        Err(TrypemaError::InvalidBucketSize(_))
    ));
}

#[test]
fn suppression_factor_cache_period_helpers_store_milliseconds() {
    let cases: [(CacheConstructor, PanicCacheConstructor, u64, u64); 5] = [
        (
            SuppressionFactorCachePeriod::milliseconds,
            SuppressionFactorCachePeriod::milliseconds_or_panic,
            1,
            1,
        ),
        (
            SuppressionFactorCachePeriod::seconds,
            SuppressionFactorCachePeriod::seconds_or_panic,
            1,
            1_000,
        ),
        (
            SuppressionFactorCachePeriod::minutes,
            SuppressionFactorCachePeriod::minutes_or_panic,
            1,
            60_000,
        ),
        (
            SuppressionFactorCachePeriod::hours,
            SuppressionFactorCachePeriod::hours_or_panic,
            1,
            3_600_000,
        ),
        (
            SuppressionFactorCachePeriod::days,
            SuppressionFactorCachePeriod::days_or_panic,
            1,
            86_400_000,
        ),
    ];

    for (constructor, panic_constructor, value, expected) in cases {
        assert_eq!(constructor(value).unwrap().as_milliseconds(), expected);
        assert_eq!(panic_constructor(value).as_milliseconds(), expected);
        assert!(std::panic::catch_unwind(|| panic_constructor(0)).is_err());
    }

    assert!(matches!(
        SuppressionFactorCachePeriod::milliseconds(0),
        Err(TrypemaError::InvalidSuppressionFactorCachePeriod(_))
    ));
    assert!(matches!(
        SuppressionFactorCachePeriod::seconds(u64::MAX),
        Err(TrypemaError::InvalidSuppressionFactorCachePeriod(_))
    ));
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn sync_interval_helpers_store_milliseconds() {
    type Constructor = fn(u64) -> Result<SyncInterval, TrypemaError>;
    type PanicConstructor = fn(u64) -> SyncInterval;

    let cases: [(Constructor, PanicConstructor, u64, u64); 4] = [
        (
            SyncInterval::milliseconds,
            SyncInterval::milliseconds_or_panic,
            1,
            1,
        ),
        (
            SyncInterval::seconds,
            SyncInterval::seconds_or_panic,
            1,
            1_000,
        ),
        (
            SyncInterval::minutes,
            SyncInterval::minutes_or_panic,
            1,
            60_000,
        ),
        (
            SyncInterval::hours,
            SyncInterval::hours_or_panic,
            1,
            3_600_000,
        ),
    ];

    for (constructor, panic_constructor, value, expected) in cases {
        assert_eq!(constructor(value).unwrap().as_milliseconds(), expected);
        assert_eq!(panic_constructor(value).as_milliseconds(), expected);
        assert!(std::panic::catch_unwind(|| panic_constructor(0)).is_err());
    }

    assert!(matches!(
        SyncInterval::milliseconds(0),
        Err(TrypemaError::InvalidSyncInterval(_))
    ));
    assert!(matches!(
        SyncInterval::seconds(u64::MAX),
        Err(TrypemaError::InvalidSyncInterval(_))
    ));
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn sync_interval_getters_convert_from_milliseconds() {
    assert_eq!(SyncInterval::milliseconds_or_panic(1_500).as_seconds(), 1.5);
    assert_eq!(SyncInterval::seconds_or_panic(90).as_minutes(), 1.5);
    assert_eq!(SyncInterval::minutes_or_panic(90).as_hours(), 1.5);
}

#[test]
fn hard_limit_factor_default_and_try_from_validate_at_least_one() {
    let d = HardLimitFactor::default();
    assert_eq!(d.as_multiplier(), 1f64);

    let h = HardLimitFactor::try_from(2f64).unwrap();
    assert_eq!(h.as_multiplier(), 2f64);

    // Exactly 1.0 is valid (the minimum)
    let h = HardLimitFactor::try_from(1f64).unwrap();
    assert_eq!(h.as_multiplier(), 1f64);

    assert!(HardLimitFactor::try_from(f64::NAN).is_err());
    assert!(HardLimitFactor::try_from(f64::INFINITY).is_err());

    // Below 1.0 is invalid
    assert_eq!(
        HardLimitFactor::try_from(0f64).unwrap_err().to_string(),
        "invalid hard limit factor: Hard limit factor must be greater than or equal to 1"
    );
    assert_eq!(
        HardLimitFactor::try_from(0.5f64).unwrap_err().to_string(),
        "invalid hard limit factor: Hard limit factor must be greater than or equal to 1"
    );
    assert_eq!(
        HardLimitFactor::try_from(-1f64).unwrap_err().to_string(),
        "invalid hard limit factor: Hard limit factor must be greater than or equal to 1"
    );
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn redis_key_try_from_str_preserves_valid_input_without_sanitizing() {
    use crate::redis::RedisKey;

    let key = RedisKey::try_from("user_123").unwrap();
    assert_eq!(key.as_str(), "user_123");
    assert!(RedisKey::try_from("user:123").is_err());
}

#[test]
fn rate_limit_comparator_matches_uses_embedded_operand_and_always_matches() {
    // (comparator, current, expected)
    let cases = [
        (RateLimitComparator::Eq(5), 5, true),
        (RateLimitComparator::Eq(5), 4, false),
        (RateLimitComparator::Eq(5), 6, false),
        (RateLimitComparator::Lt(5), 4, true),
        (RateLimitComparator::Lt(5), 5, false),
        (RateLimitComparator::Lt(5), 6, false),
        (RateLimitComparator::Gt(5), 6, true),
        (RateLimitComparator::Gt(5), 5, false),
        (RateLimitComparator::Gt(5), 4, false),
        (RateLimitComparator::Ne(5), 4, true),
        (RateLimitComparator::Ne(5), 6, true),
        (RateLimitComparator::Ne(5), 5, false),
        (RateLimitComparator::Always, 0, true),
        (RateLimitComparator::Always, u64::MAX, true),
    ];

    for (comparator, current, expected) in cases {
        assert_eq!(
            comparator.matches(current),
            expected,
            "comparator: {comparator:?}, current: {current}"
        );
    }
}
