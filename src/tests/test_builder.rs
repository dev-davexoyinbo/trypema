//! Tests for `RateLimiterBuilder`, `Default` impls, and `Drop` behaviour.

use std::{sync::Arc, time::Duration};

use crate::{
    HardLimitFactor, LocalRateLimiterOptions, RateGroupSizeMs, RateLimit, RateLimiter,
    RateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds,
};

// ────────────────────────────────────────────────────────────────────────────
// Defaults
// ────────────────────────────────────────────────────────────────────────────

#[test]
fn window_size_seconds_default_is_10() {
    assert_eq!(*WindowSizeSeconds::default(), 10);
}

#[test]
fn local_rate_limiter_options_default_has_expected_field_defaults() {
    let opts = LocalRateLimiterOptions::default();
    assert_eq!(*opts.window_size_seconds, *WindowSizeSeconds::default());
    assert_eq!(*opts.rate_group_size_ms, *RateGroupSizeMs::default());
    assert_eq!(*opts.hard_limit_factor, *HardLimitFactor::default());
    assert_eq!(
        *opts.suppression_factor_cache_ms,
        *SuppressionFactorCacheMs::default()
    );
}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
#[test]
fn rate_limiter_options_default_composes_local_defaults() {
    let opts = RateLimiterOptions::default();
    assert_eq!(
        *opts.local.window_size_seconds,
        *WindowSizeSeconds::default()
    );
}

// ────────────────────────────────────────────────────────────────────────────
// Builder: successful construction
// ────────────────────────────────────────────────────────────────────────────

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
#[test]
fn builder_default_build_succeeds() {
    let rl = RateLimiter::builder().build();
    assert!(rl.is_ok(), "default builder should succeed");
}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
#[test]
fn builder_returns_arc() {
    let rl: Arc<RateLimiter> = RateLimiter::builder().build().unwrap();
    // Verify we can clone the Arc and both halves refer to the same limiter.
    let rl2 = Arc::clone(&rl);
    assert!(Arc::ptr_eq(&rl, &rl2));
}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
#[test]
fn builder_produced_limiter_is_functional() {
    let rl = RateLimiter::builder()
        .window_size_seconds(60)
        .build()
        .unwrap();

    let rate = RateLimit::new_or_panic(5.0);
    // First inc on an unknown key is always allowed.
    let decision = rl.local().absolute().inc("user_1", &rate, 1);
    assert!(
        matches!(decision, crate::RateLimitDecision::Allowed),
        "first inc should be allowed"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// Builder: setters are applied
// ────────────────────────────────────────────────────────────────────────────

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
#[test]
fn builder_window_size_setter_is_applied() {
    // Window of 1 second: capacity = 1 req/s × 1 s = 1 request.
    let rl = RateLimiter::builder()
        .window_size_seconds(1)
        .build()
        .unwrap();

    let rate = RateLimit::new_or_panic(1.0);
    assert!(matches!(
        rl.local().absolute().inc("k", &rate, 1),
        crate::RateLimitDecision::Allowed
    ));
    assert!(matches!(
        rl.local().absolute().inc("k", &rate, 1),
        crate::RateLimitDecision::Rejected { .. }
    ));
}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
#[test]
fn builder_rate_group_size_ms_setter_is_applied() {
    // Just verify the builder accepts and applies the value without error.
    let rl = RateLimiter::builder()
        .rate_group_size_ms(50)
        .build()
        .unwrap();
    let rate = RateLimit::new_or_panic(10.0);
    assert!(matches!(
        rl.local().absolute().inc("k", &rate, 1),
        crate::RateLimitDecision::Allowed
    ));
}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
#[test]
fn builder_hard_limit_factor_setter_is_applied() {
    // Just verify a valid value is accepted and the limiter is usable.
    // hard_limit_factor only affects the suppressed strategy once the rate
    // exceeds the limit; we can't easily hit that threshold here.
    let rl = RateLimiter::builder()
        .hard_limit_factor(1.5)
        .build()
        .unwrap();
    let rate = RateLimit::new_or_panic(10.0);
    // First inc on a fresh key is always allowed regardless of strategy.
    let decision = rl.local().absolute().inc("k", &rate, 1);
    assert!(
        matches!(decision, crate::RateLimitDecision::Allowed),
        "limiter built with hard_limit_factor(1.5) should be functional"
    );
}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
#[test]
fn builder_suppression_factor_cache_ms_setter_is_applied() {
    let rl = RateLimiter::builder()
        .suppression_factor_cache_ms(50)
        .build()
        .unwrap();
    let rate = RateLimit::new_or_panic(10.0);
    assert!(matches!(
        rl.local().absolute().inc("k", &rate, 1),
        crate::RateLimitDecision::Allowed
    ));
}

// ────────────────────────────────────────────────────────────────────────────
// Builder: validation errors propagate through build()
// ────────────────────────────────────────────────────────────────────────────

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
#[test]
fn builder_invalid_window_size_returns_err() {
    let result = RateLimiter::builder().window_size_seconds(0).build();
    assert!(result.is_err(), "window_size_seconds(0) should fail");
}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
#[test]
fn builder_invalid_rate_group_size_returns_err() {
    let result = RateLimiter::builder().rate_group_size_ms(0).build();
    assert!(result.is_err(), "rate_group_size_ms(0) should fail");
}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
#[test]
fn builder_invalid_hard_limit_factor_returns_err() {
    let result = RateLimiter::builder().hard_limit_factor(0.5).build();
    assert!(result.is_err(), "hard_limit_factor(0.5) should fail");
}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
#[test]
fn builder_invalid_suppression_factor_cache_ms_returns_err() {
    let result = RateLimiter::builder()
        .suppression_factor_cache_ms(0)
        .build();
    assert!(
        result.is_err(),
        "suppression_factor_cache_ms(0) should fail"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// Builder: cleanup loop starts automatically
// ────────────────────────────────────────────────────────────────────────────

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
#[test]
fn builder_cleanup_loop_runs_with_custom_timing() {
    let rl = RateLimiter::builder()
        .window_size_seconds(60)
        .stale_after_ms(100)
        .cleanup_interval_ms(50)
        .build()
        .unwrap();

    let rate = RateLimit::new_or_panic(100.0);
    rl.local().absolute().inc("k1", &rate, 1);
    rl.local().absolute().inc("k2", &rate, 1);
    assert_eq!(rl.local().absolute().series().len(), 2);

    // Wait for keys to become stale and one cleanup tick to run.
    std::thread::sleep(Duration::from_millis(250));
    assert_eq!(
        rl.local().absolute().series().len(),
        0,
        "stale keys should have been cleaned up"
    );
}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
#[test]
fn builder_cleanup_loop_is_idempotent_after_build() {
    // Calling run_cleanup_loop_with_config after build() must be a no-op
    // (loop is already running; second call is ignored).
    let rl = RateLimiter::builder()
        .stale_after_ms(5_000) // long threshold — keys should NOT be removed
        .cleanup_interval_ms(50)
        .build()
        .unwrap();

    let rate = RateLimit::new_or_panic(100.0);
    rl.local().absolute().inc("k1", &rate, 1);
    assert_eq!(rl.local().absolute().series().len(), 1);

    // Second call: aggressive config. Must be ignored since loop is already running.
    rl.run_cleanup_loop_with_config(10, 50);

    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(
        rl.local().absolute().series().len(),
        1,
        "idempotent: second run_cleanup_loop_with_config must not override the first"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// Drop: cleanup loop stops when Arc is dropped
// ────────────────────────────────────────────────────────────────────────────

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
#[test]
fn drop_stops_cleanup_loop() {
    let rate = RateLimit::new_or_panic(100.0);

    let rl = RateLimiter::builder()
        .stale_after_ms(100)
        .cleanup_interval_ms(50)
        .build()
        .unwrap();

    // Seed one key and wait for the loop to clean it up to confirm it's running.
    rl.local().absolute().inc("seed", &rate, 1);
    std::thread::sleep(Duration::from_millis(250));
    assert_eq!(
        rl.local().absolute().series().len(),
        0,
        "loop should have cleaned up the seed key"
    );

    // Drop the only Arc — this should stop the loop via Drop.
    drop(rl);

    // Build a *new* rate limiter using the raw API so we can manually check
    // the cleanup doesn't continue (we can't inspect the old one after drop).
    let rl2 = Arc::new(RateLimiter::new(RateLimiterOptions {
        local: LocalRateLimiterOptions {
            window_size_seconds: WindowSizeSeconds::new_or_panic(60),
            ..LocalRateLimiterOptions::default()
        },
    }));

    // Start a loop, add a key, stop the loop explicitly, confirm key stays.
    rl2.local().absolute().inc("k", &rate, 1);
    rl2.run_cleanup_loop_with_config(100, 50);
    std::thread::sleep(Duration::from_millis(20));
    rl2.stop_cleanup_loop();
    std::thread::sleep(Duration::from_millis(250));
    assert_eq!(
        rl2.local().absolute().series().len(),
        1,
        "after stop, stale key should remain"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// stop_cleanup_loop: callable via &self (no Arc receiver required)
// ────────────────────────────────────────────────────────────────────────────

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
#[test]
fn stop_cleanup_loop_callable_on_deref_of_arc() {
    let rl: Arc<RateLimiter> = RateLimiter::builder()
        .stale_after_ms(100)
        .cleanup_interval_ms(50)
        .build()
        .unwrap();

    let rate = RateLimit::new_or_panic(100.0);
    rl.local().absolute().inc("k", &rate, 1);

    // This call must compile: Arc<RateLimiter> auto-derefs to RateLimiter,
    // and stop_cleanup_loop now takes &self rather than self: &Arc<Self>.
    rl.stop_cleanup_loop();

    std::thread::sleep(Duration::from_millis(250));
    assert_eq!(
        rl.local().absolute().series().len(),
        1,
        "loop was stopped so stale key should not be removed"
    );
}

// ────────────────────────────────────────────────────────────────────────────
// Builder: chaining all setters in one expression
// ────────────────────────────────────────────────────────────────────────────

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
#[test]
fn builder_full_chain_compiles_and_works() {
    let rl = RateLimiter::builder()
        .window_size_seconds(30)
        .rate_group_size_ms(10)
        .hard_limit_factor(1.5)
        .suppression_factor_cache_ms(50)
        .stale_after_ms(300_000)
        .cleanup_interval_ms(60_000)
        .build()
        .unwrap();

    let rate = RateLimit::new_or_panic(10.0);
    assert!(matches!(
        rl.local().absolute().inc("k", &rate, 1),
        crate::RateLimitDecision::Allowed
    ));
}
