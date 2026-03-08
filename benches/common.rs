//! Shared helpers for criterion benchmarks.
//!
//! Include in a bench file with:
//! ```ignore
//! #[path = "common.rs"]
//! mod common;
//! ```
//!
//! This module is split into feature-gated sections so that each bench file
//! only compiles what it actually needs.

// ---------------------------------------------------------------------------
// Helpers available under any feature flag
// ---------------------------------------------------------------------------

/// Default bench prefix used to namespace Redis keys.
#[allow(dead_code)]
pub const BENCH_PREFIX: &str = "bench";

// ---------------------------------------------------------------------------
// Redis-specific helpers (require redis-tokio or redis-smol)
// ---------------------------------------------------------------------------

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
pub mod redis {
    use std::{env, sync::Arc};

    use trypema::{
        HardLimitFactor, RateGroupSizeMs, RateLimiter, RateLimiterOptions, SuppressionFactorCacheMs,
        WindowSizeSeconds, hybrid::SyncIntervalMs, local::LocalRateLimiterOptions,
    };
    use trypema::redis::{RedisKey, RedisRateLimiterOptions};

    /// Read `REDIS_URL` from the environment, defaulting to the local dev address.
    pub fn redis_url() -> String {
        env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:16379/".to_string())
    }

    /// Parameters used to construct a `RateLimiter` for Redis benchmarks.
    pub struct LimiterConfig {
        pub window_size_seconds: u64,
        pub rate_group_size_ms: u64,
        pub hard_limit_factor: f64,
        pub suppression_factor_cache_ms: u64,
        pub prefix: &'static str,
    }

    impl Default for LimiterConfig {
        fn default() -> Self {
            Self {
                window_size_seconds: 60,
                rate_group_size_ms: 10,
                hard_limit_factor: 1.0,
                suppression_factor_cache_ms: 100,
                prefix: super::BENCH_PREFIX,
            }
        }
    }

    /// Build a `RateLimiter` backed by Redis using the given `LimiterConfig`.
    ///
    /// Must be called from inside an async context (or via `runtime::block_on`).
    pub async fn build_limiter(cfg: LimiterConfig) -> Arc<RateLimiter> {
        let client = ::redis::Client::open(redis_url()).unwrap();
        let connection_manager = client.get_connection_manager().await.unwrap();

        let window_size_seconds = WindowSizeSeconds::try_from(cfg.window_size_seconds).unwrap();
        let rate_group_size_ms = RateGroupSizeMs::try_from(cfg.rate_group_size_ms).unwrap();
        let hard_limit_factor = HardLimitFactor::try_from(cfg.hard_limit_factor).unwrap();
        let suppression_factor_cache_ms =
            SuppressionFactorCacheMs::try_from(cfg.suppression_factor_cache_ms).unwrap();
        let prefix = RedisKey::try_from(cfg.prefix.to_string()).unwrap();

        Arc::new(RateLimiter::new(RateLimiterOptions {
            local: LocalRateLimiterOptions {
                window_size_seconds,
                rate_group_size_ms,
                hard_limit_factor,
                suppression_factor_cache_ms,
            },
            redis: RedisRateLimiterOptions {
                connection_manager,
                prefix: Some(prefix),
                window_size_seconds,
                rate_group_size_ms,
                hard_limit_factor,
                suppression_factor_cache_ms,
                sync_interval_ms: SyncIntervalMs::default(),
            },
        }))
    }
}
