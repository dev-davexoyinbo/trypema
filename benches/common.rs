//! Shared helpers for criterion benchmarks.
//!
//! Include in a bench file with:
//! ```ignore
//! #[path = "common.rs"]
//! mod common;
//! ```
//!
//! Feature-dependent limiter construction lives here so the benchmark files can
//! focus on the operation being measured.

#![allow(dead_code)]

use std::sync::Arc;

use trypema::{
    BucketSize, HardLimitFactor, RateLimiterBuilder, SuppressionFactorCachePeriod, WindowSize,
    local::LocalRateLimiterProvider,
};

// ---------------------------------------------------------------------------
// Helpers available under any feature flag
// ---------------------------------------------------------------------------

/// Default bench prefix used to namespace Redis keys.
pub const BENCH_PREFIX: &str = "bench";

/// Parameters shared by local, Redis, and hybrid benchmark limiters.
pub struct LimiterConfig {
    pub window_size: u64,
    pub bucket_size: u64,
    pub hard_limit_factor: f64,
    pub suppression_factor_cache_period: u64,
    pub prefix: &'static str,
}

impl Default for LimiterConfig {
    fn default() -> Self {
        Self {
            window_size: 60,
            bucket_size: 10,
            hard_limit_factor: 1.0,
            suppression_factor_cache_period: 100,
            prefix: BENCH_PREFIX,
        }
    }
}

/// Build a limiter for a local benchmark under any feature configuration.
pub fn build_local_limiter(config: LimiterConfig) -> Arc<LocalRateLimiterProvider> {
    LocalRateLimiterProvider::builder()
        .window_size(WindowSize::seconds_or_panic(config.window_size))
        .bucket_size(BucketSize::milliseconds_or_panic(config.bucket_size))
        .hard_limit_factor(HardLimitFactor::new_or_panic(config.hard_limit_factor))
        .suppression_factor_cache_period(SuppressionFactorCachePeriod::milliseconds_or_panic(
            config.suppression_factor_cache_period,
        ))
        .cleanup_enabled(false)
        .build()
        .unwrap()
}

/// Measure exactly `iterations` operations distributed across `thread_count` workers.
///
/// Workers rendezvous before the timer starts, then wait on a second barrier for the
/// timed release. Thread creation and worker setup are therefore excluded from the
/// returned duration.
pub fn measure_parallel<F>(
    iterations: u64,
    thread_count: usize,
    operation: F,
) -> std::time::Duration
where
    F: Fn() + Sync,
{
    use std::{
        sync::Barrier,
        thread,
        time::{Duration, Instant},
    };

    assert!(thread_count > 0, "thread_count must be greater than zero");

    let ready = Barrier::new(thread_count + 1);
    let start = Barrier::new(thread_count + 1);

    thread::scope(|scope| {
        let operation = &operation;
        let mut handles = Vec::with_capacity(thread_count);

        for worker_index in 0..thread_count {
            let base_iterations = iterations / thread_count as u64;
            let remainder = iterations % thread_count as u64;
            let worker_iterations = base_iterations + u64::from((worker_index as u64) < remainder);
            let ready = &ready;
            let start = &start;

            handles.push(scope.spawn(move || {
                ready.wait();
                start.wait();

                for _ in 0..worker_iterations {
                    operation();
                }
            }));
        }

        ready.wait();
        let started = Instant::now();
        start.wait();

        for handle in handles {
            handle.join().expect("benchmark worker panicked");
        }

        let elapsed = started.elapsed();
        if iterations == 0 {
            Duration::ZERO
        } else {
            elapsed
        }
    })
}

// ---------------------------------------------------------------------------
// Redis-specific helpers (require redis-tokio or redis-smol)
// ---------------------------------------------------------------------------

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
pub mod redis {
    use std::{env, sync::Arc};

    use trypema::{
        RateLimiterBuilder,
        hybrid::{HybridRateLimiterProvider, SyncInterval},
        redis::{RedisKey, RedisRateLimiterProvider},
    };

    use super::LimiterConfig;

    /// Read `REDIS_URL` from the environment, defaulting to the local dev address.
    pub fn redis_url() -> String {
        env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:16379/".to_string())
    }

    async fn connection_manager() -> ::redis::aio::ConnectionManager {
        let client = ::redis::Client::open(redis_url()).unwrap();
        client.get_connection_manager().await.unwrap()
    }

    /// Build a Redis provider using the given benchmark configuration.
    pub async fn build_redis_limiter(cfg: LimiterConfig) -> Arc<RedisRateLimiterProvider> {
        let prefix = RedisKey::try_from(cfg.prefix.to_string()).unwrap();
        RedisRateLimiterProvider::builder(connection_manager().await)
            .prefix(prefix)
            .window_size(super::WindowSize::seconds_or_panic(cfg.window_size))
            .bucket_size(super::BucketSize::milliseconds_or_panic(cfg.bucket_size))
            .hard_limit_factor(super::HardLimitFactor::new_or_panic(cfg.hard_limit_factor))
            .suppression_factor_cache_period(
                super::SuppressionFactorCachePeriod::milliseconds_or_panic(
                    cfg.suppression_factor_cache_period,
                ),
            )
            .cleanup_enabled(false)
            .build()
            .unwrap()
    }

    /// Build a hybrid provider using the given benchmark configuration.
    pub async fn build_hybrid_limiter(cfg: LimiterConfig) -> Arc<HybridRateLimiterProvider> {
        let prefix = RedisKey::try_from(cfg.prefix.to_string()).unwrap();
        HybridRateLimiterProvider::builder(connection_manager().await)
            .prefix(prefix)
            .window_size(super::WindowSize::seconds_or_panic(cfg.window_size))
            .bucket_size(super::BucketSize::milliseconds_or_panic(cfg.bucket_size))
            .hard_limit_factor(super::HardLimitFactor::new_or_panic(cfg.hard_limit_factor))
            .suppression_factor_cache_period(
                super::SuppressionFactorCachePeriod::milliseconds_or_panic(
                    cfg.suppression_factor_cache_period,
                ),
            )
            .sync_interval(SyncInterval::default())
            .cleanup_enabled(false)
            .build()
            .unwrap()
    }
}
