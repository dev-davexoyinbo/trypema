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

use std::{ops::Deref, sync::Arc};

use trypema::local::LocalRateLimiterOptions;
use trypema::{
    HardLimitFactor, RateGroupSizeMs, RateLimiter, SuppressionFactorCacheMs, WindowSizeSeconds,
};

// ---------------------------------------------------------------------------
// Helpers available under any feature flag
// ---------------------------------------------------------------------------

/// Default bench prefix used to namespace Redis keys.
pub const BENCH_PREFIX: &str = "bench";

/// Parameters shared by local, Redis, and hybrid benchmark limiters.
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
            prefix: BENCH_PREFIX,
        }
    }
}

fn local_options(config: &LimiterConfig) -> LocalRateLimiterOptions {
    LocalRateLimiterOptions {
        window_size_seconds: WindowSizeSeconds::try_from(config.window_size_seconds).unwrap(),
        rate_group_size_ms: RateGroupSizeMs::try_from(config.rate_group_size_ms).unwrap(),
        hard_limit_factor: HardLimitFactor::try_from(config.hard_limit_factor).unwrap(),
        suppression_factor_cache_ms: SuppressionFactorCacheMs::try_from(
            config.suppression_factor_cache_ms,
        )
        .unwrap(),
    }
}

/// Owns the facade used by local benchmarks and any runtime required to construct it.
///
/// Local APIs themselves are feature-independent. With `redis-tokio`, the retained
/// runtime keeps the connection manager and hybrid background tasks created by the
/// feature-enabled facade alive for the benchmark's lifetime.
pub struct LocalBenchLimiter {
    limiter: Arc<RateLimiter>,
    #[cfg(feature = "redis-tokio")]
    _runtime: tokio::runtime::Runtime,
}

impl Deref for LocalBenchLimiter {
    type Target = RateLimiter;

    fn deref(&self) -> &Self::Target {
        self.limiter.as_ref()
    }
}

/// Build a limiter for a local benchmark under any feature configuration.
pub fn build_local_limiter(config: LimiterConfig) -> LocalBenchLimiter {
    #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
    {
        use trypema::RateLimiterOptions;

        LocalBenchLimiter {
            limiter: Arc::new(RateLimiter::new(RateLimiterOptions {
                local: local_options(&config),
            })),
        }
    }

    #[cfg(feature = "redis-tokio")]
    {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        let limiter = runtime.block_on(redis::build_limiter(config));

        LocalBenchLimiter {
            limiter,
            _runtime: runtime,
        }
    }

    #[cfg(feature = "redis-smol")]
    {
        LocalBenchLimiter {
            limiter: smol::block_on(redis::build_limiter(config)),
        }
    }
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

    use trypema::redis::{RedisKey, RedisRateLimiterOptions};
    use trypema::{RateLimiter, RateLimiterOptions, hybrid::SyncIntervalMs};

    use super::{LimiterConfig, local_options};

    /// Read `REDIS_URL` from the environment, defaulting to the local dev address.
    pub fn redis_url() -> String {
        env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:16379/".to_string())
    }

    /// Build a `RateLimiter` backed by Redis using the given `LimiterConfig`.
    ///
    /// Must be called from inside an async context (or via `runtime::block_on`).
    pub async fn build_limiter(cfg: LimiterConfig) -> Arc<RateLimiter> {
        let client = ::redis::Client::open(redis_url()).unwrap();
        let connection_manager = client.get_connection_manager().await.unwrap();

        let local = local_options(&cfg);
        let prefix = RedisKey::try_from(cfg.prefix.to_string()).unwrap();

        Arc::new(RateLimiter::new(RateLimiterOptions {
            local: local.clone(),
            redis: RedisRateLimiterOptions {
                connection_manager,
                prefix: Some(prefix),
                window_size_seconds: local.window_size_seconds,
                rate_group_size_ms: local.rate_group_size_ms,
                hard_limit_factor: local.hard_limit_factor,
                suppression_factor_cache_ms: local.suppression_factor_cache_ms,
                sync_interval_ms: SyncIntervalMs::default(),
            },
        }))
    }
}
