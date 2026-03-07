use clap::{Parser, ValueEnum};

use trypema::local::LocalRateLimiterOptions;
use trypema::{HardLimitFactor, RateGroupSizeMs, SuppressionFactorCacheMs, WindowSizeSeconds};

#[derive(Clone, Copy, Debug, PartialEq, ValueEnum)]
pub(crate) enum Provider {
    Local,
    Redis,
    Hybrid,
}

#[derive(Clone, Copy, Debug, PartialEq, ValueEnum)]
pub(crate) enum Strategy {
    Absolute,
    Suppressed,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum KeyDist {
    Hot,
    Uniform,
    Skewed,
}

#[derive(Clone, Copy, Debug, PartialEq, ValueEnum)]
pub(crate) enum Mode {
    Max,
    TargetQps,
}

/// Which local rate-limiter implementation to benchmark.
#[derive(Clone, Copy, Debug, PartialEq, ValueEnum)]
pub(crate) enum LocalLimiter {
    /// trypema local provider.
    Trypema,
    /// burster SlidingWindowLog (strict rolling window log).
    Burster,
    /// governor (GCRA).
    Governor,
}

/// Which Redis rate-limiter implementation to benchmark.
#[derive(Clone, Copy, Debug, PartialEq, ValueEnum)]
pub(crate) enum RedisLimiter {
    /// trypema's Redis provider (Lua scripts). Strategy controlled by --strategy.
    Trypema,
    /// redis-cell module (CL.THROTTLE).
    Cell,
    /// GCRA Lua script (similar to go-redis/redis_rate).
    Gcra,
}

#[derive(Parser, Debug, Clone)]
#[command(
    name = "trypema-stress",
    about = "Load test / benchmark harness for trypema"
)]
pub(crate) struct Args {
    #[arg(long, value_enum, default_value_t = Provider::Local)]
    pub(crate) provider: Provider,

    #[arg(long, value_enum, default_value_t = Strategy::Absolute)]
    pub(crate) strategy: Strategy,

    #[arg(long, value_enum, default_value_t = KeyDist::Hot)]
    pub(crate) key_dist: KeyDist,

    #[arg(long, value_enum, default_value_t = Mode::Max)]
    pub(crate) mode: Mode,

    #[arg(long, default_value_t = 8)]
    pub(crate) threads: usize,

    #[arg(long, default_value_t = 60)]
    pub(crate) duration_s: u64,

    #[arg(long, default_value_t = 10)]
    pub(crate) window_s: u64,

    #[arg(long, default_value_t = 10)]
    pub(crate) group_ms: u64,

    #[arg(long, default_value_t = 1.5)]
    pub(crate) hard_limit_factor: f64,

    #[arg(long, default_value_t = 100)]
    pub(crate) suppression_cache_ms: u64,

    #[arg(long, default_value_t = 100000)]
    pub(crate) key_space: usize,

    #[arg(long, default_value_t = 0.8)]
    pub(crate) hot_fraction: f64,

    #[arg(long, default_value_t = 100)]
    pub(crate) sample_every: u64,

    #[arg(long, default_value_t = 1000.0)]
    pub(crate) rate_limit_per_s: f64,

    /// Only used when `--provider local`.
    #[arg(long, value_enum, default_value_t = LocalLimiter::Trypema)]
    pub(crate) local_limiter: LocalLimiter,

    /// Only used when `--provider redis`.
    #[arg(long, value_enum, default_value_t = RedisLimiter::Trypema)]
    pub(crate) redis_limiter: RedisLimiter,

    /// Only used when `--redis-limiter cell`.
    #[arg(long, default_value_t = 15)]
    pub(crate) cell_burst: u64,

    /// Only used when `--redis-limiter gcra`.
    #[arg(long, default_value_t = 15)]
    pub(crate) gcra_burst: u64,

    #[arg(long)]
    pub(crate) target_qps: Option<u64>,

    #[arg(long)]
    pub(crate) burst_qps: Option<u64>,

    #[arg(long, default_value_t = 30_000)]
    pub(crate) burst_period_ms: u64,

    #[arg(long, default_value_t = 5_000)]
    pub(crate) burst_duration_ms: u64,

    #[arg(long, default_value = "redis://127.0.0.1:16379/")]
    pub(crate) redis_url: String,

    #[arg(long, default_value = "stress")]
    pub(crate) redis_prefix: String,
}

pub(crate) fn build_keys(args: &Args) -> Vec<String> {
    let n = match args.key_dist {
        KeyDist::Hot => 1,
        _ => args.key_space.max(1),
    };
    (0..n).map(|i| format!("user_{i}")).collect()
}

pub(crate) fn build_local_options(args: &Args) -> LocalRateLimiterOptions {
    LocalRateLimiterOptions {
        window_size_seconds: WindowSizeSeconds::try_from(args.window_s).unwrap(),
        rate_group_size_ms: RateGroupSizeMs::try_from(args.group_ms).unwrap(),
        hard_limit_factor: HardLimitFactor::try_from(args.hard_limit_factor).unwrap(),
        suppression_factor_cache_ms: SuppressionFactorCacheMs::try_from(args.suppression_cache_ms)
            .unwrap(),
    }
}

/// Build a `RedisRateLimiterOptions` from `Args`. Requires a connection manager.
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
pub(crate) fn build_redis_options(
    args: &Args,
    connection_manager: redis::aio::ConnectionManager,
) -> trypema::redis::RedisRateLimiterOptions {
    use trypema::redis::{RedisKey, RedisRateLimiterOptions};

    RedisRateLimiterOptions {
        connection_manager,
        prefix: Some(RedisKey::try_from(args.redis_prefix.replace(':', "_")).unwrap()),
        window_size_seconds: WindowSizeSeconds::try_from(args.window_s).unwrap(),
        rate_group_size_ms: RateGroupSizeMs::try_from(args.group_ms).unwrap(),
        hard_limit_factor: HardLimitFactor::try_from(args.hard_limit_factor).unwrap(),
        suppression_factor_cache_ms: SuppressionFactorCacheMs::try_from(args.suppression_cache_ms)
            .unwrap(),
        sync_interval_ms: trypema::hybrid::SyncIntervalMs::default(),
    }
}
