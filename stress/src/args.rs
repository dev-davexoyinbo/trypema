use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use clap::{Parser, ValueEnum};

use trypema::local::LocalRateLimiterOptions;
use trypema::{BucketSize, HardLimitFactor, SuppressionFactorCachePeriod, WindowSize};

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
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

    /// Number of concurrent worker threads or tasks.
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

    /// Record one limiter-operation latency sample per this many operations per worker.
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

    /// Aggregate operation rate across all workers in `target-qps` mode.
    #[arg(long)]
    pub(crate) target_qps: Option<u64>,

    /// Aggregate operation rate during each configured burst interval.
    #[arg(long)]
    pub(crate) burst_qps: Option<u64>,

    #[arg(long, default_value_t = 30_000)]
    pub(crate) burst_period_ms: u64,

    #[arg(long, default_value_t = 5_000)]
    pub(crate) burst_duration_ms: u64,

    #[arg(long, default_value = "redis://127.0.0.1:16379/")]
    pub(crate) redis_url: String,

    /// Base prefix; Redis and hybrid runs append a unique process/time namespace.
    #[arg(long, default_value = "stress")]
    pub(crate) redis_prefix: String,
}

impl Args {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.threads == 0 {
            return Err("--threads must be greater than zero".to_string());
        }
        if self.duration_s == 0 {
            return Err("--duration-s must be greater than zero".to_string());
        }
        if self.window_s == 0 {
            return Err("--window-s must be greater than zero".to_string());
        }
        if self.group_ms == 0 {
            return Err("--group-ms must be greater than zero".to_string());
        }
        if !self.hard_limit_factor.is_finite() || self.hard_limit_factor < 1.0 {
            return Err("--hard-limit-factor must be finite and at least 1.0".to_string());
        }
        if self.suppression_cache_ms == 0 {
            return Err("--suppression-cache-ms must be greater than zero".to_string());
        }
        if self.key_space == 0 {
            return Err("--key-space must be greater than zero".to_string());
        }
        if !self.hot_fraction.is_finite() || !(0.0..=1.0).contains(&self.hot_fraction) {
            return Err("--hot-fraction must be finite and between 0.0 and 1.0".to_string());
        }
        if self.sample_every == 0 {
            return Err("--sample-every must be greater than zero".to_string());
        }
        if !self.rate_limit_per_s.is_finite() || self.rate_limit_per_s <= 0.0 {
            return Err("--rate-limit-per-s must be finite and greater than zero".to_string());
        }
        if self.redis_prefix.is_empty() || self.redis_prefix.contains(':') {
            return Err("--redis-prefix must be non-empty and contain no colons".to_string());
        }
        if self.redis_prefix.len() > 180 {
            return Err("--redis-prefix must be at most 180 bytes".to_string());
        }

        match self.mode {
            Mode::Max => {
                if self.target_qps.is_some() || self.burst_qps.is_some() {
                    return Err(
                        "--target-qps and --burst-qps require --mode target-qps".to_string()
                    );
                }
            }
            Mode::TargetQps => {
                if self.target_qps == Some(0) || self.target_qps.is_none() {
                    return Err(
                        "--mode target-qps requires --target-qps greater than zero".to_string()
                    );
                }
                if self.target_qps > Some(1_000_000_000) {
                    return Err("--target-qps must not exceed 1,000,000,000".to_string());
                }
                if self.burst_qps == Some(0) {
                    return Err("--burst-qps must be greater than zero".to_string());
                }
                if self.burst_qps > Some(1_000_000_000) {
                    return Err("--burst-qps must not exceed 1,000,000,000".to_string());
                }
                if self.burst_qps.is_some() {
                    if self.burst_period_ms == 0 {
                        return Err("--burst-period-ms must be greater than zero".to_string());
                    }
                    if self.burst_duration_ms == 0 {
                        return Err("--burst-duration-ms must be greater than zero".to_string());
                    }
                    if self.burst_duration_ms > self.burst_period_ms {
                        return Err(
                            "--burst-duration-ms must not exceed --burst-period-ms".to_string()
                        );
                    }
                }
            }
        }

        if self.provider == Provider::Local
            && self.local_limiter != LocalLimiter::Trypema
            && self.strategy == Strategy::Suppressed
        {
            return Err("burster and governor support only --strategy absolute".to_string());
        }

        if self.provider == Provider::Redis
            && self.redis_limiter != RedisLimiter::Trypema
            && self.strategy == Strategy::Suppressed
        {
            return Err("redis-cell and GCRA support only --strategy absolute".to_string());
        }

        if self.provider == Provider::Redis && self.redis_limiter == RedisLimiter::Cell {
            if self.cell_burst == 0 {
                return Err("--cell-burst must be greater than zero".to_string());
            }
            self.validate_comparison_rate("redis-cell")?;
        }

        if self.provider == Provider::Redis && self.redis_limiter == RedisLimiter::Gcra {
            if self.gcra_burst == 0 {
                return Err("--gcra-burst must be greater than zero".to_string());
            }
            self.validate_comparison_rate("GCRA")?;
        }

        if self.provider == Provider::Local
            && self.local_limiter == LocalLimiter::Governor
            && (self.rate_limit_per_s.fract() != 0.0
                || self.rate_limit_per_s > u32::MAX as f64
                || self.rate_limit_per_s * self.window_s as f64 > u32::MAX as f64)
        {
            return Err(
                "governor requires an integer rate and rate * window capacity representable as u32"
                    .to_string(),
            );
        }

        Ok(())
    } // end method validate

    fn validate_comparison_rate(&self, limiter: &str) -> Result<(), String> {
        if self.rate_limit_per_s.fract() != 0.0 || self.rate_limit_per_s > u64::MAX as f64 {
            return Err(format!(
                "{limiter} requires an integer --rate-limit-per-s representable as u64"
            ));
        }
        Ok(())
    } // end fn validate_comparison_rate

    pub(crate) fn add_run_namespace(&mut self) {
        if !matches!(self.provider, Provider::Redis | Provider::Hybrid) {
            return;
        }

        let timestamp_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time must be after the Unix epoch")
            .as_nanos();
        self.redis_prefix = format!(
            "{}_{}_{}",
            self.redis_prefix,
            std::process::id(),
            timestamp_ns
        );
    } // end method add_run_namespace
} // end impl

pub(crate) fn build_keys(args: &Args) -> Arc<[String]> {
    let n = match args.key_dist {
        KeyDist::Hot => 1,
        _ => args.key_space,
    };
    (0..n)
        .map(|i| format!("user_{i}"))
        .collect::<Vec<_>>()
        .into()
} // end fn build_keys

pub(crate) fn build_local_options(args: &Args) -> LocalRateLimiterOptions {
    LocalRateLimiterOptions {
        window_size: WindowSize::seconds(args.window_s).unwrap(),
        bucket_size: BucketSize::milliseconds(args.group_ms).unwrap(),
        hard_limit_factor: HardLimitFactor::try_from(args.hard_limit_factor).unwrap(),
        suppression_factor_cache_period: SuppressionFactorCachePeriod::milliseconds(
            args.suppression_cache_ms,
        )
        .unwrap(),
    }
} // end fn build_local_options

/// Build a `RedisRateLimiterOptions` from `Args`. Requires a connection manager.
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
pub(crate) fn build_redis_options(
    args: &Args,
    connection_manager: redis::aio::ConnectionManager,
) -> trypema::redis::RedisRateLimiterOptions {
    use trypema::redis::{RedisKey, RedisRateLimiterOptions};

    RedisRateLimiterOptions {
        connection_manager,
        prefix: Some(RedisKey::try_from(args.redis_prefix.clone()).unwrap()),
        window_size: WindowSize::seconds(args.window_s).unwrap(),
        bucket_size: BucketSize::milliseconds(args.group_ms).unwrap(),
        hard_limit_factor: HardLimitFactor::try_from(args.hard_limit_factor).unwrap(),
        suppression_factor_cache_period: SuppressionFactorCachePeriod::milliseconds(
            args.suppression_cache_ms,
        )
        .unwrap(),
        sync_interval: trypema::hybrid::SyncInterval::default(),
    }
} // end fn build_redis_options

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    fn args(extra: &[&str]) -> Args {
        let mut raw = vec!["trypema-stress"];
        raw.extend_from_slice(extra);
        Args::parse_from(raw)
    }

    #[test]
    fn target_qps_mode_requires_a_positive_target() {
        assert!(args(&["--mode", "target-qps"]).validate().is_err());
        assert!(
            args(&["--mode", "target-qps", "--target-qps", "0"])
                .validate()
                .is_err()
        );
        assert!(
            args(&["--mode", "target-qps", "--target-qps", "100"])
                .validate()
                .is_ok()
        );
        assert!(
            args(&["--mode", "target-qps", "--target-qps", "1000000001",])
                .validate()
                .is_err()
        );
    }

    #[test]
    fn rejects_inaccurate_strategy_backend_combinations() {
        assert!(
            args(&[
                "--provider",
                "local",
                "--local-limiter",
                "burster",
                "--strategy",
                "suppressed",
            ])
            .validate()
            .is_err()
        );
        assert!(
            args(&[
                "--provider",
                "redis",
                "--redis-limiter",
                "gcra",
                "--strategy",
                "suppressed",
            ])
            .validate()
            .is_err()
        );
    }

    #[test]
    fn burst_configuration_must_be_safe_and_bounded() {
        assert!(
            args(&[
                "--mode",
                "target-qps",
                "--target-qps",
                "100",
                "--burst-qps",
                "200",
                "--burst-period-ms",
                "0",
            ])
            .validate()
            .is_err()
        );
        assert!(
            args(&[
                "--mode",
                "target-qps",
                "--target-qps",
                "100",
                "--burst-qps",
                "200",
                "--burst-period-ms",
                "10",
                "--burst-duration-ms",
                "11",
            ])
            .validate()
            .is_err()
        );
    }

    #[test]
    fn build_keys_matches_the_effective_distribution_space() {
        assert_eq!(build_keys(&args(&["--key-dist", "hot"])).len(), 1);
        assert_eq!(
            build_keys(&args(&["--key-dist", "uniform", "--key-space", "17"])).len(),
            17
        );
    }

    #[test]
    fn redis_runs_receive_a_valid_unique_namespace() {
        let mut redis_args = args(&["--provider", "redis", "--redis-prefix", "suite"]);
        redis_args.add_run_namespace();

        assert!(redis_args.redis_prefix.starts_with("suite_"));
        assert!(redis_args.redis_prefix.len() <= 255);
        assert!(!redis_args.redis_prefix.contains(':'));

        let mut local_args = args(&["--provider", "local", "--redis-prefix", "suite"]);
        local_args.add_run_namespace();
        assert_eq!(local_args.redis_prefix, "suite");
    }
}
