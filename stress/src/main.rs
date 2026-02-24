use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use clap::{Parser, ValueEnum};
use hdrhistogram::Histogram;

use trypema::local::LocalRateLimiterOptions;
use trypema::{
    HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision, RateLimiter,
    RateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds,
};

#[cfg(feature = "redis-tokio")]
mod redis_compare;

mod local_compare;

#[derive(Clone, Copy, Debug, PartialEq, ValueEnum)]
enum Provider {
    Local,
    Redis,
}

#[derive(Clone, Copy, Debug, PartialEq, ValueEnum)]
enum Strategy {
    Absolute,
    Suppressed,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum KeyDist {
    Hot,
    Uniform,
    Skewed,
}

#[derive(Clone, Copy, Debug, PartialEq, ValueEnum)]
enum Mode {
    Max,
    TargetQps,
}

#[derive(Clone, Copy, Debug, PartialEq, ValueEnum)]
enum LocalLimiter {
    /// Use trypema local provider.
    Trypema,
    /// Use burster SlidingWindowLog (strict rolling window log).
    Burster,
    /// Use governor (GCRA).
    Governor,
}

#[derive(Clone, Copy, Debug, PartialEq, ValueEnum)]
enum RedisLimiter {
    /// Use trypema's Redis provider (Lua scripts).
    Trypema,
    /// Use redis-cell module (CL.THROTTLE).
    Cell,
    /// Use GCRA Lua script (similar to go-redis/redis_rate).
    Gcra,
}

#[derive(Parser, Debug, Clone)]
#[command(
    name = "trypema-stress",
    about = "Load test / benchmark harness for trypema"
)]
struct Args {
    #[arg(long, value_enum, default_value_t = Provider::Local)]
    provider: Provider,

    #[arg(long, value_enum, default_value_t = Strategy::Absolute)]
    strategy: Strategy,

    #[arg(long, value_enum, default_value_t = KeyDist::Hot)]
    key_dist: KeyDist,

    #[arg(long, value_enum, default_value_t = Mode::Max)]
    mode: Mode,

    #[arg(long, default_value_t = 8)]
    threads: usize,

    #[arg(long, default_value_t = 60)]
    duration_s: u64,

    #[arg(long, default_value_t = 10)]
    window_s: u64,

    #[arg(long, default_value_t = 10)]
    group_ms: u128,

    #[arg(long, default_value_t = 1.5)]
    hard_limit_factor: f64,

    #[arg(long, default_value_t = 100)]
    suppression_cache_ms: u64,

    #[arg(long, default_value_t = 100000)]
    key_space: usize,

    #[arg(long, default_value_t = 0.8)]
    hot_fraction: f64,

    #[arg(long, default_value_t = 100)]
    sample_every: u64,

    #[arg(long, default_value_t = 1000.0)]
    rate_limit_per_s: f64,

    /// Only used when `--provider local`.
    #[arg(long, value_enum, default_value_t = LocalLimiter::Trypema)]
    local_limiter: LocalLimiter,

    #[arg(long, value_enum, default_value_t = RedisLimiter::Trypema)]
    redis_limiter: RedisLimiter,

    /// Only used when `--redis-limiter cell`.
    #[arg(long, default_value_t = 15)]
    cell_burst: u64,

    /// Only used when `--redis-limiter gcra`.
    #[arg(long, default_value_t = 15)]
    gcra_burst: u64,

    #[arg(long)]
    target_qps: Option<u64>,

    #[arg(long)]
    burst_qps: Option<u64>,

    #[arg(long, default_value_t = 30_000)]
    burst_period_ms: u64,

    #[arg(long, default_value_t = 5_000)]
    burst_duration_ms: u64,

    #[arg(long, default_value = "redis://127.0.0.1:16379/")]
    redis_url: String,

    #[arg(long, default_value = "stress")]
    redis_prefix: String,
}

#[derive(Default)]
struct Counts {
    allowed: AtomicU64,
    rejected: AtomicU64,
    suppressed_allowed: AtomicU64,
    suppressed_denied: AtomicU64,
    errors: AtomicU64,
}

fn build_local_options(args: &Args) -> LocalRateLimiterOptions {
    LocalRateLimiterOptions {
        window_size_seconds: WindowSizeSeconds::try_from(args.window_s).unwrap(),
        rate_group_size_ms: RateGroupSizeMs::try_from(args.group_ms).unwrap(),
        hard_limit_factor: HardLimitFactor::try_from(args.hard_limit_factor).unwrap(),
        suppression_factor_cache_ms: SuppressionFactorCacheMs::try_from(args.suppression_cache_ms)
            .unwrap(),
    }
}

fn build_keys(args: &Args) -> Vec<String> {
    let n = match args.key_dist {
        KeyDist::Hot => 1,
        _ => args.key_space.max(1),
    };
    (0..n).map(|i| format!("user_{i}")).collect()
}

fn should_sample(iter: u64, sample_every: u64) -> bool {
    if sample_every <= 1 {
        return true;
    }

    iter.is_multiple_of(sample_every)
}

fn qps_for_now(args: &Args, started: Instant) -> Option<u64> {
    if args.mode == Mode::Max {
        return None;
    }

    let base = args.target_qps?;
    let burst = args.burst_qps;

    if let Some(burst_qps) = burst {
        let elapsed_ms = started.elapsed().as_millis() as u64;
        let in_period = elapsed_ms % args.burst_period_ms;
        if in_period < args.burst_duration_ms {
            return Some(burst_qps);
        }
    }

    Some(base)
}

fn pick_key<'a>(args: &Args, keys: &'a [String], thread_rng: &mut impl FnMut() -> u64) -> &'a str {
    match args.key_dist {
        KeyDist::Hot => &keys[0],
        KeyDist::Uniform => {
            let idx = (thread_rng() as usize) % keys.len();
            &keys[idx]
        }
        KeyDist::Skewed => {
            let r = (thread_rng() % 10_000) as f64 / 10_000.0;
            if r < args.hot_fraction {
                &keys[0]
            } else {
                let idx = 1 + ((thread_rng() as usize) % (keys.len().saturating_sub(1).max(1)));
                &keys[idx % keys.len()]
            }
        }
    }
}

fn print_results(
    args: &Args,
    elapsed: Duration,
    ops: u64,
    ops_s: f64,
    hist: &Histogram<u64>,
    counts: &Counts,
) {
    println!(
        "provider={:?} strategy={:?} mode={:?}",
        args.provider, args.strategy, args.mode
    );
    println!(
        "threads={} duration_s={} window_s={} group_ms={} key_dist={:?} key_space={}",
        args.threads, args.duration_s, args.window_s, args.group_ms, args.key_dist, args.key_space
    );
    println!(
        "elapsed_s={:.3} ops={} ops_per_s={:.0}",
        elapsed.as_secs_f64(),
        ops,
        ops_s
    );
    println!(
        "allowed={} rejected={} suppressed_allowed={} suppressed_denied={} errors={}",
        counts.allowed.load(Ordering::Relaxed),
        counts.rejected.load(Ordering::Relaxed),
        counts.suppressed_allowed.load(Ordering::Relaxed),
        counts.suppressed_denied.load(Ordering::Relaxed),
        counts.errors.load(Ordering::Relaxed)
    );
    if !hist.is_empty() {
        let p50 = hist.value_at_quantile(0.50);
        let p95 = hist.value_at_quantile(0.95);
        let p99 = hist.value_at_quantile(0.99);
        let p999 = hist.value_at_quantile(0.999);
        println!(
            "lat_us p50={} p95={} p99={} p999={} max={}",
            p50,
            p95,
            p99,
            p999,
            hist.max()
        );
        println!("sample_every={} samples={}", args.sample_every, hist.len());
    } else {
        println!("no latency samples collected");
    }
}

fn run_local(args: &Args) {
    if args.local_limiter != LocalLimiter::Trypema {
        local_compare::run(args.clone());
        return;
    }

    let args = args.clone();
    let keys = build_keys(&args);

    #[cfg(feature = "redis-tokio")]
    eprintln!(
        "note: built with redis-tokio; local runs will still open a Redis connection to satisfy RateLimiterOptions"
    );

    #[cfg(not(feature = "redis-tokio"))]
    let rl = Arc::new(RateLimiter::new(RateLimiterOptions {
        local: build_local_options(&args),
    }));

    #[cfg(feature = "redis-tokio")]
    let rl = {
        use trypema::redis::{RedisKey, RedisRateLimiterOptions};

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            use trypema::redis::TrypemaRedisClient;

            let client = redis::Client::open(args.redis_url.as_str()).unwrap();
            Arc::new(RateLimiter::new(RateLimiterOptions {
                local: build_local_options(&args),
                redis: RedisRateLimiterOptions {
                    client: TrypemaRedisClient::default_from_client(client)
                        .await
                        .unwrap(),

                    prefix: Some(RedisKey::try_from(args.redis_prefix.clone()).unwrap()),
                    window_size_seconds: WindowSizeSeconds::try_from(args.window_s).unwrap(),
                    rate_group_size_ms: RateGroupSizeMs::try_from(args.group_ms).unwrap(),
                    hard_limit_factor: HardLimitFactor::try_from(args.hard_limit_factor).unwrap(),
                    suppression_factor_cache_ms: SuppressionFactorCacheMs::try_from(
                        args.suppression_cache_ms,
                    )
                    .unwrap(),
                },
            }))
        })
    };

    let rate = RateLimit::try_from(args.rate_limit_per_s).unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let counts = Arc::new(Counts::default());
    let total_ops = Arc::new(AtomicU64::new(0));

    let started = Instant::now();
    let deadline = started + Duration::from_secs(args.duration_s);

    let mut handles = Vec::with_capacity(args.threads);
    for t in 0..args.threads {
        let rl = Arc::clone(&rl);
        let keys = keys.clone();
        let stop = Arc::clone(&stop);
        let counts = Arc::clone(&counts);
        let total_ops = Arc::clone(&total_ops);
        let args = args.clone();

        handles.push(std::thread::spawn(move || {
            let mut hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
            let mut i = 0_u64;
            let mut seed = (t as u64 + 1) * 0x9E37_79B9_7F4A_7C15;
            let mut next_deadline = Instant::now();

            let mut rng_u64 = || {
                // xorshift64*
                seed ^= seed >> 12;
                seed ^= seed << 25;
                seed ^= seed >> 27;
                seed = seed.wrapping_mul(0x2545_F491_4F6C_DD1D);
                seed
            };

            while !stop.load(Ordering::Relaxed) {
                if Instant::now() >= deadline {
                    break;
                }

                if let Some(qps) = qps_for_now(&args, started) {
                    let per_op_ns = 1_000_000_000u64 / qps.max(1);
                    let now = Instant::now();
                    if now < next_deadline {
                        std::thread::sleep(next_deadline - now);
                    }
                    next_deadline += Duration::from_nanos(per_op_ns);
                }

                i = i.wrapping_add(1);
                let k = pick_key(&args, &keys, &mut rng_u64);
                let sample = should_sample(i, args.sample_every);
                let t0 = if sample { Some(Instant::now()) } else { None };

                let decision = match args.strategy {
                    Strategy::Absolute => rl.local().absolute().inc(k, &rate, 1),
                    Strategy::Suppressed => rl.local().suppressed().inc(k, &rate, 1),
                };

                if let Some(t0) = t0 {
                    let us = t0.elapsed().as_micros() as u64;
                    let _ = hist.record(us.max(1));
                }

                total_ops.fetch_add(1, Ordering::Relaxed);
                match decision {
                    RateLimitDecision::Allowed => {
                        counts.allowed.fetch_add(1, Ordering::Relaxed);
                    }
                    RateLimitDecision::Rejected { .. } => {
                        counts.rejected.fetch_add(1, Ordering::Relaxed);
                    }
                    RateLimitDecision::Suppressed { is_allowed, .. } => {
                        if is_allowed {
                            counts.suppressed_allowed.fetch_add(1, Ordering::Relaxed);
                        } else {
                            counts.suppressed_denied.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }

            hist
        }));
    }

    std::thread::sleep(Duration::from_secs(args.duration_s));
    stop.store(true, Ordering::Relaxed);

    let mut merged = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    for h in handles {
        let hist = h.join().unwrap();
        merged.add(&hist).unwrap();
    }

    let elapsed = started.elapsed();
    let ops = total_ops.load(Ordering::Relaxed);
    let ops_s = ops as f64 / elapsed.as_secs_f64();
    print_results(&args, elapsed, ops, ops_s, &merged, &counts);
}

#[cfg(feature = "redis-tokio")]
fn run_redis(args: &Args) {
    use trypema::redis::{RedisKey, RedisRateLimiterOptions};

    let args2 = args.clone();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(args2.threads.max(2))
        .build()
        .unwrap();

    rt.block_on(async move {
        use trypema::redis::TrypemaRedisClient;

        if args2.redis_limiter != RedisLimiter::Trypema {
            let limiter = match args2.redis_limiter {
                RedisLimiter::Cell => redis_compare::RedisLimiter::Cell,
                RedisLimiter::Gcra => redis_compare::RedisLimiter::Gcra,
                RedisLimiter::Trypema => unreachable!(),
            };

            // Note: this path compares alternative Redis rate limiters.
            // Strategy is ignored because semantics differ.
            redis_compare::run(args2.clone(), limiter).await;
            return;
        }

        let keys = build_keys(&args2);
        let client = redis::Client::open(args2.redis_url.as_str()).unwrap();

        let rate = RateLimit::try_from(args2.rate_limit_per_s).unwrap();

        let rl = Arc::new(RateLimiter::new(RateLimiterOptions {
            local: build_local_options(&args2),
            redis: RedisRateLimiterOptions {
                client: TrypemaRedisClient::default_from_client(client)
                    .await
                    .unwrap(),
                prefix: Some(RedisKey::try_from(args2.redis_prefix.clone()).unwrap()),
                window_size_seconds: WindowSizeSeconds::try_from(args2.window_s).unwrap(),
                rate_group_size_ms: RateGroupSizeMs::try_from(args2.group_ms).unwrap(),
                hard_limit_factor: HardLimitFactor::try_from(args2.hard_limit_factor).unwrap(),
                suppression_factor_cache_ms: SuppressionFactorCacheMs::try_from(
                    args2.suppression_cache_ms,
                )
                .unwrap(),
            },
        }));

        let stop = Arc::new(AtomicBool::new(false));
        let counts = Arc::new(Counts::default());
        let total_ops = Arc::new(AtomicU64::new(0));

        let started = Instant::now();
        let deadline = started + Duration::from_secs(args2.duration_s);

        let mut join = Vec::with_capacity(args2.threads);
        for t in 0..args2.threads {
            let rl = Arc::clone(&rl);
            let stop = Arc::clone(&stop);
            let counts = Arc::clone(&counts);
            let total_ops = Arc::clone(&total_ops);
            let args = args2.clone();
            let keys = keys.clone();

            join.push(tokio::spawn(async move {
                let mut hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
                let mut i = 0_u64;
                let mut seed = (t as u64 + 1) * 0xD134_2543_DE82_EF95;
                let mut next_deadline = Instant::now();

                let mut rng_u64 = || {
                    seed ^= seed >> 12;
                    seed ^= seed << 25;
                    seed ^= seed >> 27;
                    seed = seed.wrapping_mul(0x2545_F491_4F6C_DD1D);
                    seed
                };

                let redis_keys: Vec<RedisKey> = keys
                    .iter()
                    .map(|k| RedisKey::try_from(k.to_string()).unwrap())
                    .collect();

                while !stop.load(Ordering::Relaxed) {
                    if Instant::now() >= deadline {
                        break;
                    }

                    if let Some(qps) = qps_for_now(&args, started) {
                        let per_op_ns = 1_000_000_000u64 / qps.max(1);
                        let now = Instant::now();
                        if now < next_deadline {
                            tokio::time::sleep(next_deadline - now).await;
                        }
                        next_deadline += Duration::from_nanos(per_op_ns);
                    }

                    i = i.wrapping_add(1);
                    let idx = match args.key_dist {
                        KeyDist::Hot => 0,
                        KeyDist::Uniform => (rng_u64() as usize) % redis_keys.len(),
                        KeyDist::Skewed => {
                            let r = (rng_u64() % 10_000) as f64 / 10_000.0;
                            if r < args.hot_fraction {
                                0
                            } else {
                                let tail = redis_keys.len().saturating_sub(1).max(1);
                                1 + ((rng_u64() as usize) % tail)
                            }
                        }
                    };
                    let k = &redis_keys[idx % redis_keys.len()];
                    let sample = should_sample(i, args.sample_every);
                    let t0 = if sample { Some(Instant::now()) } else { None };

                    let res = match args.strategy {
                        Strategy::Absolute => rl.redis().absolute().inc(k, &rate, 1).await,
                        Strategy::Suppressed => rl.redis().suppressed().inc(k, &rate, 1).await,
                    };

                    if let Some(t0) = t0 {
                        let us = t0.elapsed().as_micros() as u64;
                        let _ = hist.record(us.max(1));
                    }

                    total_ops.fetch_add(1, Ordering::Relaxed);
                    match res {
                        Ok(RateLimitDecision::Allowed) => {
                            counts.allowed.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(RateLimitDecision::Rejected { .. }) => {
                            counts.rejected.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(RateLimitDecision::Suppressed { is_allowed, .. }) => {
                            if is_allowed {
                                counts.suppressed_allowed.fetch_add(1, Ordering::Relaxed);
                            } else {
                                counts.suppressed_denied.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(_) => {
                            counts.errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }

                hist
            }));
        }

        tokio::time::sleep(Duration::from_secs(args2.duration_s)).await;
        stop.store(true, Ordering::Relaxed);

        let mut merged = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
        for j in join {
            let hist = j.await.unwrap();
            merged.add(&hist).unwrap();
        }

        let elapsed = started.elapsed();
        let ops = total_ops.load(Ordering::Relaxed);
        let ops_s = ops as f64 / elapsed.as_secs_f64();
        print_results(&args2, elapsed, ops, ops_s, &merged, &counts);
    });
}

#[cfg(not(feature = "redis-tokio"))]
fn run_redis(_: &Args) {
    eprintln!("redis provider requires: cargo run -p trypema-stress --features redis-tokio -- ...");
    std::process::exit(2);
}

fn main() {
    let args = Args::parse();
    match args.provider {
        Provider::Local => run_local(&args),
        Provider::Redis => run_redis(&args),
    }
}
