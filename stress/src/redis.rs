//! Redis provider benchmark runner.
//!
//! Supports three limiter backends (controlled by `--redis-limiter`):
//!   - `trypema`  — trypema's own Redis Lua-script provider; `--strategy` selects
//!                  absolute vs suppressed
//!   - `cell`     — redis-cell module (`CL.THROTTLE`)
//!   - `gcra`     — GCRA Lua script (port of go-redis/redis_rate)

#![cfg(any(feature = "redis-tokio", feature = "redis-smol"))]

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use trypema::{RateLimit, RateLimiter, RateLimiterOptions, redis::RedisKey};

use crate::args::{Args, RedisLimiter, Strategy, build_keys};
use crate::runner::{Counts, ErrorStats, WorkerLoop, print_error_stats, print_results};
use crate::runtime::{async_sleep, join_task, spawn_task};

// ---------------------------------------------------------------------------
// GCRA Lua script (verbatim from go-redis/redis_rate)
// ---------------------------------------------------------------------------

const GCRA_ALLOW_N_LUA: &str = r#"
redis.replicate_commands()

local rate_limit_key = KEYS[1]
local burst = ARGV[1]
local rate = ARGV[2]
local period = ARGV[3]
local cost = tonumber(ARGV[4])

local emission_interval = period / rate
local increment = emission_interval * cost
local burst_offset = emission_interval * burst

local jan_1_2017 = 1483228800
local now = redis.call("TIME")
now = (now[1] - jan_1_2017) + (now[2] / 1000000)

local tat = redis.call("GET", rate_limit_key)

if not tat then
  tat = now
else
  tat = tonumber(tat)
end

tat = math.max(tat, now)

local new_tat = tat + increment
local allow_at = new_tat - burst_offset

local diff = now - allow_at
local remaining = diff / emission_interval

if remaining < 0 then
  local reset_after = tat - now
  local retry_after = diff * -1
  return { 0, 0, tostring(retry_after), tostring(reset_after) }
end

local reset_after = new_tat - now
if reset_after > 0 then
  redis.call("SET", rate_limit_key, new_tat, "EX", math.ceil(reset_after))
end
return { cost, remaining, tostring(-1), tostring(reset_after) }
"#;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

pub(crate) fn run(args: &Args) {
    #[cfg(feature = "redis-tokio")]
    {
        let args = args.clone();
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(args.threads.max(2))
            .build()
            .unwrap()
            .block_on(run_async(args));
    }

    #[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
    {
        let args = args.clone();
        smol::block_on(run_async(args));
    }
}

// ---------------------------------------------------------------------------
// Async core (runtime-agnostic — uses runtime.rs primitives)
// ---------------------------------------------------------------------------

async fn run_async(args: Args) {
    let client = redis::Client::open(args.redis_url.as_str()).unwrap();
    let cm = Arc::new(client.get_connection_manager().await.unwrap());

    // Build trypema rate limiter (used for RedisLimiter::Trypema arm).
    let rl = Arc::new(RateLimiter::new(RateLimiterOptions {
        local: crate::args::build_local_options(&args),
        redis: crate::args::build_redis_options(&args, (*cm).clone()),
    }));
    let rate = RateLimit::try_from(args.rate_limit_per_s).unwrap();

    // Warm up scripts / modules before the measurement window.
    warmup(&args, &cm).await;

    let keys = build_keys(&args);
    let counts = Arc::new(Counts::default());
    let total_ops = Arc::new(AtomicU64::new(0));
    let error_stats = Arc::new(ErrorStats::default());
    let stop = Arc::new(AtomicBool::new(false));

    let started = Instant::now();
    let deadline = started + Duration::from_secs(args.duration_s);

    let mut join_handles = Vec::with_capacity(args.threads);

    for t in 0..args.threads {
        let stop = Arc::clone(&stop);
        let counts = Arc::clone(&counts);
        let total_ops = Arc::clone(&total_ops);
        let error_stats = Arc::clone(&error_stats);
        let args = args.clone();
        let keys = keys.clone();
        let cm = Arc::clone(&cm);
        let rl = Arc::clone(&rl);
        let rate = rate.clone();

        // Pre-build RedisKeys for the trypema arm.
        let trypema_keys: Vec<RedisKey> = keys
            .iter()
            .map(|k| RedisKey::try_from(k.clone()).unwrap())
            .collect();

        join_handles.push(spawn_task(async move {
            let mut wl = WorkerLoop::new(
                args.clone(),
                keys,
                counts,
                total_ops,
                error_stats,
                t,
                started,
                deadline,
            );

            // Per-task connection manager clone.
            let mut conn = (*cm).clone();
            let gcra_script = redis::Script::new(GCRA_ALLOW_N_LUA);

            while !stop.load(Ordering::Relaxed) && wl.should_continue() {
                // Async QPS pacing.
                if args.mode != crate::args::Mode::Max {
                    if let Some(qps) = crate::runner::qps_for_now(&args, started) {
                        let per_op_ns = 1_000_000_000u64 / qps.max(1);
                        let now = Instant::now();
                        if now < wl.next_deadline {
                            async_sleep(wl.next_deadline - now).await;
                        }
                        wl.next_deadline += Duration::from_nanos(per_op_ns);
                    }
                }

                let idx = wl.pick_idx();
                let t0 = wl.begin_iter();

                match args.redis_limiter {
                    RedisLimiter::Trypema => {
                        let k = &trypema_keys[idx % trypema_keys.len()];
                        let res = match args.strategy {
                            Strategy::Absolute => rl.redis().absolute().inc(k, &rate, 1).await,
                            Strategy::Suppressed => rl.redis().suppressed().inc(k, &rate, 1).await,
                        };
                        wl.end_iter(t0);
                        match res {
                            Ok(decision) => wl.record_decision(decision),
                            Err(err) => wl.record_error(
                                err.to_string(),
                                Some(format!("provider=redis limiter=trypema key={k:?} err={err:?}")),
                            ),
                        }
                    }

                    RedisLimiter::Cell => {
                        let key = wl.key(idx);
                        let full_key = format!("{}:{}", args.redis_prefix, key);
                        let res: redis::RedisResult<(i64, i64, i64, i64, i64)> =
                            redis::cmd("CL.THROTTLE")
                                .arg(&full_key)
                                .arg(args.cell_burst)
                                .arg(args.rate_limit_per_s.max(1.0).round() as u64)
                                .arg(1u64) // period = 1 s
                                .arg(1u64) // quantity = 1
                                .query_async(&mut conn)
                                .await;
                        wl.end_iter(t0);
                        match res {
                            Ok((limited, _, _, _, _)) => wl.record_allowed(limited == 0),
                            Err(err) => wl.record_error(
                                err.to_string(),
                                Some(format!(
                                    "provider=redis limiter=cell key={full_key} err={err:?}"
                                )),
                            ),
                        }
                    }

                    RedisLimiter::Gcra => {
                        let key = wl.key(idx);
                        let full_key = format!("{}:{}", args.redis_prefix, key);
                        let res: redis::RedisResult<(i64, f64, String, String)> = gcra_script
                            .key(&full_key)
                            .arg(args.gcra_burst)
                            .arg(args.rate_limit_per_s.max(1.0).round() as u64)
                            .arg(1u64) // period = 1 s
                            .arg(1u64) // cost = 1
                            .invoke_async(&mut conn)
                            .await;
                        wl.end_iter(t0);
                        match res {
                            Ok((allowed, _, _, _)) => wl.record_allowed(allowed != 0),
                            Err(err) => wl.record_error(
                                err.to_string(),
                                Some(format!(
                                    "provider=redis limiter=gcra key={full_key} err={err:?}"
                                )),
                            ),
                        }
                    }
                }
            }

            wl.into_hist()
        }));
    }

    async_sleep(Duration::from_secs(args.duration_s)).await;
    stop.store(true, Ordering::Relaxed);

    let mut merged = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    for handle in join_handles {
        merged.add(join_task(handle).await).unwrap();
    }

    let elapsed = started.elapsed();
    let ops = total_ops.load(Ordering::Relaxed);
    println!("redis_limiter={:?}", args.redis_limiter);
    print_results(&args, elapsed, ops, ops as f64 / elapsed.as_secs_f64(), &merged, &counts);
    print_error_stats(counts.errors.load(Ordering::Relaxed), &error_stats);
}

// ---------------------------------------------------------------------------
// Warmup
// ---------------------------------------------------------------------------

async fn warmup(args: &Args, cm: &Arc<redis::aio::ConnectionManager>) {
    let mut conn = (**cm).clone();
    let key = format!("{}:warmup", args.redis_prefix);

    match args.redis_limiter {
        RedisLimiter::Cell => {
            let _: redis::RedisResult<(i64, i64, i64, i64, i64)> = redis::cmd("CL.THROTTLE")
                .arg(&key)
                .arg(args.cell_burst)
                .arg(args.rate_limit_per_s.max(1.0).round() as u64)
                .arg(1u64)
                .arg(1u64)
                .query_async(&mut conn)
                .await;
        }
        RedisLimiter::Gcra => {
            let script = redis::Script::new(GCRA_ALLOW_N_LUA);
            let _: redis::RedisResult<(i64, f64, String, String)> = script
                .key(&key)
                .arg(args.gcra_burst)
                .arg(args.rate_limit_per_s.max(1.0).round() as u64)
                .arg(1u64)
                .arg(1u64)
                .invoke_async(&mut conn)
                .await;
        }
        RedisLimiter::Trypema => {} // no script warmup needed
    }
}
