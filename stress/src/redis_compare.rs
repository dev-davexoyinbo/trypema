use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

use redis::aio::ConnectionManager;

use crate::{
    Args, Counts, ErrorStats, KeyDist, Mode, print_error_stats, qps_for_now, should_sample,
};

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub(crate) enum RedisLimiter {
    TrypemaAbsolute,
    TrypemaSuppressed,
    Cell,
    Gcra,
}

pub(crate) async fn run(args2: Args, limiter: RedisLimiter) {
    // Keys for distribution
    let keys = crate::build_keys(&args2);
    let redis_keys: Vec<String> = keys.iter().map(|k| k.to_string()).collect();

    let client = redis::Client::open(args2.redis_url.as_str()).unwrap();
    let connection_manager = client.get_connection_manager().await.unwrap();

    // Create separate connection managers per task to reduce shared contention.
    let cm = Arc::new(connection_manager);

    // Warm up scripts / module
    warmup(&args2, &cm, limiter, &redis_keys).await;

    let stop = Arc::new(AtomicBool::new(false));
    let counts = Arc::new(Counts::default());
    let total_ops = Arc::new(AtomicU64::new(0));
    let error_stats = Arc::new(ErrorStats::default());

    let started = Instant::now();
    let deadline = started + Duration::from_secs(args2.duration_s);

    let mut join = Vec::with_capacity(args2.threads);
    for t in 0..args2.threads {
        let stop = Arc::clone(&stop);
        let counts = Arc::clone(&counts);
        let total_ops = Arc::clone(&total_ops);
        let error_stats = Arc::clone(&error_stats);
        let args = args2.clone();
        let redis_keys = redis_keys.clone();
        let cm = Arc::clone(&cm);

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

            // Per-task connection manager clone
            let mut conn = (*cm).clone();

            // Script handles (so we use EVALSHA path)
            let gcra_script = redis::Script::new(GCRA_ALLOW_N_LUA);

            while !stop.load(Ordering::Relaxed) {
                if Instant::now() >= deadline {
                    break;
                }

                if args.mode != Mode::Max
                    && let Some(qps) = qps_for_now(&args, started)
                {
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
                let key = &redis_keys[idx % redis_keys.len()];

                let sample = should_sample(i, args.sample_every);
                let t0 = if sample { Some(Instant::now()) } else { None };

                let ok = match limiter {
                    RedisLimiter::TrypemaAbsolute | RedisLimiter::TrypemaSuppressed => true,
                    RedisLimiter::Cell => {
                        // CL.THROTTLE <key> <max_burst> <count per period> <period> [<quantity>]
                        // Map args.rate_limit_per_s into count-per-period (per second) with period=1.
                        let max_burst: u64 = args.cell_burst;
                        let count_per_period: u64 = args.rate_limit_per_s.max(1.0).round() as u64;
                        let period_s: u64 = 1;
                        let quantity: u64 = 1;
                        let full_key = format!("{}:{}", args.redis_prefix, key);
                        let res: redis::RedisResult<(i64, i64, i64, i64, i64)> =
                            redis::cmd("CL.THROTTLE")
                                .arg(&full_key)
                                .arg(max_burst)
                                .arg(count_per_period)
                                .arg(period_s)
                                .arg(quantity)
                                .query_async(&mut conn)
                                .await;
                        match res {
                            Ok((limited, _, _, _, _)) => {
                                if limited == 0 {
                                    counts.allowed.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    counts.rejected.fetch_add(1, Ordering::Relaxed);
                                }
                                true
                            }
                            Err(err) => {
                                error_stats.record(
                                    err.to_string(),
                                    Some(format!(
                                        "provider=redis_compare limiter=cell key={full_key} err={err:?}"
                                    )),
                                );
                                false
                            }
                        }
                    }
                    RedisLimiter::Gcra => {
                        // Equivalent to go-redis/redis_rate allowN script.
                        // KEYS[1]=key
                        // ARGV: burst, rate, period, cost
                        let burst: u64 = args.gcra_burst;
                        let rate: u64 = args.rate_limit_per_s.max(1.0).round() as u64;
                        let period: u64 = 1;
                        let cost: u64 = 1;
                        let full_key = format!("{}:{}", args.redis_prefix, key);
                        let res: redis::RedisResult<(i64, f64, String, String)> = gcra_script
                            .key(&full_key)
                            .arg(burst)
                            .arg(rate)
                            .arg(period)
                            .arg(cost)
                            .invoke_async(&mut conn)
                            .await;
                        match res {
                            Ok((allowed, _remaining, _retry_after, _reset_after)) => {
                                if allowed != 0 {
                                    counts.allowed.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    counts.rejected.fetch_add(1, Ordering::Relaxed);
                                }
                                true
                            }
                            Err(err) => {
                                error_stats.record(
                                    err.to_string(),
                                    Some(format!(
                                        "provider=redis_compare limiter=gcra key={full_key} err={err:?}"
                                    )),
                                );
                                false
                            }
                        }
                    }
                };

                if let Some(t0) = t0 {
                    let us = t0.elapsed().as_micros() as u64;
                    let _ = hist.record(us.max(1));
                }

                total_ops.fetch_add(1, Ordering::Relaxed);
                if !ok {
                    counts.errors.fetch_add(1, Ordering::Relaxed);
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

    println!("redis_limiter={:?}", limiter);
    crate::print_results(&args2, elapsed, ops, ops_s, &merged, &counts);

    print_error_stats(counts.errors.load(Ordering::Relaxed), &error_stats);
}

async fn warmup(args: &Args, cm: &Arc<ConnectionManager>, limiter: RedisLimiter, keys: &[String]) {
    let mut conn = (**cm).clone();
    match limiter {
        RedisLimiter::Cell => {
            let full_key = format!("{}:{}", args.redis_prefix, keys.first().unwrap());
            let _: redis::RedisResult<(i64, i64, i64, i64, i64)> = redis::cmd("CL.THROTTLE")
                .arg(full_key)
                .arg(args.cell_burst)
                .arg(args.rate_limit_per_s.max(1.0).round() as u64)
                .arg(1u64)
                .arg(1u64)
                .query_async(&mut conn)
                .await;
        }
        RedisLimiter::Gcra => {
            let script = redis::Script::new(GCRA_ALLOW_N_LUA);
            let full_key = format!("{}:{}", args.redis_prefix, keys.first().unwrap());
            let _: redis::RedisResult<(i64, f64, String, String)> = script
                .key(full_key)
                .arg(args.gcra_burst)
                .arg(args.rate_limit_per_s.max(1.0).round() as u64)
                .arg(1u64)
                .arg(1u64)
                .invoke_async(&mut conn)
                .await;
        }
        _ => {}
    }
}

// Copied verbatim (with backticks removed) from go-redis/redis_rate allowN script.
// Source: https://github.com/go-redis/redis_rate/blob/v10/lua.go
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
  return {
    0,
    0,
    tostring(retry_after),
    tostring(reset_after),
  }
end

local reset_after = new_tat - now
if reset_after > 0 then
  redis.call("SET", rate_limit_key, new_tat, "EX", math.ceil(reset_after))
end
local retry_after = -1
return {cost, remaining, tostring(retry_after), tostring(reset_after)}
"#;
