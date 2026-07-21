//! Redis-provider and Redis-backend comparison stress runner.

#![cfg(any(feature = "redis-tokio", feature = "redis-smol"))]

use std::sync::Arc;

use trypema::{
    BucketSize, HardLimitFactor, RateLimit, RateLimiterBuilder, SuppressionFactorCachePeriod,
    WindowSize,
    redis::{RedisKey, RedisRateLimiterProvider},
};

use crate::{
    args::{Args, RedisLimiter, Strategy, build_keys},
    runner::{RunState, StartGate, WorkerLoop, finish_run},
    runtime::{async_sleep, join_task, spawn_task},
};

// Port of go-redis/redis_rate's allow-N GCRA script.
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
    smol::block_on(run_async(args.clone()));
} // end fn run

async fn run_async(args: Args) {
    let connection_manager = Arc::new(
        crate::runtime::create_redis_connection_manager(&args.redis_url)
            .await
            .unwrap_or_else(|error| {
                eprintln!("failed to connect to Redis: {error}");
                std::process::exit(2);
            }),
    );
    let rate_limiter = RedisRateLimiterProvider::builder((*connection_manager).clone())
        .prefix(RedisKey::try_from(args.redis_prefix.as_str()).unwrap())
        .window_size(WindowSize::seconds_or_panic(args.window_s))
        .bucket_size(BucketSize::milliseconds_or_panic(args.group_ms))
        .hard_limit_factor(HardLimitFactor::new_or_panic(args.hard_limit_factor))
        .suppression_factor_cache_period(SuppressionFactorCachePeriod::milliseconds_or_panic(
            args.suppression_cache_ms,
        ))
        .cleanup_enabled(false)
        .build()
        .unwrap();
    let rate_limit = RateLimit::per_second(args.rate_limit_per_s).unwrap();

    warmup(&args, &connection_manager, &rate_limiter, &rate_limit).await;

    let args = Arc::new(args);
    let keys = build_keys(&args);
    let trypema_keys: Arc<[RedisKey]> = keys
        .iter()
        .cloned()
        .map(RedisKey::try_from)
        .collect::<Result<Vec<_>, _>>()
        .expect("generated stress keys must be valid Redis keys")
        .into();
    let external_keys: Arc<[String]> = keys
        .iter()
        .map(|key| format!("{}:{key}", args.redis_prefix))
        .collect::<Vec<_>>()
        .into();
    let state = RunState::default();
    let start_gate = Arc::new(StartGate::new(args.threads));
    let mut join_handles = Vec::with_capacity(args.threads);

    for worker_index in 0..args.threads {
        let args = Arc::clone(&args);
        let trypema_keys = Arc::clone(&trypema_keys);
        let external_keys = Arc::clone(&external_keys);
        let connection_manager = Arc::clone(&connection_manager);
        let rate_limiter = Arc::clone(&rate_limiter);
        let state = state.clone();
        let start_gate = Arc::clone(&start_gate);

        join_handles.push(spawn_task(async move {
            let started = start_gate.wait_async().await;
            let mut worker = WorkerLoop::new(
                Arc::clone(&args),
                trypema_keys.len(),
                state,
                worker_index,
                started,
            );
            let mut connection = (*connection_manager).clone();
            let gcra_script = redis::Script::new(GCRA_ALLOW_N_LUA);

            while worker.should_continue() {
                if let Some(delay) = worker.pacing_delay() {
                    async_sleep(delay).await;
                }
                if !worker.should_continue() {
                    break;
                }

                let key_index = worker.pick_index();
                let sample_started = worker.begin_iteration();

                match args.redis_limiter {
                    RedisLimiter::Trypema => {
                        let key = &trypema_keys[key_index];
                        let result = match args.strategy {
                            Strategy::Absolute => {
                                rate_limiter.absolute().inc(key, &rate_limit, 1).await
                            }
                            Strategy::Suppressed => {
                                rate_limiter.suppressed().inc(key, &rate_limit, 1).await
                            }
                        };
                        worker.end_iteration(sample_started);

                        match result {
                            Ok(decision) => worker.record_decision(args.strategy, decision, || {
                                format!("provider=redis limiter=trypema key={key:?}")
                            }),
                            Err(error) => worker.record_error(
                                error.to_string(),
                                Some(format!(
                                    "provider=redis limiter=trypema key={key:?} error={error:?}"
                                )),
                            ),
                        }
                    }
                    RedisLimiter::Cell => {
                        let key = &external_keys[key_index];
                        let result: redis::RedisResult<(i64, i64, i64, i64, i64)> =
                            redis::cmd("CL.THROTTLE")
                                .arg(key)
                                .arg(args.cell_burst)
                                .arg(args.rate_limit_per_s as u64)
                                .arg(1u64)
                                .arg(1u64)
                                .query_async(&mut connection)
                                .await;
                        worker.end_iteration(sample_started);

                        match result {
                            Ok((is_limited, _, _, _, _)) => worker.record_allowed(is_limited == 0),
                            Err(error) => worker.record_error(
                                error.to_string(),
                                Some(format!(
                                    "provider=redis limiter=cell key={key} error={error:?}"
                                )),
                            ),
                        }
                    }
                    RedisLimiter::Gcra => {
                        let key = &external_keys[key_index];
                        let result: redis::RedisResult<(i64, f64, String, String)> = gcra_script
                            .key(key)
                            .arg(args.gcra_burst)
                            .arg(args.rate_limit_per_s as u64)
                            .arg(1u64)
                            .arg(1u64)
                            .invoke_async(&mut connection)
                            .await;
                        worker.end_iteration(sample_started);

                        match result {
                            Ok((allowed_count, _, _, _)) => {
                                worker.record_allowed(allowed_count != 0)
                            }
                            Err(error) => worker.record_error(
                                error.to_string(),
                                Some(format!(
                                    "provider=redis limiter=gcra key={key} error={error:?}"
                                )),
                            ),
                        }
                    }
                }
            }

            worker.into_histogram()
        }));
    }

    let started = start_gate.start_async().await;
    let mut histograms = Vec::with_capacity(join_handles.len());
    for handle in join_handles {
        histograms.push(join_task(handle).await);
    }
    match args.redis_limiter {
        RedisLimiter::Trypema => println!("redis_limiter=Trypema"),
        RedisLimiter::Cell => println!("redis_limiter=Cell cell_burst={}", args.cell_burst),
        RedisLimiter::Gcra => println!("redis_limiter=Gcra gcra_burst={}", args.gcra_burst),
    }
    finish_run(&args, started, histograms, &state);
} // end fn run_async

async fn warmup(
    args: &Args,
    connection_manager: &redis::aio::ConnectionManager,
    rate_limiter: &RedisRateLimiterProvider,
    rate_limit: &RateLimit,
) {
    let result = match args.redis_limiter {
        RedisLimiter::Trypema => {
            let key = RedisKey::new_or_panic("stress_warmup".to_string());
            match args.strategy {
                Strategy::Absolute => rate_limiter
                    .absolute()
                    .inc(&key, rate_limit, 1)
                    .await
                    .map(|_| ()),
                Strategy::Suppressed => rate_limiter
                    .suppressed()
                    .inc(&key, rate_limit, 1)
                    .await
                    .map(|_| ()),
            }
            .map_err(|error| error.to_string())
        }
        RedisLimiter::Cell => {
            let mut connection = connection_manager.clone();
            let key = format!("{}:warmup", args.redis_prefix);
            redis::cmd("CL.THROTTLE")
                .arg(key)
                .arg(args.cell_burst)
                .arg(args.rate_limit_per_s as u64)
                .arg(1u64)
                .arg(1u64)
                .query_async::<(i64, i64, i64, i64, i64)>(&mut connection)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        }
        RedisLimiter::Gcra => {
            let mut connection = connection_manager.clone();
            let key = format!("{}:warmup", args.redis_prefix);
            redis::Script::new(GCRA_ALLOW_N_LUA)
                .key(key)
                .arg(args.gcra_burst)
                .arg(args.rate_limit_per_s as u64)
                .arg(1u64)
                .arg(1u64)
                .invoke_async::<(i64, f64, String, String)>(&mut connection)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        }
    };

    if let Err(error) = result {
        eprintln!("redis backend warmup failed: {error}");
        std::process::exit(2);
    }
} // end fn warmup
