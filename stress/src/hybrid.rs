//! Hybrid-provider stress runner.

#![cfg(any(feature = "redis-tokio", feature = "redis-smol"))]

use std::sync::Arc;

use trypema::{RateLimit, RateLimiter, RateLimiterOptions, redis::RedisKey};

use crate::{
    args::{Args, Strategy, build_keys},
    runner::{RunState, StartGate, WorkerLoop, finish_run},
    runtime::{async_sleep, join_task, spawn_task},
};

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
    let connection_manager = crate::runtime::create_redis_connection_manager(&args.redis_url)
        .await
        .unwrap_or_else(|error| {
            eprintln!("failed to connect to Redis: {error}");
            std::process::exit(2);
        });
    let rate_limiter = Arc::new(RateLimiter::new(RateLimiterOptions {
        local: crate::args::build_local_options(&args),
        redis: crate::args::build_redis_options(&args, connection_manager),
    }));
    let rate_limit = RateLimit::per_second(args.rate_limit_per_s).unwrap();

    warmup(&args, &rate_limiter, &rate_limit).await;

    let args = Arc::new(args);
    let keys = build_keys(&args);
    let redis_keys: Arc<[RedisKey]> = keys
        .iter()
        .cloned()
        .map(RedisKey::try_from)
        .collect::<Result<Vec<_>, _>>()
        .expect("generated stress keys must be valid Redis keys")
        .into();
    let state = RunState::default();
    let start_gate = Arc::new(StartGate::new(args.threads));
    let mut join_handles = Vec::with_capacity(args.threads);

    for worker_index in 0..args.threads {
        let args = Arc::clone(&args);
        let redis_keys = Arc::clone(&redis_keys);
        let rate_limiter = Arc::clone(&rate_limiter);
        let state = state.clone();
        let start_gate = Arc::clone(&start_gate);

        join_handles.push(spawn_task(async move {
            let started = start_gate.wait_async().await;
            let mut worker = WorkerLoop::new(
                Arc::clone(&args),
                redis_keys.len(),
                state,
                worker_index,
                started,
            );

            while worker.should_continue() {
                if let Some(delay) = worker.pacing_delay() {
                    async_sleep(delay).await;
                }
                if !worker.should_continue() {
                    break;
                }

                let key_index = worker.pick_index();
                let key = &redis_keys[key_index];
                let sample_started = worker.begin_iteration();
                let result = match args.strategy {
                    Strategy::Absolute => {
                        rate_limiter
                            .hybrid()
                            .absolute()
                            .inc(key, &rate_limit, 1)
                            .await
                    }
                    Strategy::Suppressed => {
                        rate_limiter
                            .hybrid()
                            .suppressed()
                            .inc(key, &rate_limit, 1)
                            .await
                    }
                };
                worker.end_iteration(sample_started);

                match result {
                    Ok(decision) => worker.record_decision(args.strategy, decision, || {
                        format!("provider=hybrid key={key:?}")
                    }),
                    Err(error) => worker.record_error(
                        error.to_string(),
                        Some(format!("provider=hybrid key={key:?} error={error:?}")),
                    ),
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
    finish_run(&args, started, histograms, &state);
} // end fn run_async

async fn warmup(args: &Args, rate_limiter: &RateLimiter, rate_limit: &RateLimit) {
    let key = RedisKey::new_or_panic("stress_warmup".to_string());
    let result = match args.strategy {
        Strategy::Absolute => {
            rate_limiter
                .hybrid()
                .absolute()
                .inc(&key, rate_limit, 1)
                .await
        }
        Strategy::Suppressed => {
            rate_limiter
                .hybrid()
                .suppressed()
                .inc(&key, rate_limit, 1)
                .await
        }
    };

    if let Err(error) = result {
        eprintln!("hybrid warmup failed: {error}");
        std::process::exit(2);
    }
} // end fn warmup
