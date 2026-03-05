//! Hybrid provider benchmark runner.
//!
//! Calls `rl.hybrid().absolute()` or `rl.hybrid().suppressed()` depending on
//! `--strategy`.  The hybrid limiter maintains local state synchronised with
//! Redis in the background, giving lower latency than the pure Redis provider.

#![cfg(any(feature = "redis-tokio", feature = "redis-smol"))]

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use trypema::{RateLimit, RateLimiter, RateLimiterOptions, RateLimitDecision, redis::RedisKey};

use crate::args::{Args, Strategy, build_keys};
use crate::runner::{Counts, ErrorStats, WorkerLoop, print_error_stats, print_results};
use crate::runtime::{async_sleep, join_task, spawn_task};

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
// Async core
// ---------------------------------------------------------------------------

async fn run_async(args: Args) {
    let client = redis::Client::open(args.redis_url.as_str()).unwrap();
    let cm = client.get_connection_manager().await.unwrap();

    let rl = Arc::new(RateLimiter::new(RateLimiterOptions {
        local: crate::args::build_local_options(&args),
        redis: crate::args::build_redis_options(&args, cm),
    }));
    let rate = RateLimit::try_from(args.rate_limit_per_s).unwrap();

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
        let rl = Arc::clone(&rl);
        let rate = rate.clone();

        let redis_keys: Vec<RedisKey> = keys
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
                let k = &redis_keys[idx % redis_keys.len()];
                let t0 = wl.begin_iter();

                let res = match args.strategy {
                    Strategy::Absolute => rl.hybrid().absolute().inc(k, &rate, 1).await,
                    Strategy::Suppressed => rl.hybrid().suppressed().inc(k, &rate, 1).await,
                };

                wl.end_iter(t0);

                match res {
                    Ok(decision) => {
                        // Hybrid absolute must never return Suppressed.
                        if args.strategy == Strategy::Absolute
                            && matches!(decision, RateLimitDecision::Suppressed { .. })
                        {
                            wl.record_error(
                                "unexpected suppressed decision".to_string(),
                                Some(format!(
                                    "provider=hybrid key={k:?} err=unexpected_suppressed"
                                )),
                            );
                        } else {
                            wl.record_decision(decision);
                        }
                    }
                    Err(err) => wl.record_error(
                        err.to_string(),
                        Some(format!("provider=hybrid key={k:?} err={err:?}")),
                    ),
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
    print_results(&args, elapsed, ops, ops as f64 / elapsed.as_secs_f64(), &merged, &counts);
    print_error_stats(counts.errors.load(Ordering::Relaxed), &error_stats);
}
