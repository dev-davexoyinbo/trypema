use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use burster::Limiter;
use dashmap::DashMap;
use governor::{DefaultKeyedRateLimiter, Quota};
use hdrhistogram::Histogram;

use trypema::RateLimit;

use crate::args::{Args, LocalLimiter, Strategy};
use crate::runner::{Counts, ErrorStats, WorkerLoop, print_error_stats, print_results};

// ---------------------------------------------------------------------------
// Burster support
//
// burster::SlidingWindowLog<_, W> has the window width in ms baked into the
// const generic.  We support 10 s, 60 s and 300 s windows; other values
// produce a friendly error at startup.
// ---------------------------------------------------------------------------

static BURSTER_EPOCH: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

fn burster_now() -> Duration {
    BURSTER_EPOCH.get_or_init(Instant::now).elapsed()
}

type BursterMap<const W: usize> =
    Arc<DashMap<String, std::sync::Mutex<burster::SlidingWindowLog<fn() -> Duration, W>>>>;

enum BursterStore {
    W10(BursterMap<10_000>),
    W60(BursterMap<60_000>),
    W300(BursterMap<300_000>),
}

impl BursterStore {
    fn for_window_s(window_s: u64) -> Option<Self> {
        match window_s {
            10 => Some(BursterStore::W10(Arc::new(DashMap::new()))),
            60 => Some(BursterStore::W60(Arc::new(DashMap::new()))),
            300 => Some(BursterStore::W300(Arc::new(DashMap::new()))),
            _ => None,
        }
    }

    fn try_consume_one(&self, key: &str, capacity: u64) -> bool {
        macro_rules! consume {
            ($map:expr, $w:literal) => {{
                // get_or_insert pattern: try existing entry first, insert on miss.
                if let Some(entry) = $map.get(key) {
                    entry.value().lock().unwrap().try_consume_one().is_ok()
                } else {
                    $map.insert(
                        key.to_owned(),
                        std::sync::Mutex::new(
                            burster::SlidingWindowLog::new_with_time_provider(
                                capacity,
                                burster_now as fn() -> Duration,
                            ),
                        ),
                    );
                    $map.get(key)
                        .unwrap()
                        .value()
                        .lock()
                        .unwrap()
                        .try_consume_one()
                        .is_ok()
                }
            }};
        }

        match self {
            BursterStore::W10(m) => consume!(m, 10_000),
            BursterStore::W60(m) => consume!(m, 60_000),
            BursterStore::W300(m) => consume!(m, 300_000),
        }
    }
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

pub(crate) fn run(args: &Args) {
    if args.mode != crate::args::Mode::Max && args.target_qps.is_none() {
        eprintln!("note: --mode target-qps without --target-qps behaves like --mode max");
    }
    if args.local_limiter != LocalLimiter::Trypema && args.strategy == Strategy::Suppressed {
        eprintln!(
            "note: --strategy suppressed is ignored for --local-limiter {:?}",
            args.local_limiter
        );
    }

    match args.local_limiter {
        LocalLimiter::Trypema => run_trypema(args),
        LocalLimiter::Burster => run_burster(args),
        LocalLimiter::Governor => run_governor(args),
    }
}

// ---------------------------------------------------------------------------
// Trypema local
// ---------------------------------------------------------------------------

fn run_trypema(args: &Args) {
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    eprintln!(
        "note: built with redis support; local runs still open a Redis connection to satisfy RateLimiterOptions"
    );

    let rl = Arc::new(build_trypema_rl(args));
    let rate = RateLimit::try_from(args.rate_limit_per_s).unwrap();
    let keys = crate::args::build_keys(args);

    let counts = Arc::new(Counts::default());
    let total_ops = Arc::new(AtomicU64::new(0));
    let error_stats = Arc::new(ErrorStats::default());

    let started = Instant::now();
    let deadline = started + Duration::from_secs(args.duration_s);

    let mut handles = Vec::with_capacity(args.threads);
    for t in 0..args.threads {
        let rl = Arc::clone(&rl);
        let rate = rate.clone();
        let keys = keys.clone();
        let counts = Arc::clone(&counts);
        let total_ops = Arc::clone(&total_ops);
        let error_stats = Arc::clone(&error_stats);
        let args = args.clone();

        handles.push(
            std::thread::Builder::new()
                .spawn(move || {
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

                    while wl.should_continue() {
                        wl.pace_sync();
                        let idx = wl.pick_idx();
                        let key = wl.key(idx).to_owned();
                        let t0 = wl.begin_iter();

                        let decision = match args.strategy {
                            Strategy::Absolute => rl.local().absolute().inc(&key, &rate, 1),
                            Strategy::Suppressed => rl.local().suppressed().inc(&key, &rate, 1),
                        };

                        wl.end_iter(t0);
                        wl.record_decision(decision);
                    }

                    wl.into_hist()
                })
                .unwrap(),
        );
    }

    std::thread::sleep(Duration::from_secs(args.duration_s));

    let mut merged = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    for h in handles {
        merged.add(h.join().unwrap()).unwrap();
    }

    let elapsed = started.elapsed();
    let ops = total_ops.load(Ordering::Relaxed);
    print_results(args, elapsed, ops, ops as f64 / elapsed.as_secs_f64(), &merged, &counts);
    print_error_stats(counts.errors.load(Ordering::Relaxed), &error_stats);
}

// ---------------------------------------------------------------------------
// Burster local
// ---------------------------------------------------------------------------

fn run_burster(args: &Args) {
    // burster stores a [u64; W] on the stack — give threads extra stack space.
    const STACK_SIZE: usize = 16 * 1024 * 1024;

    let store = match BursterStore::for_window_s(args.window_s) {
        Some(s) => Arc::new(s),
        None => {
            eprintln!(
                "unsupported --window-s {} for burster (supported: 10, 60, 300)",
                args.window_s
            );
            std::process::exit(2);
        }
    };

    let capacity = (args.rate_limit_per_s * args.window_s as f64).max(1.0).round() as u64;
    let keys = crate::args::build_keys(args);

    let counts = Arc::new(Counts::default());
    let total_ops = Arc::new(AtomicU64::new(0));
    let error_stats = Arc::new(ErrorStats::default());

    let started = Instant::now();
    let deadline = started + Duration::from_secs(args.duration_s);

    let mut handles = Vec::with_capacity(args.threads);
    for t in 0..args.threads {
        let store = Arc::clone(&store);
        let keys = keys.clone();
        let counts = Arc::clone(&counts);
        let total_ops = Arc::clone(&total_ops);
        let error_stats = Arc::clone(&error_stats);
        let args = args.clone();

        handles.push(
            std::thread::Builder::new()
                .stack_size(STACK_SIZE)
                .spawn(move || {
                    let mut wl = WorkerLoop::new(
                        args,
                        keys,
                        counts,
                        total_ops,
                        error_stats,
                        t,
                        started,
                        deadline,
                    );

                    while wl.should_continue() {
                        wl.pace_sync();
                        let idx = wl.pick_idx();
                        let key = wl.key(idx).to_owned();
                        let t0 = wl.begin_iter();

                        let allowed = store.try_consume_one(&key, capacity);

                        wl.end_iter(t0);
                        wl.record_allowed(allowed);
                    }

                    wl.into_hist()
                })
                .unwrap(),
        );
    }

    std::thread::sleep(Duration::from_secs(args.duration_s));

    let mut merged = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    for h in handles {
        merged.add(h.join().unwrap()).unwrap();
    }

    let elapsed = started.elapsed();
    let ops = total_ops.load(Ordering::Relaxed);
    println!("local_limiter=Burster burster_capacity_per_window={capacity}");
    print_results(args, elapsed, ops, ops as f64 / elapsed.as_secs_f64(), &merged, &counts);
    print_error_stats(counts.errors.load(Ordering::Relaxed), &error_stats);
}

// ---------------------------------------------------------------------------
// Governor local
// ---------------------------------------------------------------------------

fn run_governor(args: &Args) {
    // governor also uses extra stack space with many keys.
    const STACK_SIZE: usize = 16 * 1024 * 1024;

    let rate_u32 = u32::try_from(
        (args.rate_limit_per_s.max(1.0).round() as u64).min(u32::MAX as u64),
    )
    .unwrap_or(u32::MAX);

    let burst_u32 = u32::try_from(
        ((args.rate_limit_per_s * args.window_s as f64).max(1.0).round() as u64)
            .min(u32::MAX as u64),
    )
    .unwrap_or(u32::MAX)
    .max(1);

    let quota = Quota::per_second(std::num::NonZeroU32::new(rate_u32).unwrap())
        .allow_burst(std::num::NonZeroU32::new(burst_u32).unwrap());
    let rl: Arc<DefaultKeyedRateLimiter<String>> =
        Arc::new(governor::RateLimiter::keyed(quota));

    let capacity = burst_u32 as u64;
    let keys = crate::args::build_keys(args);

    let counts = Arc::new(Counts::default());
    let total_ops = Arc::new(AtomicU64::new(0));
    let error_stats = Arc::new(ErrorStats::default());

    let started = Instant::now();
    let deadline = started + Duration::from_secs(args.duration_s);

    let mut handles = Vec::with_capacity(args.threads);
    for t in 0..args.threads {
        let rl = Arc::clone(&rl);
        let keys = keys.clone();
        let counts = Arc::clone(&counts);
        let total_ops = Arc::clone(&total_ops);
        let error_stats = Arc::clone(&error_stats);
        let args = args.clone();

        handles.push(
            std::thread::Builder::new()
                .stack_size(STACK_SIZE)
                .spawn(move || {
                    let mut wl = WorkerLoop::new(
                        args,
                        keys,
                        counts,
                        total_ops,
                        error_stats,
                        t,
                        started,
                        deadline,
                    );

                    while wl.should_continue() {
                        wl.pace_sync();
                        let idx = wl.pick_idx();
                        let key = wl.key(idx).to_owned();
                        let t0 = wl.begin_iter();

                        let allowed = rl.check_key(&key).is_ok();

                        wl.end_iter(t0);
                        wl.record_allowed(allowed);
                    }

                    wl.into_hist()
                })
                .unwrap(),
        );
    }

    std::thread::sleep(Duration::from_secs(args.duration_s));

    let mut merged = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    for h in handles {
        merged.add(h.join().unwrap()).unwrap();
    }

    let elapsed = started.elapsed();
    let ops = total_ops.load(Ordering::Relaxed);
    println!("local_limiter=Governor governor_capacity_per_window={capacity}");
    print_results(args, elapsed, ops, ops as f64 / elapsed.as_secs_f64(), &merged, &counts);
    print_error_stats(counts.errors.load(Ordering::Relaxed), &error_stats);
}

// ---------------------------------------------------------------------------
// RateLimiter construction helper
// ---------------------------------------------------------------------------

fn build_trypema_rl(args: &Args) -> trypema::RateLimiter {
    #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
    {
        trypema::RateLimiter::new(trypema::RateLimiterOptions {
            local: crate::args::build_local_options(args),
        })
    }

    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    {
        let connection_manager = crate::runtime::block_on(async {
            let client = redis::Client::open(args.redis_url.as_str()).unwrap();
            client.get_connection_manager().await.unwrap()
        });
        trypema::RateLimiter::new(trypema::RateLimiterOptions {
            local: crate::args::build_local_options(args),
            redis: crate::args::build_redis_options(args, connection_manager),
        })
    }
}
