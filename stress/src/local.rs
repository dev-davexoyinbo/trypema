use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use burster::Limiter;
use dashmap::DashMap;
use governor::{DefaultKeyedRateLimiter, Quota};
use trypema::{RateLimit, RateLimitDecision};

use crate::{
    args::{Args, LocalLimiter, Strategy, build_keys},
    runner::{RunState, StartGate, WorkerLoop, finish_run},
};

static BURSTER_EPOCH: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

fn burster_now() -> Duration {
    BURSTER_EPOCH.get_or_init(Instant::now).elapsed()
} // end fn burster_now

type BursterMap<const WINDOW_MS: usize> =
    Arc<DashMap<String, std::sync::Mutex<burster::SlidingWindowLog<fn() -> Duration, WINDOW_MS>>>>;

enum BursterStore {
    Window10(BursterMap<10_000>),
    Window60(BursterMap<60_000>),
    Window300(BursterMap<300_000>),
}

impl BursterStore {
    fn for_window_seconds(window_size_seconds: u64) -> Option<Self> {
        match window_size_seconds {
            10 => Some(Self::Window10(Arc::new(DashMap::new()))),
            60 => Some(Self::Window60(Arc::new(DashMap::new()))),
            300 => Some(Self::Window300(Arc::new(DashMap::new()))),
            _ => None,
        }
    } // end fn for_window_seconds

    fn try_consume_one(&self, key: &str, window_limit: u64) -> bool {
        macro_rules! consume {
            ($series:expr) => {{
                let series = match $series.get(key) {
                    Some(series) => series,
                    None => $series
                        .entry(key.to_owned())
                        .or_insert_with(|| {
                            std::sync::Mutex::new(
                                burster::SlidingWindowLog::new_with_time_provider(
                                    window_limit,
                                    burster_now as fn() -> Duration,
                                ),
                            )
                        })
                        .downgrade(),
                };

                series
                    .value()
                    .lock()
                    .expect("burster series mutex poisoned")
                    .try_consume_one()
                    .is_ok()
            }};
        }

        match self {
            Self::Window10(series) => consume!(series),
            Self::Window60(series) => consume!(series),
            Self::Window300(series) => consume!(series),
        }
    } // end fn try_consume_one
} // end impl

enum LocalIterationOutcome {
    Decision(RateLimitDecision),
    Allowed(bool),
}

pub(crate) fn run(args: &Args) {
    match args.local_limiter {
        LocalLimiter::Trypema => run_trypema(args),
        LocalLimiter::Burster => run_burster(args),
        LocalLimiter::Governor => run_governor(args),
    }
} // end fn run

fn run_trypema(args: &Args) {
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    eprintln!(
        "note: a Redis-enabled stress build must initialize Redis even for --provider local; use the no-feature build for isolated local measurements"
    );

    let rate_limiter = Arc::new(build_trypema_rate_limiter(args));
    let rate_limit = RateLimit::try_from(args.rate_limit_per_s).unwrap();
    let strategy = args.strategy;

    println!("local_limiter=Trypema");
    run_sync(args, None, move |key| {
        let decision = match strategy {
            Strategy::Absolute => rate_limiter.local().absolute().inc(key, &rate_limit, 1),
            Strategy::Suppressed => rate_limiter.local().suppressed().inc(key, &rate_limit, 1),
        };
        LocalIterationOutcome::Decision(decision)
    });
} // end fn run_trypema

fn run_burster(args: &Args) {
    const STACK_SIZE: usize = 16 * 1024 * 1024;

    let store = match BursterStore::for_window_seconds(args.window_s) {
        Some(store) => Arc::new(store),
        None => {
            eprintln!(
                "unsupported --window-s {} for burster (supported: 10, 60, 300)",
                args.window_s
            );
            std::process::exit(2);
        }
    };
    let window_limit = (args.rate_limit_per_s * args.window_s as f64) as u64;

    println!("local_limiter=Burster window_limit={window_limit}");
    run_sync(args, Some(STACK_SIZE), move |key| {
        LocalIterationOutcome::Allowed(store.try_consume_one(key, window_limit))
    });
} // end fn run_burster

fn run_governor(args: &Args) {
    const STACK_SIZE: usize = 16 * 1024 * 1024;

    let rate_limit_per_second = args.rate_limit_per_s as u32;
    let burst_count = (args.rate_limit_per_s * args.window_s as f64) as u32;
    let quota = Quota::per_second(
        std::num::NonZeroU32::new(rate_limit_per_second)
            .expect("validated governor rate must be non-zero"),
    )
    .allow_burst(
        std::num::NonZeroU32::new(burst_count).expect("validated governor burst must be non-zero"),
    );
    let rate_limiter: Arc<DefaultKeyedRateLimiter<String>> =
        Arc::new(governor::RateLimiter::keyed(quota));

    println!(
        "local_limiter=Governor rate_limit_per_s={rate_limit_per_second} burst_count={burst_count}"
    );
    run_sync(args, Some(STACK_SIZE), move |key| {
        LocalIterationOutcome::Allowed(rate_limiter.check_key(key).is_ok())
    });
} // end fn run_governor

fn run_sync(
    args: &Args,
    stack_size: Option<usize>,
    operation: impl Fn(&String) -> LocalIterationOutcome + Send + Sync + 'static,
) {
    let args = Arc::new(args.clone());
    let keys = build_keys(&args);
    let operation = Arc::new(operation);
    let state = RunState::default();
    let start_gate = Arc::new(StartGate::new(args.threads));
    let mut handles = Vec::with_capacity(args.threads);

    for worker_index in 0..args.threads {
        let args = Arc::clone(&args);
        let keys = Arc::clone(&keys);
        let operation = Arc::clone(&operation);
        let state = state.clone();
        let start_gate = Arc::clone(&start_gate);
        let mut builder = std::thread::Builder::new();
        if let Some(stack_size) = stack_size {
            builder = builder.stack_size(stack_size);
        }

        handles.push(
            builder
                .spawn(move || {
                    let started = start_gate.wait_sync();
                    let mut worker = WorkerLoop::new(
                        Arc::clone(&args),
                        keys.len(),
                        state,
                        worker_index,
                        started,
                    );

                    while worker.should_continue() {
                        if let Some(delay) = worker.pacing_delay() {
                            std::thread::sleep(delay);
                        }
                        if !worker.should_continue() {
                            break;
                        }

                        let key_index = worker.pick_index();
                        let key = &keys[key_index];
                        let sample_started = worker.begin_iteration();
                        let outcome = operation(key);
                        worker.end_iteration(sample_started);

                        match outcome {
                            LocalIterationOutcome::Decision(decision) => {
                                worker.record_decision(args.strategy, decision, || {
                                    format!("provider=local key={key}")
                                })
                            }
                            LocalIterationOutcome::Allowed(is_allowed) => {
                                worker.record_allowed(is_allowed)
                            }
                        }
                    }

                    worker.into_histogram()
                })
                .unwrap(),
        );
    }

    let started = start_gate.start_sync();
    let histograms = handles
        .into_iter()
        .map(|handle| handle.join().expect("stress worker thread panicked"))
        .collect::<Vec<_>>();
    finish_run(&args, started, histograms, &state);
} // end fn run_sync

fn build_trypema_rate_limiter(args: &Args) -> trypema::RateLimiter {
    #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
    {
        trypema::RateLimiter::new(trypema::RateLimiterOptions {
            local: crate::args::build_local_options(args),
        })
    }

    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    {
        let connection_manager = crate::runtime::block_on(
            crate::runtime::create_redis_connection_manager(&args.redis_url),
        )
        .unwrap_or_else(|error| {
            eprintln!("failed to connect to Redis: {error}");
            std::process::exit(2);
        });
        trypema::RateLimiter::new(trypema::RateLimiterOptions {
            local: crate::args::build_local_options(args),
            redis: crate::args::build_redis_options(args, connection_manager),
        })
    }
} // end fn build_trypema_rate_limiter
