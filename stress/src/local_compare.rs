use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

use burster::Limiter;
use dashmap::DashMap;
use governor::{DefaultKeyedRateLimiter, Quota};
use hdrhistogram::Histogram;

use crate::{
    Args, Counts, KeyDist, LocalLimiter, Mode, Provider, Strategy, qps_for_now, should_sample,
};

static BURSTER_EPOCH: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

fn burster_now() -> Duration {
    BURSTER_EPOCH.get_or_init(Instant::now).elapsed()
}

type BursterLimiter10 = burster::SlidingWindowLog<fn() -> Duration, 10_000>;
type BursterLimiter60 = burster::SlidingWindowLog<fn() -> Duration, 60_000>;
type BursterLimiter300 = burster::SlidingWindowLog<fn() -> Duration, 300_000>;

enum BursterStore {
    W10(Arc<DashMap<String, std::sync::Mutex<BursterLimiter10>>>),
    W60(Arc<DashMap<String, std::sync::Mutex<BursterLimiter60>>>),
    W300(Arc<DashMap<String, std::sync::Mutex<BursterLimiter300>>>),
}

fn burster_store_for_window_s(window_s: u64) -> Option<BursterStore> {
    match window_s {
        10 => Some(BursterStore::W10(Arc::new(DashMap::new()))),
        60 => Some(BursterStore::W60(Arc::new(DashMap::new()))),
        300 => Some(BursterStore::W300(Arc::new(DashMap::new()))),
        _ => None,
    }
}

fn mk_burster<const W: usize>(
    capacity_per_window: u64,
) -> burster::SlidingWindowLog<fn() -> Duration, W> {
    burster::SlidingWindowLog::new_with_time_provider(
        capacity_per_window,
        burster_now as fn() -> Duration,
    )
}

fn pick_key_idx(args: &Args, key_count: usize, rng: &mut impl FnMut() -> u64) -> usize {
    match args.key_dist {
        KeyDist::Hot => 0,
        KeyDist::Uniform => (rng() as usize) % key_count,
        KeyDist::Skewed => {
            let r = (rng() % 10_000) as f64 / 10_000.0;
            if r < args.hot_fraction {
                0
            } else {
                let tail = key_count.saturating_sub(1).max(1);
                1 + ((rng() as usize) % tail)
            }
        }
    }
}

pub(crate) fn run(args2: Args) {
    let args = args2.clone();

    if args.provider != Provider::Local {
        eprintln!("internal error: local_compare called with provider != local");
        std::process::exit(2);
    }

    let keys = crate::build_keys(&args);

    // burster::SlidingWindowLog stores a large `[u64; W]` where `W` is window width in ms.
    // Some competitor implementations also have larger stack frames than trypema.
    // Give threads extra stack for all comparison modes to avoid platform-default limits.
    let thread_stack_size = match args.local_limiter {
        LocalLimiter::Trypema => None,
        LocalLimiter::Burster | LocalLimiter::Governor => Some(16 * 1024 * 1024),
    };

    if args.mode != Mode::Max && args.target_qps.is_none() {
        eprintln!("note: --mode target-qps without --target-qps behaves like --mode max");
    }

    if args.local_limiter != LocalLimiter::Trypema && args.strategy == Strategy::Suppressed {
        eprintln!(
            "note: --strategy suppressed is ignored for --local-limiter {:?}",
            args.local_limiter
        );
    }

    let stop = Arc::new(AtomicBool::new(false));
    let counts = Arc::new(Counts::default());
    let total_ops = Arc::new(AtomicU64::new(0));

    let started = Instant::now();
    let deadline = started + Duration::from_secs(args.duration_s);

    // Shared limiter state
    let trypema_rl = if args.local_limiter == LocalLimiter::Trypema {
        Some({
            #[cfg(not(feature = "redis-tokio"))]
            {
                Arc::new(trypema::RateLimiter::new(trypema::RateLimiterOptions {
                    local: crate::build_local_options(&args),
                }))
            }

            // When built with redis support, RateLimiterOptions requires a Redis config.
            // Local comparisons still run against the local provider, but we satisfy the
            // struct shape by opening a connection manager (same approach as stress/src/main.rs).
            #[cfg(feature = "redis-tokio")]
            {
                use trypema::redis::{RedisKey, RedisRateLimiterOptions};
                use trypema::{
                    HardLimitFactor, RateGroupSizeMs, SuppressionFactorCacheMs, WindowSizeSeconds,
                };

                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                rt.block_on(async {
                    let client = redis::Client::open(args.redis_url.as_str()).unwrap();
                    let connection_manager = client.get_connection_manager().await.unwrap();

                    Arc::new(trypema::RateLimiter::new(trypema::RateLimiterOptions {
                        local: crate::build_local_options(&args),
                        redis: RedisRateLimiterOptions {
                            connection_manager,
                            prefix: Some(RedisKey::try_from(args.redis_prefix.clone()).unwrap()),
                            window_size_seconds: WindowSizeSeconds::try_from(args.window_s)
                                .unwrap(),
                            rate_group_size_ms: RateGroupSizeMs::try_from(args.group_ms).unwrap(),
                            hard_limit_factor: HardLimitFactor::try_from(args.hard_limit_factor)
                                .unwrap(),
                            suppression_factor_cache_ms: SuppressionFactorCacheMs::try_from(
                                args.suppression_cache_ms,
                            )
                            .unwrap(),
                        },
                    }))
                })
            }
        })
    } else {
        None
    };

    let burster_store = if args.local_limiter == LocalLimiter::Burster {
        match burster_store_for_window_s(args.window_s) {
            Some(s) => Some(Arc::new(s)),
            None => {
                eprintln!(
                    "unsupported --window-s for burster: {} (supported: 10, 60, 300)",
                    args.window_s
                );
                std::process::exit(2);
            }
        }
    } else {
        None
    };

    let governor_rl: Option<Arc<DefaultKeyedRateLimiter<String>>> =
        if args.local_limiter == LocalLimiter::Governor {
            let rate_u64 = args.rate_limit_per_s.max(1.0).round() as u64;
            let rate_u32 = u32::try_from(rate_u64.min(u32::MAX as u64)).unwrap_or(u32::MAX);

            // Match sliding-window burst: up to `rate * window_s` in an empty window.
            let burst_u64 = (args.rate_limit_per_s * args.window_s as f64)
                .max(1.0)
                .round() as u64;
            let burst_u32 = u32::try_from(burst_u64.min(u32::MAX as u64))
                .unwrap_or(u32::MAX)
                .max(1);

            let quota = Quota::per_second(std::num::NonZeroU32::new(rate_u32).unwrap())
                .allow_burst(std::num::NonZeroU32::new(burst_u32).unwrap());
            Some(Arc::new(governor::RateLimiter::keyed(quota)))
        } else {
            None
        };

    let capacity_per_window = (args.rate_limit_per_s * args.window_s as f64)
        .max(1.0)
        .round() as u64;

    let mut handles = Vec::with_capacity(args.threads);
    for t in 0..args.threads {
        let stop = Arc::clone(&stop);
        let counts = Arc::clone(&counts);
        let total_ops = Arc::clone(&total_ops);
        let args = args2.clone();
        let keys = keys.clone();
        let trypema_rl = trypema_rl.clone();
        let burster_store = burster_store.clone();
        let governor_rl = governor_rl.clone();

        let builder = {
            let mut b = std::thread::Builder::new();
            if let Some(sz) = thread_stack_size {
                b = b.stack_size(sz);
            }
            b
        };

        handles.push(
            builder
                .spawn(move || {
                    let mut hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
                    let mut i = 0_u64;
                    let mut seed = (t as u64 + 1) * 0x9E37_79B9_7F4A_7C15;
                    let mut next_deadline = Instant::now();

                    let mut rng_u64 = || {
                        seed ^= seed >> 12;
                        seed ^= seed << 25;
                        seed ^= seed >> 27;
                        seed = seed.wrapping_mul(0x2545_F491_4F6C_DD1D);
                        seed
                    };

                    let trypema_rate = trypema::RateLimit::try_from(args.rate_limit_per_s).unwrap();
                    let capacity_per_window = (args.rate_limit_per_s * args.window_s as f64)
                        .max(1.0)
                        .round() as u64;
                    let key_count = keys.len().max(1);

                    while !stop.load(Ordering::Relaxed) {
                        if Instant::now() >= deadline {
                            break;
                        }

                        if args.mode != Mode::Max {
                            if let Some(qps) = qps_for_now(&args, started) {
                                let per_op_ns = 1_000_000_000u64 / qps.max(1);
                                let now = Instant::now();
                                if now < next_deadline {
                                    std::thread::sleep(next_deadline - now);
                                }
                                next_deadline += Duration::from_nanos(per_op_ns);
                            }
                        }

                        i = i.wrapping_add(1);
                        let idx = pick_key_idx(&args, key_count, &mut rng_u64);
                        let key = &keys[idx % key_count];

                        let sample = should_sample(i, args.sample_every);
                        let t0 = if sample { Some(Instant::now()) } else { None };

                        match args.local_limiter {
                            LocalLimiter::Trypema => {
                                let rl = trypema_rl.as_ref().unwrap();
                                let decision = match args.strategy {
                                    Strategy::Absolute => {
                                        rl.local().absolute().inc(key, &trypema_rate, 1)
                                    }
                                    Strategy::Suppressed => {
                                        rl.local().suppressed().inc(key, &trypema_rate, 1)
                                    }
                                };

                                match decision {
                                    trypema::RateLimitDecision::Allowed => {
                                        counts.allowed.fetch_add(1, Ordering::Relaxed);
                                    }
                                    trypema::RateLimitDecision::Rejected { .. } => {
                                        counts.rejected.fetch_add(1, Ordering::Relaxed);
                                    }
                                    trypema::RateLimitDecision::Suppressed {
                                        is_allowed, ..
                                    } => {
                                        if is_allowed {
                                            counts
                                                .suppressed_allowed
                                                .fetch_add(1, Ordering::Relaxed);
                                        } else {
                                            counts
                                                .suppressed_denied
                                                .fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                }
                            }
                            LocalLimiter::Burster => {
                                let store = burster_store.as_ref().unwrap();
                                let allowed = match store.as_ref() {
                                    BursterStore::W10(map) => {
                                        if let Some(entry) = map.get(key.as_str()) {
                                            let mut lim = entry.value().lock().unwrap();
                                            lim.try_consume_one().is_ok()
                                        } else {
                                            map.insert(
                                                key.clone(),
                                                std::sync::Mutex::new(mk_burster::<10_000>(
                                                    capacity_per_window,
                                                )),
                                            );
                                            let entry = map.get(key.as_str()).unwrap();
                                            let mut lim = entry.value().lock().unwrap();
                                            lim.try_consume_one().is_ok()
                                        }
                                    }
                                    BursterStore::W60(map) => {
                                        if let Some(entry) = map.get(key.as_str()) {
                                            let mut lim = entry.value().lock().unwrap();
                                            lim.try_consume_one().is_ok()
                                        } else {
                                            map.insert(
                                                key.clone(),
                                                std::sync::Mutex::new(mk_burster::<60_000>(
                                                    capacity_per_window,
                                                )),
                                            );
                                            let entry = map.get(key.as_str()).unwrap();
                                            let mut lim = entry.value().lock().unwrap();
                                            lim.try_consume_one().is_ok()
                                        }
                                    }
                                    BursterStore::W300(map) => {
                                        if let Some(entry) = map.get(key.as_str()) {
                                            let mut lim = entry.value().lock().unwrap();
                                            lim.try_consume_one().is_ok()
                                        } else {
                                            map.insert(
                                                key.clone(),
                                                std::sync::Mutex::new(mk_burster::<300_000>(
                                                    capacity_per_window,
                                                )),
                                            );
                                            let entry = map.get(key.as_str()).unwrap();
                                            let mut lim = entry.value().lock().unwrap();
                                            lim.try_consume_one().is_ok()
                                        }
                                    }
                                };

                                if allowed {
                                    counts.allowed.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    counts.rejected.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            LocalLimiter::Governor => {
                                let rl = governor_rl.as_ref().unwrap();
                                let allowed = rl.check_key(key).is_ok();
                                if allowed {
                                    counts.allowed.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    counts.rejected.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }

                        if let Some(t0) = t0 {
                            let us = t0.elapsed().as_micros() as u64;
                            let _ = hist.record(us.max(1));
                        }

                        total_ops.fetch_add(1, Ordering::Relaxed);
                    }

                    hist
                })
                .unwrap(),
        );
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

    println!("local_limiter={:?}", args2.local_limiter);
    if args2.local_limiter == LocalLimiter::Burster {
        println!("burster_capacity_per_window={}", capacity_per_window);
    }
    if args2.local_limiter == LocalLimiter::Governor {
        println!("governor_capacity_per_window={}", capacity_per_window);
    }
    crate::print_results(&args2, elapsed, ops, ops_s, &merged, &counts);
}
