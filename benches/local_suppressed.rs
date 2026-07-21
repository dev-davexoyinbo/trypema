use criterion::{criterion_group, criterion_main};

#[path = "common.rs"]
mod common;

mod benchmarks {
    use criterion::{BenchmarkId, Criterion};
    use std::hint::black_box;

    use trypema::{HistoryPreservation, RateLimit, RateLimitComparator};

    use super::common::{LimiterConfig, build_local_limiter};

    fn config(window_s: u64, group_ms: u64, cache_ms: u64, hard: f64) -> LimiterConfig {
        LimiterConfig {
            window_size: window_s,
            bucket_size: group_ms,
            hard_limit_factor: hard,
            suppression_factor_cache_period: cache_ms,
            ..LimiterConfig::default()
        }
    }

    pub fn bench_below_capacity(c: &mut Criterion) {
        let mut group = c.benchmark_group("local_suppressed/below_capacity");
        group.sample_size(200);

        for group_ms in [1_u64, 10, 100] {
            group.bench_function(format!("inc/group_ms={group_ms}"), |b| {
                let rl = build_local_limiter(config(60, group_ms, 100, 1.5));
                let limiter = rl.suppressed();
                let rate = RateLimit::max();
                limiter.inc("k", &rate, 1);

                b.iter(|| {
                    black_box(limiter.inc(black_box("k"), black_box(&rate), black_box(1)));
                });
            });
        }

        group.finish();
    }

    pub fn bench_over_hard_limit(c: &mut Criterion) {
        let mut group = c.benchmark_group("local_suppressed/over_hard_limit");
        group.sample_size(150);

        group.bench_function("inc/suppressed_full", |b| {
            let rl = build_local_limiter(config(60, 10, 10_000, 1.5));
            let limiter = rl.suppressed();

            // Choose values that avoid too much f64->u64 truncation noise.
            let rate = RateLimit::per_second(50.0).unwrap();
            let k = "k";

            // Prefill enough to push suppression_factor to 1.0.
            let hard_window_limit = (60_f64 * rate.as_per_second() * 1.5) as u64;
            for _ in 0..(hard_window_limit + 100) {
                let _ = limiter.inc(k, &rate, 1);
            }

            // Warm cache.
            let _ = limiter.get_suppression_factor(k);

            b.iter(|| {
                black_box(limiter.inc(black_box(k), black_box(&rate), black_box(1)));
            });
        });

        group.finish();
    }

    pub fn bench_get_suppression_factor(c: &mut Criterion) {
        let mut group = c.benchmark_group("local_suppressed/get_suppression_factor");
        group.sample_size(200);

        group.bench_function("cache_hit", |b| {
            let rl = build_local_limiter(config(60, 10, 1_000, 1.5));
            let limiter = rl.suppressed();
            let rate = RateLimit::per_second(5.0).unwrap();
            let k = "k";
            for _ in 0..500 {
                let _ = limiter.inc(k, &rate, 1);
            }
            let _ = limiter.get_suppression_factor(k);

            b.iter(|| {
                black_box(limiter.get_suppression_factor(black_box(k)));
            });
        });

        group.bench_function("cache_miss_many_keys", |b| {
            let rl = build_local_limiter(config(60, 10, 1, 1.5));
            let limiter = rl.suppressed();
            let rate = RateLimit::per_second(5.0).unwrap();
            let keys: Vec<String> = (0..50_000).map(|i| format!("user_{i}")).collect();

            // Ensure series exists for each key so we measure calculation, not insertion.
            for k in &keys {
                let _ = limiter.inc(k, &rate, 1);
            }

            let mut idx = 0_usize;
            b.iter(|| {
                let k = &keys[idx % keys.len()];
                idx = idx.wrapping_add(1);
                black_box(limiter.get_suppression_factor(black_box(k)));
            });
        });

        group.finish();
    }

    pub fn bench_get(c: &mut Criterion) {
        let mut group = c.benchmark_group("local_suppressed/get");
        group.sample_size(50);

        let rl = build_local_limiter(config(3_600, 100, 1_000, 1.5));
        let limiter = rl.suppressed();
        let rate = RateLimit::per_second(100.0).unwrap();
        let key = "k";
        let _ = limiter.inc(key, &rate, 1);

        group.bench_function("hot_key/single_thread", |b| {
            b.iter(|| {
                black_box(limiter.get(black_box(key)));
            });
        });

        for thread_count in [2_usize, 8, 16] {
            group.bench_with_input(
                BenchmarkId::new("hot_key/concurrent", thread_count),
                &thread_count,
                |b, &thread_count| {
                    b.iter_custom(|iterations| {
                        super::common::measure_parallel(iterations, thread_count, || {
                            black_box(limiter.get(black_box(key)));
                        })
                    });
                },
            );
        }

        group.finish();
    }

    pub fn bench_conditional_set(c: &mut Criterion) {
        let mut group = c.benchmark_group("local_suppressed/conditional_set");
        group.sample_size(50);
        let rl = build_local_limiter(config(3_600, 100, 1_000, 1.5));
        let limiter = rl.suppressed();
        let rate = RateLimit::per_second(100.0).unwrap();
        limiter.set_if("guard", &rate, RateLimitComparator::Always, 100);

        group.bench_function("set_if/guard_miss", |b| {
            b.iter(|| {
                black_box(limiter.set_if(
                    black_box("guard"),
                    black_box(&rate),
                    black_box(RateLimitComparator::Eq(0)),
                    black_box(50),
                ));
            });
        });

        limiter.set_if("replace", &rate, RateLimitComparator::Always, 100);
        let mut replace_high = false;
        group.bench_function("set_if/replace", |b| {
            b.iter(|| {
                replace_high = !replace_high;
                let target = if replace_high { 150 } else { 50 };
                black_box(limiter.set_if(
                    black_box("replace"),
                    black_box(&rate),
                    RateLimitComparator::Always,
                    black_box(target),
                ));
            });
        });

        for thread_count in [2_usize, 8, 16] {
            group.bench_with_input(
                BenchmarkId::new("set_if/guard_miss_concurrent", thread_count),
                &thread_count,
                |b, &thread_count| {
                    b.iter_custom(|iterations| {
                        super::common::measure_parallel(iterations, thread_count, || {
                            black_box(limiter.set_if(
                                "guard",
                                &rate,
                                RateLimitComparator::Eq(0),
                                50,
                            ));
                        })
                    });
                },
            );
        }

        for preservation in [
            HistoryPreservation::PreserveNewest,
            HistoryPreservation::PreserveOldest,
        ] {
            let key = match preservation {
                HistoryPreservation::PreserveNewest => "newest",
                HistoryPreservation::PreserveOldest => "oldest",
            };
            limiter.set_if(key, &rate, RateLimitComparator::Always, 100);
            let mut high = false;
            group.bench_function(format!("preserve/{preservation:?}"), |b| {
                b.iter(|| {
                    high = !high;
                    let target = if high { 150 } else { 50 };
                    black_box(limiter.set_if_preserve_history(
                        black_box(key),
                        black_box(&rate),
                        RateLimitComparator::Always,
                        black_box(target),
                        preservation,
                    ));
                });
            });
        }
        group.finish();
    }
}

use benchmarks::{
    bench_below_capacity, bench_conditional_set, bench_get, bench_get_suppression_factor,
    bench_over_hard_limit,
};

criterion_group!(
    benches,
    bench_below_capacity,
    bench_over_hard_limit,
    bench_get_suppression_factor,
    bench_get,
    bench_conditional_set
);
criterion_main!(benches);
