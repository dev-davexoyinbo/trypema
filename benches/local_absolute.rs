use criterion::{criterion_group, criterion_main};

#[path = "common.rs"]
mod common;

mod benchmarks {
    use criterion::{BatchSize, BenchmarkId, Criterion};
    use std::hint::black_box;

    use trypema::{HistoryPreservation, RateLimit, RateLimitComparator};

    use super::common::{LimiterConfig, build_local_limiter};

    fn config(window_s: u64, group_ms: u64) -> LimiterConfig {
        LimiterConfig {
            window_size_seconds: window_s,
            rate_group_size_ms: group_ms,
            ..LimiterConfig::default()
        }
    }

    pub fn bench_hot_key_allowed(c: &mut Criterion) {
        let mut group = c.benchmark_group("local_absolute/hot_key_allowed");
        group.sample_size(200);

        for group_ms in [1_u64, 10, 100] {
            group.bench_function(format!("inc/group_ms={group_ms}"), |b| {
                let rl = build_local_limiter(config(60, group_ms));
                let limiter = rl.local().absolute();
                let rate = RateLimit::max();
                limiter.inc("k", &rate, 1);

                b.iter(|| {
                    black_box(limiter.inc(black_box("k"), black_box(&rate), black_box(1)));
                });
            });
        }

        group.finish();
    }

    pub fn bench_many_keys_allowed(c: &mut Criterion) {
        let mut group = c.benchmark_group("local_absolute/many_keys_allowed");
        group.sample_size(100);

        for key_space in [1_000_usize, 100_000] {
            for group_ms in [1_u64, 10, 100] {
                group.bench_function(format!("inc/keys={key_space}/group_ms={group_ms}"), |b| {
                    let rl = build_local_limiter(config(60, group_ms));
                    let limiter = rl.local().absolute();
                    let rate = RateLimit::max();

                    let keys: Vec<String> = (0..key_space).map(|i| format!("user_{i}")).collect();

                    b.iter_batched(
                        || 0_usize,
                        |mut idx| {
                            idx = idx.wrapping_add(1);
                            let k = &keys[idx % keys.len()];
                            black_box(limiter.inc(black_box(k), black_box(&rate), black_box(1)));
                            idx
                        },
                        BatchSize::SmallInput,
                    );
                });
            }
        }

        group.finish();
    }

    pub fn bench_reject_path(c: &mut Criterion) {
        let mut group = c.benchmark_group("local_absolute/reject_path");
        group.sample_size(200);

        group.bench_function("inc/rejected", |b| {
            let rl = build_local_limiter(config(60, 10));
            let limiter = rl.local().absolute();
            let rate = RateLimit::try_from(10.0).unwrap();
            let k = "k";

            // Fill to (or past) capacity so we take the reject fast-path.
            let capacity = (60_f64 * *rate) as u64;
            for _ in 0..(capacity + 10) {
                let _ = limiter.inc(k, &rate, 1);
            }

            b.iter(|| {
                black_box(limiter.inc(black_box(k), black_box(&rate), black_box(1)));
            });
        });

        group.bench_function("is_allowed/rejected", |b| {
            let rl = build_local_limiter(config(60, 10));
            let limiter = rl.local().absolute();
            let rate = RateLimit::try_from(10.0).unwrap();
            let k = "k";

            let capacity = (60_f64 * *rate) as u64;
            for _ in 0..(capacity + 10) {
                let _ = limiter.inc(k, &rate, 1);
            }

            b.iter(|| {
                black_box(limiter.is_allowed(black_box(k)));
            });
        });

        group.finish();
    }

    pub fn bench_get(c: &mut Criterion) {
        let mut group = c.benchmark_group("local_absolute/get");
        group.sample_size(50);

        let rl = build_local_limiter(config(3_600, 100));
        let limiter = rl.local().absolute();
        let rate = RateLimit::try_from(100.0).unwrap();
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
        let mut group = c.benchmark_group("local_absolute/conditional_set");
        group.sample_size(50);
        let rl = build_local_limiter(config(3_600, 100));
        let limiter = rl.local().absolute();
        let rate = RateLimit::try_from(100.0).unwrap();
        limiter.set_if("guard", &rate, RateLimitComparator::Nil, 100);

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

        limiter.set_if("replace", &rate, RateLimitComparator::Nil, 100);
        let mut replace_high = false;
        group.bench_function("set_if/replace", |b| {
            b.iter(|| {
                replace_high = !replace_high;
                let target = if replace_high { 150 } else { 50 };
                black_box(limiter.set_if(
                    black_box("replace"),
                    black_box(&rate),
                    RateLimitComparator::Nil,
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
            limiter.set_if(key, &rate, RateLimitComparator::Nil, 100);
            let mut high = false;
            group.bench_function(format!("preserve/{preservation:?}"), |b| {
                b.iter(|| {
                    high = !high;
                    let target = if high { 150 } else { 50 };
                    black_box(limiter.set_if_preserve_history(
                        black_box(key),
                        black_box(&rate),
                        RateLimitComparator::Nil,
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
    bench_conditional_set, bench_get, bench_hot_key_allowed, bench_many_keys_allowed,
    bench_reject_path,
};

criterion_group!(
    benches,
    bench_hot_key_allowed,
    bench_many_keys_allowed,
    bench_reject_path,
    bench_get,
    bench_conditional_set
);
criterion_main!(benches);
