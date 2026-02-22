use criterion::{Criterion, criterion_group, criterion_main};

// When redis features are enabled, `RateLimiterOptions` requires a Redis configuration.
// Keep local microbenches buildable without needing Redis by disabling them under redis features.
#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
mod enabled {
    use std::sync::Arc;

    use criterion::{BatchSize, Criterion};
    use std::hint::black_box;

    use trypema::local::LocalRateLimiterOptions;
    use trypema::{
        HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimiter, RateLimiterOptions,
        SuppressionFactorCacheMs, WindowSizeSeconds,
    };

    fn opts(window_s: u64, group_ms: u64) -> RateLimiterOptions {
        RateLimiterOptions {
            local: LocalRateLimiterOptions {
                window_size_seconds: WindowSizeSeconds::try_from(window_s).unwrap(),
                rate_group_size_ms: RateGroupSizeMs::try_from(group_ms).unwrap(),
                hard_limit_factor: HardLimitFactor::default(),
                suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
            },
        }
    }

    pub fn bench_hot_key_allowed(c: &mut Criterion) {
        let mut group = c.benchmark_group("local_absolute/hot_key_allowed");
        group.sample_size(200);

        for group_ms in [1_u64, 10, 100] {
            group.bench_function(format!("inc/group_ms={group_ms}"), |b| {
                let rl = Arc::new(RateLimiter::new(opts(60, group_ms)));
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
                    let rl = Arc::new(RateLimiter::new(opts(60, group_ms)));
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
            let rl = Arc::new(RateLimiter::new(opts(60, 10)));
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
            let rl = Arc::new(RateLimiter::new(opts(60, 10)));
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
}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
fn bench_hot_key_allowed(c: &mut Criterion) {
    enabled::bench_hot_key_allowed(c)
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
fn bench_hot_key_allowed(_: &mut Criterion) {}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
fn bench_many_keys_allowed(c: &mut Criterion) {
    enabled::bench_many_keys_allowed(c)
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
fn bench_many_keys_allowed(_: &mut Criterion) {}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
fn bench_reject_path(c: &mut Criterion) {
    enabled::bench_reject_path(c)
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
fn bench_reject_path(_: &mut Criterion) {}

criterion_group!(
    benches,
    bench_hot_key_allowed,
    bench_many_keys_allowed,
    bench_reject_path
);
criterion_main!(benches);
