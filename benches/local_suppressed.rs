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

    fn opts(window_s: u64, group_ms: u64, cache_ms: u64, hard: f64) -> RateLimiterOptions {
        RateLimiterOptions {
            local: LocalRateLimiterOptions {
                window_size_seconds: WindowSizeSeconds::try_from(window_s).unwrap(),
                rate_group_size_ms: RateGroupSizeMs::try_from(group_ms).unwrap(),
                hard_limit_factor: HardLimitFactor::try_from(hard).unwrap(),
                suppression_factor_cache_ms: SuppressionFactorCacheMs::try_from(cache_ms).unwrap(),
            },
        }
    }

    pub fn bench_below_capacity(c: &mut Criterion) {
        let mut group = c.benchmark_group("local_suppressed/below_capacity");
        group.sample_size(200);

        for group_ms in [1_u64, 10, 100] {
            group.bench_function(format!("inc/group_ms={group_ms}"), |b| {
                let rl = Arc::new(RateLimiter::new(opts(60, group_ms, 100, 1.5)));
                let limiter = rl.local().suppressed();
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
            let rl = Arc::new(RateLimiter::new(opts(60, 10, 10_000, 1.5)));
            let limiter = rl.local().suppressed();

            // Choose values that avoid too much f64->u64 truncation noise.
            let rate = RateLimit::try_from(50.0).unwrap();
            let k = "k";

            // Prefill enough to push suppression_factor to 1.0.
            let hard_window_limit = (60_f64 * *rate * 1.5) as u64;
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
            let rl = Arc::new(RateLimiter::new(opts(60, 10, 1_000, 1.5)));
            let limiter = rl.local().suppressed();
            let rate = RateLimit::try_from(5.0).unwrap();
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
            let rl = Arc::new(RateLimiter::new(opts(60, 10, 1, 1.5)));
            let limiter = rl.local().suppressed();
            let rate = RateLimit::try_from(5.0).unwrap();
            let keys: Vec<String> = (0..50_000).map(|i| format!("user_{i}")).collect();

            // Ensure series exists for each key so we measure calculation, not insertion.
            for k in keys.iter().take(1_000) {
                let _ = limiter.inc(k, &rate, 1);
            }

            b.iter_batched(
                || 0_usize,
                |mut idx| {
                    idx = idx.wrapping_add(1);
                    let k = &keys[idx % keys.len()];
                    black_box(limiter.get_suppression_factor(black_box(k)));
                    idx
                },
                BatchSize::SmallInput,
            );
        });

        group.finish();
    }
}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
fn bench_below_capacity(c: &mut Criterion) {
    enabled::bench_below_capacity(c)
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
fn bench_below_capacity(_: &mut Criterion) {}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
fn bench_over_hard_limit(c: &mut Criterion) {
    enabled::bench_over_hard_limit(c)
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
fn bench_over_hard_limit(_: &mut Criterion) {}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
fn bench_get_suppression_factor(c: &mut Criterion) {
    enabled::bench_get_suppression_factor(c)
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
fn bench_get_suppression_factor(_: &mut Criterion) {}

criterion_group!(
    benches,
    bench_below_capacity,
    bench_over_hard_limit,
    bench_get_suppression_factor
);
criterion_main!(benches);
