use criterion::{Criterion, criterion_group, criterion_main};

#[path = "runtime.rs"]
mod runtime;

#[path = "common.rs"]
mod common;

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
mod enabled {
    use std::{hint::black_box, time::Duration};

    use criterion::{BatchSize, Criterion};
    use trypema::redis::RedisKey;
    use trypema::{HistoryPreservation, RateLimit, RateLimitComparator};

    use super::common::{LimiterConfig, redis::build_limiter};
    use super::runtime;

    pub fn bench_public_api(c: &mut Criterion) {
        let mut group = c.benchmark_group("hybrid_suppressed");
        group.sample_size(40);
        let rt = runtime::build();
        let rl = runtime::block_on(
            &rt,
            build_limiter(LimiterConfig {
                hard_limit_factor: 1.5,
                prefix: "bench_hybrid_suppressed",
                ..LimiterConfig::default()
            }),
        );
        let rate = RateLimit::max();
        let hot_key = RedisKey::try_from("hot_key".to_string()).unwrap();
        runtime::block_on(&rt, async {
            rl.hybrid()
                .suppressed()
                .inc(&hot_key, &rate, 1)
                .await
                .unwrap();
        });

        group.bench_function("inc/hot_key", |b| {
            b.iter(|| {
                black_box(runtime::block_on(&rt, async {
                    rl.hybrid()
                        .suppressed()
                        .inc(black_box(&hot_key), black_box(&rate), black_box(1))
                        .await
                }))
            });
        });

        group.bench_function("get/redis_synchronized", |b| {
            b.iter(|| {
                black_box(runtime::block_on(&rt, async {
                    rl.hybrid().suppressed().get(black_box(&hot_key)).await
                }))
            });
        });

        group.bench_function("get_inferred/local_fast_path", |b| {
            b.iter(|| {
                black_box(runtime::block_on(&rt, async {
                    rl.hybrid()
                        .suppressed()
                        .get_inferred(black_box(&hot_key))
                        .await
                }))
            });
        });

        let refresh_key = RedisKey::try_from("refresh_key".to_string()).unwrap();
        group.bench_function("get_inferred/redis_refresh", |b| {
            b.iter_batched(
                || {
                    runtime::block_on(&rt, async {
                        rl.hybrid()
                            .suppressed()
                            .set_if(&refresh_key, &rate, RateLimitComparator::Nil, 1)
                            .await
                            .unwrap();
                    });
                },
                |_| {
                    black_box(runtime::block_on(&rt, async {
                        rl.hybrid()
                            .suppressed()
                            .get_inferred(black_box(&refresh_key))
                            .await
                    }))
                },
                BatchSize::SmallInput,
            );
        });

        group.bench_function("get_suppression_factor/hot_key", |b| {
            b.iter(|| {
                black_box(runtime::block_on(&rt, async {
                    rl.hybrid()
                        .suppressed()
                        .get_suppression_factor(black_box(&hot_key))
                        .await
                }))
            });
        });

        let guard_key = RedisKey::try_from("guard".to_string()).unwrap();
        runtime::block_on(&rt, async {
            rl.hybrid()
                .suppressed()
                .set_if(&guard_key, &rate, RateLimitComparator::Nil, 100)
                .await
                .unwrap();
        });

        group.bench_function("set_if/guard_miss", |b| {
            b.iter(|| {
                black_box(runtime::block_on(&rt, async {
                    rl.hybrid()
                        .suppressed()
                        .set_if(&guard_key, &rate, RateLimitComparator::Eq(0), 50)
                        .await
                }))
            });
        });

        let replace_key = RedisKey::try_from("replace".to_string()).unwrap();
        runtime::block_on(&rt, async {
            rl.hybrid()
                .suppressed()
                .set_if(&replace_key, &rate, RateLimitComparator::Nil, 100)
                .await
                .unwrap();
        });
        let mut replace_high = false;
        group.bench_function("set_if/replace", |b| {
            b.iter(|| {
                replace_high = !replace_high;
                let target = if replace_high { 150 } else { 50 };
                black_box(runtime::block_on(&rt, async {
                    rl.hybrid()
                        .suppressed()
                        .set_if(&replace_key, &rate, RateLimitComparator::Nil, target)
                        .await
                }))
            });
        });

        for preservation in [
            HistoryPreservation::PreserveNewest,
            HistoryPreservation::PreserveOldest,
        ] {
            let key = RedisKey::try_from(format!("preserve_{preservation:?}")).unwrap();
            runtime::block_on(&rt, async {
                rl.hybrid()
                    .suppressed()
                    .set_if(&key, &rate, RateLimitComparator::Nil, 100)
                    .await
                    .unwrap();
            });
            let mut high = false;
            group.bench_function(format!("set_if_preserve_history/{preservation:?}"), |b| {
                b.iter(|| {
                    high = !high;
                    let target = if high { 150 } else { 50 };
                    black_box(runtime::block_on(&rt, async {
                        rl.hybrid()
                            .suppressed()
                            .set_if_preserve_history(
                                &key,
                                &rate,
                                RateLimitComparator::Nil,
                                target,
                                preservation,
                            )
                            .await
                    }))
                });
            });
        }

        std::thread::sleep(Duration::from_millis(50));
        group.finish();
    }
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
fn bench_public_api(c: &mut Criterion) {
    enabled::bench_public_api(c)
}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
fn bench_public_api(_: &mut Criterion) {}

criterion_group!(benches, bench_public_api);
criterion_main!(benches);
