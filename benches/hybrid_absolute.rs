use criterion::{Criterion, criterion_group, criterion_main};

#[path = "runtime.rs"]
mod runtime;

#[path = "common.rs"]
mod common;

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
mod enabled {
    use std::{hint::black_box, time::Duration};

    use criterion::Criterion;
    use trypema::redis::RedisKey;
    use trypema::{HistoryPreservation, RateLimit, RateLimitComparator};

    use super::common::{LimiterConfig, redis::build_limiter};
    use super::runtime;

    pub fn bench_conditional_set(c: &mut Criterion) {
        let mut group = c.benchmark_group("hybrid_absolute/conditional_set");
        group.sample_size(40);
        let rt = runtime::build();
        let rl = runtime::block_on(
            &rt,
            build_limiter(LimiterConfig {
                prefix: "bench_hybrid_absolute",
                ..LimiterConfig::default()
            }),
        );
        let rate = RateLimit::try_from(100.0).unwrap();
        let guard_key = RedisKey::try_from("guard".to_string()).unwrap();
        runtime::block_on(&rt, async {
            rl.hybrid()
                .absolute()
                .set_if(&guard_key, &rate, RateLimitComparator::Nil, 100)
                .await
                .unwrap();
        });

        group.bench_function("set_if/guard_miss", |b| {
            b.iter(|| {
                black_box(runtime::block_on(&rt, async {
                    rl.hybrid()
                        .absolute()
                        .set_if(&guard_key, &rate, RateLimitComparator::Eq(0), 50)
                        .await
                }))
            });
        });

        let replace_key = RedisKey::try_from("replace".to_string()).unwrap();
        runtime::block_on(&rt, async {
            rl.hybrid()
                .absolute()
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
                        .absolute()
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
                    .absolute()
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
                            .absolute()
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
fn bench_conditional_set(c: &mut Criterion) {
    enabled::bench_conditional_set(c)
}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
fn bench_conditional_set(_: &mut Criterion) {}

criterion_group!(benches, bench_conditional_set);
criterion_main!(benches);
