use criterion::{Criterion, criterion_group, criterion_main};

#[path = "runtime.rs"]
mod runtime;

#[path = "common.rs"]
mod common;

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
mod enabled {
    use std::time::Duration;

    use criterion::Criterion;
    use std::hint::black_box;

    use trypema::redis::RedisKey;
    use trypema::{HistoryPreservation, RateLimit, RateLimitComparator};

    use super::common::{LimiterConfig, redis::build_limiter};
    use super::runtime;

    pub fn bench_inc(c: &mut Criterion) {
        let mut group = c.benchmark_group("redis_suppressed");
        group.sample_size(40);

        let rt = runtime::build();

        let rl = runtime::block_on(
            &rt,
            build_limiter(LimiterConfig {
                hard_limit_factor: 1.5,
                ..LimiterConfig::default()
            }),
        );

        let key = RedisKey::try_from("user_1".to_string()).unwrap();
        let rate = RateLimit::try_from(5.0).unwrap();

        // Warm.
        runtime::block_on(&rt, async {
            let _ = rl.redis().suppressed().inc(&key, &rate, 1).await.unwrap();
            let _ = rl
                .redis()
                .suppressed()
                .get_suppression_factor(&key)
                .await
                .unwrap();
        });

        group.bench_function("inc/hot_key", |b| {
            b.iter(|| {
                let _ = runtime::block_on(&rt, async {
                    let res = rl
                        .redis()
                        .suppressed()
                        .inc(black_box(&key), black_box(&rate), black_box(1))
                        .await;
                    black_box(res)
                });
            });
        });

        group.bench_function("get_suppression_factor/hot_key", |b| {
            b.iter(|| {
                let _ = runtime::block_on(&rt, async {
                    let res = rl
                        .redis()
                        .suppressed()
                        .get_suppression_factor(black_box(&key))
                        .await;
                    black_box(res)
                });
            });
        });

        let set_key = RedisKey::try_from("set_user".to_string()).unwrap();
        runtime::block_on(&rt, async {
            rl.redis()
                .suppressed()
                .set_if(&set_key, &rate, RateLimitComparator::Nil, 100)
                .await
                .unwrap();
        });
        group.bench_function("set_if/guard_miss", |b| {
            b.iter(|| {
                black_box(runtime::block_on(&rt, async {
                    rl.redis()
                        .suppressed()
                        .set_if(&set_key, &rate, RateLimitComparator::Eq(0), 50)
                        .await
                }))
            });
        });

        let replace_key = RedisKey::try_from("replace_user".to_string()).unwrap();
        runtime::block_on(&rt, async {
            rl.redis()
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
                    rl.redis()
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
                rl.redis()
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
                        rl.redis()
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
fn bench_inc(c: &mut Criterion) {
    enabled::bench_inc(c)
}

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
fn bench_inc(_: &mut Criterion) {}

criterion_group!(benches, bench_inc);
criterion_main!(benches);
