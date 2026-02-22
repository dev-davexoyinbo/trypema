use criterion::{Criterion, criterion_group, criterion_main};

#[cfg(feature = "redis-tokio")]
mod enabled {
    use std::{env, sync::Arc, time::Duration};

    use criterion::Criterion;
    use std::hint::black_box;

    use trypema::local::LocalRateLimiterOptions;
    use trypema::redis::{RedisKey, RedisRateLimiterOptions};
    use trypema::{
        HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimiter, RateLimiterOptions,
        SuppressionFactorCacheMs, WindowSizeSeconds,
    };

    fn redis_url() -> String {
        env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:16379/".to_string())
    }

    pub fn bench_inc(c: &mut Criterion) {
        let mut group = c.benchmark_group("redis_suppressed");
        group.sample_size(40);

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();

        let rl = rt.block_on(async {
            let client = redis::Client::open(redis_url()).unwrap();
            let connection_manager = client.get_connection_manager().await.unwrap();

            Arc::new(RateLimiter::new(RateLimiterOptions {
                local: LocalRateLimiterOptions {
                    window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
                    rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
                    hard_limit_factor: HardLimitFactor::try_from(1.5).unwrap(),
                    suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
                },
                redis: RedisRateLimiterOptions {
                    connection_manager,
                    prefix: Some(RedisKey::try_from("bench".to_string()).unwrap()),
                    window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
                    rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
                    hard_limit_factor: HardLimitFactor::try_from(1.5).unwrap(),
                    suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
                },
            }))
        });

        let key = RedisKey::try_from("user_1".to_string()).unwrap();
        let rate = RateLimit::try_from(5.0).unwrap();

        // Warm.
        rt.block_on(async {
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
                let _ = rt.block_on(async {
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
                let _ = rt.block_on(async {
                    let res = rl
                        .redis()
                        .suppressed()
                        .get_suppression_factor(black_box(&key))
                        .await;
                    black_box(res)
                });
            });
        });

        std::thread::sleep(Duration::from_millis(50));
        group.finish();
    }
}

#[cfg(feature = "redis-tokio")]
fn bench_inc(c: &mut Criterion) {
    enabled::bench_inc(c)
}

#[cfg(not(feature = "redis-tokio"))]
fn bench_inc(_: &mut Criterion) {}

criterion_group!(benches, bench_inc);
criterion_main!(benches);
