use std::time::Duration;

use super::wait::wait_until;
use crate::{
    BucketSize, RateLimit, RateLimitDecision, WindowSize, local::LocalRateLimiterProvider,
};

#[test]
fn local_cleanup_removes_stale_entries_and_keeps_active_entries() {
    // Cleanup judges a key by its newest bucket. The 10 ms bucket keeps that bucket of the
    // active key close to its last increment, far inside the 200 ms `stale_after`.
    let provider = LocalRateLimiterProvider::builder()
        .window_size(WindowSize::seconds_or_panic(60))
        .bucket_size(BucketSize::milliseconds_or_panic(10))
        .stale_after(Duration::from_millis(200))
        .cleanup_interval(Duration::from_millis(20))
        .build()
        .unwrap();
    let rate = RateLimit::per_second_or_panic(1_000.0);

    assert!(matches!(
        provider.absolute().inc("stale", &rate, 1),
        RateLimitDecision::Allowed
    ));
    let mut active_count = 0;
    wait_until(
        || {
            assert!(matches!(
                provider.absolute().inc("active", &rate, 1),
                RateLimitDecision::Allowed
            ));
            active_count += 1;
            provider.absolute().series().get("stale").is_none()
        },
        "the loop must remove the stale entry",
    );

    // A pass that removed the active entry would lose its earlier increments.
    assert_eq!(provider.absolute().get("active"), active_count);
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn redis_and_hybrid_cleanup_start_by_default() {
    use redis::AsyncCommands;

    use super::wait::wait_until_async;
    use crate::{
        BucketSize,
        common::RateType,
        hybrid::{HybridRateLimiterProvider, SyncInterval},
        redis::RedisRateLimiterProvider,
    };

    super::runtime::block_on(async {
        let connection_manager = super::common::connection_manager().await;
        let rate = RateLimit::per_second_or_panic(100.0);
        let cleanup_interval = Duration::from_millis(500);
        let started = std::time::Instant::now();

        let redis_prefix = super::common::unique_prefix();
        let redis = RedisRateLimiterProvider::builder(connection_manager.clone())
            .prefix(redis_prefix.clone())
            .window_size(WindowSize::seconds_or_panic(60))
            .stale_after(Duration::from_millis(50))
            .cleanup_interval(cleanup_interval)
            .build()
            .unwrap();
        let redis_key = super::common::key("redis_cleanup");
        assert!(matches!(
            redis.absolute().inc(&redis_key, &rate, 1).await.unwrap(),
            RateLimitDecision::Allowed
        ));

        let hybrid_prefix = super::common::unique_prefix();
        let hybrid = HybridRateLimiterProvider::builder(connection_manager.clone())
            .prefix(hybrid_prefix.clone())
            .window_size(WindowSize::seconds_or_panic(60))
            .bucket_size(BucketSize::milliseconds_or_panic(5))
            .sync_interval(SyncInterval::milliseconds_or_panic(5))
            .stale_after(Duration::from_millis(50))
            .cleanup_interval(cleanup_interval)
            .build()
            .unwrap();
        let hybrid_key = super::common::key("hybrid_cleanup");
        assert!(matches!(
            hybrid.absolute().inc(&hybrid_key, &rate, 1).await.unwrap(),
            RateLimitDecision::Allowed
        ));

        wait_until_async(
            async || {
                for (prefix, rate_type, key) in [
                    (&redis_prefix, RateType::Absolute, &redis_key),
                    (&hybrid_prefix, RateType::HybridAbsolute, &hybrid_key),
                ] {
                    let key_generator = super::common::key_gen(prefix, rate_type);
                    let mut connection = connection_manager.clone();
                    for entity_key in key_generator.get_all_entity_keys(key) {
                        let exists: bool = connection.exists(entity_key).await.unwrap();
                        if exists {
                            return false;
                        }
                    }
                    let score: Option<f64> = connection
                        .zscore(key_generator.get_active_entities_key(), key.as_str())
                        .await
                        .unwrap();
                    if score.is_some() {
                        return false;
                    }
                }
                true
            },
            "default cleanup must remove the stale state and active membership",
        )
        .await;

        // A second pass cannot run before two intervals. Smol must not discard its first tick.
        let elapsed = started.elapsed();
        assert!(
            elapsed < cleanup_interval * 2,
            "the stale state was removed after {elapsed:?}, not by the first pass"
        );
    });
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn redis_and_hybrid_cleanup_support_opt_out_stop_and_restart() {
    use redis::AsyncCommands;

    use super::wait::wait_until_async;
    use crate::{
        BucketSize,
        common::RateType,
        hybrid::{HybridRateLimiterProvider, SyncInterval},
        redis::{ConnectionManager, RedisKey, RedisRateLimiterProvider},
    };

    async fn total_exists(
        connection_manager: &ConnectionManager,
        prefix: &RedisKey,
        rate_type: RateType,
        key: &RedisKey,
    ) -> bool {
        connection_manager
            .clone()
            .exists(super::common::key_gen(prefix, rate_type).get_total_count_key(key))
            .await
            .unwrap()
    }

    super::runtime::block_on(async {
        let connection_manager = super::common::connection_manager().await;
        let rate = RateLimit::per_second_or_panic(100.0);

        let redis_prefix = super::common::unique_prefix();
        let redis = RedisRateLimiterProvider::builder(connection_manager.clone())
            .prefix(redis_prefix.clone())
            .window_size(WindowSize::seconds_or_panic(60))
            .stale_after(Duration::from_millis(100))
            .cleanup_interval(Duration::from_millis(40))
            .cleanup_enabled(false)
            .build()
            .unwrap();
        let redis_key = super::common::key("redis_lifecycle");
        redis.absolute().inc(&redis_key, &rate, 1).await.unwrap();

        let hybrid_prefix = super::common::unique_prefix();
        let hybrid = HybridRateLimiterProvider::builder(connection_manager.clone())
            .prefix(hybrid_prefix.clone())
            .window_size(WindowSize::seconds_or_panic(60))
            .bucket_size(BucketSize::milliseconds_or_panic(5))
            .sync_interval(SyncInterval::milliseconds_or_panic(5))
            .stale_after(Duration::from_millis(100))
            .cleanup_interval(Duration::from_millis(40))
            .cleanup_enabled(false)
            .build()
            .unwrap();
        let hybrid_key = super::common::key("hybrid_lifecycle");
        hybrid.absolute().inc(&hybrid_key, &rate, 1).await.unwrap();

        // The keys turn stale after 100 ms. Loops at the 40 ms interval would remove them in this
        // window.
        super::runtime::async_sleep(Duration::from_millis(220)).await;
        for (prefix, rate_type, key) in [
            (&redis_prefix, RateType::Absolute, &redis_key),
            (&hybrid_prefix, RateType::HybridAbsolute, &hybrid_key),
        ] {
            assert!(
                total_exists(&connection_manager, prefix, rate_type, key).await,
                "cleanup opt-out lost state for {key:?}"
            );
        }

        redis.start_cleanup_loop();
        redis.start_cleanup_loop();
        hybrid.start_cleanup_loop();
        hybrid.start_cleanup_loop();
        for (prefix, rate_type, key) in [
            (&redis_prefix, RateType::Absolute, &redis_key),
            (&hybrid_prefix, RateType::HybridAbsolute, &hybrid_key),
        ] {
            wait_until_async(
                async || !total_exists(&connection_manager, prefix, rate_type, key).await,
                &format!("started cleanup must remove the stale state for {key:?}"),
            )
            .await;
        }

        redis.stop_cleanup_loop();
        redis.stop_cleanup_loop();
        hybrid.stop_cleanup_loop();
        hybrid.stop_cleanup_loop();

        let redis_key = super::common::key("redis_stopped");
        let hybrid_key = super::common::key("hybrid_stopped");
        redis.absolute().inc(&redis_key, &rate, 1).await.unwrap();
        hybrid.absolute().inc(&hybrid_key, &rate, 1).await.unwrap();
        // The keys turn stale after 100 ms. Loops still running at the 40 ms interval would
        // remove them in this window.
        super::runtime::async_sleep(Duration::from_millis(220)).await;

        for (prefix, rate_type, key) in [
            (&redis_prefix, RateType::Absolute, &redis_key),
            (&hybrid_prefix, RateType::HybridAbsolute, &hybrid_key),
        ] {
            assert!(
                total_exists(&connection_manager, prefix, rate_type, key).await,
                "stopped cleanup removed state for {key:?}"
            );
        }

        redis.start_cleanup_loop();
        redis.stop_cleanup_loop();
        redis.start_cleanup_loop();
        hybrid.start_cleanup_loop();
        hybrid.stop_cleanup_loop();
        hybrid.start_cleanup_loop();
        for (prefix, rate_type, key) in [
            (&redis_prefix, RateType::Absolute, &redis_key),
            (&hybrid_prefix, RateType::HybridAbsolute, &hybrid_key),
        ] {
            wait_until_async(
                async || !total_exists(&connection_manager, prefix, rate_type, key).await,
                &format!("restarted cleanup must remove the stale state for {key:?}"),
            )
            .await;
        }
    });
}

/// Tests of `CleanupLoop` itself. A loop holds a `Weak` of the `Arc` that it was started for, so
/// `Arc::weak_count` of that `Arc` is the number of loops that run for it. A limiter holds its
/// `CleanupLoop`, so the drop of its last `Arc` drops the `CleanupLoop`. These tests hold the
/// `CleanupLoop` beside the `Arc`, so that they can see the weak count after that drop.
mod loop_lifecycle {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU64, AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use crate::{
        builder::CleanupConfig,
        cleanup_loop::{Cleanup, CleanupLoop},
        tests::wait::wait_until,
    };

    /// Longer than each test, so that a loop that ends in a test did not end at a wake.
    pub(super) const LONG_INTERVAL: Duration = Duration::from_secs(60);
    pub(super) const SHORT_INTERVAL: Duration = Duration::from_millis(20);
    /// A stopped or dropped loop must end within this bound.
    pub(super) const END_BOUND: Duration = Duration::from_secs(1);
    pub(super) const POLL_INTERVAL: Duration = Duration::from_millis(5);
    pub(super) const STALE_AFTER: Duration = Duration::from_millis(7);

    /// An owner that counts its passes and keeps the stale-after value of the last pass.
    #[derive(Default)]
    pub(super) struct Owner {
        pub(super) passes: AtomicUsize,
        pub(super) stale_after_ms: AtomicU64,
    }

    impl Owner {
        pub(super) fn passes(&self) -> usize {
            self.passes.load(Ordering::Acquire)
        }

        pub(super) fn record_pass(&self, stale_after_ms: u64) {
            self.stale_after_ms.store(stale_after_ms, Ordering::Release);
            self.passes.fetch_add(1, Ordering::AcqRel);
        }
    }

    impl Cleanup for Owner {
        fn cleanup(&self, stale_after_ms: u64) {
            self.record_pass(stale_after_ms);
        }
    }

    pub(super) fn cleanup_loop(interval: Duration) -> CleanupLoop {
        CleanupLoop::new(CleanupConfig {
            enabled: false,
            stale_after: STALE_AFTER,
            interval,
        })
    }

    /// Wait until no loop holds `owner`, or fail with `message` after `END_BOUND`.
    fn assert_loop_ends_at_once<T>(owner: &Arc<T>, message: &str) {
        let deadline = Instant::now() + END_BOUND;
        while Arc::weak_count(owner) > 0 {
            assert!(Instant::now() < deadline, "{message} within {END_BOUND:?}");
            std::thread::sleep(POLL_INTERVAL);
        }
    }

    #[test]
    fn thread_loop_runs_one_pass_per_interval() {
        let owner = Arc::new(Owner::default());
        let cleanup_loop = cleanup_loop(SHORT_INTERVAL);
        cleanup_loop.start_thread(&owner);

        wait_until(|| owner.passes() >= 3, "the started loop must run passes");
        assert_eq!(
            owner.stale_after_ms.load(Ordering::Acquire),
            STALE_AFTER.as_millis() as u64
        );

        cleanup_loop.stop();
        assert_loop_ends_at_once(&owner, "the loop must end after stop");
    }

    #[test]
    fn thread_loop_ends_at_once_when_its_cleanup_loop_drops() {
        let owner = Arc::new(Owner::default());
        let cleanup_loop = cleanup_loop(LONG_INTERVAL);
        cleanup_loop.start_thread(&owner);
        assert_eq!(Arc::weak_count(&owner), 1, "the started loop must run");

        drop(cleanup_loop);

        assert_loop_ends_at_once(&owner, "the loop must end after the drop");
        assert_eq!(owner.passes(), 0);
    }

    #[test]
    fn thread_loop_ends_at_once_after_stop() {
        let owner = Arc::new(Owner::default());
        let cleanup_loop = cleanup_loop(LONG_INTERVAL);
        cleanup_loop.start_thread(&owner);
        assert_eq!(Arc::weak_count(&owner), 1, "the started loop must run");

        cleanup_loop.stop();
        cleanup_loop.stop();

        assert_loop_ends_at_once(&owner, "the loop must end after stop");
        assert_eq!(owner.passes(), 0);
    }

    #[test]
    fn thread_loop_stop_then_start_leaves_one_loop() {
        let cleanup_loop = cleanup_loop(LONG_INTERVAL);
        let first = Arc::new(Owner::default());
        let second = Arc::new(Owner::default());
        let third = Arc::new(Owner::default());

        cleanup_loop.start_thread(&first);
        cleanup_loop.stop();
        cleanup_loop.start_thread(&second);
        cleanup_loop.start_thread(&third);

        assert_eq!(
            Arc::weak_count(&third),
            0,
            "a start while a loop runs must not start a second loop"
        );
        assert_loop_ends_at_once(&first, "the stopped loop must end");
        assert_eq!(first.passes(), 0, "the stopped loop must not run a pass");
        assert_eq!(
            Arc::weak_count(&second),
            1,
            "the restarted loop must keep running"
        );

        cleanup_loop.stop();
        assert_loop_ends_at_once(&second, "the restarted loop must end after stop");
    }

    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    mod task {
        use std::{
            future::Future,
            sync::{
                Arc,
                atomic::{AtomicUsize, Ordering},
            },
            time::Instant,
        };

        use super::{
            END_BOUND, LONG_INTERVAL, Owner, POLL_INTERVAL, SHORT_INTERVAL, STALE_AFTER,
            cleanup_loop,
        };
        use crate::{
            TrypemaError,
            cleanup_loop::AsyncCleanup,
            tests::{runtime, wait::wait_until_async},
        };

        impl AsyncCleanup for Owner {
            const FAILURE_MESSAGE: &'static str = "test cleanup failed";

            async fn cleanup(&self, stale_after_ms: u64) -> Result<(), TrypemaError> {
                self.record_pass(stale_after_ms);
                Ok(())
            }
        }

        /// An owner whose passes all fail.
        #[derive(Default)]
        struct FailingOwner {
            passes: AtomicUsize,
        }

        impl AsyncCleanup for FailingOwner {
            const FAILURE_MESSAGE: &'static str = "test cleanup failed";

            async fn cleanup(&self, _stale_after_ms: u64) -> Result<(), TrypemaError> {
                self.passes.fetch_add(1, Ordering::AcqRel);
                Err(TrypemaError::CustomError("test pass failed".to_string()))
            }
        }

        async fn assert_loop_ends_at_once<T>(owner: &Arc<T>, message: &str) {
            let deadline = Instant::now() + END_BOUND;
            while Arc::weak_count(owner) > 0 {
                assert!(Instant::now() < deadline, "{message} within {END_BOUND:?}");
                runtime::async_sleep(POLL_INTERVAL).await;
            }
        }

        fn on_runtime(test: impl Future<Output = ()>) {
            runtime::block_on(test);
        }

        #[test]
        fn task_loop_runs_one_pass_per_interval() {
            on_runtime(async {
                let owner = Arc::new(Owner::default());
                let cleanup_loop = cleanup_loop(SHORT_INTERVAL);
                cleanup_loop.start_task(&owner);

                wait_until_async(
                    async || owner.passes() >= 3,
                    "the started loop must run passes",
                )
                .await;
                assert_eq!(
                    owner.stale_after_ms.load(Ordering::Acquire),
                    STALE_AFTER.as_millis() as u64
                );

                cleanup_loop.stop();
                assert_loop_ends_at_once(&owner, "the loop must end after stop").await;
            });
        }

        #[test]
        fn task_loop_keeps_running_after_failed_passes() {
            on_runtime(async {
                let owner = Arc::new(FailingOwner::default());
                let cleanup_loop = cleanup_loop(SHORT_INTERVAL);
                cleanup_loop.start_task(&owner);

                wait_until_async(
                    async || owner.passes.load(Ordering::Acquire) >= 3,
                    "the loop must try again after a failed pass",
                )
                .await;

                cleanup_loop.stop();
                assert_loop_ends_at_once(&owner, "the loop must end after stop").await;
            });
        }

        #[test]
        fn task_loop_ends_at_once_when_its_cleanup_loop_drops() {
            on_runtime(async {
                let owner = Arc::new(Owner::default());
                let cleanup_loop = cleanup_loop(LONG_INTERVAL);
                cleanup_loop.start_task(&owner);
                assert_eq!(Arc::weak_count(&owner), 1, "the started loop must run");

                drop(cleanup_loop);

                assert_loop_ends_at_once(&owner, "the loop must end after the drop").await;
                assert_eq!(owner.passes(), 0);
            });
        }

        #[test]
        fn task_loop_ends_at_once_after_stop() {
            on_runtime(async {
                let owner = Arc::new(Owner::default());
                let cleanup_loop = cleanup_loop(LONG_INTERVAL);
                cleanup_loop.start_task(&owner);
                assert_eq!(Arc::weak_count(&owner), 1, "the started loop must run");

                cleanup_loop.stop();
                cleanup_loop.stop();

                assert_loop_ends_at_once(&owner, "the loop must end after stop").await;
                assert_eq!(owner.passes(), 0);
            });
        }

        #[test]
        fn task_loop_stop_then_start_leaves_one_loop() {
            on_runtime(async {
                let cleanup_loop = cleanup_loop(LONG_INTERVAL);
                let first = Arc::new(Owner::default());
                let second = Arc::new(Owner::default());
                let third = Arc::new(Owner::default());

                cleanup_loop.start_task(&first);
                cleanup_loop.stop();
                cleanup_loop.start_task(&second);
                cleanup_loop.start_task(&third);

                assert_eq!(
                    Arc::weak_count(&third),
                    0,
                    "a start while a loop runs must not start a second loop"
                );
                assert_loop_ends_at_once(&first, "the stopped loop must end").await;
                assert_eq!(first.passes(), 0, "the stopped loop must not run a pass");
                assert_eq!(
                    Arc::weak_count(&second),
                    1,
                    "the restarted loop must keep running"
                );

                cleanup_loop.stop();
                assert_loop_ends_at_once(&second, "the restarted loop must end after stop").await;
            });
        }
    }
}
