use std::time::Duration;

use crate::{RateLimit, WindowSize, local::LocalRateLimiterProvider};

#[test]
fn local_cleanup_removes_stale_entries_and_keeps_active_entries() {
    let provider = LocalRateLimiterProvider::builder()
        .window_size(WindowSize::seconds_or_panic(1))
        .stale_after(Duration::from_millis(100))
        .cleanup_interval(Duration::from_millis(40))
        .build()
        .unwrap();
    let rate = RateLimit::per_second_or_panic(100.0);

    provider.absolute().inc("stale", &rate, 1);
    provider.absolute().inc("active", &rate, 1);
    for _ in 0..4 {
        std::thread::sleep(Duration::from_millis(40));
        provider.absolute().inc("active", &rate, 1);
    }
    std::thread::sleep(Duration::from_millis(60));

    assert!(provider.absolute().series().get("stale").is_none());
    assert!(provider.absolute().series().get("active").is_some());
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn redis_and_hybrid_cleanup_start_by_default() {
    use redis::AsyncCommands;

    use crate::{
        BucketSize,
        common::RateType,
        hybrid::{HybridRateLimiterProvider, SyncInterval},
        redis::RedisRateLimiterProvider,
    };

    super::runtime::block_on(async {
        let connection_manager = super::common::connection_manager().await;
        let rate = RateLimit::per_second_or_panic(100.0);

        let redis_prefix = super::common::unique_prefix();
        let redis = RedisRateLimiterProvider::builder(connection_manager.clone())
            .prefix(redis_prefix.clone())
            .window_size(WindowSize::seconds_or_panic(60))
            .stale_after(Duration::from_millis(50))
            .cleanup_interval(Duration::from_millis(200))
            .build()
            .unwrap();
        let redis_key = super::common::key("redis_cleanup");
        redis.absolute().inc(&redis_key, &rate, 1).await.unwrap();

        let hybrid_prefix = super::common::unique_prefix();
        let hybrid = HybridRateLimiterProvider::builder(connection_manager.clone())
            .prefix(hybrid_prefix.clone())
            .window_size(WindowSize::seconds_or_panic(60))
            .bucket_size(BucketSize::milliseconds_or_panic(5))
            .sync_interval(SyncInterval::milliseconds_or_panic(5))
            .stale_after(Duration::from_millis(50))
            .cleanup_interval(Duration::from_millis(200))
            .build()
            .unwrap();
        let hybrid_key = super::common::key("hybrid_cleanup");
        hybrid.absolute().inc(&hybrid_key, &rate, 1).await.unwrap();

        // This is after one cleanup interval but before two; Smol must not discard its first tick.
        super::runtime::async_sleep(Duration::from_millis(325)).await;

        for (prefix, rate_type, key) in [
            (&redis_prefix, RateType::Absolute, &redis_key),
            (&hybrid_prefix, RateType::HybridAbsolute, &hybrid_key),
        ] {
            let key_generator = super::common::key_gen(prefix, rate_type);
            let mut connection = connection_manager.clone();
            for entity_key in key_generator.get_all_entity_keys(key) {
                let exists: bool = connection.exists(entity_key).await.unwrap();
                assert!(!exists, "default cleanup left stale state for {key:?}");
            }
            let score: Option<f64> = connection
                .zscore(key_generator.get_active_entities_key(), key.as_str())
                .await
                .unwrap();
            assert!(score.is_none(), "default cleanup left active membership");
        }
    });
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[test]
fn redis_and_hybrid_cleanup_support_opt_out_stop_and_restart() {
    use redis::AsyncCommands;

    use crate::{
        BucketSize,
        common::RateType,
        hybrid::{HybridRateLimiterProvider, SyncInterval},
        redis::RedisRateLimiterProvider,
    };

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

        super::runtime::async_sleep(Duration::from_millis(220)).await;
        for (prefix, rate_type, key) in [
            (&redis_prefix, RateType::Absolute, &redis_key),
            (&hybrid_prefix, RateType::HybridAbsolute, &hybrid_key),
        ] {
            let key_generator = super::common::key_gen(prefix, rate_type);
            let mut connection = connection_manager.clone();
            let exists: bool = connection
                .exists(key_generator.get_total_count_key(key))
                .await
                .unwrap();
            assert!(exists, "cleanup opt-out lost state for {key:?}");
        }

        redis.start_cleanup_loop();
        redis.start_cleanup_loop();
        hybrid.start_cleanup_loop();
        hybrid.start_cleanup_loop();
        super::runtime::async_sleep(Duration::from_millis(220)).await;

        for (prefix, rate_type, key) in [
            (&redis_prefix, RateType::Absolute, &redis_key),
            (&hybrid_prefix, RateType::HybridAbsolute, &hybrid_key),
        ] {
            let key_generator = super::common::key_gen(prefix, rate_type);
            let mut connection = connection_manager.clone();
            let exists: bool = connection
                .exists(key_generator.get_total_count_key(key))
                .await
                .unwrap();
            assert!(!exists, "started cleanup retained stale state for {key:?}");
        }

        redis.stop_cleanup_loop();
        redis.stop_cleanup_loop();
        hybrid.stop_cleanup_loop();
        hybrid.stop_cleanup_loop();

        let redis_key = super::common::key("redis_stopped");
        let hybrid_key = super::common::key("hybrid_stopped");
        redis.absolute().inc(&redis_key, &rate, 1).await.unwrap();
        hybrid.absolute().inc(&hybrid_key, &rate, 1).await.unwrap();
        super::runtime::async_sleep(Duration::from_millis(220)).await;

        for (prefix, rate_type, key) in [
            (&redis_prefix, RateType::Absolute, &redis_key),
            (&hybrid_prefix, RateType::HybridAbsolute, &hybrid_key),
        ] {
            let key_generator = super::common::key_gen(prefix, rate_type);
            let mut connection = connection_manager.clone();
            let exists: bool = connection
                .exists(key_generator.get_total_count_key(key))
                .await
                .unwrap();
            assert!(exists, "stopped cleanup removed state for {key:?}");
        }

        redis.start_cleanup_loop();
        redis.stop_cleanup_loop();
        redis.start_cleanup_loop();
        hybrid.start_cleanup_loop();
        hybrid.stop_cleanup_loop();
        hybrid.start_cleanup_loop();
        super::runtime::async_sleep(Duration::from_millis(220)).await;

        for (prefix, rate_type, key) in [
            (&redis_prefix, RateType::Absolute, &redis_key),
            (&hybrid_prefix, RateType::HybridAbsolute, &hybrid_key),
        ] {
            let key_generator = super::common::key_gen(prefix, rate_type);
            let mut connection = connection_manager.clone();
            let exists: bool = connection
                .exists(key_generator.get_total_count_key(key))
                .await
                .unwrap();
            assert!(
                !exists,
                "restarted cleanup retained stale state for {key:?}"
            );
        }
    });
}

/// Tests of `CleanupLoop` itself. Each test owns the cleanup function that the loop runs: a
/// counter of its calls and a `LoopEnd` that the loop drops when it ends.
mod loop_lifecycle {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        time::{Duration, Instant},
    };

    use crate::{builder::CleanupConfig, cleanup_loop::CleanupLoop};

    pub(super) const INTERVAL: Duration = Duration::from_millis(20);
    /// Interval for the restart tests: long enough that a stop and a start run before the first
    /// loop wakes.
    pub(super) const LONG_INTERVAL: Duration = Duration::from_millis(200);
    pub(super) const BOUND: Duration = Duration::from_secs(3);

    pub(super) struct Owner {
        pub(super) cleanup_loop: CleanupLoop,
    }

    pub(super) fn owner(interval: Duration) -> Arc<Owner> {
        Arc::new(Owner {
            cleanup_loop: CleanupLoop::new(CleanupConfig {
                enabled: false,
                stale_after: Duration::from_millis(1),
                interval,
            }),
        })
    }

    /// Sends one message when the loop that owns it drops it.
    pub(super) struct LoopEnd(mpsc::Sender<()>);

    impl Drop for LoopEnd {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }

    /// The call counter and drop signal of one loop.
    pub(super) struct Probe {
        pub(super) calls: Arc<AtomicUsize>,
        ended: mpsc::Receiver<()>,
    }

    impl Probe {
        pub(super) fn new() -> (Self, Arc<AtomicUsize>, LoopEnd) {
            let calls = Arc::new(AtomicUsize::new(0));
            let (sender, ended) = mpsc::channel();
            (
                Self {
                    calls: Arc::clone(&calls),
                    ended,
                },
                calls,
                LoopEnd(sender),
            )
        }

        pub(super) fn calls(&self) -> usize {
            self.calls.load(Ordering::Acquire)
        }

        pub(super) fn has_ended(&self) -> bool {
            self.ended.try_recv().is_ok()
        }
    }

    pub(super) fn thread_cleanup(
        calls: Arc<AtomicUsize>,
        loop_end: LoopEnd,
    ) -> impl Fn(&Owner, u64) + Send + 'static {
        move |_, _| {
            let _ = &loop_end;
            calls.fetch_add(1, Ordering::AcqRel);
        }
    }

    pub(super) fn start_thread(owner: &Arc<Owner>, probe: (Arc<AtomicUsize>, LoopEnd)) {
        CleanupLoop::start_thread(
            owner,
            |owner| &owner.cleanup_loop,
            thread_cleanup(probe.0, probe.1),
        );
    }

    /// Wait until `condition` holds, or fail with `message` after `BOUND`.
    pub(super) fn wait_until(mut condition: impl FnMut() -> bool, message: &str) {
        let deadline = Instant::now() + BOUND;
        while !condition() {
            assert!(Instant::now() < deadline, "{message} within {BOUND:?}");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    #[test]
    fn thread_loop_ends_after_last_owner_arc_drops() {
        let owner = owner(INTERVAL);
        let (probe, calls, loop_end) = Probe::new();
        start_thread(&owner, (calls, loop_end));
        wait_until(|| probe.calls() >= 1, "the started loop must run");

        drop(owner);

        wait_until(
            || probe.has_ended(),
            "the loop must end after the owner drops",
        );
        let calls_at_end = probe.calls();
        std::thread::sleep(INTERVAL * 5);
        assert_eq!(probe.calls(), calls_at_end);
    }

    #[test]
    fn thread_loop_ends_after_stop() {
        let owner = owner(INTERVAL);
        let (probe, calls, loop_end) = Probe::new();
        start_thread(&owner, (calls, loop_end));
        wait_until(|| probe.calls() >= 1, "the started loop must run");

        owner.cleanup_loop.stop();

        wait_until(|| probe.has_ended(), "the loop must end after stop");
        let calls_at_end = probe.calls();
        std::thread::sleep(INTERVAL * 5);
        assert_eq!(probe.calls(), calls_at_end);
        drop(owner);
    }

    #[test]
    fn thread_loop_stop_then_start_before_wake_leaves_one_loop() {
        let owner = owner(LONG_INTERVAL);
        let (first, calls, loop_end) = Probe::new();
        start_thread(&owner, (calls, loop_end));
        // The first loop sleeps for one 200 ms interval before it runs, so this restart is
        // before it wakes.
        owner.cleanup_loop.stop();
        let (second, calls, loop_end) = Probe::new();
        start_thread(&owner, (calls, loop_end));
        let (third, calls, loop_end) = Probe::new();
        start_thread(&owner, (calls, loop_end));

        assert!(
            third.has_ended(),
            "a start while a loop runs must not start a second loop"
        );
        wait_until(
            || first.has_ended(),
            "the stopped loop must end at its wake",
        );
        wait_until(|| second.calls() >= 3, "the restarted loop must run");
        assert_eq!(first.calls(), 0, "the stopped loop must not run a pass");
        assert_eq!(third.calls(), 0);
        assert!(!second.has_ended(), "the restarted loop must keep running");

        drop(owner);
        wait_until(
            || second.has_ended(),
            "the loop must end after the owner drops",
        );
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

        use super::{BOUND, INTERVAL, LONG_INTERVAL, LoopEnd, Owner, Probe, owner};
        use crate::{TrypemaError, cleanup_loop::CleanupLoop, tests::runtime};

        fn task_cleanup(
            calls: Arc<AtomicUsize>,
            loop_end: LoopEnd,
        ) -> impl Fn(Arc<Owner>, u64) -> std::future::Ready<Result<(), TrypemaError>> + Send + 'static
        {
            move |_, _| {
                let _ = &loop_end;
                calls.fetch_add(1, Ordering::AcqRel);
                std::future::ready(Ok(()))
            }
        }

        fn start_task(owner: &Arc<Owner>, probe: (Arc<AtomicUsize>, LoopEnd)) {
            CleanupLoop::start_task(
                owner,
                |owner| &owner.cleanup_loop,
                task_cleanup(probe.0, probe.1),
                "test cleanup failed",
            );
        }

        /// Wait on the runtime until `condition` holds, or fail with `message` after `BOUND`.
        async fn wait_until(mut condition: impl FnMut() -> bool, message: &str) {
            let deadline = Instant::now() + BOUND;
            while !condition() {
                assert!(Instant::now() < deadline, "{message} within {BOUND:?}");
                runtime::async_sleep(std::time::Duration::from_millis(5)).await;
            }
        }

        fn on_runtime(test: impl Future<Output = ()>) {
            runtime::block_on(test);
        }

        #[test]
        fn task_loop_ends_after_last_owner_arc_drops() {
            on_runtime(async {
                let owner = owner(INTERVAL);
                let (probe, calls, loop_end) = Probe::new();
                start_task(&owner, (calls, loop_end));
                wait_until(|| probe.calls() >= 1, "the started loop must run").await;

                drop(owner);

                wait_until(
                    || probe.has_ended(),
                    "the loop must end after the owner drops",
                )
                .await;
                let calls_at_end = probe.calls();
                runtime::async_sleep(INTERVAL * 5).await;
                assert_eq!(probe.calls(), calls_at_end);
            });
        }

        #[test]
        fn task_loop_ends_after_stop() {
            on_runtime(async {
                let owner = owner(INTERVAL);
                let (probe, calls, loop_end) = Probe::new();
                start_task(&owner, (calls, loop_end));
                wait_until(|| probe.calls() >= 1, "the started loop must run").await;

                owner.cleanup_loop.stop();

                wait_until(|| probe.has_ended(), "the loop must end after stop").await;
                let calls_at_end = probe.calls();
                runtime::async_sleep(INTERVAL * 5).await;
                assert_eq!(probe.calls(), calls_at_end);
                drop(owner);
            });
        }

        #[test]
        fn task_loop_stop_then_start_before_wake_leaves_one_loop() {
            on_runtime(async {
                let owner = owner(LONG_INTERVAL);
                let (first, calls, loop_end) = Probe::new();
                start_task(&owner, (calls, loop_end));
                // No await runs between these calls, so the first task has not run yet.
                owner.cleanup_loop.stop();
                let (second, calls, loop_end) = Probe::new();
                start_task(&owner, (calls, loop_end));
                let (third, calls, loop_end) = Probe::new();
                start_task(&owner, (calls, loop_end));

                assert!(
                    third.has_ended(),
                    "a start while a loop runs must not start a second loop"
                );
                wait_until(
                    || first.has_ended(),
                    "the stopped loop must end at its wake",
                )
                .await;
                wait_until(|| second.calls() >= 3, "the restarted loop must run").await;
                assert_eq!(first.calls(), 0, "the stopped loop must not run a pass");
                assert_eq!(third.calls(), 0);
                assert!(!second.has_ended(), "the restarted loop must keep running");

                drop(owner);
                wait_until(
                    || second.has_ended(),
                    "the loop must end after the owner drops",
                )
                .await;
            });
        }
    }
}
