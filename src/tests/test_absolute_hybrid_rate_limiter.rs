use std::{env, thread, time::Duration};

use super::runtime;

use crate::{
    BucketSize, HistoryPreservation, RateLimit, RateLimitComparator, RateLimitDecision,
    RateLimiterBuilder, WindowSize,
    hybrid::{HybridRateLimiterProvider, SyncInterval},
    redis::RedisKey,
};

fn window_capacity(window_size: u64, rate_limit: &RateLimit) -> u64 {
    ((window_size as f64) * rate_limit.as_per_second()) as u64
}

fn record_decision(
    decision: RateLimitDecision,
    count: u64,
    accepted_volume: &mut u64,
    rejected_volume: &mut u64,
    allowed_ops: &mut u64,
    rejected_ops: &mut u64,
) {
    match decision {
        RateLimitDecision::Allowed => {
            *accepted_volume += count;
            *allowed_ops += 1;
        }
        RateLimitDecision::Rejected { .. } => {
            *rejected_volume += count;
            *rejected_ops += 1;
        }
        RateLimitDecision::Suppressed { .. } => {
            panic!("suppressed decision is not expected in absolute strategy")
        }
    }
}

fn assert_allowed(decision: RateLimitDecision, context: &str) {
    assert!(
        matches!(decision, RateLimitDecision::Allowed),
        "{context}: expected allowed decision, got {decision:?}"
    );
}

fn assert_rejected(decision: RateLimitDecision, context: &str) {
    assert!(
        matches!(decision, RateLimitDecision::Rejected { .. }),
        "{context}: expected rejected decision, got {decision:?}"
    );
}

fn redis_url() -> String {
    env::var("REDIS_URL").unwrap_or_else(|_| {
        panic!(
            "REDIS_URL env var must be set for Redis-backed tests (e.g. REDIS_URL=redis://127.0.0.1:16379/)"
        )
    })
}

fn unique_prefix() -> RedisKey {
    let n: u64 = rand::random();
    RedisKey::try_from(format!("trypema_test_{n}")).unwrap()
}

fn key(s: &str) -> RedisKey {
    RedisKey::try_from(s.to_string()).unwrap()
}

async fn build_limiter_with_prefix(
    url: &str,
    window_size: u64,
    bucket_size: u64,
    sync_interval: u64,
    prefix: RedisKey,
) -> std::sync::Arc<HybridRateLimiterProvider> {
    let client = redis::Client::open(url).unwrap();
    let cm = client.get_connection_manager().await.unwrap();

    HybridRateLimiterProvider::builder(cm)
        .prefix(prefix)
        .window_size(WindowSize::seconds(window_size).unwrap())
        .bucket_size(BucketSize::milliseconds(bucket_size).unwrap())
        .sync_interval(SyncInterval::milliseconds(sync_interval).unwrap())
        .cleanup_enabled(false)
        .build()
        .unwrap()
}

async fn wait_for_hybrid_sync(sync_interval: u64) {
    // Wait past two interval boundaries so the assertion does not depend on where
    // construction landed within the committer's current interval.
    runtime::async_sleep(Duration::from_millis(sync_interval * 2 + 50)).await;
}

#[test]
fn hybrid_allows_until_capacity_then_rejects() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size = 1_u64;
        let bucket_size = 1_000_u64;
        let sync_interval = 25_u64;

        let rl =
            build_limiter_with_prefix(&url, window_size, bucket_size, sync_interval, prefix).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(5f64).unwrap();
        let cap = window_capacity(window_size, &rate_limit);

        for _ in 0..cap {
            let d = rl.absolute().inc(&k, &rate_limit, 1).await.unwrap();
            assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");
        }

        let d = rl.absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(d, RateLimitDecision::Rejected { .. }), "d: {d:?}");
    });
}

#[test]
fn hybrid_absolute_never_returns_suppressed() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 1, 1000, 25, unique_prefix()).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(50f64).unwrap();

        for _ in 0..200_u64 {
            let d = rl.absolute().inc(&k, &rate_limit, 1).await.unwrap();
            if matches!(d, RateLimitDecision::Suppressed { .. }) {
                panic!("suppressed decision is not expected in hybrid absolute: {d:?}");
            }
        }
    });
}

#[test]
fn hybrid_absolute_pending_only_rejection_reports_window_ttl_and_full_released_capacity() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size = 1_u64;
        let bucket_size = 1_000_u64;
        let sync_interval = 25_u64;

        let rl =
            build_limiter_with_prefix(&url, window_size, bucket_size, sync_interval, prefix).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(5f64).unwrap();
        let cap = window_capacity(window_size, &rate_limit);

        for _ in 0..cap {
            assert_allowed(
                rl.absolute().inc(&k, &rate_limit, 1).await.unwrap(),
                "capacity setup increment",
            );
        }

        let d = rl.absolute().inc(&k, &rate_limit, 1).await.unwrap();
        let RateLimitDecision::Rejected {
            retry_after,
            remaining_after_waiting,
            ..
        } = d
        else {
            panic!("expected rejected decision, got: {d:?}");
        };

        assert!(
            retry_after >= Duration::from_millis(900)
                && retry_after <= Duration::from_millis(1_000),
            "pending usage should retain approximately one window of TTL: {d:?}"
        );
        assert_eq!(
            remaining_after_waiting, cap,
            "all locally pending usage is one releasable grouped bucket: {d:?}"
        );
    });
}

#[test]
fn hybrid_absolute_oversized_request_on_empty_key_has_no_backoff_wait() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size = 1_u64;
        let rl = build_limiter_with_prefix(&url, window_size, 1_000, 25, unique_prefix()).await;
        let k = key("k");
        let rate_limit = RateLimit::per_second(5f64).unwrap();
        let capacity = window_capacity(window_size, &rate_limit);

        let decision = rl
            .absolute()
            .inc(&k, &rate_limit, capacity + 1)
            .await
            .unwrap();
        let RateLimitDecision::Rejected {
            retry_after,
            remaining_after_waiting,
            ..
        } = decision
        else {
            panic!("expected oversized request to be rejected, got {decision:?}");
        };

        assert_eq!(
            retry_after,
            Duration::ZERO,
            "no existing bucket needs to expire"
        );
        assert_eq!(remaining_after_waiting, 0, "no bucket releases capacity");
        assert_eq!(rl.absolute().get(&k).await.unwrap(), 0);
    });
}

#[test]
fn hybrid_absolute_known_oldest_ttl_is_not_inflated_to_sync_interval() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size = 60_u64;
        let sync_interval = 120_000_u64;
        let k = key("k");
        let rate_limit = RateLimit::per_second(5f64).unwrap();
        let capacity = window_capacity(window_size, &rate_limit);

        let seed =
            build_limiter_with_prefix(&url, window_size, 1_000, sync_interval, prefix.clone())
                .await;
        assert_eq!(
            seed.absolute()
                .set_if(&k, &rate_limit, RateLimitComparator::Always, capacity)
                .await
                .unwrap(),
            (capacity, 0)
        );

        runtime::async_sleep(Duration::from_millis(50)).await;

        let observer =
            build_limiter_with_prefix(&url, window_size, 1_000, sync_interval, prefix).await;
        let decision = observer.absolute().is_allowed(&k).await.unwrap();
        let RateLimitDecision::Rejected {
            retry_after,
            remaining_after_waiting,
            ..
        } = decision
        else {
            panic!("expected rejected admission check, got {decision:?}");
        };

        assert!(
            retry_after >= Duration::from_millis(50_000)
                && retry_after <= Duration::from_millis(60_000),
            "Redis bucket TTL must not be inflated to the 120s sync interval: {decision:?}"
        );
        assert_eq!(remaining_after_waiting, capacity);
    });
}

#[test]
fn hybrid_usage_is_committed_to_redis_on_flush_and_then_visible_to_others() {
    let url = redis_url();

    runtime::block_on(async {
        // Use a shared prefix so both instances address the same Redis keys.
        let prefix = unique_prefix();

        let window_size = 1_u64;
        let bucket_size = 1_000_u64;
        let sync_interval = 25_u64;

        let rl_a = build_limiter_with_prefix(
            &url,
            window_size,
            bucket_size,
            sync_interval,
            prefix.clone(),
        )
        .await;
        let rl_b = build_limiter_with_prefix(
            &url,
            window_size,
            bucket_size,
            sync_interval,
            prefix.clone(),
        )
        .await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(5f64).unwrap();

        // Fill the hybrid local accept budget (does not write to Redis yet).
        for _ in 0..5_u64 {
            let d = rl_a.absolute().inc(&k, &rate_limit, 1).await.unwrap();
            assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");
        }

        // Other instances should still see no limit state in the hybrid keyspace.
        let d0 = rl_b.absolute().is_allowed(&k).await.unwrap();
        assert!(matches!(d0, RateLimitDecision::Allowed), "d0: {d0:?}");

        // Crossing the accept limit queues a commit (and rejects).
        let d1 = rl_a.absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(
            matches!(d1, RateLimitDecision::Rejected { .. }),
            "d1: {d1:?}"
        );

        // Exhaustion commits synchronously before the rejecting decision returns.
        let d2 = rl_b.absolute().is_allowed(&k).await.unwrap();
        assert_rejected(d2, "overflow commit must be visible to another instance");
    });
}

#[test]
fn hybrid_absolute_does_not_touch_redis_until_commit() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();

        let window_size = 1_u64;
        let bucket_size = 1_000_u64;
        let sync_interval = 25_u64;

        let rl_a = build_limiter_with_prefix(
            &url,
            window_size,
            bucket_size,
            sync_interval,
            prefix.clone(),
        )
        .await;
        let rl_b = build_limiter_with_prefix(
            &url,
            window_size,
            bucket_size,
            sync_interval,
            prefix.clone(),
        )
        .await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(5f64).unwrap();
        let cap = window_capacity(window_size, &rate_limit);

        // Fill local accept budget; no commit should have happened yet.
        for _ in 0..cap {
            let d = rl_a.absolute().inc(&k, &rate_limit, 1).await.unwrap();
            assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");
        }

        // Other instances should not observe a limit until a commit is flushed.
        let d0 = rl_b.absolute().is_allowed(&k).await.unwrap();
        assert!(matches!(d0, RateLimitDecision::Allowed), "d0: {d0:?}");
    });
}

#[test]
fn hybrid_absolute_unblocks_after_window_expires() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();

        let window_size = 1_u64;
        let bucket_size = 1_000_u64;
        let sync_interval = 25_u64;

        let rl =
            build_limiter_with_prefix(&url, window_size, bucket_size, sync_interval, prefix).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(5f64).unwrap();
        let cap = window_capacity(window_size, &rate_limit);

        for _ in 0..cap {
            assert_allowed(
                rl.absolute().inc(&k, &rate_limit, 1).await.unwrap(),
                "capacity setup increment",
            );
        }

        let d = rl.absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(d, RateLimitDecision::Rejected { .. }), "d: {d:?}");

        // Wait for the full window to expire. We poll to avoid flakiness.
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            if std::time::Instant::now() >= deadline {
                panic!("timed out waiting for window to expire");
            }

            thread::sleep(Duration::from_millis(25));
            let d2 = rl.absolute().inc(&k, &rate_limit, 1).await.unwrap();
            if matches!(d2, RateLimitDecision::Allowed) {
                break;
            }
        }
    });
}

#[test]
fn hybrid_absolute_per_key_state_is_independent() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 1, 1000, 25, unique_prefix()).await;

        let a = key("a");
        let b = key("b");
        let rate_limit = RateLimit::per_second(2f64).unwrap();
        let cap = window_capacity(1, &rate_limit);

        for _ in 0..cap {
            assert_allowed(
                rl.absolute().inc(&a, &rate_limit, 1).await.unwrap(),
                "key A capacity setup increment",
            );
        }

        let da = rl.absolute().inc(&a, &rate_limit, 1).await.unwrap();
        assert!(
            matches!(da, RateLimitDecision::Rejected { .. }),
            "da: {da:?}"
        );

        let db = rl.absolute().inc(&b, &rate_limit, 1).await.unwrap();
        assert!(matches!(db, RateLimitDecision::Allowed), "db: {db:?}");
    });
}

#[test]
fn hybrid_absolute_prefix_isolation() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size = 1_u64;
        let bucket_size = 1_000_u64;
        let sync_interval = 25_u64;

        let rl_a = build_limiter_with_prefix(
            &url,
            window_size,
            bucket_size,
            sync_interval,
            unique_prefix(),
        )
        .await;
        let rl_b = build_limiter_with_prefix(
            &url,
            window_size,
            bucket_size,
            sync_interval,
            unique_prefix(),
        )
        .await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(5f64).unwrap();
        let cap = window_capacity(window_size, &rate_limit);

        for _ in 0..cap {
            assert_allowed(
                rl_a.absolute().inc(&k, &rate_limit, 1).await.unwrap(),
                "prefix A capacity setup increment",
            );
        }
        let d = rl_a.absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(d, RateLimitDecision::Rejected { .. }), "d: {d:?}");

        let d_other = rl_b.absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(
            matches!(d_other, RateLimitDecision::Allowed),
            "d_other: {d_other:?}"
        );
    });
}

#[test]
fn hybrid_absolute_remaining_after_waiting_reflects_oldest_bucket_when_seeded_from_redis() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();

        let window_size = 6_u64;
        // Make grouping easy to satisfy: commits separated by ~2*sync_interval must be < group.
        let bucket_size = 1_000_u64;
        let sync_interval = 200_u64;

        let k = key("k");
        let rate_limit = RateLimit::per_second(1f64).unwrap();

        // Seed Redis state through the hybrid public API (no direct Redis commands).
        // We write 2, wait for it to be committed, then write 4; the commit times are close enough
        // that Redis groups them into one bucket.
        let rl_seed = build_limiter_with_prefix(
            &url,
            window_size,
            bucket_size,
            sync_interval,
            prefix.clone(),
        )
        .await;
        assert_allowed(
            rl_seed.absolute().inc(&k, &rate_limit, 2).await.unwrap(),
            "oldest bucket setup increment",
        );
        wait_for_hybrid_sync(sync_interval).await;
        assert_allowed(
            rl_seed.absolute().inc(&k, &rate_limit, 4).await.unwrap(),
            "newest bucket setup increment",
        );
        wait_for_hybrid_sync(sync_interval).await;

        // Create a hybrid limiter that will read the current Redis state.
        let rl =
            build_limiter_with_prefix(&url, window_size, bucket_size, sync_interval, prefix).await;

        let d_prime = rl.absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(
            matches!(d_prime, RateLimitDecision::Rejected { .. }),
            "d_prime: {d_prime:?}"
        );

        // At capacity (2 + 4). Next increment should be rejected and remaining_after_waiting
        // should reflect the oldest bucket count (6 when grouped).
        let d = rl.absolute().inc(&k, &rate_limit, 1).await.unwrap();
        let RateLimitDecision::Rejected {
            remaining_after_waiting,
            ..
        } = d
        else {
            panic!("expected rejected decision, got: {d:?}");
        };
        assert_eq!(remaining_after_waiting, 6, "d: {d:?}");
    });
}

#[test]
fn hybrid_absolute_remaining_after_waiting_is_capacity_released_by_oldest_bucket() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size = 10_u64;
        let bucket_size = 100_u64;
        let sync_interval = 200_u64;
        let k = key("k");
        let rate_limit = RateLimit::per_second(1f64).unwrap();

        let seed = build_limiter_with_prefix(
            &url,
            window_size,
            bucket_size,
            sync_interval,
            prefix.clone(),
        )
        .await;
        assert_allowed(
            seed.absolute().inc(&k, &rate_limit, 3).await.unwrap(),
            "oldest bucket setup increment",
        );
        wait_for_hybrid_sync(sync_interval).await;
        assert_allowed(
            seed.absolute().inc(&k, &rate_limit, 7).await.unwrap(),
            "newest bucket setup increment",
        );
        wait_for_hybrid_sync(sync_interval).await;

        let observer =
            build_limiter_with_prefix(&url, window_size, bucket_size, sync_interval, prefix).await;
        let decision = observer.absolute().is_allowed(&k).await.unwrap();
        let RateLimitDecision::Rejected {
            remaining_after_waiting,
            ..
        } = decision
        else {
            panic!("expected rejected decision, got {decision:?}");
        };

        assert_eq!(
            remaining_after_waiting, 3,
            "the oldest bucket releases three count units: {decision:?}"
        );

        let cached_decision = observer.absolute().inc(&k, &rate_limit, 1).await.unwrap();
        let RateLimitDecision::Rejected {
            remaining_after_waiting,
            ..
        } = cached_decision
        else {
            panic!("expected cached rejected decision, got {cached_decision:?}");
        };
        assert_eq!(remaining_after_waiting, 3);
        assert_eq!(observer.absolute().get(&k).await.unwrap(), 10);
    });
}

#[test]
fn hybrid_absolute_concurrent_smoke_does_not_panic() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 1, 1000, 25, unique_prefix()).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(10f64).unwrap();

        let mut tasks = Vec::new();
        for _ in 0..16 {
            let rl = rl.clone();
            let k = k.clone();
            let rate_limit = rate_limit.clone();
            tasks.push(runtime::spawn(async move {
                for _ in 0..50 {
                    let d = rl.absolute().inc(&k, &rate_limit, 1).await.unwrap();
                    if matches!(d, RateLimitDecision::Suppressed { .. }) {
                        panic!("unexpected suppressed decision in hybrid absolute");
                    }
                }
            }));
        }

        for t in tasks {
            runtime::join(t).await;
        }
    });
}

#[test]
fn volume_unit_increments_accepts_exact_capacity_then_rejects_rest() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size = 1_u64;
        let bucket_size = 1000_u64;
        let sync_interval = 25_u64;

        let rl =
            build_limiter_with_prefix(&url, window_size, bucket_size, sync_interval, prefix).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(50f64).unwrap();
        let capacity = window_capacity(window_size, &rate_limit);
        assert_eq!(capacity, 50);

        let mut accepted_volume = 0_u64;
        let mut rejected_volume = 0_u64;
        let mut allowed_ops = 0_u64;
        let mut rejected_ops = 0_u64;

        for _ in 0..80_u64 {
            let decision = rl.absolute().inc(&k, &rate_limit, 1).await.unwrap();
            record_decision(
                decision,
                1,
                &mut accepted_volume,
                &mut rejected_volume,
                &mut allowed_ops,
                &mut rejected_ops,
            );
        }

        assert_eq!(accepted_volume, capacity);
        assert_eq!(rejected_volume, 80 - capacity);
        assert_eq!(allowed_ops, capacity);
        assert_eq!(rejected_ops, 80 - capacity);
    });
}

#[test]
fn volume_batch_increment_is_all_or_nothing_and_matches_expected_volumes() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size = 1_u64;
        let bucket_size = 1000_u64;
        let sync_interval = 25_u64;

        let rl =
            build_limiter_with_prefix(&url, window_size, bucket_size, sync_interval, prefix).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(10f64).unwrap();
        let capacity = window_capacity(window_size, &rate_limit);
        assert_eq!(capacity, 10);

        let mut accepted_volume = 0_u64;
        let mut rejected_volume = 0_u64;
        let mut allowed_ops = 0_u64;
        let mut rejected_ops = 0_u64;

        // Allowed: consumes 9 of 10.
        let d1 = rl.absolute().inc(&k, &rate_limit, 9).await.unwrap();
        record_decision(
            d1,
            9,
            &mut accepted_volume,
            &mut rejected_volume,
            &mut allowed_ops,
            &mut rejected_ops,
        );

        // Rejected: would push total to 11.
        let d2 = rl.absolute().inc(&k, &rate_limit, 2).await.unwrap();
        assert!(
            matches!(d2, RateLimitDecision::Rejected { .. }),
            "d2: {d2:?}"
        );
        record_decision(
            d2,
            2,
            &mut accepted_volume,
            &mut rejected_volume,
            &mut allowed_ops,
            &mut rejected_ops,
        );

        // Allowed: proves the rejected batch did not consume capacity.
        let d3 = rl.absolute().inc(&k, &rate_limit, 1).await.unwrap();
        record_decision(
            d3,
            1,
            &mut accepted_volume,
            &mut rejected_volume,
            &mut allowed_ops,
            &mut rejected_ops,
        );

        assert_eq!(accepted_volume, capacity);
        assert_eq!(rejected_volume, 2);
        assert_eq!(allowed_ops, 2);
        assert_eq!(rejected_ops, 1);
    });
}

#[test]
fn volume_rejections_do_not_consume_and_capacity_resets_after_window_expiry() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size = 1_u64;
        let bucket_size = 1000_u64;
        let sync_interval = 25_u64;

        let rl =
            build_limiter_with_prefix(&url, window_size, bucket_size, sync_interval, prefix).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(2f64).unwrap();
        let capacity = window_capacity(window_size, &rate_limit);
        assert_eq!(capacity, 2);

        // Fill capacity.
        for _ in 0..capacity {
            let decision = rl.absolute().inc(&k, &rate_limit, 1).await.unwrap();
            assert!(
                matches!(decision, RateLimitDecision::Allowed),
                "decision: {decision:?}"
            );
        }

        // Many rejected attempts should not change what we can do after the window expires.
        let mut rejected_ops = 0_u64;
        for _ in 0..20_u64 {
            let decision = rl.absolute().inc(&k, &rate_limit, 1).await.unwrap();
            assert!(
                matches!(decision, RateLimitDecision::Rejected { .. }),
                "decision: {decision:?}"
            );
            rejected_ops += 1;
        }
        assert_eq!(rejected_ops, 20);

        runtime::async_sleep(Duration::from_millis(1100)).await;

        let mut accepted_after_expiry = 0_u64;
        for _ in 0..capacity {
            let decision = rl.absolute().inc(&k, &rate_limit, 1).await.unwrap();
            assert!(
                matches!(decision, RateLimitDecision::Allowed),
                "decision: {decision:?}"
            );
            accepted_after_expiry += 1;
        }
        assert_eq!(accepted_after_expiry, capacity);
    });
}

#[test]
fn get_returns_zero_for_untouched_key() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 6, 1000, 25, unique_prefix()).await;

        let k = key("k");
        assert_eq!(rl.absolute().get_estimate(&k).await.unwrap(), 0);
        assert_eq!(rl.absolute().get(&k).await.unwrap(), 0);
    });
}

#[test]
fn cleanup_keeps_rejecting_state_while_retry_ttl_is_live() {
    let url = redis_url();

    runtime::block_on(async {
        let stale_after_ms = 50_u64;
        let window_size_ms = 1_000_u64;
        let rl = build_limiter_with_prefix(&url, 1, 1_000, 2_000, unique_prefix()).await;
        let k = key("k");
        let rate = RateLimit::per_second(10f64).unwrap();

        assert_allowed(
            rl.absolute()
                .inc(&k, &rate, window_capacity(1, &rate))
                .await
                .unwrap(),
            "fill rejection setup capacity",
        );
        assert_rejected(
            rl.absolute().inc(&k, &rate, 1).await.unwrap(),
            "create rejection cache",
        );
        assert_eq!(rl.absolute().local_state_count(), 1);

        runtime::async_sleep(Duration::from_millis(100)).await;
        rl.absolute().cleanup(stale_after_ms).await.unwrap();
        assert_eq!(
            rl.absolute().local_state_count(),
            1,
            "live rejection TTL must outlive the shorter stale horizon"
        );

        runtime::async_sleep(Duration::from_millis(window_size_ms)).await;
        rl.absolute().cleanup(stale_after_ms).await.unwrap();
        assert_eq!(rl.absolute().local_state_count(), 0);
    });
}

#[test]
fn cleanup_keeps_rejecting_state_until_stale_horizon_after_retry_ttl_expiry() {
    let url = redis_url();

    runtime::block_on(async {
        let stale_after_ms = 300_u64;
        let window_size_ms = 1_000_u64;
        let rl = build_limiter_with_prefix(&url, 1, 1_000, 2_000, unique_prefix()).await;
        let k = key("k");
        let rate = RateLimit::per_second(10f64).unwrap();

        assert_allowed(
            rl.absolute()
                .inc(&k, &rate, window_capacity(1, &rate))
                .await
                .unwrap(),
            "fill rejection setup capacity",
        );
        assert_rejected(
            rl.absolute().inc(&k, &rate, 1).await.unwrap(),
            "create rejection cache",
        );
        assert_eq!(rl.absolute().local_state_count(), 1);

        runtime::async_sleep(Duration::from_millis(window_size_ms + 100)).await;
        rl.absolute().cleanup(stale_after_ms).await.unwrap();
        assert_eq!(
            rl.absolute().local_state_count(),
            1,
            "the stale horizon must begin after the retry TTL expires"
        );

        runtime::async_sleep(Duration::from_millis(stale_after_ms)).await;
        rl.absolute().cleanup(stale_after_ms).await.unwrap();
        assert_eq!(rl.absolute().local_state_count(), 0);
    });
}

#[test]
fn get_estimate_refreshes_undefined_state_then_uses_local_snapshot() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let writer = build_limiter_with_prefix(&url, 6, 1_000, 2_000, prefix.clone()).await;
        let reader = build_limiter_with_prefix(&url, 6, 1_000, 2_000, prefix).await;
        let k = key("k");
        let rate = RateLimit::per_second(100f64).unwrap();

        assert_eq!(
            writer
                .absolute()
                .set_if(&k, &rate, RateLimitComparator::Always, 5)
                .await
                .unwrap(),
            (5, 0)
        );

        assert_eq!(reader.absolute().get_estimate(&k).await.unwrap(), 5);

        assert_eq!(
            writer
                .absolute()
                .set_if(&k, &rate, RateLimitComparator::Always, 9)
                .await
                .unwrap(),
            (9, 5)
        );

        assert_eq!(
            reader.absolute().get_estimate(&k).await.unwrap(),
            5,
            "usable local state must stay on the inference fast path"
        );
        assert_eq!(reader.absolute().get(&k).await.unwrap(), 9);
    });
}

#[test]
fn get_includes_local_pending_increments() {
    let url = redis_url();

    runtime::block_on(async {
        // Very slow sync tick so pending local increments are not flushed in the background.
        let rl = build_limiter_with_prefix(&url, 6, 1000, 2_000, unique_prefix()).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(100f64).unwrap();

        for _ in 0..3 {
            let d = rl.absolute().inc(&k, &rate_limit, 1).await.unwrap();
            assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");
        }

        // Redis holds nothing yet; the 3 increments are local pending.
        let inferred_total = rl.absolute().get_estimate(&k).await.unwrap();
        assert_eq!(inferred_total, 3);

        let total = rl.absolute().get(&k).await.unwrap();
        assert_eq!(total, 3);
    });
}

#[test]
fn get_methods_keep_the_exact_total_during_background_sync() {
    let url = redis_url();

    runtime::block_on(async {
        let sync_interval = 500_u64;
        let rl = build_limiter_with_prefix(&url, 60, 1_000, sync_interval, unique_prefix()).await;
        let k = key("k");
        let rate = RateLimit::per_second(100f64).unwrap();

        assert_allowed(
            rl.absolute().inc(&k, &rate, 3).await.unwrap(),
            "pending usage setup increment",
        );

        // The first tick refreshes local state. Historically it only queued the
        // captured count, leaving a gap until the second tick wrote it to Redis.
        runtime::async_sleep(Duration::from_millis(sync_interval + 100)).await;
        assert_eq!(rl.absolute().get_estimate(&k).await.unwrap(), 3);
        assert_eq!(rl.absolute().get(&k).await.unwrap(), 3);
    });
}

#[test]
fn get_estimate_refreshes_an_expired_rejection_cache() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let limiter = build_limiter_with_prefix(&url, 1, 1_000, 25, prefix.clone()).await;
        let writer = build_limiter_with_prefix(&url, 1, 1_000, 25, prefix).await;
        let k = key("k");
        let rate = RateLimit::per_second(10f64).unwrap();
        let capacity = window_capacity(1, &rate);

        assert_allowed(
            limiter.absolute().inc(&k, &rate, capacity).await.unwrap(),
            "rejection setup increment",
        );
        assert_rejected(
            limiter.absolute().inc(&k, &rate, 1).await.unwrap(),
            "rejection cache setup",
        );

        runtime::async_sleep(Duration::from_millis(1_100)).await;
        assert_eq!(
            writer
                .absolute()
                .set_if(&k, &rate, RateLimitComparator::Always, 4)
                .await
                .unwrap(),
            (4, 0)
        );

        assert_eq!(limiter.absolute().get_estimate(&k).await.unwrap(), 4);
    });
}

#[test]
fn inc_keeps_the_first_rate_limit_for_an_existing_key() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 6, 1_000, 2_000, unique_prefix()).await;
        let low_rate = RateLimit::per_second(1f64).unwrap();
        let high_rate = RateLimit::per_second(10f64).unwrap();

        let low_first = key("low_first");
        assert_allowed(
            rl.absolute().inc(&low_first, &low_rate, 4).await.unwrap(),
            "initial low-rate increment",
        );
        assert_rejected(
            rl.absolute().inc(&low_first, &high_rate, 3).await.unwrap(),
            "later high rate must not expand the stored window limit",
        );
        assert_eq!(rl.absolute().get(&low_first).await.unwrap(), 4);

        let high_first = key("high_first");
        assert_allowed(
            rl.absolute()
                .inc(&high_first, &high_rate, 40)
                .await
                .unwrap(),
            "initial high-rate increment",
        );
        assert_allowed(
            rl.absolute().inc(&high_first, &low_rate, 10).await.unwrap(),
            "later low rate must not shrink the stored window limit",
        );
        assert_eq!(rl.absolute().get(&high_first).await.unwrap(), 50);
    });
}

#[test]
fn set_if_lt_primes_empty_key_and_reprime_is_noop() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 6, 1000, 25, unique_prefix()).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(100f64).unwrap();

        let outcome = rl
            .absolute()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(100), 100)
            .await
            .unwrap();
        let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
        assert_eq!((new_total, old_total), (100, 0));

        // The guard no longer matches: idempotent re-prime.
        let outcome = rl
            .absolute()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(100), 100)
            .await
            .unwrap();
        let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
        assert_eq!((new_total, old_total), (100, 100));

        let total = rl.absolute().get(&k).await.unwrap();
        assert_eq!(total, 100);
    });
}

#[test]
fn set_if_lt_with_lower_target_is_noop() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 6, 1000, 25, unique_prefix()).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(100f64).unwrap();

        assert_eq!(
            rl.absolute()
                .set_if(&k, &rate_limit, RateLimitComparator::Lt(100), 100)
                .await
                .unwrap(),
            (100, 0)
        );

        // Raise-only: a lower target must never lower the total.
        let outcome = rl
            .absolute()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(50), 50)
            .await
            .unwrap();
        let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
        assert_eq!((new_total, old_total), (100, 100));
    });
}

#[test]
fn set_if_always_overwrites_unconditionally_including_lowering() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 6, 1000, 25, unique_prefix()).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(100f64).unwrap();

        assert_eq!(
            rl.absolute()
                .set_if(&k, &rate_limit, RateLimitComparator::Always, 100)
                .await
                .unwrap(),
            (100, 0)
        );

        let outcome = rl
            .absolute()
            .set_if(&k, &rate_limit, RateLimitComparator::Always, 30)
            .await
            .unwrap();
        let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
        assert_eq!((new_total, old_total), (30, 100));

        let total = rl.absolute().get(&k).await.unwrap();
        assert_eq!(total, 30);
    });
}

#[test]
fn set_if_eq_zero_sets_only_when_window_is_empty() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 6, 1000, 25, unique_prefix()).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(100f64).unwrap();

        let outcome = rl
            .absolute()
            .set_if(&k, &rate_limit, RateLimitComparator::Eq(0), 25)
            .await
            .unwrap();
        let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
        assert_eq!((new_total, old_total), (25, 0));

        let outcome = rl
            .absolute()
            .set_if(&k, &rate_limit, RateLimitComparator::Eq(0), 99)
            .await
            .unwrap();
        let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
        assert_eq!((new_total, old_total), (25, 25));
    });
}

#[test]
fn set_if_gt_and_ne_compare_against_the_current_total() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 6, 1_000, 25, unique_prefix()).await;
        let k = key("k");
        let rate = RateLimit::per_second(100f64).unwrap();

        assert_eq!(
            rl.absolute()
                .set_if(&k, &rate, RateLimitComparator::Always, 7)
                .await
                .unwrap(),
            (7, 0)
        );
        assert_eq!(
            rl.absolute()
                .set_if(&k, &rate, RateLimitComparator::Gt(6), 9)
                .await
                .unwrap(),
            (9, 7)
        );
        assert_eq!(
            rl.absolute()
                .set_if(&k, &rate, RateLimitComparator::Ne(9), 12)
                .await
                .unwrap(),
            (9, 9)
        );
        assert_eq!(
            rl.absolute()
                .set_if(&k, &rate, RateLimitComparator::Ne(8), 11)
                .await
                .unwrap(),
            (11, 9)
        );
        assert_eq!(rl.absolute().get(&k).await.unwrap(), 11);
    });
}

#[test]
fn preserve_history_creates_missing_positive_keys_in_both_directions() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 6, 1_000, 25, unique_prefix()).await;
        let rate = RateLimit::per_second(100f64).unwrap();

        for (name, preservation) in [
            ("newest", HistoryPreservation::PreserveNewest),
            ("oldest", HistoryPreservation::PreserveOldest),
        ] {
            let k = key(name);
            assert_eq!(
                rl
                    .absolute()
                    .set_if_preserve_history(
                        &k,
                        &rate,
                        RateLimitComparator::Eq(0),
                        4,
                        preservation,
                    )
                    .await
                    .unwrap(),
                (4, 0)
            );
            assert_eq!(rl.absolute().get(&k).await.unwrap(), 4);
        }
    });
}

#[test]
fn unchanged_preserve_history_target_redefines_the_window_limit() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 6, 1_000, 25, unique_prefix()).await;
        let k = key("k");
        let high_rate = RateLimit::per_second(100f64).unwrap();
        let low_rate = RateLimit::per_second(1f64).unwrap();

        assert_eq!(
            rl.absolute()
                .set_if(&k, &high_rate, RateLimitComparator::Always, 5)
                .await
                .unwrap(),
            (5, 0)
        );
        assert_eq!(
            rl.absolute()
                .set_if_preserve_history(
                    &k,
                    &low_rate,
                    RateLimitComparator::Eq(5),
                    5,
                    HistoryPreservation::PreserveNewest,
                )
                .await
                .unwrap(),
            (5, 5)
        );

        assert_allowed(
            rl.absolute().inc(&k, &high_rate, 1).await.unwrap(),
            "one unit remains under the redefined limit",
        );
        assert_rejected(
            rl.absolute().inc(&k, &high_rate, 1).await.unwrap(),
            "the redefined six-unit window limit must be enforced",
        );
        assert_eq!(rl.absolute().get(&k).await.unwrap(), 6);
    });
}

#[test]
fn conditional_set_zero_handles_missing_and_present_keys() {
    let url = redis_url();

    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 6, 1000, 25, unique_prefix()).await;
        let rate = RateLimit::per_second(100f64).unwrap();

        for (key_name, preservation) in [
            ("replace", None),
            ("preserve", Some(HistoryPreservation::PreserveOldest)),
        ] {
            let k = key(key_name);
            let missing_result = match preservation {
                Some(preservation) => rl
                    .absolute()
                    .set_if_preserve_history(&k, &rate, RateLimitComparator::Eq(0), 0, preservation)
                    .await
                    .unwrap(),
                None => rl
                    .absolute()
                    .set_if(&k, &rate, RateLimitComparator::Eq(0), 0)
                    .await
                    .unwrap(),
            };
            assert_eq!(missing_result, (0, 0));
            assert_eq!(rl.absolute().get(&k).await.unwrap(), 0);

            assert_eq!(
                rl.absolute()
                    .set_if(&k, &rate, RateLimitComparator::Always, 9)
                    .await
                    .unwrap(),
                (9, 0)
            );

            let present_result = match preservation {
                Some(preservation) => rl
                    .absolute()
                    .set_if_preserve_history(
                        &k,
                        &rate,
                        RateLimitComparator::Always,
                        0,
                        preservation,
                    )
                    .await
                    .unwrap(),
                None => rl
                    .absolute()
                    .set_if(&k, &rate, RateLimitComparator::Always, 0)
                    .await
                    .unwrap(),
            };
            assert_eq!(present_result, (0, 9));
            assert_eq!(rl.absolute().get(&k).await.unwrap(), 0);
        }
    });
}

#[test]
fn set_if_prime_then_inc_enforces_remaining_budget() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size = 6_u64;
        let rl = build_limiter_with_prefix(&url, window_size, 1000, 25, unique_prefix()).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(5f64).unwrap();
        let capacity = window_capacity(window_size, &rate_limit);
        assert_eq!(capacity, 30);

        // Prime 27 of 30: exactly 3 units of budget remain.
        let outcome = rl
            .absolute()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(27), 27)
            .await
            .unwrap();
        let new_total = outcome.current_total;
        assert_eq!(new_total, 27);

        for i in 0..3_u64 {
            let d = rl.absolute().inc(&k, &rate_limit, 1).await.unwrap();
            assert!(matches!(d, RateLimitDecision::Allowed), "i: {i}, d: {d:?}");
        }

        let d = rl.absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(d, RateLimitDecision::Rejected { .. }), "d: {d:?}");

        let d = rl.absolute().is_allowed(&k).await.unwrap();
        let RateLimitDecision::Rejected {
            remaining_after_waiting,
            ..
        } = d
        else {
            panic!("expected rejected admission check, got {d:?}");
        };
        assert_eq!(
            remaining_after_waiting, 27,
            "the set_if bucket is older than the three pending increments: {d:?}"
        );
    });
}

#[test]
fn set_if_prime_at_capacity_rejects_next_inc() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size = 6_u64;
        let rl = build_limiter_with_prefix(&url, window_size, 1000, 25, unique_prefix()).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(5f64).unwrap();
        let capacity = window_capacity(window_size, &rate_limit);

        let outcome = rl
            .absolute()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(capacity), capacity)
            .await
            .unwrap();
        let new_total = outcome.current_total;
        assert_eq!(new_total, capacity);

        let d = rl.absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(d, RateLimitDecision::Rejected { .. }), "d: {d:?}");

        let d = rl.absolute().is_allowed(&k).await.unwrap();
        let RateLimitDecision::Rejected {
            remaining_after_waiting,
            ..
        } = d
        else {
            panic!("expected rejected admission check, got {d:?}");
        };
        assert_eq!(
            remaining_after_waiting, capacity,
            "set_if created one bucket containing the full capacity: {d:?}"
        );
    });
}

#[test]
fn set_if_folds_pending_local_increments_before_comparing() {
    let url = redis_url();

    runtime::block_on(async {
        // Very slow sync tick: the 5 increments stay local until set_if folds them.
        let rl = build_limiter_with_prefix(&url, 6, 1000, 2_000, unique_prefix()).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(100f64).unwrap();

        for _ in 0..5 {
            let d = rl.absolute().inc(&k, &rate_limit, 1).await.unwrap();
            assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");
        }

        // The pending 5 are included in the Redis comparison overlay, so Lt(3)
        // sees 5 and the write is skipped — increments are never lost.
        let outcome = rl
            .absolute()
            .set_if(&k, &rate_limit, RateLimitComparator::Lt(3), 3)
            .await
            .unwrap();
        let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
        assert_eq!((new_total, old_total), (5, 5));

        let total = rl.absolute().get(&k).await.unwrap();
        assert_eq!(total, 5);
    });
}

#[test]
fn set_if_after_rejection_transition_has_no_stale_commit_to_land_later() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size = 6_u64;
        let sync_interval = 2_000_u64;
        let rl =
            build_limiter_with_prefix(&url, window_size, 1_000, sync_interval, unique_prefix())
                .await;
        let k = key("k");
        let rate = RateLimit::per_second(1f64).unwrap();
        let capacity = window_capacity(window_size, &rate);

        assert!(matches!(
            rl.absolute().inc(&k, &rate, capacity).await.unwrap(),
            RateLimitDecision::Allowed
        ));
        assert!(matches!(
            rl.absolute().inc(&k, &rate, 1).await.unwrap(),
            RateLimitDecision::Rejected { .. }
        ));

        assert_eq!(
            rl.absolute()
                .set_if(&k, &rate, RateLimitComparator::Eq(capacity), 2)
                .await
                .unwrap(),
            (2, capacity)
        );
        assert_eq!(rl.absolute().get(&k).await.unwrap(), 2);

        // A previously queued capacity commit must not arrive after the reset and
        // raise the total again.
        runtime::async_sleep(Duration::from_millis(sync_interval + 100)).await;
        assert_eq!(rl.absolute().get(&k).await.unwrap(), 2);
    });
}

#[test]
fn set_if_invalidates_local_state_so_next_inc_observes_written_total() {
    let url = redis_url();

    runtime::block_on(async {
        let window_size = 6_u64;
        let rl = build_limiter_with_prefix(&url, window_size, 1000, 2_000, unique_prefix()).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(4f64).unwrap();
        let capacity = window_capacity(window_size, &rate_limit);
        assert_eq!(capacity, 24);

        for _ in 0..2 {
            let d = rl.absolute().inc(&k, &rate_limit, 1).await.unwrap();
            assert!(matches!(d, RateLimitDecision::Allowed), "d: {d:?}");
        }

        // Unconditional overwrite to 20 (the folded pending 2 are part of old_total).
        let outcome = rl
            .absolute()
            .set_if(&k, &rate_limit, RateLimitComparator::Always, 20)
            .await
            .unwrap();
        let (new_total, old_total) = (outcome.current_total, outcome.previous_total);
        assert_eq!((new_total, old_total), (20, 2));

        // No stale local fast-path: budget continues from the written total (4 left of 24).
        for i in 0..4_u64 {
            let d = rl.absolute().inc(&k, &rate_limit, 1).await.unwrap();
            assert!(matches!(d, RateLimitDecision::Allowed), "i: {i}, d: {d:?}");
        }

        let d = rl.absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(matches!(d, RateLimitDecision::Rejected { .. }), "d: {d:?}");
    });
}

#[test]
fn set_if_window_limit_is_sticky_for_other_instances() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size = 6_u64;

        let rl_a = build_limiter_with_prefix(&url, window_size, 1000, 25, prefix.clone()).await;
        let rl_b = build_limiter_with_prefix(&url, window_size, 1000, 25, prefix.clone()).await;

        let k = key("k");

        // Instance A defines the window limit via a positive set: 6s * 2/s = 12.
        let rate_a = RateLimit::per_second(2f64).unwrap();
        assert_eq!(
            rl_a.absolute()
                .set_if(&k, &rate_a, RateLimitComparator::Always, 1)
                .await
                .unwrap(),
            (1, 0)
        );

        // Instance B incs with a much higher rate, but the stored window limit wins.
        let rate_b = RateLimit::per_second(100f64).unwrap();

        for i in 0..11_u64 {
            let d = rl_b.absolute().inc(&k, &rate_b, 1).await.unwrap();
            assert!(matches!(d, RateLimitDecision::Allowed), "i: {i}, d: {d:?}");
        }

        let d = rl_b.absolute().inc(&k, &rate_b, 1).await.unwrap();
        assert!(matches!(d, RateLimitDecision::Rejected { .. }), "d: {d:?}");
    });
}

#[test]
fn set_if_preserve_history_includes_pending_and_guard_miss_keeps_local_state() {
    let url = redis_url();
    runtime::block_on(async {
        let rl = build_limiter_with_prefix(&url, 60, 1000, 2_000, unique_prefix()).await;
        let k = key("k");
        let rate = RateLimit::per_second(100f64).unwrap();

        for _ in 0..4 {
            assert!(matches!(
                rl.absolute().inc(&k, &rate, 1).await.unwrap(),
                RateLimitDecision::Allowed
            ));
        }

        assert_eq!(
            rl.absolute()
                .set_if_preserve_history(
                    &k,
                    &rate,
                    RateLimitComparator::Eq(99),
                    10,
                    HistoryPreservation::PreserveNewest,
                )
                .await
                .unwrap(),
            (4, 4)
        );
        assert_eq!(rl.absolute().get(&k).await.unwrap(), 4);

        assert_eq!(
            rl.absolute()
                .set_if_preserve_history(
                    &k,
                    &rate,
                    RateLimitComparator::Always,
                    10,
                    HistoryPreservation::PreserveNewest,
                )
                .await
                .unwrap(),
            (10, 4)
        );
        assert_eq!(rl.absolute().get(&k).await.unwrap(), 10);
    });
}

#[test]
fn concurrent_increments_remain_exact_across_background_refreshes() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let sync_interval = 25_u64;
        let rl = build_limiter_with_prefix(&url, 60, 1_000, sync_interval, prefix.clone()).await;
        let k = key("k");
        let rate = RateLimit::per_second(1_000f64).unwrap();
        let worker_count = 8_u64;
        let increments_per_worker = 1_000_u64;

        let mut tasks = Vec::new();
        for _ in 0..worker_count {
            let rl = rl.clone();
            let k = k.clone();
            let rate = rate.clone();
            tasks.push(runtime::spawn(async move {
                for index in 0..increments_per_worker {
                    let decision = rl.absolute().inc(&k, &rate, 1).await.unwrap();
                    assert_allowed(decision, "concurrent increment");

                    if index % 25 == 0 {
                        runtime::async_sleep(Duration::from_millis(1)).await;
                    }
                }
            }));
        }
        for task in tasks {
            runtime::join(task).await;
        }

        wait_for_hybrid_sync(sync_interval).await;
        let observer = build_limiter_with_prefix(&url, 60, 1_000, sync_interval, prefix).await;
        assert_eq!(
            observer.absolute().get(&k).await.unwrap(),
            worker_count * increments_per_worker
        );
    });
}

#[test]
fn volume_non_integer_rate_uses_truncating_capacity() {
    let url = redis_url();

    runtime::block_on(async {
        let prefix = unique_prefix();
        let window_size = 1_u64;
        let bucket_size = 1000_u64;
        let sync_interval = 25_u64;

        let rl =
            build_limiter_with_prefix(&url, window_size, bucket_size, sync_interval, prefix).await;

        let k = key("k");
        let rate_limit = RateLimit::per_second(2.9f64).unwrap();
        let capacity = window_capacity(window_size, &rate_limit);
        assert_eq!(capacity, 2);

        for _ in 0..capacity {
            let decision = rl.absolute().inc(&k, &rate_limit, 1).await.unwrap();
            assert!(
                matches!(decision, RateLimitDecision::Allowed),
                "decision: {decision:?}"
            );
        }

        let decision = rl.absolute().is_allowed(&k).await.unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Rejected { .. }),
            "the truncated capacity must be full: {decision:?}"
        );

        let decision = rl.absolute().inc(&k, &rate_limit, 1).await.unwrap();
        assert!(
            matches!(decision, RateLimitDecision::Rejected { .. }),
            "decision: {decision:?}"
        );
    });
}
