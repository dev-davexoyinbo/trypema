//! Scale tests for the Redis Lua scripts.
//!
//! Redis Lua cannot pass more than approximately 8,000 values to one command. These tests use
//! the public API to make state that is larger than that limit. Then they make sure that each
//! script continues to operate and leaves the correct raw Redis state.

use std::{future::Future, sync::Arc, time::Duration};

use redis::AsyncCommands;

use super::common::{connection_manager, key, key_gen, unique_prefix, wait_for_hybrid_sync};
use super::runtime;

use crate::common::RateType;
use crate::redis::common::RedisKeyGenerator;
use crate::{
    BucketSize, HistoryPreservation, RateLimit, RateLimitComparator, RateLimitDecision,
    RateLimiterBuilder, TrypemaError, WindowSize,
    hybrid::{HybridRateLimiterProvider, SyncInterval},
    redis::{RedisKey, RedisRateLimiterProvider},
};

/// More entities than one call of the cleanup script removes. Their keys are also more than
/// Redis Lua can pass to one `DEL`.
const MANY_ENTITIES: usize = 2_000;

/// More buckets than Redis Lua can pass to one command.
const MANY_BUCKETS: usize = 8_200;

const SYNC_INTERVAL_MS: u64 = 5;

/// A limit that no test reaches.
fn high_rate() -> RateLimit {
    RateLimit::per_second_or_panic(1_000_000.0)
}

fn assert_allowed(decision: RateLimitDecision, context: &str) {
    assert!(
        matches!(decision, RateLimitDecision::Allowed),
        "{context}: {decision:?}"
    );
}

/// A Redis provider and a hybrid provider that share one prefix.
struct Providers {
    redis: Arc<RedisRateLimiterProvider>,
    hybrid: Arc<HybridRateLimiterProvider>,
    prefix: RedisKey,
}

impl Providers {
    async fn build() -> Self {
        let prefix = unique_prefix();
        let redis = RedisRateLimiterProvider::builder(connection_manager().await)
            .prefix(prefix.clone())
            .window_size(WindowSize::seconds_or_panic(60))
            .cleanup_enabled(false)
            .build()
            .unwrap();
        let hybrid = HybridRateLimiterProvider::builder(connection_manager().await)
            .prefix(prefix.clone())
            .window_size(WindowSize::seconds_or_panic(60))
            .sync_interval(SyncInterval::milliseconds_or_panic(SYNC_INTERVAL_MS))
            .cleanup_enabled(false)
            .build()
            .unwrap();

        Self {
            redis,
            hybrid,
            prefix,
        }
    }

    async fn inc(&self, rate_type: RateType, key: &RedisKey) {
        let rate = high_rate();
        let decision = match rate_type {
            RateType::Absolute => self.redis.absolute().inc(key, &rate, 1).await,
            RateType::Suppressed => self.redis.suppressed().inc(key, &rate, 1).await,
            RateType::HybridAbsolute => self.hybrid.absolute().inc(key, &rate, 1).await,
            RateType::HybridSuppressed => self.hybrid.suppressed().inc(key, &rate, 1).await,
        };

        assert_allowed(decision.unwrap(), "seeding an entity");
    }

    async fn cleanup(&self, rate_type: RateType, stale_after_ms: u64) -> Result<(), TrypemaError> {
        match rate_type {
            RateType::Absolute => self.redis.absolute().cleanup(stale_after_ms).await,
            RateType::Suppressed => self.redis.suppressed().cleanup(stale_after_ms).await,
            RateType::HybridAbsolute => self.hybrid.absolute().cleanup(stale_after_ms).await,
            RateType::HybridSuppressed => self.hybrid.suppressed().cleanup(stale_after_ms).await,
        }
    }

    fn key_generator(&self, rate_type: RateType) -> RedisKeyGenerator {
        key_gen(&self.prefix, rate_type)
    }
}

fn numbered_keys(name: &str, count: usize) -> Vec<RedisKey> {
    (0..count)
        .map(|index| key(&format!("{name}_{index}")))
        .collect()
}

/// Counts the per-entity Redis keys that exist for `keys`.
async fn count_entity_keys(
    conn: &mut redis::aio::ConnectionManager,
    key_generator: &RedisKeyGenerator,
    keys: &[RedisKey],
) -> usize {
    let mut found = 0;

    for batch in keys.chunks(100) {
        let names: Vec<String> = batch
            .iter()
            .flat_map(|key| key_generator.get_all_entity_keys(key))
            .collect();
        let existing: usize = conn.exists(names).await.unwrap();
        found += existing;
    }

    found
}

// ---------------------------------------------------------------------------
// Stale entities
// ---------------------------------------------------------------------------

/// One cleanup pass removes every stale entity and keeps the active one.
async fn cleanup_removes_many_stale_entities(rate_type: RateType) {
    let providers = Providers::build().await;
    let key_generator = providers.key_generator(rate_type);
    let active_entities_key = key_generator.get_active_entities_key();
    let mut conn = connection_manager().await;
    let stale = numbered_keys("stale", MANY_ENTITIES);
    let active = key("active");

    for stale_key in &stale {
        providers.inc(rate_type, stale_key).await;
    }
    wait_for_hybrid_sync(SYNC_INTERVAL_MS).await;
    runtime::async_sleep(Duration::from_millis(1_500)).await;
    providers.inc(rate_type, &active).await;
    wait_for_hybrid_sync(SYNC_INTERVAL_MS).await;

    let members_before: usize = conn.zcard(&active_entities_key).await.unwrap();
    assert_eq!(
        members_before,
        MANY_ENTITIES + 1,
        "the setup must register every entity"
    );

    providers.cleanup(rate_type, 1_000).await.unwrap();

    assert_eq!(
        count_entity_keys(&mut conn, &key_generator, &stale).await,
        0,
        "cleanup retained keys of stale entities"
    );
    let members_after: Vec<String> = conn.zrange(&active_entities_key, 0, -1).await.unwrap();
    assert_eq!(members_after, vec![active.to_string()]);
    assert!(
        count_entity_keys(&mut conn, &key_generator, std::slice::from_ref(&active)).await > 0,
        "cleanup removed the keys of the active entity"
    );
}

#[test]
fn redis_absolute_cleanup_removes_many_stale_entities() {
    runtime::block_on(cleanup_removes_many_stale_entities(RateType::Absolute));
}

#[test]
fn redis_suppressed_cleanup_removes_many_stale_entities() {
    runtime::block_on(cleanup_removes_many_stale_entities(RateType::Suppressed));
}

#[test]
fn hybrid_absolute_cleanup_removes_many_stale_entities() {
    runtime::block_on(cleanup_removes_many_stale_entities(
        RateType::HybridAbsolute,
    ));
}

#[test]
fn hybrid_suppressed_cleanup_removes_many_stale_entities() {
    runtime::block_on(cleanup_removes_many_stale_entities(
        RateType::HybridSuppressed,
    ));
}

/// `clear` removes every entity of the strategy, also when there are many.
async fn redis_clear_removes_many_entities(
    rate_type: RateType,
    clear: impl AsyncFnOnce(&RedisRateLimiterProvider) -> Result<(), TrypemaError>,
) {
    let providers = Providers::build().await;
    let key_generator = providers.key_generator(rate_type);
    let active_entities_key = key_generator.get_active_entities_key();
    let mut conn = connection_manager().await;
    let keys = numbered_keys("entity", MANY_ENTITIES);

    for entity_key in &keys {
        providers.inc(rate_type, entity_key).await;
    }

    let members_before: usize = conn.zcard(&active_entities_key).await.unwrap();
    assert_eq!(members_before, MANY_ENTITIES);

    clear(&providers.redis).await.unwrap();

    assert_eq!(count_entity_keys(&mut conn, &key_generator, &keys).await, 0);
    let members_after: usize = conn.zcard(&active_entities_key).await.unwrap();
    assert_eq!(members_after, 0);
}

#[test]
fn redis_absolute_clear_removes_many_entities() {
    runtime::block_on(redis_clear_removes_many_entities(
        RateType::Absolute,
        async |redis| redis.absolute().clear().await,
    ));
}

#[test]
fn redis_suppressed_clear_removes_many_entities() {
    runtime::block_on(redis_clear_removes_many_entities(
        RateType::Suppressed,
        async |redis| redis.suppressed().clear().await,
    ));
}

// ---------------------------------------------------------------------------
// A failed cleanup step
// ---------------------------------------------------------------------------

/// The cleanup of one strategy fails. The cleanup of the other strategy must run.
async fn cleanup_of_suppressed_runs_when_absolute_fails(
    absolute_type: RateType,
    suppressed_type: RateType,
) {
    let providers = Providers::build().await;
    let suppressed_generator = providers.key_generator(suppressed_type);
    let mut conn = connection_manager().await;
    let stale = key("stale");

    providers.inc(suppressed_type, &stale).await;
    wait_for_hybrid_sync(SYNC_INTERVAL_MS).await;
    runtime::async_sleep(Duration::from_millis(300)).await;

    // A string where the script expects a sorted set makes the absolute cleanup fail.
    let broken_key = providers
        .key_generator(absolute_type)
        .get_active_entities_key();
    let _: () = conn.set(&broken_key, "not a sorted set").await.unwrap();

    let result = match absolute_type {
        RateType::Absolute => providers.redis.cleanup(100).await,
        _ => providers.hybrid.cleanup(100).await,
    };

    assert!(
        result.is_err(),
        "the absolute cleanup must report its error"
    );
    assert_eq!(
        count_entity_keys(
            &mut conn,
            &suppressed_generator,
            std::slice::from_ref(&stale)
        )
        .await,
        0,
        "the suppressed cleanup did not run"
    );
    let _: () = conn.del(&broken_key).await.unwrap();
}

#[test]
fn redis_suppressed_cleanup_runs_when_absolute_cleanup_fails() {
    runtime::block_on(cleanup_of_suppressed_runs_when_absolute_fails(
        RateType::Absolute,
        RateType::Suppressed,
    ));
}

#[test]
fn hybrid_suppressed_cleanup_runs_when_absolute_cleanup_fails() {
    runtime::block_on(cleanup_of_suppressed_runs_when_absolute_fails(
        RateType::HybridAbsolute,
        RateType::HybridSuppressed,
    ));
}

// ---------------------------------------------------------------------------
// Expired buckets of one key
// ---------------------------------------------------------------------------

/// Calls `inc` until `ordering_key` holds `MANY_BUCKETS` buckets, and returns the number of
/// calls. A new bucket starts when the newest bucket is 2 ms old. The 1 ms pause after each
/// call keeps the load on Redis low. This takes approximately 25 s.
async fn fill_buckets<F, Fut>(ordering_key: String, mut inc: F) -> u64
where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()>,
{
    let mut conn = connection_manager().await;
    let mut calls = 0;

    loop {
        for _ in 0..200 {
            inc().await;
            calls += 1;
            runtime::async_sleep(Duration::from_millis(1)).await;
        }

        let buckets: usize = conn.zcard(&ordering_key).await.unwrap();

        if buckets >= MANY_BUCKETS {
            return calls;
        }
    }
}

/// Returns `(history buckets, ordered buckets, total)` for one key.
async fn bucket_state(
    conn: &mut redis::aio::ConnectionManager,
    key_generator: &RedisKeyGenerator,
    key: &RedisKey,
) -> (usize, usize, u64) {
    let history: usize = conn.hlen(key_generator.get_hash_key(key)).await.unwrap();
    let ordered: usize = conn
        .zcard(key_generator.get_active_keys(key))
        .await
        .unwrap();
    let total: Option<u64> = conn
        .get(key_generator.get_total_count_key(key))
        .await
        .unwrap();

    (history, ordered, total.unwrap_or(0))
}

/// Each absolute script operates on a key that has more expired buckets than Lua can unpack.
#[test]
fn redis_absolute_scripts_handle_many_expired_buckets() {
    runtime::block_on(async {
        let prefix = unique_prefix();
        let rl = RedisRateLimiterProvider::builder(connection_manager().await)
            .prefix(prefix.clone())
            .window_size(WindowSize::seconds_or_panic(1))
            .bucket_size(BucketSize::milliseconds_or_panic(1))
            .cleanup_enabled(false)
            .build()
            .unwrap();
        let key_generator = key_gen(&prefix, RateType::Absolute);
        let mut conn = connection_manager().await;
        let rate = high_rate();
        let [get_key, inc_key, set_key, preserve_key, delete_key] =
            ["get", "inc", "set_if", "preserve", "delete"].map(key);

        // The absolute `inc` removes expired buckets only at capacity. Below capacity, they
        // collect past the window.
        let calls = futures::future::join_all(
            [&get_key, &inc_key, &set_key, &preserve_key, &delete_key].map(|bucket_key| {
                fill_buckets(key_generator.get_active_keys(bucket_key), || async {
                    assert_allowed(
                        rl.absolute().inc(bucket_key, &rate, 1).await.unwrap(),
                        "filling buckets",
                    );
                })
            }),
        )
        .await;
        runtime::async_sleep(Duration::from_millis(1_200)).await;

        let (history, ordered, total) = bucket_state(&mut conn, &key_generator, &get_key).await;
        assert!(history >= MANY_BUCKETS && ordered >= MANY_BUCKETS);
        assert_eq!(total, calls[0]);

        assert_eq!(rl.absolute().get(&get_key).await.unwrap(), 0);
        assert_eq!(
            bucket_state(&mut conn, &key_generator, &get_key).await,
            (0, 0, 0)
        );

        // This count is at capacity, so the script must remove the expired buckets first.
        assert_allowed(
            rl.absolute().inc(&inc_key, &rate, 1_000_000).await.unwrap(),
            "an increment after the window",
        );
        assert_eq!(
            bucket_state(&mut conn, &key_generator, &inc_key).await,
            (1, 1, 1_000_000)
        );

        let outcome = rl
            .absolute()
            .set_if(&set_key, &rate, RateLimitComparator::Always, 7)
            .await
            .unwrap();
        assert_eq!(outcome, (7, 0));
        assert_eq!(
            bucket_state(&mut conn, &key_generator, &set_key).await,
            (1, 1, 7)
        );

        let outcome = rl
            .absolute()
            .set_if_preserve_history(
                &preserve_key,
                &rate,
                RateLimitComparator::Always,
                7,
                HistoryPreservation::PreserveNewest,
            )
            .await
            .unwrap();
        assert_eq!(outcome, (7, 0));
        assert_eq!(
            bucket_state(&mut conn, &key_generator, &preserve_key).await,
            (1, 1, 7)
        );

        assert_eq!(rl.absolute().delete(&delete_key).await.unwrap(), Some(0));
        assert_eq!(
            count_entity_keys(&mut conn, &key_generator, std::slice::from_ref(&delete_key)).await,
            0
        );
    });
}

/// Each suppressed script operates on a key that has more expired buckets than Lua can unpack.
#[test]
fn redis_suppressed_scripts_handle_many_expired_buckets() {
    runtime::block_on(async {
        // The suppressed `inc` removes expired buckets on each call. Thus, a provider with a
        // long window fills the buckets, and they are expired for a provider with a short
        // window on the same prefix.
        let prefix = unique_prefix();
        let build = async |window: WindowSize| {
            RedisRateLimiterProvider::builder(connection_manager().await)
                .prefix(prefix.clone())
                .window_size(window)
                .bucket_size(BucketSize::milliseconds_or_panic(1))
                .cleanup_enabled(false)
                .build()
                .unwrap()
        };
        let filler = build(WindowSize::hours_or_panic(1)).await;
        let rl = build(WindowSize::seconds_or_panic(1)).await;
        let key_generator = key_gen(&prefix, RateType::Suppressed);
        let mut conn = connection_manager().await;
        let rate = high_rate();
        let [
            get_key,
            factor_key,
            inc_key,
            set_key,
            preserve_key,
            delete_key,
        ] = ["get", "factor", "inc", "set_if", "preserve", "delete"].map(key);

        let calls = futures::future::join_all(
            [
                &get_key,
                &factor_key,
                &inc_key,
                &set_key,
                &preserve_key,
                &delete_key,
            ]
            .map(|bucket_key| {
                fill_buckets(key_generator.get_active_keys(bucket_key), || async {
                    assert_allowed(
                        filler.suppressed().inc(bucket_key, &rate, 1).await.unwrap(),
                        "filling buckets",
                    );
                })
            }),
        )
        .await;
        runtime::async_sleep(Duration::from_millis(1_200)).await;

        let (history, ordered, total) = bucket_state(&mut conn, &key_generator, &get_key).await;
        assert!(history >= MANY_BUCKETS && ordered >= MANY_BUCKETS);
        assert_eq!(total, calls[0]);

        let snapshot = rl.suppressed().get(&get_key).await.unwrap();
        assert_eq!((snapshot.total, snapshot.total_declined), (0, 0));
        assert_eq!(snapshot.suppression_factor, 0.0);
        assert_eq!(
            bucket_state(&mut conn, &key_generator, &get_key).await,
            (0, 0, 0)
        );

        assert_eq!(
            rl.suppressed()
                .get_suppression_factor(&factor_key)
                .await
                .unwrap(),
            0.0
        );
        assert_eq!(
            bucket_state(&mut conn, &key_generator, &factor_key).await,
            (0, 0, 0)
        );

        assert_allowed(
            rl.suppressed().inc(&inc_key, &rate, 3).await.unwrap(),
            "an increment after the window",
        );
        assert_eq!(
            bucket_state(&mut conn, &key_generator, &inc_key).await,
            (1, 1, 3)
        );

        let outcome = rl
            .suppressed()
            .set_if(&set_key, &rate, RateLimitComparator::Always, 7)
            .await
            .unwrap();
        assert_eq!(outcome, (7, 0));
        assert_eq!(
            bucket_state(&mut conn, &key_generator, &set_key).await,
            (1, 1, 7)
        );

        let outcome = rl
            .suppressed()
            .set_if_preserve_history(
                &preserve_key,
                &rate,
                RateLimitComparator::Always,
                7,
                HistoryPreservation::PreserveNewest,
            )
            .await
            .unwrap();
        assert_eq!(outcome, (7, 0));
        assert_eq!(
            bucket_state(&mut conn, &key_generator, &preserve_key).await,
            (1, 1, 7)
        );

        assert_eq!(rl.suppressed().delete(&delete_key).await.unwrap(), Some(0));
        assert_eq!(
            count_entity_keys(&mut conn, &key_generator, std::slice::from_ref(&delete_key)).await,
            0
        );
    });
}
