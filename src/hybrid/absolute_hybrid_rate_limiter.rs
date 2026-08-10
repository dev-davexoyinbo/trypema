use std::{
    ops::Deref,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use dashmap::{DashMap, mapref::entry::Entry, mapref::one::Ref};
use tokio::sync::{RwLock, mpsc, watch};

use crate::{
    ConditionalSetOutcome, HistoryPreservation, RateLimit, RateLimitComparator, RateLimitDecision,
    TrypemaError, WindowSize,
    common::{HistoryUpdateMode, RandomState, duration_from_milliseconds},
    hybrid::{
        AbsoluteHybridCommitterSignal, RedisCommitter, RedisCommitterOptions, RedisProxyCommitter,
        absolute_hybrid_redis_proxy::{
            AbsoluteHybridCommit, AbsoluteHybridRedisProxy, AbsoluteHybridRedisProxyOptions,
            AbsoluteHybridRedisProxyReadStateResult,
        },
        common::{EPOCH_CHANGE_INTERVAL, RedisRateLimiterSignal, StateRevision},
        hybrid_rate_limiter_provider::HybridRateLimiterConfig,
    },
    redis::{RedisKey, mutex_lock, spawn_task},
    runtime,
};

mod helpers;

#[derive(Debug)]
enum AbsoluteRedisLimitingState {
    Accepting {
        window_limit: Mutex<f64>,
        accept_limit: Mutex<u64>,
        starting_count: Mutex<u64>,
        count: AtomicU64,
        time_instant: Mutex<Instant>,
        oldest_bucket_ttl: Mutex<Option<u64>>,
        oldest_bucket_count: Mutex<Option<u64>>,
        last_modified: Mutex<Instant>,
        state_revision: Mutex<StateRevision>,
    },
    Undefined,
    Rejecting {
        time_instant: Mutex<Instant>,
        ttl_ms: Mutex<u64>,
        count_after_release: Mutex<u64>,
        committed_count: u64,
        committed_at: Instant,
        state_revision: Mutex<StateRevision>,
    },
}

/// Sliding-window allow/reject limiter with a local fast path and periodic Redis sync.
///
/// This is the hybrid counterpart to [`AbsoluteRedisRateLimiter`](crate::redis::AbsoluteRedisRateLimiter).
/// Instead of executing a Redis round-trip on every `inc()` call, it maintains local
/// in-memory state per key and periodically flushes accumulated increments to Redis via
/// a background actor (the `RedisCommitter`).
///
/// # State Machine
///
/// Each key transitions through three states:
///
/// - **`Undefined`** — No local state exists. The first `inc()` call reads Redis to
///   initialise local state, then transitions to `Accepting` or `Rejecting`.
///
/// - **`Accepting`** — Serving from a local counter. Each `inc()` is a fast atomic
///   increment with no Redis I/O. When the local counter exceeds the available capacity,
///   it commits the count to Redis and transitions based on the result.
///
/// - **`Rejecting`** — A rejection cache based on the oldest bucket's TTL. All calls
///   return `Rejected` until the TTL expires, at which point the state resets to
///   `Undefined` for a fresh Redis read.
///
/// # Thundering Herd Prevention
///
/// Each key has its own `tokio::sync::Mutex`. When local state is exhausted and a
/// Redis read is needed, only one task performs the read+state-update; other tasks
/// wait on the mutex and then re-check the refreshed state.
///
/// # When to Use
///
/// Use this over the pure Redis provider when per-request Redis latency is unacceptable.
/// The trade-off is that admission decisions may lag behind the true Redis state by up
/// to `sync_interval`.
#[derive(Debug)]
pub struct AbsoluteHybridRateLimiter {
    window_size: WindowSize,
    commiter_sender: mpsc::Sender<AbsoluteHybridCommitterSignal<AbsoluteHybridCommit>>,
    redis_proxy: AbsoluteHybridRedisProxy,
    limiting_state: DashMap<RedisKey, AbsoluteRedisLimitingState, RandomState>,
    /// Per-key async mutex that serializes the "Redis read + state update" operation.
    /// Only one tokio task at a time may perform the read+update for a given key;
    /// all other tasks wait on this lock and then re-check the (now-updated) state.
    reset_locks: DashMap<RedisKey, Arc<tokio::sync::Mutex<()>>, RandomState>,
    epoch: AtomicU64,
    last_commited_epoch: AtomicU64,
    is_active_watch: watch::Sender<u64>,
    maintenance_lock: Arc<RwLock<()>>,
}

impl AbsoluteHybridRateLimiter {
    pub(crate) fn new(options: HybridRateLimiterConfig) -> Arc<Self> {
        let prefix = options.prefix.unwrap_or_else(RedisKey::default_prefix);

        let (tx, rx) = mpsc::channel::<RedisRateLimiterSignal>(1);

        let redis_proxy = AbsoluteHybridRedisProxy::new(AbsoluteHybridRedisProxyOptions {
            prefix: prefix.clone(),
            connection_manager: options.connection_manager,
            window_size: options.provider.window_size,
            bucket_size: options.provider.bucket_size,
        });

        let is_active_watch = watch::Sender::new(0u64);
        let maintenance_lock = Arc::new(RwLock::new(()));

        let commiter_sender = RedisCommitter::run(RedisCommitterOptions {
            sync_interval: Duration::from_millis(options.sync_interval.as_milliseconds()),
            channel_capacity: 8192,
            max_batch_size: 4,
            limiter_sender: tx,
            redis_proxy: Box::new(redis_proxy.clone()),
            is_active_watch: is_active_watch.subscribe(),
            maintenance_lock: Arc::clone(&maintenance_lock),
        });

        let limiter = Self {
            window_size: options.provider.window_size,
            commiter_sender,
            redis_proxy,
            limiting_state: DashMap::default(),
            reset_locks: DashMap::default(),
            epoch: AtomicU64::new(0),
            last_commited_epoch: AtomicU64::new(0),
            is_active_watch,
            maintenance_lock,
        };

        let limiter = Arc::new(limiter);

        limiter.listen_for_committer_signals(rx);
        limiter.epoch_change_task();

        limiter
    } // end method with_rate_type

    fn epoch_change_task(self: &Arc<Self>) {
        self.epoch.fetch_add(1, Ordering::AcqRel);
        let limiter = Arc::downgrade(self);

        spawn_task(async move {
            loop {
                runtime::sleep(EPOCH_CHANGE_INTERVAL).await;
                let Some(limiter) = limiter.upgrade() else {
                    break;
                };

                limiter.epoch.fetch_add(1, Ordering::AcqRel);
            }
        });
    }

    fn listen_for_committer_signals(
        self: &Arc<Self>,
        mut rx: mpsc::Receiver<RedisRateLimiterSignal>,
    ) {
        let limitter = Arc::downgrade(self);

        spawn_task(async move {
            while let Some(signal) = rx.recv().await {
                let Some(limiter) = limitter.upgrade() else {
                    break;
                };

                match signal {
                    RedisRateLimiterSignal::Flush => {
                        if let Err(err) = limiter.flush().await {
                            tracing::error!(error = ?err, "Failed to flush redis rate limiter");
                        }
                    }
                }
            }
        });
    } // end method listen_for_committer_signals

    // #[inline(always)]
    fn send_epoch_change_if_needed(&self) {
        let epoch = self.epoch.load(Ordering::Acquire);

        if self.last_commited_epoch.load(Ordering::Acquire) < epoch {
            let _ = self.is_active_watch.send(epoch);
            self.last_commited_epoch.store(epoch, Ordering::Release);
        }
    }

    /// Acquire (or create) the per-key reset lock and return an `Arc` to it.
    fn get_or_create_reset_lock(&self, key: &RedisKey) -> Arc<tokio::sync::Mutex<()>> {
        if let Some(lock) = self.reset_locks.get(key) {
            return Arc::clone(&lock);
        }

        self.reset_locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .downgrade()
            .clone()
    }

    fn get_or_create_limiting_state(
        &self,
        key: &RedisKey,
    ) -> Ref<'_, RedisKey, AbsoluteRedisLimitingState> {
        match self.limiting_state.get(key) {
            Some(state) => state,
            None => self
                .limiting_state
                .entry(key.clone())
                .or_insert_with(|| AbsoluteRedisLimitingState::Undefined)
                .downgrade(),
        }
    }

    /// Check admission and, if allowed, record the increment for `key`.
    ///
    /// This is the primary method for rate limiting with the hybrid absolute strategy.
    /// On the fast-path (key in `Accepting` state with capacity remaining), this is a
    /// single atomic increment with no Redis I/O. When local capacity is exhausted,
    /// it flushes to Redis and refreshes local state.
    ///
    /// # Arguments
    ///
    /// - `key`: Validated [`RedisKey`] identifying the rate-limited resource
    /// - `rate_limit`: Per-second rate limit (sticky — stored on first call per key)
    /// - `count`: Amount to increment (typically `1`)
    ///
    /// # Returns
    ///
    /// - `Ok(Allowed)` — under limit, increment recorded locally
    /// - `Ok(Rejected { .. })` — over limit (may be served from local rejection cache)
    /// - `Err(TrypemaError)` — Redis error during state refresh
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use trypema::{RateLimiterBuilder, hybrid::HybridRateLimiterProvider};
    /// # async fn example(connection_manager: trypema::redis::ConnectionManager) {
    /// let rl = HybridRateLimiterProvider::builder(connection_manager).build().unwrap();
    /// use trypema::{RateLimit, RateLimitDecision};
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from("user_123").unwrap();
    /// let rate = RateLimit::per_second(10.0).unwrap();
    /// assert!(matches!(
    ///     rl.absolute().inc(&key, &rate, 1).await.unwrap(),
    ///     RateLimitDecision::Allowed
    /// ));
    /// # }
    /// ```
    pub async fn inc(
        &self,
        key: &RedisKey,
        rate_limit: &RateLimit,
        count: u64,
    ) -> Result<RateLimitDecision, TrypemaError> {
        self.send_epoch_change_if_needed();

        let decision = self
            .is_allowed_with_count_increment(key, count, count, Some(rate_limit))
            .await?;

        Ok(decision)
    } // end method inc

    /// Check if `key` is currently under its rate limit (read-only).
    ///
    /// Performs an admission check **without** recording an increment. On the fast-path
    /// (key in `Accepting` state), this is served from local state with no Redis I/O.
    ///
    /// Useful for previewing whether a request would be allowed before doing expensive work.
    ///
    /// # Returns
    ///
    /// - `Ok(Allowed)` — under limit
    /// - `Ok(Rejected { .. })` — over limit, includes best-effort backoff hints
    /// - `Err(TrypemaError)` — Redis error during state refresh
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use trypema::{RateLimiterBuilder, hybrid::HybridRateLimiterProvider};
    /// # async fn example(connection_manager: trypema::redis::ConnectionManager) {
    /// let rl = HybridRateLimiterProvider::builder(connection_manager).build().unwrap();
    /// use trypema::{RateLimit, RateLimitDecision};
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from("user_123").unwrap();
    /// assert!(matches!(
    ///     rl.absolute().is_allowed(&key).await.unwrap(),
    ///     RateLimitDecision::Allowed
    /// ));
    /// # }
    /// ```
    pub async fn is_allowed(&self, key: &RedisKey) -> Result<RateLimitDecision, TrypemaError> {
        self.send_epoch_change_if_needed();

        self.is_allowed_with_count_increment(key, 1, 0, None).await
    } // end method is_allowed

    /// Infer the current window total for `key` from this instance's limiting state.
    ///
    /// This follows the same state-refresh path as [`Self::is_allowed`], without checking or
    /// recording an increment. An initialized, usable state stays on the local fast path. An
    /// undefined state or expired rejection cache is refreshed from Redis first, then the total
    /// is inferred from the updated state. The result can still lag later commits from other
    /// instances and Redis-side expiration. Use [`Self::get`] when every read must consult Redis.
    ///
    /// # Errors
    ///
    /// Returns an error when required Redis or locally cached state cannot be read.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use trypema::{RateLimiterBuilder, hybrid::HybridRateLimiterProvider};
    /// # async fn example(connection_manager: trypema::redis::ConnectionManager) {
    /// let rl = HybridRateLimiterProvider::builder(connection_manager).build().unwrap();
    /// use trypema::RateLimit;
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from("user_123").unwrap();
    /// assert_eq!(
    ///     rl.absolute().get_estimate(&key).await.unwrap(),
    ///     0
    /// );
    ///
    /// let rate = RateLimit::per_second(10.0).unwrap();
    /// rl.absolute().inc(&key, &rate, 3).await.unwrap();
    /// assert_eq!(
    ///     rl.absolute().get_estimate(&key).await.unwrap(),
    ///     3
    /// );
    /// # }
    /// ```
    pub async fn get_estimate(&self, key: &RedisKey) -> Result<u64, TrypemaError> {
        self.send_epoch_change_if_needed();

        self.is_allowed_with_count_increment(key, 0, 0, None)
            .await?;

        let Some(state) = self.limiting_state.get(key) else {
            return Ok(0);
        };

        match state.deref() {
            AbsoluteRedisLimitingState::Accepting {
                starting_count,
                count,
                ..
            } => {
                let starting_count = *mutex_lock(starting_count, "accepting.starting_count")?;

                Ok(starting_count.saturating_add(count.load(Ordering::Acquire)))
            }
            AbsoluteRedisLimitingState::Rejecting {
                committed_count,
                committed_at,
                ..
            } if committed_at.elapsed() < Duration::from_secs(self.window_size.as_seconds()) => {
                Ok(*committed_count)
            }
            AbsoluteRedisLimitingState::Undefined
            | AbsoluteRedisLimitingState::Rejecting { .. } => Ok(0),
        }
    } // end method get_estimate

    /// Read the current live window total for `key` from Redis and local state.
    ///
    /// Returns the Redis window total after lazily evicting expired buckets, plus this
    /// instance's pending local increments for the key (increments accepted on the
    /// fast-path that have not been flushed yet). Pending increments held by
    /// **other** instances are not visible until their next flush, so the result
    /// may lag the true global total by up to `sync_interval`.
    ///
    /// Unlike [`Self::get_estimate`], this method performs a Redis round-trip on every call rather
    /// than using a valid local state.
    ///
    /// # Errors
    ///
    /// Returns an error when Redis cannot be read.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use trypema::{RateLimiterBuilder, hybrid::HybridRateLimiterProvider};
    /// # async fn example(connection_manager: trypema::redis::ConnectionManager) {
    /// let rl = HybridRateLimiterProvider::builder(connection_manager).build().unwrap();
    /// use trypema::RateLimit;
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from("user_123").unwrap();
    /// assert_eq!(rl.absolute().get(&key).await.unwrap(), 0);
    ///
    /// let rate = RateLimit::per_second(10.0).unwrap();
    /// rl.absolute().inc(&key, &rate, 3).await.unwrap();
    /// assert_eq!(rl.absolute().get(&key).await.unwrap(), 3);
    /// # }
    /// ```
    pub async fn get(&self, key: &RedisKey) -> Result<u64, TrypemaError> {
        self.send_epoch_change_if_needed();

        let lock = self.get_or_create_reset_lock(key);
        let _guard = lock.lock().await;

        let read_state_result = self.redis_proxy.read_state(key).await?;

        let revision_mismatch = match self.limiting_state.get(key) {
            Some(state) => match state.deref() {
                AbsoluteRedisLimitingState::Accepting { state_revision, .. } => {
                    *mutex_lock(state_revision, "accepting.state_revision")?
                        != read_state_result.state_revision
                }
                AbsoluteRedisLimitingState::Rejecting { state_revision, .. } => {
                    *mutex_lock(state_revision, "rejecting.state_revision")?
                        != read_state_result.state_revision
                }
                AbsoluteRedisLimitingState::Undefined => false,
            },
            None => false,
        };

        if revision_mismatch {
            let state = self
                .resolve_redis_state_and_commit(read_state_result)
                .await?;
            self.limiting_state.remove(key);
            return Ok(state.current_total);
        }

        let mut total_count = read_state_result.current_total_count;

        if let Some(state) = self.limiting_state.get(key) {
            match state.deref() {
                AbsoluteRedisLimitingState::Accepting { count, .. } => {
                    total_count = total_count.saturating_add(count.load(Ordering::Acquire));
                }
                AbsoluteRedisLimitingState::Rejecting {
                    committed_count,
                    committed_at,
                    ..
                } if committed_at.elapsed()
                    < Duration::from_secs(self.window_size.as_seconds()) =>
                {
                    total_count = total_count.max(*committed_count);
                }
                AbsoluteRedisLimitingState::Undefined
                | AbsoluteRedisLimitingState::Rejecting { .. } => {}
            }
        }

        Ok(total_count)
    } // end method get

    /// Change the stored rate limit for an existing key without changing its history.
    ///
    /// This instance's pending increments are committed first. A changed limit invalidates local
    /// cached admission state and advances the key's state revision so other hybrid instances
    /// ignore stale cached totals and limits on their next Redis interaction. Pending increments
    /// from those instances still apply as fresh usage.
    ///
    /// # Errors
    ///
    /// Returns an error for Redis failures or an invalid legacy stored limit.
    pub async fn set_rate_limit(
        &self,
        key: &RedisKey,
        rate_limit: &RateLimit,
    ) -> Result<Option<RateLimit>, TrypemaError> {
        self.send_epoch_change_if_needed();

        let lock = self.get_or_create_reset_lock(key);
        let _reset_guard = lock.lock().await;

        let committed_pending = self.flush_local_pending_for_lifecycle(key).await?;
        RedisCommitter::barrier(&self.commiter_sender).await?;

        let _maintenance_guard = self.maintenance_lock.write().await;
        let window_limit = self.window_size.as_seconds() as f64 * rate_limit.as_per_second();
        let result = self.redis_proxy.set_rate_limit(key, window_limit).await?;

        match result {
            Some((_, changed)) if changed || committed_pending => {
                self.limiting_state.remove(key);
            }
            None => {
                self.limiting_state.remove(key);
            }
            Some(_) => {}
        }

        result
            .map(|(previous_window_limit, _)| {
                RateLimit::from_stored_window_limit(previous_window_limit, self.window_size, 1.0)
            })
            .transpose()
    } // end method set_rate_limit

    /// Delete all committed and caller-local state for `key`.
    ///
    /// Returns the live total at deletion, including this instance's pending increments, or
    /// `None` when no committed or caller-local state existed.
    ///
    /// Other instances discard stale cached state on their next Redis interaction. Pending
    /// increments already accepted there may recreate the key as fresh usage.
    ///
    /// # Errors
    ///
    /// Returns an error when queued commits cannot drain or Redis cannot delete the state. Local
    /// state is restored when deletion fails.
    pub async fn delete(&self, key: &RedisKey) -> Result<Option<u64>, TrypemaError> {
        self.send_epoch_change_if_needed();

        let lock = self.get_or_create_reset_lock(key);
        let _reset_guard = lock.lock().await;
        let local_state = self.limiting_state.remove(key).map(|(_, state)| state);
        let local_values = match local_state.as_ref() {
            Some(AbsoluteRedisLimitingState::Accepting {
                count,
                state_revision,
                ..
            }) => mutex_lock(state_revision, "accepting.state_revision")
                .map(|state_revision| (count.load(Ordering::Acquire), 0, *state_revision, true)),
            Some(AbsoluteRedisLimitingState::Rejecting {
                committed_count,
                committed_at,
                state_revision,
                ..
            }) => mutex_lock(state_revision, "rejecting.state_revision").map(|state_revision| {
                let cached_committed_count = if committed_at.elapsed()
                    < Duration::from_secs(self.window_size.as_seconds())
                {
                    *committed_count
                } else {
                    0
                };
                (0, cached_committed_count, *state_revision, true)
            }),
            Some(AbsoluteRedisLimitingState::Undefined) | None => {
                Ok((0, 0, StateRevision::default(), false))
            }
        };
        let (pending_count, cached_committed_count, state_revision, local_existed) =
            match local_values {
                Ok(local_values) => local_values,
                Err(err) => {
                    if let Some(state) = local_state {
                        self.store_absolute_local_state(key, state);
                    }
                    return Err(err);
                }
            };

        if let Err(err) = RedisCommitter::barrier(&self.commiter_sender).await {
            if let Some(state) = local_state {
                self.store_absolute_local_state(key, state);
            }
            return Err(err);
        }

        let _maintenance_guard = self.maintenance_lock.write().await;
        match self
            .redis_proxy
            .delete(
                key,
                pending_count,
                cached_committed_count,
                state_revision,
                local_existed,
            )
            .await
        {
            Ok(deleted_total) => {
                self.limiting_state.remove(key);
                Ok(deleted_total)
            }
            Err(err) => {
                if let Some(state) = local_state {
                    self.store_absolute_local_state(key, state);
                }
                Err(err)
            }
        }
    } // end method delete

    /// Delete every key tracked by this hybrid absolute limiter's prefix and strategy.
    ///
    /// The namespace state revision invalidates other instances when they next interact with
    /// Redis. Concurrent or remote pending increments may recreate keys after this call.
    ///
    /// # Errors
    ///
    /// Returns an error when queued commits cannot drain or Redis cannot complete the clear.
    pub async fn clear(&self) -> Result<(), TrypemaError> {
        self.send_epoch_change_if_needed();
        RedisCommitter::barrier(&self.commiter_sender).await?;

        let _maintenance_guard = self.maintenance_lock.write().await;
        self.redis_proxy.clear().await?;
        self.limiting_state.clear();

        Ok(())
    } // end method clear

    fn store_absolute_local_state(&self, key: &RedisKey, state: AbsoluteRedisLimitingState) {
        match self.limiting_state.entry(key.clone()) {
            Entry::Occupied(mut entry) => *entry.get_mut() = state,
            Entry::Vacant(entry) => {
                entry.insert(state);
            }
        }
    } // end fn store_absolute_local_state

    /// Conditionally replace the window total for `key` (atomic on Redis).
    ///
    /// When `comparator` matches the key's current post-eviction window total, the
    /// window contents are replaced by a single current-timestamp bucket holding
    /// `count`; otherwise nothing is written. On a match the key's window limit is
    /// (re)defined as `window_size × rate_limit` and its TTL refreshed.
    /// A matched target of zero removes the committed entity state; increments that
    /// race after the protected pending snapshot are committed on top of that reset.
    ///
    /// This instance's pending local increments are included in the atomic Redis
    /// comparison as a newest virtual bucket. Local state remains untouched on a
    /// miss. After a match it is invalidated, and increments that arrived after the
    /// snapshot are committed on top of the requested target, so:
    ///
    /// - the comparator evaluates a total that includes this instance's own traffic, and
    /// - the next `inc` on this instance re-reads Redis and observes the new total.
    ///
    /// Pending increments on **other** instances flush later (within
    /// `sync_interval`) and land *on top of* the written value, so after a
    /// matched write the total may drift above `count` — it is never lowered below
    /// it by those flushes.
    ///
    /// # Arguments
    ///
    /// - `key`: Validated [`RedisKey`] identifying the rate-limited resource
    /// - `rate_limit`: Per-second rate limit used to (re)define the window limit
    /// - `comparator`: Guard evaluated against the current window total
    /// - `count`: The total to write when the guard matches
    ///
    /// # Returns
    ///
    /// A [`ConditionalSetOutcome`] describing whether the comparator matched and the totals
    /// before and after the operation.
    ///
    /// # Priming Idiom
    ///
    /// `set_if(key, rate, RateLimitComparator::Lt(count), count)` raises the window
    /// total to at least `count` and never lowers it — idempotent and safe to retry,
    /// e.g. for seeding a quota window from an external usage store.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use trypema::{RateLimiterBuilder, hybrid::HybridRateLimiterProvider};
    /// # async fn example(connection_manager: trypema::redis::ConnectionManager) {
    /// let rl = HybridRateLimiterProvider::builder(connection_manager).build().unwrap();
    /// use trypema::{RateLimit, RateLimitComparator};
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from("user_123").unwrap();
    /// let rate = RateLimit::per_second(10.0).unwrap();
    ///
    /// // Prime the window to 40.
    /// let outcome = rl
    ///     .absolute()
    ///     .set_if(&key, &rate, RateLimitComparator::Lt(40), 40)
    ///     .await
    ///     .unwrap();
    /// assert!(outcome.matched);
    /// assert_eq!((outcome.current_total, outcome.previous_total), (40, 0));
    ///
    /// // Re-priming is a no-op: the guard no longer matches.
    /// let outcome = rl
    ///     .absolute()
    ///     .set_if(&key, &rate, RateLimitComparator::Lt(40), 40)
    ///     .await
    ///     .unwrap();
    /// assert!(!outcome.matched);
    /// assert_eq!((outcome.current_total, outcome.previous_total), (40, 40));
    /// # }
    /// ```
    pub async fn set_if(
        &self,
        key: &RedisKey,
        rate_limit: &RateLimit,
        comparator: RateLimitComparator,
        count: u64,
    ) -> Result<ConditionalSetOutcome, TrypemaError> {
        let (current_total, previous_total) = self
            .set_if_with_history_mode(
                key,
                rate_limit,
                comparator,
                count,
                HistoryUpdateMode::Replace,
            )
            .await?;

        Ok(ConditionalSetOutcome {
            matched: comparator.matches(previous_total),
            previous_total,
            current_total,
        })
    } // end method set_if

    /// Conditionally set the total while retaining the selected side of the shared
    /// Redis sliding-window history.
    pub async fn set_if_preserve_history(
        &self,
        key: &RedisKey,
        rate_limit: &RateLimit,
        comparator: RateLimitComparator,
        count: u64,
        preservation: HistoryPreservation,
    ) -> Result<ConditionalSetOutcome, TrypemaError> {
        let (current_total, previous_total) = self
            .set_if_with_history_mode(
                key,
                rate_limit,
                comparator,
                count,
                HistoryUpdateMode::Preserve(preservation),
            )
            .await?;

        Ok(ConditionalSetOutcome {
            matched: comparator.matches(previous_total),
            previous_total,
            current_total,
        })
    } // end method set_if_preserve_history

    #[cfg(test)]
    pub(crate) fn local_state_count(&self) -> usize {
        self.limiting_state.len()
    }

    fn should_cleanup_local_state(state: &AbsoluteRedisLimitingState, stale_after_ms: u64) -> bool {
        match state {
            AbsoluteRedisLimitingState::Undefined => true,
            AbsoluteRedisLimitingState::Accepting { last_modified, .. } => {
                let last_modified = match last_modified.lock() {
                    Ok(last_modified) => last_modified,
                    Err(err) => {
                        tracing::warn!("last_modified is poisoned: {err:?}");
                        return true;
                    }
                };

                last_modified.elapsed().as_millis() >= stale_after_ms as u128
            }
            AbsoluteRedisLimitingState::Rejecting {
                time_instant,
                ttl_ms,
                ..
            } => {
                let elapsed_ms = match time_instant.lock() {
                    Ok(time_instant) => time_instant.elapsed().as_millis(),
                    Err(err) => {
                        tracing::warn!("time_instant is poisoned: {err:?}");
                        return true;
                    }
                };

                let retry_ttl_ms = match ttl_ms.lock() {
                    Ok(ttl_ms) => *ttl_ms as u128,
                    Err(err) => {
                        tracing::warn!("ttl_ms is poisoned: {err:?}");
                        return true;
                    }
                };

                elapsed_ms.saturating_sub(retry_ttl_ms) >= stale_after_ms as u128
            }
        }
    } // end fn should_cleanup_local_state

    async fn set_if_with_history_mode(
        &self,
        key: &RedisKey,
        rate_limit: &RateLimit,
        comparator: RateLimitComparator,
        count: u64,
        mode: HistoryUpdateMode,
    ) -> Result<(u64, u64), TrypemaError> {
        self.send_epoch_change_if_needed();

        let lock = self.get_or_create_reset_lock(key);
        let _guard = lock.lock().await;

        let (pending_count, state_revision) = match self.limiting_state.get(key) {
            Some(state) => match state.deref() {
                AbsoluteRedisLimitingState::Accepting {
                    count,
                    state_revision,
                    ..
                } => (
                    count.load(Ordering::Acquire),
                    *mutex_lock(state_revision, "accepting.state_revision")?,
                ),
                AbsoluteRedisLimitingState::Undefined
                | AbsoluteRedisLimitingState::Rejecting { .. } => (0, StateRevision::default()),
            },
            None => (0, StateRevision::default()),
        };

        let window_limit = self.window_size.as_seconds() as f64 * rate_limit.as_per_second();

        let (new_total, old_total, changed) = self
            .redis_proxy
            .set_if(key, window_limit, comparator, count, mode, pending_count)
            .await?;

        if !changed {
            return Ok((new_total, old_total));
        }

        if let Some((_, state)) = self.limiting_state.remove(key)
            && let AbsoluteRedisLimitingState::Accepting {
                count: local_count, ..
            } = state
        {
            let extra_count = local_count.into_inner().saturating_sub(pending_count);
            if extra_count > 0 {
                let commit = AbsoluteHybridCommit {
                    key: key.clone(),
                    window_limit,
                    count: extra_count,
                    state_revision,
                };

                self.send_commit(commit).await?;

                // if let Err(err) = self
                //     .redis_proxy
                //     .batch_commit_state(std::slice::from_ref(&commit))
                //     .await
                // {
                //     tracing::warn!(error = ?err, "direct post-set commit failed; queued for retry");
                //     self.send_commit(commit).await?;
                // }
            }
        }

        Ok((new_total, old_total))
    }

    /// Remove stale Redis state and local state stale since its rejection TTL elapsed.
    pub(crate) async fn cleanup(&self, stale_after_ms: u64) -> Result<(), TrypemaError> {
        self.redis_proxy.cleanup(stale_after_ms).await?;

        let candidates = self
            .limiting_state
            .iter()
            .filter(|state| Self::should_cleanup_local_state(state.value(), stale_after_ms))
            .map(|state| state.key().clone())
            .collect::<Vec<_>>();

        for key in candidates {
            let lock = self.get_or_create_reset_lock(&key);
            let _guard = lock.lock().await;

            let should_remove = self.limiting_state.get(&key).is_some_and(|state| {
                Self::should_cleanup_local_state(state.value(), stale_after_ms)
            });

            if !should_remove {
                continue;
            }

            match self.limiting_state.entry(key) {
                Entry::Occupied(entry)
                    if Self::should_cleanup_local_state(entry.get(), stale_after_ms) =>
                {
                    entry.remove();
                }
                Entry::Occupied(_) | Entry::Vacant(_) => {}
            }
        }

        Ok(())
    }

    async fn flush(&self) -> Result<(), TrypemaError> {
        let mut resets: Vec<RedisKey> = Vec::new();
        let mut stale: Vec<RedisKey> = Vec::new();

        for state in self.limiting_state.iter() {
            let key = state.key();

            if let AbsoluteRedisLimitingState::Accepting {
                last_modified,
                count,
                ..
            } = state.deref()
            {
                let elapsed = {
                    let last_modified = match last_modified.lock() {
                        Ok(last_modified) => last_modified,
                        Err(err) => {
                            tracing::warn!("last_modified is poisoned: {err:?}");
                            continue;
                        }
                    };

                    last_modified.elapsed()
                };

                if elapsed.as_millis() > self.window_size.as_milliseconds() {
                    stale.push(key.clone());
                    continue;
                }

                if count.load(Ordering::Acquire) == 0 {
                    continue;
                }

                resets.push(key.clone());
            }
        }

        let mut guards = Vec::with_capacity(resets.len() + stale.len());
        for key in resets.iter().chain(&stale) {
            guards.push(self.get_or_create_reset_lock(key).lock_owned().await);
        }

        for key in stale {
            let should_reset = self.limiting_state.get(&key).is_some_and(|state| {
                matches!(
                    state.deref(),
                    AbsoluteRedisLimitingState::Accepting { last_modified, .. }
                        if mutex_lock(last_modified, "accepting.last_modified")
                            .is_ok_and(|last_modified| {
                                last_modified.elapsed().as_millis()
                                    > self.window_size.as_milliseconds()
                            })
                )
            });

            if !should_reset {
                continue;
            }

            if let Some(mut state) = self.limiting_state.get_mut(&key)
                && let AbsoluteRedisLimitingState::Accepting { last_modified, .. } = state.deref()
            {
                let is_stale = mutex_lock(last_modified, "accepting.last_modified")?
                    .elapsed()
                    .as_millis()
                    > self.window_size.as_milliseconds();

                if is_stale {
                    *state = AbsoluteRedisLimitingState::Undefined;
                }
            }
        }

        resets.retain(|key| {
            self.limiting_state.get(key).is_some_and(|state| {
                matches!(
                    state.deref(),
                    AbsoluteRedisLimitingState::Accepting { count, .. }
                        if count.load(Ordering::Acquire) > 0
                )
            })
        });

        if resets.is_empty() {
            return Ok(());
        }

        let read_state_results = self.redis_proxy.batch_read_state(&resets).await?;

        for result in read_state_results {
            if let Err(err) = self
                .reset_single_state_from_read_result(result, 0, 0, None)
                .await
            {
                tracing::error!(error = ?err, "Failed to reset state from redis read result");
                continue;
            }
        }

        Ok(())
    } // end method flush
}
