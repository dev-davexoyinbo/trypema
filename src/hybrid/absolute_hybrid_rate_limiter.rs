use std::{
    ops::Deref,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use dashmap::{DashMap, mapref::entry::Entry, mapref::one::Ref};
use tokio::sync::{mpsc, watch};

use crate::{
    HistoryPreservation, RateGroupSizeMs, RateLimit, RateLimitComparator, RateLimitDecision,
    RedisKey, RedisRateLimiterOptions, TrypemaError, WindowSizeSeconds,
    common::{HistoryUpdateMode, RandomState},
    hybrid::{
        AbsoluteHybridCommitterSignal, RedisCommitter, RedisCommitterOptions, RedisProxyCommitter,
        SyncIntervalMs,
        absolute_hybrid_redis_proxy::{
            AbsoluteHybridCommit, AbsoluteHybridRedisProxy, AbsoluteHybridRedisProxyOptions,
            AbsoluteHybridRedisProxyReadStateResult,
        },
        common::{EPOCH_CHANGE_INTERVAL, RedisRateLimiterSignal},
    },
    redis::{mutex_lock, spawn_task},
    runtime,
};

mod helpers;

#[derive(Debug)]
enum AbsoluteRedisLimitingState {
    Accepting {
        window_limit: Mutex<u64>,
        accept_limit: Mutex<u64>,
        starting_count: Mutex<u64>,
        count: AtomicU64,
        time_instant: Mutex<Instant>,
        oldest_bucket_ttl: Mutex<Option<u64>>,
        oldest_bucket_count: Mutex<Option<u64>>,
        last_modified: Mutex<Instant>,
    },
    Undefined,
    Rejecting {
        time_instant: Mutex<Instant>,
        ttl_ms: Mutex<u64>,
        count_after_release: Mutex<u64>,
        committed_count: u64,
        committed_at: Instant,
    },
}

/// Strict sliding-window rate limiter with a local fast-path and periodic Redis sync.
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
/// to `sync_interval_ms`.
#[derive(Debug)]
pub struct AbsoluteHybridRateLimiter {
    window_size_seconds: WindowSizeSeconds,
    rate_group_size_ms: RateGroupSizeMs,
    commiter_sender: mpsc::Sender<AbsoluteHybridCommitterSignal<AbsoluteHybridCommit>>,
    redis_proxy: AbsoluteHybridRedisProxy,
    limiting_state: DashMap<RedisKey, AbsoluteRedisLimitingState, RandomState>,
    /// Per-key async mutex that serializes the "Redis read + state update" operation.
    /// Only one tokio task at a time may perform the read+update for a given key;
    /// all other tasks wait on this lock and then re-check the (now-updated) state.
    reset_locks: DashMap<RedisKey, Arc<tokio::sync::Mutex<()>>, RandomState>,
    sync_interval_ms: SyncIntervalMs,
    epoch: AtomicU64,
    last_commited_epoch: AtomicU64,
    is_active_watch: watch::Sender<u64>,
}

impl AbsoluteHybridRateLimiter {
    pub(crate) fn new(options: RedisRateLimiterOptions) -> Arc<Self> {
        let prefix = options.prefix.unwrap_or_else(RedisKey::default_prefix);

        let (tx, rx) = mpsc::channel::<RedisRateLimiterSignal>(1);

        let redis_proxy = AbsoluteHybridRedisProxy::new(AbsoluteHybridRedisProxyOptions {
            prefix: prefix.clone(),
            connection_manager: options.connection_manager,
            window_size_seconds: options.window_size_seconds,
            rate_group_size_ms: options.rate_group_size_ms,
        });

        let is_active_watch = watch::Sender::new(0u64);

        let commiter_sender = RedisCommitter::run(RedisCommitterOptions {
            sync_interval: Duration::from_millis(*options.sync_interval_ms),
            channel_capacity: 8192,
            max_batch_size: 4,
            limiter_sender: tx,
            redis_proxy: Box::new(redis_proxy.clone()),
            is_active_watch: is_active_watch.subscribe(),
        });

        let limiter = Self {
            window_size_seconds: options.window_size_seconds,
            rate_group_size_ms: options.rate_group_size_ms,
            commiter_sender,
            redis_proxy,
            limiting_state: DashMap::default(),
            reset_locks: DashMap::default(),
            sync_interval_ms: options.sync_interval_ms,
            epoch: AtomicU64::new(0),
            last_commited_epoch: AtomicU64::new(0),
            is_active_watch,
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
    /// ```
    /// # trypema::__doctest_helpers::with_redis_rate_limiter(|rl| async move {
    /// use trypema::{RateLimit, RateLimitDecision};
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from(trypema::__doctest_helpers::unique_key()).unwrap();
    /// let rate = RateLimit::try_from(10.0).unwrap();
    /// assert!(matches!(
    ///     rl.hybrid().absolute().inc(&key, &rate, 1).await.unwrap(),
    ///     RateLimitDecision::Allowed
    /// ));
    /// # });
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
    /// ```
    /// # trypema::__doctest_helpers::with_redis_rate_limiter(|rl| async move {
    /// use trypema::{RateLimit, RateLimitDecision};
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from(trypema::__doctest_helpers::unique_key()).unwrap();
    /// assert!(matches!(
    ///     rl.hybrid().absolute().is_allowed(&key).await.unwrap(),
    ///     RateLimitDecision::Allowed
    /// ));
    /// # });
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
    /// ```
    /// # trypema::__doctest_helpers::with_redis_rate_limiter(|rl| async move {
    /// use trypema::RateLimit;
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from(trypema::__doctest_helpers::unique_key()).unwrap();
    /// assert_eq!(
    ///     rl.hybrid().absolute().get_inferred(&key).await.unwrap(),
    ///     0
    /// );
    ///
    /// let rate = RateLimit::try_from(10.0).unwrap();
    /// rl.hybrid().absolute().inc(&key, &rate, 3).await.unwrap();
    /// assert_eq!(
    ///     rl.hybrid().absolute().get_inferred(&key).await.unwrap(),
    ///     3
    /// );
    /// # });
    /// ```
    pub async fn get_inferred(&self, key: &RedisKey) -> Result<u64, TrypemaError> {
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
            } if committed_at.elapsed() < Duration::from_secs(*self.window_size_seconds) => {
                Ok(*committed_count)
            }
            AbsoluteRedisLimitingState::Undefined
            | AbsoluteRedisLimitingState::Rejecting { .. } => Ok(0),
        }
    } // end method get_inferred

    /// Read the current live window total for `key` from Redis and local state.
    ///
    /// Returns the Redis window total after lazily evicting expired buckets, plus this
    /// instance's pending local increments for the key (increments accepted on the
    /// fast-path that have not been flushed yet). Pending increments held by
    /// **other** instances are not visible until their next flush, so the result
    /// may lag the true global total by up to `sync_interval_ms`.
    ///
    /// Unlike [`Self::get_inferred`], this method performs a Redis round-trip on every call rather
    /// than using a valid local state.
    ///
    /// # Errors
    ///
    /// Returns an error when Redis cannot be read.
    ///
    /// # Examples
    ///
    /// ```
    /// # trypema::__doctest_helpers::with_redis_rate_limiter(|rl| async move {
    /// use trypema::RateLimit;
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from(trypema::__doctest_helpers::unique_key()).unwrap();
    /// assert_eq!(rl.hybrid().absolute().get(&key).await.unwrap(), 0);
    ///
    /// let rate = RateLimit::try_from(10.0).unwrap();
    /// rl.hybrid().absolute().inc(&key, &rate, 3).await.unwrap();
    /// assert_eq!(rl.hybrid().absolute().get(&key).await.unwrap(), 3);
    /// # });
    /// ```
    pub async fn get(&self, key: &RedisKey) -> Result<u64, TrypemaError> {
        self.send_epoch_change_if_needed();

        let lock = self.get_or_create_reset_lock(key);
        let _guard = lock.lock().await;

        let read_state_result = self.redis_proxy.read_state(key).await?;

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
                } if committed_at.elapsed() < Duration::from_secs(*self.window_size_seconds) => {
                    total_count = total_count.max(*committed_count);
                }
                AbsoluteRedisLimitingState::Undefined
                | AbsoluteRedisLimitingState::Rejecting { .. } => {}
            }
        }

        Ok(total_count)
    } // end method get

        if let AbsoluteRedisLimitingState::Accepting {
            window_limit,
            accept_limit,
            starting_count,
            count,
            time_instant,
            last_rate_group_ttl,
            last_rate_group_count,
            last_modified,
        } = state.deref()
        {
            *mutex_lock(window_limit, "accepting.window_limit")? = new_window_limit;
            *mutex_lock(accept_limit, "accepting.accept_limit")? = new_accept_limit;
            *mutex_lock(starting_count, "accepting.starting_count")? = redis_total;
            *mutex_lock(time_instant, "accepting.time_instant")? = new_time_instant;
            *mutex_lock(last_modified, "accepting.last_modified")? = new_time_instant;
            *mutex_lock(last_rate_group_ttl, "accepting.last_rate_group_ttl")? =
                read_state_result.last_rate_group_ttl;
            *mutex_lock(last_rate_group_count, "accepting.last_rate_group_count")? =
                read_state_result.last_rate_group_count;
            count.store(increment, Ordering::Release);
        } else {
            drop(state);
            let mut state = self
                .limiting_state
                .get_mut(key)
                .expect("key should be present");

            *state = AbsoluteRedisLimitingState::Accepting {
                window_limit: Mutex::new(new_window_limit),
                accept_limit: Mutex::new(new_accept_limit),
                starting_count: Mutex::new(redis_total),
                count: AtomicU64::new(increment),
                time_instant: Mutex::new(new_time_instant),
                last_modified: Mutex::new(new_time_instant),
                last_rate_group_ttl: Mutex::new(read_state_result.last_rate_group_ttl),
                last_rate_group_count: Mutex::new(read_state_result.last_rate_group_count),
            };
        }

        Ok(RateLimitDecision::Allowed)
    }

    async fn send_commit(&self, commit: AbsoluteHybridCommit) -> Result<(), TrypemaError> {
        self.commiter_sender
            .send(commit.into())
            .await
            .map_err(|err| TrypemaError::CustomError(format!("Failed to send commit: {err:?}")))?;

        Ok(())
    } // end method send_commit

    async fn is_allowed_with_count_increment(
        &self,
        key: &RedisKey,
        check_count: u64,
        increment: u64,
        rate_limit: Option<&RateLimit>,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let state_entry = match self.limiting_state.get(key) {
            Some(state) => state,
            None => {
                self.limiting_state
                    .entry(key.clone())
                    .or_insert_with(|| AbsoluteRedisLimitingState::Undefined);

                self.limiting_state.get(key).expect("Key should be present")
            }
        };

        if let AbsoluteRedisLimitingState::Rejecting {
            time_instant,
            ttl_ms,
            count_after_release,
            ..
        } = state_entry.deref()
        {
            let elapsed_ms = mutex_lock(time_instant, "rejecting.time_instant")?
                .elapsed()
                .as_millis();
            let ttl_ms_val = *mutex_lock(ttl_ms, "rejecting.ttl_ms")? as u128;

            if elapsed_ms < ttl_ms_val {
                let remaining_after_waiting =
                    *mutex_lock(count_after_release, "rejecting.count_after_release")?;
                return Ok(RateLimitDecision::Rejected {
                    window_size_seconds: *self.window_size_seconds,
                    retry_after_ms: ttl_ms_val.saturating_sub(elapsed_ms),
                    remaining_after_waiting,
                });
            }

            drop(state_entry);
        } else if let AbsoluteRedisLimitingState::Accepting {
            accept_limit,
            count,
            last_modified,
            ..
        } = state_entry.deref()
        {
            let accept_limit = *mutex_lock(accept_limit, "accepting.accept_limit")?;
            if count.load(Ordering::Acquire) + check_count <= accept_limit {
                *mutex_lock(last_modified, "accepting.last_modified")? = Instant::now();
                count.fetch_add(increment, Ordering::AcqRel);
                return Ok(RateLimitDecision::Allowed);
            }

            drop(state_entry);

            let permit = self.commiter_sender.reserve().await.map_err(|_| {
                TrypemaError::CustomError("Failed to reserve commiter sender".to_string())
            })?;

            let mut state_entry = self
                .limiting_state
                .get_mut(key)
                .expect("key should be present after drop");

            if let AbsoluteRedisLimitingState::Accepting {
                window_limit,
                starting_count,
                count,
                time_instant,
                last_rate_group_ttl,
                last_rate_group_count,
                accept_limit,
                last_modified,
                ..
            } = state_entry.deref()
            {
                let local_count = count.load(Ordering::Acquire);
                let starting_count = *mutex_lock(starting_count, "accepting.starting_count")?;
                let window_limit = *mutex_lock(window_limit, "accepting.window_limit")?;
                let accept_limit = *mutex_lock(accept_limit, "accepting.accept_limit")?;

                if local_count + check_count <= accept_limit {
                    *mutex_lock(last_modified, "accepting.last_modified")? = Instant::now();
                    count.fetch_add(increment, Ordering::AcqRel);
                    return Ok(RateLimitDecision::Allowed);
                }

                // True expected Redis total once the commit we're about to send lands.
                let expected_redis_total = starting_count.saturating_add(local_count);

                let last_rate_group_ttl: u128 =
                    (*mutex_lock(last_rate_group_ttl, "accepting.last_rate_group_ttl")?)
                        .map(|el| el as u128)
                        .unwrap_or((*self.sync_interval_ms).min(*self.rate_group_size_ms) as u128);
                let elapsed = mutex_lock(time_instant, "accepting.time_instant")?
                    .elapsed()
                    .as_millis();
                let retry_after_ms = last_rate_group_ttl
                    .saturating_sub(elapsed)
                    .max(*self.sync_interval_ms as u128);
                let remaining_after_waiting =
                    (*mutex_lock(last_rate_group_count, "accepting.last_rate_group_count")?)
                        .unwrap_or(expected_redis_total);

                if local_count >= accept_limit {
                    if local_count > 0 {
                        let commit = AbsoluteHybridCommit {
                            key: key.clone(),
                            window_limit,
                            count: local_count,
                        };

                        permit.send(commit.into());
                    }

                    *state_entry = AbsoluteRedisLimitingState::Rejecting {
                        time_instant: Mutex::new(Instant::now()),
                        ttl_ms: Mutex::new(retry_after_ms as u64),
                        count_after_release: Mutex::new(remaining_after_waiting),
                        // Use the full expected Redis total, not just the local count.
                        committed_count: expected_redis_total,
                        committed_at: Instant::now(),
                    };
                }

                return Ok(RateLimitDecision::Rejected {
                    window_size_seconds: *self.window_size_seconds,
                    retry_after_ms,
                    remaining_after_waiting,
                });
            } else if let AbsoluteRedisLimitingState::Rejecting {
                time_instant,
                ttl_ms,
                count_after_release,
                ..
            } = state_entry.deref()
            {
                let elapsed_ms = mutex_lock(time_instant, "rejecting.time_instant")?
                    .elapsed()
                    .as_millis();
                let ttl_ms = *mutex_lock(ttl_ms, "rejecting.ttl_ms")? as u128;

                if elapsed_ms < ttl_ms {
                    let remaining_after_waiting =
                        *mutex_lock(count_after_release, "rejecting.count_after_release")?;
                    return Ok(RateLimitDecision::Rejected {
                        window_size_seconds: *self.window_size_seconds,
                        retry_after_ms: ttl_ms.saturating_sub(elapsed_ms),
                        remaining_after_waiting,
                    });
                }
            }

            drop(state_entry);
        } else {
            drop(state_entry);
        }

        self.reset_state_from_redis_read_result_and_get_decision(
            key,
            check_count,
            increment,
            rate_limit,
        )
        .await
    } // end method is_allowed_with_count_increment

    /// Evict expired buckets and update the total count.
    pub(crate) async fn cleanup(&self, stale_after_ms: u64) -> Result<(), TrypemaError> {
        self.redis_proxy.cleanup(stale_after_ms).await?;
        self.limiting_state.retain(|_, state| match state {
            AbsoluteRedisLimitingState::Undefined => false,
            AbsoluteRedisLimitingState::Accepting {
                last_modified: time_instant,
                ..
            }
            | AbsoluteRedisLimitingState::Rejecting { time_instant, .. } => {
                let time_instant = match time_instant.get_mut() {
                    Ok(time_instant) => time_instant,
                    Err(err) => {
                        tracing::warn!("time_instant is poisoned: {err:?}");
                        return false;
                    }
                };

                time_instant.elapsed().as_millis() < stale_after_ms as u128
            }
        });

        Ok(())
    }

    async fn flush(&self) -> Result<(), TrypemaError> {
        let mut resets: Vec<RedisKey> = Vec::new();

        for mut state in self.limiting_state.iter_mut() {
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

                if elapsed.as_millis() > (*self.window_size_seconds * 1000) as u128 {
                    *state = AbsoluteRedisLimitingState::Undefined;

                    continue;
                }

                if count.load(Ordering::Acquire) == 0 {
                    continue;
                }

                resets.push(key.clone());
            }
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
