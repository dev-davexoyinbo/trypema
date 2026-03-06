use std::{
    ops::Deref,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use dashmap::DashMap;
use tokio::sync::{mpsc, watch};

use crate::{
    RateGroupSizeMs, RateLimit, RateLimitDecision, RedisKey, RedisRateLimiterOptions, TrypemaError,
    WindowSizeSeconds,
    hybrid::{
        AbsoluteHybridCommitterSignal, RedisCommitter, RedisCommitterOptions, SyncIntervalMs,
        absolute_hybrid_redis_proxy::{
            AbsoluteHybridCommit, AbsoluteHybridRedisProxy, AbsoluteHybridRedisProxyOptions,
            AbsoluteHybridRedisProxyReadStateResult,
        },
        common::{EPOCH_CHANGE_INTERVAL, RedisRateLimiterSignal},
    },
    redis::{mutex_lock, spawn_task},
    runtime,
};

#[derive(Debug)]
enum AbsoluteRedisLimitingState {
    Accepting {
        window_limit: Mutex<u64>,
        accept_limit: Mutex<u64>,
        starting_count: Mutex<u64>,
        count: AtomicU64,
        time_instant: Mutex<Instant>,
        last_rate_group_ttl: Mutex<Option<u64>>,
        last_rate_group_count: Mutex<Option<u64>>,
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

/// A rate limiter backed by Redis.
#[derive(Debug)]
pub struct AbsoluteHybridRateLimiter {
    window_size_seconds: WindowSizeSeconds,
    rate_group_size_ms: RateGroupSizeMs,
    commiter_sender: mpsc::Sender<AbsoluteHybridCommitterSignal<AbsoluteHybridCommit>>,
    redis_proxy: AbsoluteHybridRedisProxy,
    limiting_state: DashMap<RedisKey, AbsoluteRedisLimitingState>,
    /// Per-key async mutex that serializes the "Redis read + state update" operation.
    /// Only one tokio task at a time may perform the read+update for a given key;
    /// all other tasks wait on this lock and then re-check the (now-updated) state.
    reset_locks: DashMap<RedisKey, Arc<tokio::sync::Mutex<()>>>,
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
            limiting_state: DashMap::new(),
            reset_locks: DashMap::new(),
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
        self.epoch.fetch_add(1, Ordering::Relaxed);
        let limiter = Arc::downgrade(self);

        spawn_task(async move {
            loop {
                runtime::sleep(EPOCH_CHANGE_INTERVAL).await;
                let Some(limiter) = limiter.upgrade() else {
                    break;
                };
                limiter.epoch.fetch_add(1, Ordering::Relaxed);
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

    #[inline(always)]
    fn send_epoch_change_if_needed(&self) {
        let epoch = self.epoch.load(Ordering::Relaxed);

        if self.last_commited_epoch.load(Ordering::Relaxed) < epoch {
            let _ = self.is_active_watch.send(epoch);
            self.last_commited_epoch.store(epoch, Ordering::Relaxed);
        }
    }

    /// Check admission and, if allowed, increment the observed count for `key`.
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

    /// Determine whether `key` is currently allowed.
    ///
    /// Returns [`RateLimitDecision::Allowed`] if the current sliding window total
    /// is below the window limit, otherwise returns [`RateLimitDecision::Rejected`]
    /// with a best-effort `retry_after_ms`.
    ///
    /// This method performs lazy eviction of expired buckets for the key.
    pub async fn is_allowed(&self, key: &RedisKey) -> Result<RateLimitDecision, TrypemaError> {
        self.send_epoch_change_if_needed();

        self.is_allowed_with_count_increment(key, 1, 0, None).await
    } // end method is_allowed

    /// Acquire (or create) the per-key reset lock and return an `Arc` to it.
    fn get_or_create_reset_lock(&self, key: &RedisKey) -> Arc<tokio::sync::Mutex<()>> {
        if let Some(lock) = self.reset_locks.get(key) {
            return Arc::clone(&lock);
        }
        self.reset_locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    async fn reset_state_from_redis_read_result_and_get_decision(
        &self,
        key: &RedisKey,
        check_count: u64,
        increment: u64,
        rate_limit: Option<&RateLimit>,
    ) -> Result<RateLimitDecision, TrypemaError> {
        // Acquire the per-key lock so that only one task at a time runs the
        // Redis read + state update for this key.  All other tasks wait here
        // and then re-check the (now-updated) state via the fast path below.
        let lock = self.get_or_create_reset_lock(key);
        let _guard = lock.lock().await;

        // Re-check the current state now that we hold the lock.  If another
        // task already transitioned us to Accepting while we were waiting, we
        // can serve the request from the local counter without another Redis
        // read.
        {
            let state_entry = self.limiting_state.get(key);
            if let Some(state_entry) = state_entry
                && let AbsoluteRedisLimitingState::Accepting {
                    accept_limit,
                    count,
                    ..
                } = state_entry.deref()
            {
                let accept_limit_val =
                    *mutex_lock(accept_limit, "accepting.accept_limit (recheck)")?;
                let current = count.load(Ordering::Acquire);
                if current + check_count <= accept_limit_val {
                    count.fetch_add(increment, Ordering::AcqRel);
                    return Ok(RateLimitDecision::Allowed);
                }
                // Still over limit after lock — fall through to Redis read.
            }
        }

        let read_state_result =
            self.redis_proxy.read_state(key).await.map_err(|err| {
                TrypemaError::CustomError(format!("Failed to read state: {err:?}"))
            })?;

        self.reset_single_state_from_read_result(
            read_state_result,
            check_count,
            increment,
            rate_limit,
        )
        .await
    } // end method reset_state_from_redis_read_result_and_get_decision

    async fn reset_single_state_from_read_result(
        &self,
        read_state_result: AbsoluteHybridRedisProxyReadStateResult,
        check_count: u64,
        increment: u64,
        rate_limit: Option<&RateLimit>,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let redis_total = read_state_result.current_total_count;
        let mut current_total_count = redis_total;
        let mut current_window_limit = read_state_result.window_limit;
        let key = &read_state_result.key;

        let state = match self.limiting_state.get(key) {
            Some(state) => state,
            None => {
                self.limiting_state
                    .entry(key.clone())
                    .or_insert_with(|| AbsoluteRedisLimitingState::Undefined);

                self.limiting_state.get(key).expect("Key should be present")
            }
        };

        let state = match state.deref() {
            AbsoluteRedisLimitingState::Undefined => state,
            AbsoluteRedisLimitingState::Rejecting {
                committed_count,
                committed_at,
                ..
            } => {
                let window_size_ms = (*self.window_size_seconds as u128).saturating_mul(1_000);
                let age_ms = committed_at.elapsed().as_millis();

                let effective_committed = if age_ms < window_size_ms {
                    *committed_count
                } else {
                    0
                };

                current_total_count = current_total_count.max(effective_committed);

                state
            }
            AbsoluteRedisLimitingState::Accepting {
                window_limit,
                count,
                ..
            } => {
                let local_count = count.load(Ordering::Acquire);

                if current_window_limit.is_none() {
                    current_window_limit =
                        Some(*mutex_lock(window_limit, "accepting.window_limit")?);
                }

                if local_count > 0 {
                    current_total_count += local_count;
                }

                state
            }
        };

        let new_window_limit = match current_window_limit {
            Some(window_limit) => window_limit,
            None => {
                let Some(rate_limit) = rate_limit else {
                    let is_undefined =
                        matches!(state.deref(), AbsoluteRedisLimitingState::Undefined);

                    if !is_undefined {
                        drop(state);
                        *self
                            .limiting_state
                            .get_mut(key)
                            .expect("key should be present") =
                            AbsoluteRedisLimitingState::Undefined;
                    }

                    return Ok(RateLimitDecision::Allowed);
                };

                ((*self.window_size_seconds as f64) * **rate_limit) as u64
            }
        };

        let new_time_instant = Instant::now();

        if current_total_count.saturating_add(check_count) > new_window_limit {
            let new_ttl_ms = read_state_result
                .last_rate_group_ttl
                .unwrap_or((*self.sync_interval_ms).min(*self.rate_group_size_ms))
                .max(*self.sync_interval_ms);

            let new_count_after_release = read_state_result.last_rate_group_count.unwrap_or(0);

            if let AbsoluteRedisLimitingState::Rejecting {
                time_instant,
                ttl_ms,
                count_after_release,
                ..
            } = state.deref()
            {
                *mutex_lock(time_instant, "rejecting.time_instant")? = new_time_instant;
                *mutex_lock(ttl_ms, "rejecting.ttl_ms")? = new_ttl_ms;
                *mutex_lock(count_after_release, "rejecting.count_after_release")? =
                    new_count_after_release;
            } else {
                drop(state);
                let mut state = self
                    .limiting_state
                    .get_mut(key)
                    .expect("key should be present");

                *state = AbsoluteRedisLimitingState::Rejecting {
                    time_instant: Mutex::new(new_time_instant),
                    ttl_ms: Mutex::new(new_ttl_ms),
                    count_after_release: Mutex::new(new_count_after_release),
                    committed_count: current_total_count,
                    committed_at: new_time_instant,
                };
            }

            return Ok(RateLimitDecision::Rejected {
                window_size_seconds: *self.window_size_seconds,
                retry_after_ms: new_ttl_ms as u128,
                remaining_after_waiting: new_count_after_release,
            });
        }

        let new_accept_limit = (new_window_limit as u64).saturating_sub(current_total_count);

        if let AbsoluteRedisLimitingState::Accepting {
            window_limit,
            accept_limit,
            starting_count,
            count,
            time_instant,
            last_rate_group_ttl,
            last_rate_group_count,
        } = state.deref()
        {
            *mutex_lock(window_limit, "accepting.window_limit")? = new_window_limit;
            *mutex_lock(accept_limit, "accepting.accept_limit")? = new_accept_limit;
            *mutex_lock(starting_count, "accepting.starting_count")? = redis_total;
            *mutex_lock(time_instant, "accepting.time_instant")? = new_time_instant;
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
                last_rate_group_ttl: Mutex::new(read_state_result.last_rate_group_ttl),
                last_rate_group_count: Mutex::new(read_state_result.last_rate_group_count),
            };
        }

        Ok(RateLimitDecision::Allowed)
    }

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
            ..
        } = state_entry.deref()
        {
            let accept_limit = *mutex_lock(accept_limit, "accepting.accept_limit")?;
            if count.load(Ordering::Acquire) + check_count <= accept_limit {
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
                ..
            } = state_entry.deref()
            {
                let local_count = count.load(Ordering::Acquire);
                let starting_count = *mutex_lock(starting_count, "accepting.starting_count")?;
                let window_limit = *mutex_lock(window_limit, "accepting.window_limit")?;
                let accept_limit = *mutex_lock(accept_limit, "accepting.accept_limit")?;

                if local_count + check_count <= accept_limit {
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
        self.redis_proxy.cleanup(stale_after_ms).await
    }

    async fn flush(&self) -> Result<(), TrypemaError> {
        let mut resets: Vec<RedisKey> = Vec::new();

        for state in self.limiting_state.iter() {
            let key = state.key();

            if let AbsoluteRedisLimitingState::Accepting { .. } = state.deref() {
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
