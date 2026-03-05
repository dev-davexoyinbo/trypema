use std::{
    ops::Deref,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use dashmap::DashMap;
use tokio::sync::mpsc;

use crate::{
    RateGroupSizeMs, RateLimit, RateLimitDecision, RedisKey, RedisRateLimiterOptions, TrypemaError,
    WindowSizeSeconds,
    hybrid::{
        AbsoluteHybridCommitterSignal, RedisCommitter, RedisCommitterOptions, SyncIntervalMs,
        absolute_hybrid_redis_proxy::{
            AbsoluteHybridCommit, AbsoluteHybridRedisProxy, AbsoluteHybridRedisProxyReadStateResult,
        },
        common::RedisRateLimiterSignal,
    },
    redis::{mutex_lock, spawn_task},
};

#[derive(Debug)]
enum AbsoluteRedisLimitingState {
    Accepting {
        window_limit: Mutex<u64>,
        accept_limit: Mutex<u64>,
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
    },
}

/// A rate limiter backed by Redis.
#[derive(Debug)]
pub struct AbsoluteHybridRateLimiter {
    window_size_seconds: WindowSizeSeconds,
    window_size_ms: u128,
    rate_group_size_ms: RateGroupSizeMs,
    commiter_sender: mpsc::Sender<AbsoluteHybridCommitterSignal<AbsoluteHybridCommit>>,
    redis_proxy: AbsoluteHybridRedisProxy,
    limiting_state: DashMap<RedisKey, AbsoluteRedisLimitingState>,
    sync_interval_ms: SyncIntervalMs,
}

impl AbsoluteHybridRateLimiter {
    pub(crate) fn new(options: RedisRateLimiterOptions) -> Arc<Self> {
        let prefix = options.prefix.unwrap_or_else(RedisKey::default_prefix);

        let (tx, rx) = tokio::sync::mpsc::channel::<RedisRateLimiterSignal>(1);

        let redis_proxy = AbsoluteHybridRedisProxy::new(prefix.clone(), options.connection_manager);

        let commiter_sender = RedisCommitter::run(RedisCommitterOptions {
            sync_interval: Duration::from_millis(*options.sync_interval_ms),
            channel_capacity: 8192,
            max_batch_size: 4,
            limiter_sender: tx,
            redis_proxy: Box::new(redis_proxy.clone()),
        });

        let limiter = Self {
            window_size_ms: (*options.window_size_seconds as u128).saturating_mul(1000),
            window_size_seconds: options.window_size_seconds,
            rate_group_size_ms: options.rate_group_size_ms,
            commiter_sender,
            redis_proxy,
            limiting_state: DashMap::new(),
            sync_interval_ms: options.sync_interval_ms,
        };

        let limiter = Arc::new(limiter);

        limiter.listen_for_committer_signals(rx);

        limiter
    } // end method with_rate_type

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

    /// Check admission and, if allowed, increment the observed count for `key`.
    pub async fn inc(
        &self,
        key: &RedisKey,
        rate_limit: &RateLimit,
        count: u64,
    ) -> Result<RateLimitDecision, TrypemaError> {
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
        self.is_allowed_with_count_increment(key, 1, 0, None).await
    } // end method is_allowed

    async fn reset_state_from_redis_read_result_and_get_decision(
        &self,
        key: &RedisKey,
        check_count: u64,
        increment: u64,
        rate_limit: Option<&RateLimit>,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let read_state_result = self
            .redis_proxy
            .read_state(key, *self.window_size_seconds as u128 * 1000)
            .await
            .map_err(|err| TrypemaError::CustomError(format!("Failed to read state: {err:?}")))?;

        self.reset_single_state_from_read_result(
            read_state_result,
            check_count,
            increment,
            rate_limit,
        )
        .await
    } // end method reset_state_from_redis_read_result

    async fn send_commit(&self, commit: AbsoluteHybridCommit) -> Result<(), TrypemaError> {
        self.commiter_sender
            .send(commit.into())
            .await
            .map_err(|err| TrypemaError::CustomError(format!("Failed to send commit: {err:?}")))?;

        Ok(())
    } // end method send_commit

    async fn reset_single_state_from_read_result(
        &self,
        read_state_result: AbsoluteHybridRedisProxyReadStateResult,
        check_count: u64,
        increment: u64,
        rate_limit: Option<&RateLimit>,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let mut current_total_count = read_state_result.current_total_count;
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
            AbsoluteRedisLimitingState::Undefined
            | AbsoluteRedisLimitingState::Rejecting { .. } => state,
            AbsoluteRedisLimitingState::Accepting {
                window_limit,
                count,
                ..
            } => {
                let count = count.load(Ordering::Relaxed);

                if current_window_limit.is_none() {
                    current_window_limit =
                        Some(*mutex_lock(window_limit, "accepting.window_limit")?);
                }

                if count > 0 {
                    current_total_count += count;

                    let commit = AbsoluteHybridCommit {
                        key: key.clone(),
                        window_size_seconds: *self.window_size_seconds,
                        window_limit: current_window_limit.expect("Window limit should be set"),
                        rate_group_size_ms: *self.rate_group_size_ms,
                        count,
                    };

                    drop(state);
                    self.send_commit(commit).await?;

                    self.limiting_state.get(key).expect("Key should be present")
                } else {
                    state
                }
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
                            .expect("key should be present") = AbsoluteRedisLimitingState::Undefined;
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
                .unwrap_or((*self.sync_interval_ms).min(*self.rate_group_size_ms));

            let new_count_after_release = read_state_result.last_rate_group_count.unwrap_or(0);

            if let AbsoluteRedisLimitingState::Rejecting {
                time_instant,
                ttl_ms,
                count_after_release,
            } = state.deref()
            {
                *mutex_lock(time_instant, "rejecting.time_instant")? = new_time_instant;
                *mutex_lock(ttl_ms, "rejecting.ttl_ms")? = new_ttl_ms;
                *mutex_lock(count_after_release, "rejecting.count_after_release")? =
                    new_count_after_release;
            } else {
                drop(state);
                *self
                    .limiting_state
                    .get_mut(key)
                    .expect("key should be present") = AbsoluteRedisLimitingState::Rejecting {
                    time_instant: Mutex::new(new_time_instant),
                    ttl_ms: Mutex::new(new_ttl_ms),
                    count_after_release: Mutex::new(new_count_after_release),
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
            count,
            time_instant,
            last_rate_group_ttl,
            last_rate_group_count,
        } = state.deref()
        {
            *mutex_lock(window_limit, "accepting.window_limit")? = new_window_limit;
            *mutex_lock(accept_limit, "accepting.accept_limit")? = new_accept_limit;
            count.store(increment, Ordering::Relaxed);
            *mutex_lock(time_instant, "accepting.time_instant")? = new_time_instant;
            *mutex_lock(last_rate_group_ttl, "accepting.last_rate_group_ttl")? =
                read_state_result.last_rate_group_ttl;
            *mutex_lock(last_rate_group_count, "accepting.last_rate_group_count")? =
                read_state_result.last_rate_group_count;
        } else {
            drop(state);
            *self
                .limiting_state
                .get_mut(key)
                .expect("key should be present") = AbsoluteRedisLimitingState::Accepting {
                window_limit: Mutex::new(new_window_limit),
                accept_limit: Mutex::new(new_accept_limit),
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

        if let AbsoluteRedisLimitingState::Accepting {
            accept_limit,
            count,
            ..
        } = state_entry.deref()
        {
            let accept_limit = *mutex_lock(accept_limit, "accepting.accept_limit")?;
            if count.load(Ordering::Relaxed) + check_count <= accept_limit {
                count.fetch_add(increment, Ordering::Relaxed);
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
                count,
                time_instant,
                last_rate_group_ttl,
                last_rate_group_count,
                ..
            } = state_entry.deref()
            {
                let current_total_count = count.load(Ordering::Relaxed);
                let window_limit = *mutex_lock(window_limit, "accepting.window_limit")?;

                if current_total_count > 0 {
                    let commit = AbsoluteHybridCommit {
                        key: key.clone(),
                        window_size_seconds: *self.window_size_seconds,
                        window_limit,
                        rate_group_size_ms: *self.rate_group_size_ms,
                        count: current_total_count,
                    };

                    permit.send(commit.into());
                }

                let last_rate_group_ttl: u128 =
                    (*mutex_lock(last_rate_group_ttl, "accepting.last_rate_group_ttl")?)
                        .map(|el| el as u128)
                        .unwrap_or((*self.sync_interval_ms).min(*self.rate_group_size_ms) as u128);
                let elapsed = mutex_lock(time_instant, "accepting.time_instant")?
                    .elapsed()
                    .as_millis();
                let retry_after_ms = last_rate_group_ttl.saturating_sub(elapsed);
                let remaining_after_waiting =
                    (*mutex_lock(last_rate_group_count, "accepting.last_rate_group_count")?)
                        .unwrap_or(current_total_count);

                // If we can still increment by at least one, then we don't set the state to
                // rejecting
                if current_total_count >= window_limit {
                    *state_entry = AbsoluteRedisLimitingState::Rejecting {
                        time_instant: Mutex::new(Instant::now()),
                        ttl_ms: Mutex::new(retry_after_ms as u64),
                        count_after_release: Mutex::new(remaining_after_waiting),
                    };
                }

                return Ok(RateLimitDecision::Rejected {
                    window_size_seconds: *self.window_size_seconds,
                    retry_after_ms,
                    remaining_after_waiting,
                });
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

        let read_state_results = self
            .redis_proxy
            .batch_read_state(&resets, self.window_size_ms)
            .await?;

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
