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
    HardLimitFactor, RateLimit, RateLimitDecision, RedisKey, RedisRateLimiterOptions,
    SuppressionFactorCacheMs, TrypemaError, WindowSizeSeconds,
    hybrid::{
        AbsoluteHybridCommitterSignal, RedisCommitter, RedisCommitterOptions,
        SuppressedHybridCommit, SuppressedHybridRedisProxy, SuppressedHybridRedisProxyOptions,
        SuppressedHybridRedisProxyReadStateResult, common::RedisRateLimiterSignal,
    },
    redis::{mutex_lock, spawn_task},
};

#[derive(Debug)]
enum SuppressedRedisLimitingState {
    Accepting {
        window_limit: Mutex<u64>,
        starting_count: Mutex<u64>,
        count: AtomicU64,
        time_instant: Mutex<Instant>,
    },
    Undefined,
    Suppressing {
        time_instant: Mutex<Instant>,
        window_limit: Mutex<u64>,
        suppression_factor: Mutex<f64>,
        suppression_factor_ttl_ms: Mutex<u64>,
        starting_count: Mutex<u64>,
        count: AtomicU64,
    },
}

/// A rate limiter backed by Redis.
#[derive(Debug)]
pub struct SuppressedHybridRateLimiter {
    window_size_seconds: WindowSizeSeconds,
    commiter_sender: mpsc::Sender<AbsoluteHybridCommitterSignal<SuppressedHybridCommit>>,
    redis_proxy: SuppressedHybridRedisProxy,
    limiting_state: DashMap<RedisKey, SuppressedRedisLimitingState>,
    hard_limit_factor: HardLimitFactor,
    suppression_factor_cache_ms: SuppressionFactorCacheMs,
}

impl SuppressedHybridRateLimiter {
    pub(crate) fn new(options: RedisRateLimiterOptions) -> Arc<Self> {
        let prefix = options.prefix.unwrap_or_else(RedisKey::default_prefix);

        let (tx, rx) = tokio::sync::mpsc::channel::<RedisRateLimiterSignal>(1);
        let hard_limit_factor = options.hard_limit_factor;
        let suppression_factor_cache_ms = options.suppression_factor_cache_ms;
        let window_size_seconds = options.window_size_seconds;
        let rate_group_size_ms = options.rate_group_size_ms;

        let redis_proxy = SuppressedHybridRedisProxy::new(SuppressedHybridRedisProxyOptions {
            prefix: prefix.clone(),
            connection_manager: options.connection_manager,
            hard_limit_factor,
            suppression_factor_cache_ms,
            rate_group_size_ms,
            window_size_seconds,
        });

        let commiter_sender = RedisCommitter::run(RedisCommitterOptions {
            sync_interval: Duration::from_millis(*options.sync_interval_ms),
            channel_capacity: 8192,
            max_batch_size: 4,
            limiter_sender: tx,
            redis_proxy: Box::new(redis_proxy.clone()),
        });

        let limiter = Self {
            window_size_seconds,
            commiter_sender,
            redis_proxy,
            limiting_state: DashMap::new(),
            hard_limit_factor,
            suppression_factor_cache_ms,
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
                    eprintln!(
                        "limiter arc does not exist anymore, breaking the committer signals loop"
                    );
                    break;
                };

                match signal {
                    RedisRateLimiterSignal::Flush => {
                        if let Err(err) = limiter.flush().await {
                            tracing::error!(error = ?err, "Failed to flush redis rate limiter");
                            eprintln!("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx flush failed");
                            eprintln!("Failed to flush redis rate limiter: {err:?}");
                        }
                    }
                }
            }
        });
    } // end method listen_for_committer_signals

    /// Determine whether `key` is currently allowed.
    ///
    /// Returns [`RateLimitDecision::Allowed`] if the current sliding window total
    /// is below the window limit, otherwise returns [`RateLimitDecision::Rejected`]
    /// with a best-effort `retry_after_ms`.
    ///
    /// This method performs lazy eviction of expired buckets for the key.
    pub async fn get_suppression_factor(&self, key: &RedisKey) -> Result<f64, TrypemaError> {
        let mut rng = |p: f64| rand::random_bool(p);

        let decision = self.inc_with_rng(key, 0, None, &mut rng).await?;

        match decision {
            RateLimitDecision::Allowed => Ok(0f64),
            RateLimitDecision::Rejected { .. } => {
                unreachable!("rejected should not be possible when using the suppressed strategy")
            }
            RateLimitDecision::Suppressed {
                suppression_factor, ..
            } => Ok(suppression_factor),
        }
    }

    /// Check admission and, if allowed, increment the observed count for `key`.
    pub async fn inc(
        &self,
        key: &RedisKey,
        rate_limit: &RateLimit,
        count: u64,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let mut rng = |p: f64| rand::random_bool(p);

        let decision = self
            .inc_with_rng(key, count, Some(rate_limit), &mut rng)
            .await?;

        Ok(decision)
    } // end method inc

    pub(crate) async fn inc_with_rng(
        &self,
        key: &RedisKey,
        increment: u64,
        rate_limit: Option<&RateLimit>,
        random_bool: &mut impl FnMut(f64) -> bool,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let state_entry = match self.limiting_state.get(key) {
            Some(state) => state,
            None => {
                self.limiting_state
                    .entry(key.clone())
                    .or_insert_with(|| SuppressedRedisLimitingState::Undefined);

                self.limiting_state.get(key).expect("Key should be present")
            }
        };

        if let SuppressedRedisLimitingState::Suppressing {
            time_instant,
            suppression_factor_ttl_ms,
            suppression_factor,
            count,
            starting_count,
            window_limit,
            ..
        } = state_entry.deref()
        {
            let window_limit = *mutex_lock(window_limit, "accepting.window_limit")?;
            let mut suppression_factor =
                *mutex_lock(suppression_factor, "suppressing.suppression_factor")?;
            let elapsed_ms = mutex_lock(time_instant, "suppressing.time_instant")?
                .elapsed()
                .as_millis();
            let ttl_ms = *mutex_lock(suppression_factor_ttl_ms, "suppressing.ttl_ms")? as u128;
            let starting_count = *mutex_lock(starting_count, "suppressing.starting_count")?;

            // Guard hard limit
            if starting_count.saturating_add(count.load(Ordering::Relaxed)) > window_limit {
                suppression_factor = 1f64;
            }

            let should_allow = if suppression_factor == 0f64 {
                true
            } else if suppression_factor == 1f64 {
                false
            } else {
                random_bool(1f64 - suppression_factor)
            };

            if elapsed_ms < ttl_ms {
                count.fetch_add(increment, Ordering::Relaxed);
                return Ok(RateLimitDecision::Suppressed {
                    suppression_factor,
                    is_allowed: should_allow,
                });
            }
        } else if let SuppressedRedisLimitingState::Accepting {
            window_limit,
            starting_count,
            count,
            ..
        } = state_entry.deref()
        {
            let starting_count = *mutex_lock(starting_count, "accepting.starting_count")?;
            let window_limit = *mutex_lock(window_limit, "accepting.accept_limit")?;
            let soft_window_limit = (window_limit as f64 / *self.hard_limit_factor) as u64;

            if starting_count
                .saturating_add(count.load(Ordering::Relaxed))
                .saturating_add(increment)
                < soft_window_limit
            {
                count.fetch_add(increment, Ordering::Relaxed);
                return Ok(RateLimitDecision::Allowed);
            }

            drop(state_entry);

            eprintln!("trying to reserve commiter sender .....");
            let permit = self.commiter_sender.reserve().await.map_err(|_| {
                TrypemaError::CustomError("Failed to reserve commiter sender".to_string())
            })?;
            eprintln!("reserved commiter sender ..... <<<<<<<");

            // since the permit can take some time, we need to check again if the key is still in the map
            let Some(state_entry) = self.limiting_state.get(key) else {
                unreachable!("Key should be present");
            };

            if let SuppressedRedisLimitingState::Accepting {
                window_limit,
                count,
                starting_count,
                ..
            } = state_entry.deref()
            {
                let starting_count = *mutex_lock(starting_count, "accepting.starting_count")?;
                let window_limit = *mutex_lock(window_limit, "accepting.window_limit")?;
                let soft_window_limit = (window_limit as f64 / *self.hard_limit_factor) as u64;

                if starting_count
                    .saturating_add(count.load(Ordering::Relaxed))
                    .saturating_add(increment)
                    >= soft_window_limit
                {
                    let count_value = count.swap(0, Ordering::Relaxed);
                    let current_total_count = starting_count.saturating_add(count_value);

                    drop(state_entry);

                    if count_value > 0 {
                        let commit = SuppressedHybridCommit {
                            key: key.clone(),
                            window_limit,
                            count: count_value,
                        };

                        permit.send(commit.into());
                    }

                    let Some(mut state_entry) = self.limiting_state.get_mut(key) else {
                        unreachable!("Key should be present");
                    };

                    let forcasted_total_count = current_total_count.saturating_add(increment);

                    let (suppression_factor, should_allow) = if forcasted_total_count > window_limit
                    {
                        (1f64, false)
                    } else if forcasted_total_count >= soft_window_limit
                        && soft_window_limit < window_limit
                        && forcasted_total_count < window_limit
                    {
                        (0f64, true)
                    } else {
                        (1f64, true)
                    };

                    *state_entry = SuppressedRedisLimitingState::Suppressing {
                        time_instant: Mutex::new(Instant::now()),
                        suppression_factor: Mutex::new(suppression_factor),
                        window_limit: Mutex::new(window_limit),
                        suppression_factor_ttl_ms: Mutex::new(*self.suppression_factor_cache_ms),
                        starting_count: Mutex::new(current_total_count),
                        count: AtomicU64::new(increment),
                    };

                    return Ok(RateLimitDecision::Suppressed {
                        suppression_factor,
                        is_allowed: should_allow,
                    });
                } else {
                    // we are still in the accepting state, so we need to set the starting_count to the current total count
                    count.fetch_add(increment, Ordering::Relaxed);
                    return Ok(RateLimitDecision::Allowed);
                }
            } else if let SuppressedRedisLimitingState::Suppressing {
                time_instant,
                suppression_factor_ttl_ms,
                suppression_factor,
                count,
                starting_count,
                window_limit,
                ..
            } = state_entry.deref()
            {
                let window_limit = *mutex_lock(window_limit, "accepting.window_limit")?;
                let mut suppression_factor =
                    *mutex_lock(suppression_factor, "suppressing.suppression_factor")?;
                let elapsed_ms = mutex_lock(time_instant, "suppressing.time_instant")?
                    .elapsed()
                    .as_millis();
                let ttl_ms = *mutex_lock(suppression_factor_ttl_ms, "suppressing.ttl_ms")? as u128;
                let starting_count = *mutex_lock(starting_count, "suppressing.starting_count")?;

                // Guard hard limit
                if starting_count.saturating_add(count.load(Ordering::Relaxed)) > window_limit {
                    suppression_factor = 1f64;
                }

                let should_allow = if suppression_factor == 0f64 {
                    true
                } else if suppression_factor == 1f64 {
                    false
                } else {
                    random_bool(1f64 - suppression_factor)
                };

                if elapsed_ms < ttl_ms {
                    count.fetch_add(increment, Ordering::Relaxed);
                    return Ok(RateLimitDecision::Suppressed {
                        suppression_factor,
                        is_allowed: should_allow,
                    });
                }
            }

            drop(state_entry);
        } else {
            drop(state_entry);
        }

        self.reset_state_from_redis_read_result_and_get_decision(
            key,
            increment,
            increment,
            rate_limit,
            random_bool,
        )
        .await
    } // end method 

    async fn reset_state_from_redis_read_result_and_get_decision(
        &self,
        key: &RedisKey,
        check_count: u64,
        increment: u64,
        rate_limit: Option<&RateLimit>,
        random_bool: &mut impl FnMut(f64) -> bool,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let read_state_result =
            self.redis_proxy.read_state(key).await.map_err(|err| {
                TrypemaError::CustomError(format!("Failed to read state: {err:?}"))
            })?;

        self.reset_single_state_from_read_result(
            read_state_result,
            check_count,
            increment,
            rate_limit,
            random_bool,
        )
        .await
    } // end method reset_state_from_redis_read_result

    async fn send_commit(&self, commit: SuppressedHybridCommit) -> Result<(), TrypemaError> {
        eprintln!("xxxx -> sending commit");
        self.commiter_sender
            .send(commit.into())
            .await
            .map_err(|err| TrypemaError::CustomError(format!("Failed to send commit: {err:?}")))?;
        eprintln!("<<<<<< commit sent successfully");

        Ok(())
    } // end method send_commit

    async fn reset_single_state_from_read_result(
        &self,
        read_state_result: SuppressedHybridRedisProxyReadStateResult,
        check_count: u64,
        increment: u64,
        rate_limit: Option<&RateLimit>,
        random_bool: &mut impl FnMut(f64) -> bool,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let mut current_total_count = read_state_result.current_total_count;
        let mut current_suppression_factor_ttl_ms = read_state_result.suppression_factor_ttl_ms;
        let current_suppression_factor = read_state_result.suppression_factor;

        let mut hard_window_limit = read_state_result.window_limit;
        let key = &read_state_result.key;

        eprintln!("rs 11..................");
        let state = match self.limiting_state.get(key) {
            Some(state) => state,
            None => {
                self.limiting_state
                    .entry(key.clone())
                    .or_insert_with(|| SuppressedRedisLimitingState::Undefined);

                self.limiting_state.get(key).expect("Key should be present")
            }
        };

        eprintln!("rs 22..................");

        let state = match state.deref() {
            SuppressedRedisLimitingState::Undefined => state,
            SuppressedRedisLimitingState::Suppressing {
                suppression_factor_ttl_ms,
                count,
                window_limit,
                ..
            } => {
                if current_suppression_factor_ttl_ms.is_none() {
                    current_suppression_factor_ttl_ms = Some(*mutex_lock(
                        suppression_factor_ttl_ms,
                        "suppressing.suppression_factor_ttl_ms",
                    )?);
                }

                if hard_window_limit.is_none() {
                    hard_window_limit = Some(*mutex_lock(window_limit, "accepting.window_limit")?);
                }

                let count = count.load(Ordering::Relaxed);

                if count > 0 {
                    current_total_count = current_total_count.saturating_add(count);

                    let commit = SuppressedHybridCommit {
                        key: key.clone(),
                        window_limit: hard_window_limit.expect("Window limit should be set"),
                        count,
                    };

                    drop(state);
                    self.send_commit(commit).await?;

                    self.limiting_state.get(key).expect("Key should be present")
                } else {
                    state
                }
            }

            SuppressedRedisLimitingState::Accepting {
                window_limit,
                count,
                ..
            } => {
                let count = count.load(Ordering::Relaxed);

                if hard_window_limit.is_none() {
                    hard_window_limit = Some(*mutex_lock(window_limit, "accepting.window_limit")?);
                }

                if count > 0 {
                    current_total_count = current_total_count.saturating_add(count);

                    let commit = SuppressedHybridCommit {
                        key: key.clone(),
                        window_limit: hard_window_limit.expect("Window limit should be set"),
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

        eprintln!("rs 33..................");

        let hard_window_limit = match hard_window_limit {
            Some(window_limit) => window_limit,
            None => {
                let Some(rate_limit) = rate_limit else {
                    let is_undefined =
                        matches!(state.deref(), SuppressedRedisLimitingState::Undefined);

                    if !is_undefined {
                        drop(state);
                        *self
                            .limiting_state
                            .get_mut(key)
                            .expect("Key should be present") =
                            SuppressedRedisLimitingState::Undefined;
                    }

                    return Ok(RateLimitDecision::Allowed);
                };

                ((*self.window_size_seconds as f64) * **rate_limit * *self.hard_limit_factor) as u64
            }
        };

        eprintln!("rs 44..................");

        let soft_window_limit = (hard_window_limit as f64 / *self.hard_limit_factor) as u64;
        let new_ttl_ms =
            current_suppression_factor_ttl_ms.unwrap_or(*self.suppression_factor_cache_ms);

        let new_time_instant = Instant::now();

        if current_total_count.saturating_add(check_count) > soft_window_limit {
            eprintln!("rs 45..................");
            if let SuppressedRedisLimitingState::Suppressing {
                time_instant,
                suppression_factor,
                suppression_factor_ttl_ms,
                starting_count,
                count,
                ..
            } = state.deref()
            {
                *mutex_lock(time_instant, "suppressing.time_instant")? = new_time_instant;
                *mutex_lock(suppression_factor_ttl_ms, "suppressing.ttl_ms")? = new_ttl_ms;
                *mutex_lock(suppression_factor, "suppressing.suppression_factor")? =
                    current_suppression_factor;
                *mutex_lock(starting_count, "suppressing.starting_count")? = current_total_count;
                count.store(increment, Ordering::Relaxed);
            } else {
                drop(state);
                let mut state = self
                    .limiting_state
                    .get_mut(key)
                    .expect("Key should be present");

                *state = SuppressedRedisLimitingState::Suppressing {
                    time_instant: Mutex::new(new_time_instant),
                    window_limit: Mutex::new(hard_window_limit),
                    suppression_factor: Mutex::new(current_suppression_factor),
                    suppression_factor_ttl_ms: Mutex::new(new_ttl_ms),
                    starting_count: Mutex::new(current_total_count),
                    count: AtomicU64::new(increment),
                };
            }

            let should_allow = if current_suppression_factor == 0f64 {
                true
            } else if current_suppression_factor == 1f64 {
                false
            } else {
                random_bool(1f64 - current_suppression_factor)
            };

            return Ok(RateLimitDecision::Suppressed {
                suppression_factor: current_suppression_factor,
                is_allowed: should_allow,
            });
        }

        eprintln!("rs 46..................");

        if let SuppressedRedisLimitingState::Accepting {
            window_limit,
            count,
            time_instant,
            starting_count,
            ..
        } = state.deref()
        {
            eprintln!("rs 47..................");
            *mutex_lock(window_limit, "accepting.window_limit")? = hard_window_limit;
            *mutex_lock(time_instant, "accepting.time_instant")? = new_time_instant;
            *mutex_lock(starting_count, "accepting.starting_count")? = current_total_count;
            count.store(increment, Ordering::Relaxed);
        } else {
            eprintln!("rs 48..................");
            drop(state);
            eprintln!("rs 49..................");
            let mut state = self
                .limiting_state
                .get_mut(key)
                .expect("Key should be present");

            *state = SuppressedRedisLimitingState::Accepting {
                window_limit: Mutex::new(hard_window_limit),
                time_instant: Mutex::new(new_time_instant),
                starting_count: Mutex::new(current_total_count),
                count: AtomicU64::new(increment),
            };
        }

        eprintln!("rs 55..................");

        Ok(RateLimitDecision::Allowed)
    }

    /// Evict expired buckets and update the total count.
    pub(crate) async fn cleanup(&self, stale_after_ms: u64) -> Result<(), TrypemaError> {
        self.redis_proxy.cleanup(stale_after_ms).await
    }

    async fn flush(&self) -> Result<(), TrypemaError> {
        let mut resets: Vec<RedisKey> = Vec::new();
        eprintln!("flushing <<<<<<<<<<<<<<<<<<<<<<<<<<<<");

        for state in self.limiting_state.iter() {
            let key = state.key();

            if let SuppressedRedisLimitingState::Accepting { .. }
            | SuppressedRedisLimitingState::Suppressing { .. } = state.deref()
            {
                resets.push(key.clone());
            }
        }

        eprintln!("111111 ....................................................");

        if resets.is_empty() {
            eprintln!("flushed complete >>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            return Ok(());
        }

        eprintln!("reset size {}", resets.len());

        eprintln!("22222 ....................................................");

        let read_state_results = self.redis_proxy.batch_read_state(&resets).await?;

        eprintln!("33333 ....................................................");

        let mut rng = |p: f64| rand::random_bool(p);

        for result in read_state_results {
            if let Err(err) = self
                .reset_single_state_from_read_result(result, 0, 0, None, &mut rng)
                .await
            {
                tracing::error!(error = ?err, "Failed to reset state from redis read result");
                eprintln!("Failed to reset state from redis read result: {err:?}");
                continue;
            }
        }

        eprintln!("44444 ....................................................");

        eprintln!("flushed complete >>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        Ok(())
    } // end method flush

    fn _calculate_suppression_factor(
        &self,
        hard_window_limit: u64,
        count: u64,
        last_second_count: u64,
    ) -> f64 {
        if count < (hard_window_limit as f64 / *self.hard_limit_factor) as u64 {
            return 0f64;
        } else if count > hard_window_limit {
            return 1f64;
        }

        let average_rate_in_window = count as f64 / *self.window_size_seconds as f64;
        let perceived_rate_limit = average_rate_in_window.max(last_second_count as f64);

        let rate_limit =
            hard_window_limit as f64 / *self.window_size_seconds as f64 / *self.hard_limit_factor;

        1f64 - (rate_limit / perceived_rate_limit)
    }
}
