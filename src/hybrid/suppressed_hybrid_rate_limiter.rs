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
    HardLimitFactor, RateLimit, RateLimitDecision, RedisKey, RedisRateLimiterOptions,
    SuppressionFactorCacheMs, TrypemaError, WindowSizeSeconds,
    hybrid::{
        AbsoluteHybridCommitterSignal, RedisCommitter, RedisCommitterOptions,
        SuppressedHybridCommit, SuppressedHybridRedisProxy, SuppressedHybridRedisProxyOptions,
        SuppressedHybridRedisProxyReadStateResult,
        common::{EPOCH_CHANGE_INTERVAL, RedisRateLimiterSignal},
    },
    redis::{mutex_lock, spawn_task},
    runtime,
};

#[derive(Debug)]
enum SuppressedRedisLimitingState {
    Accepting {
        window_limit: Mutex<u64>,
        starting_count: Mutex<u64>,
        count: AtomicU64,
        declined_count: Mutex<u64>,
        time_instant: Mutex<Instant>,
        // TODO: Add last modified
    },
    Undefined,
    Suppressing {
        time_instant: Mutex<Instant>,
        window_limit: Mutex<u64>,
        suppression_factor: Mutex<f64>,
        suppression_factor_ttl_ms: Mutex<u64>,
        starting_count: Mutex<u64>,
        count: AtomicU64,
        declined_count: AtomicU64,
        // TODO: Add last modified
    },
}

/// Probabilistic suppression rate limiter with a local fast-path and periodic Redis sync.
///
/// This is the hybrid counterpart to [`SuppressedRedisRateLimiter`](crate::redis::SuppressedRedisRateLimiter).
/// Like the absolute hybrid limiter, it maintains local in-memory state per key and
/// periodically flushes accumulated increments (both observed and declined) to Redis.
///
/// # State Machine
///
/// Each key transitions through three states:
///
/// - **`Undefined`** — No local state exists. The first call reads Redis to initialise.
///
/// - **`Accepting`** — Below capacity. All requests are allowed from local state with
///   no Redis I/O. When the local counter exceeds the soft capacity, it commits to Redis
///   and transitions based on the result.
///
/// - **`Suppressing`** — At or above capacity. Probabilistic admission based on the
///   suppression factor from the last Redis read. Continues until the local state is
///   flushed and refreshed from Redis.
///
/// # Thundering Herd Prevention
///
/// Same per-key `tokio::sync::Mutex` mechanism as the absolute hybrid limiter.
#[derive(Debug)]
pub struct SuppressedHybridRateLimiter {
    window_size_seconds: WindowSizeSeconds,
    commiter_sender: mpsc::Sender<AbsoluteHybridCommitterSignal<SuppressedHybridCommit>>,
    redis_proxy: SuppressedHybridRedisProxy,
    limiting_state: DashMap<RedisKey, SuppressedRedisLimitingState>,
    hard_limit_factor: HardLimitFactor,
    suppression_factor_cache_ms: SuppressionFactorCacheMs,
    epoch: AtomicU64,
    last_commited_epoch: AtomicU64,
    is_active_watch: watch::Sender<u64>,
    reset_locks: DashMap<RedisKey, Arc<tokio::sync::Mutex<()>>>,
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
            window_size_seconds,
            commiter_sender,
            redis_proxy,
            limiting_state: DashMap::new(),
            hard_limit_factor,
            suppression_factor_cache_ms,
            is_active_watch,
            epoch: AtomicU64::new(0),
            last_commited_epoch: AtomicU64::new(0),
            reset_locks: DashMap::new(),
        };

        let limiter = Arc::new(limiter);

        limiter.listen_for_committer_signals(rx);
        limiter.epoch_change_task();

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

    #[inline(always)]
    fn send_epoch_change_if_needed(&self) {
        let epoch = self.epoch.load(Ordering::Relaxed);

        if self.last_commited_epoch.load(Ordering::Relaxed) < epoch {
            let _ = self.is_active_watch.send(epoch);
            self.last_commited_epoch.store(epoch, Ordering::Relaxed);
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
            .clone()
    }

    /// Get the current suppression factor for `key`.
    ///
    /// Returns a value in the range `[0.0, 1.0]`:
    /// - `0.0` — no suppression (below capacity or key not found)
    /// - `0.0 < sf < 1.0` — partial suppression (at capacity)
    /// - `1.0` — full suppression (over hard limit)
    ///
    /// On the fast-path (key in `Suppressing` state with a fresh cached factor), this is
    /// served entirely from local state with no Redis I/O. Otherwise, it performs a Redis
    /// read to refresh the state.
    ///
    /// This method is read-only with respect to request counts. It is useful for metrics
    /// and observability dashboards.
    pub async fn get_suppression_factor(&self, key: &RedisKey) -> Result<f64, TrypemaError> {
        self.send_epoch_change_if_needed();

        let state = match self.limiting_state.get(key) {
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
        } = state.deref()
        {
            let window_limit = *mutex_lock(window_limit, "suppressing.window_limit")?;
            let mut suppression_factor =
                *mutex_lock(suppression_factor, "suppressing.suppression_factor")?;
            let elapsed_ms = mutex_lock(time_instant, "suppressing.time_instant")?
                .elapsed()
                .as_millis();
            let ttl_ms = *mutex_lock(suppression_factor_ttl_ms, "suppressing.ttl_ms")? as u128;
            let starting_count = *mutex_lock(starting_count, "suppressing.starting_count")?;

            // Guard hard limit
            if starting_count.saturating_add(count.load(Ordering::Acquire)) > window_limit {
                suppression_factor = 1f64;
            }

            if elapsed_ms < ttl_ms {
                return Ok(suppression_factor);
            }
        } else if let SuppressedRedisLimitingState::Accepting {
            window_limit,
            count,
            starting_count,
            declined_count,
            ..
        } = state.deref()
        {
            let window_limit = *mutex_lock(window_limit, "accepting.window_limit")?;
            let soft_window_limit = (window_limit as f64 / *self.hard_limit_factor) as u64;
            let starting_count = *mutex_lock(starting_count, "accepting.starting_count")?;
            let declined = *mutex_lock(declined_count, "accepting.declined_count")?;
            let effective_total = starting_count
                .saturating_add(count.load(Ordering::Acquire))
                .saturating_sub(declined);

            let suppression_factor = if effective_total < soft_window_limit {
                0f64
            } else if effective_total == soft_window_limit && soft_window_limit == window_limit {
                // soft == hard: full suppression.
                1f64
            } else {
                // soft < hard: local state hasn't been committed to Redis yet so we return 0 as a
                // conservative approximation (Redis read will give the accurate value).
                0f64
            };
            return Ok(suppression_factor);
        }

        drop(state);

        let mut rng = |p: f64| rand::random_bool(p);

        let decision = self
            .reset_state_from_redis_read_result_and_get_decision(key, 0, 0, None, &mut rng)
            .await?;

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

    /// Check admission and record the increment for `key` using probabilistic suppression.
    ///
    /// On the fast-path (key in `Accepting` state with capacity remaining), this is a
    /// single atomic increment with no Redis I/O. When local capacity is exhausted or the
    /// key enters the `Suppressing` state, probabilistic admission is applied based on the
    /// suppression factor from the last Redis read.
    ///
    /// # Arguments
    ///
    /// - `key`: Validated [`RedisKey`] identifying the rate-limited resource
    /// - `rate_limit`: Per-second rate limit (sticky — stored on first call per key)
    /// - `count`: Amount to increment (typically `1`)
    ///
    /// # Returns
    ///
    /// - `Ok(Allowed)` — below capacity, no suppression active
    /// - `Ok(Suppressed { is_allowed, suppression_factor })` — at/above capacity, check `is_allowed`
    /// - `Err(TrypemaError)` — Redis error during state refresh
    ///
    /// The total observed counter is always incremented. If `is_allowed` is `false`,
    /// the declined counter is also incremented.
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
        self.send_epoch_change_if_needed();

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
            declined_count,
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
            if starting_count.saturating_add(count.load(Ordering::Acquire)) > window_limit {
                suppression_factor = 1f64;
            }

            if elapsed_ms < ttl_ms {
                let should_allow = if suppression_factor == 0f64 {
                    true
                } else if suppression_factor == 1f64 {
                    false
                } else {
                    random_bool(1f64 - suppression_factor)
                };

                count.fetch_add(increment, Ordering::AcqRel);

                if !should_allow {
                    declined_count.fetch_add(increment, Ordering::AcqRel);
                }

                return Ok(RateLimitDecision::Suppressed {
                    suppression_factor,
                    is_allowed: should_allow,
                });
            }
        } else if let SuppressedRedisLimitingState::Accepting {
            window_limit,
            starting_count,
            count,
            declined_count,
            ..
        } = state_entry.deref()
        {
            // TODO: Before performing the check, check if the values here are stale
            // stale would mean the time elapsed is more than the flush interval

            let starting_count = *mutex_lock(starting_count, "accepting.starting_count")?;
            let window_limit = *mutex_lock(window_limit, "accepting.accept_limit")?;
            let declined = *mutex_lock(declined_count, "accepting.declined_count")?;
            let soft_window_limit = (window_limit as f64 / *self.hard_limit_factor) as u64;

            if starting_count
                .saturating_add(count.load(Ordering::Acquire))
                .saturating_sub(declined)
                .saturating_add(increment)
                <= soft_window_limit
            {
                count.fetch_add(increment, Ordering::AcqRel);
                return Ok(RateLimitDecision::Allowed);
            }
        }

        drop(state_entry);

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
            if let Some(ref state_entry) = state_entry
                && let SuppressedRedisLimitingState::Accepting {
                    window_limit,
                    count,
                    starting_count,
                    declined_count,
                    ..
                } = state_entry.deref()
            {
                // TODO: Before performing the check, check if the values here are stale
                // stale would mean the time elapsed is more than the flush interval
                let starting_count = *mutex_lock(starting_count, "accepting.starting_count")?;
                let window_limit = *mutex_lock(window_limit, "accepting.accept_limit")?;
                let declined = *mutex_lock(declined_count, "accepting.declined_count")?;
                let soft_window_limit = (window_limit as f64 / *self.hard_limit_factor) as u64;

                if starting_count
                    .saturating_add(count.load(Ordering::Acquire))
                    .saturating_sub(declined)
                    .saturating_add(increment)
                    <= soft_window_limit
                {
                    count.fetch_add(increment, Ordering::AcqRel);
                    return Ok(RateLimitDecision::Allowed);
                }
                // Still over limit after lock — fall through to Redis read.
            } else if let Some(state_entry) = state_entry
                && let SuppressedRedisLimitingState::Suppressing {
                    time_instant,
                    suppression_factor_ttl_ms,
                    suppression_factor,
                    count,
                    declined_count,
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
                if starting_count.saturating_add(count.load(Ordering::Acquire)) > window_limit {
                    suppression_factor = 1f64;
                }

                if elapsed_ms < ttl_ms {
                    let should_allow = if suppression_factor == 0f64 {
                        true
                    } else if suppression_factor == 1f64 {
                        false
                    } else {
                        random_bool(1f64 - suppression_factor)
                    };

                    count.fetch_add(increment, Ordering::AcqRel);

                    if !should_allow {
                        declined_count.fetch_add(increment, Ordering::AcqRel);
                    }

                    return Ok(RateLimitDecision::Suppressed {
                        suppression_factor,
                        is_allowed: should_allow,
                    });
                }
                // Suppression has expired - fail through to Redis read.
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
            random_bool,
        )
        .await
    } // end method reset_state_from_redis_read_result

    async fn send_commit(&self, commit: SuppressedHybridCommit) -> Result<(), TrypemaError> {
        self.commiter_sender
            .send(commit.into())
            .await
            .map_err(|err| TrypemaError::CustomError(format!("Failed to send commit: {err:?}")))?;

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
        let mut current_declined_count = read_state_result.current_total_declined;

        let mut current_suppression_factor_ttl_ms = read_state_result.suppression_factor_ttl_ms;
        let current_suppression_factor = read_state_result.suppression_factor;

        let mut hard_window_limit = read_state_result.window_limit;
        let key = &read_state_result.key;

        let state = match self.limiting_state.get(key) {
            Some(state) => state,
            None => {
                self.limiting_state
                    .entry(key.clone())
                    .or_insert_with(|| SuppressedRedisLimitingState::Undefined);

                self.limiting_state.get(key).expect("Key should be present")
            }
        };

        let state = match state.deref() {
            SuppressedRedisLimitingState::Undefined => state,
            SuppressedRedisLimitingState::Suppressing {
                suppression_factor_ttl_ms,
                count,
                declined_count,
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

                let count = count.load(Ordering::Acquire);
                let declined = declined_count.load(Ordering::Acquire);

                current_total_count = current_total_count.saturating_add(count);
                current_declined_count = current_declined_count.saturating_add(declined);

                if count > 0 {
                    let commit = SuppressedHybridCommit {
                        key: key.clone(),
                        window_limit: hard_window_limit.expect("Window limit should be set"),
                        count,
                        declined_count: declined,
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
                let count = count.load(Ordering::Acquire);

                if hard_window_limit.is_none() {
                    hard_window_limit = Some(*mutex_lock(window_limit, "accepting.window_limit")?);
                }

                current_total_count = current_total_count.saturating_add(count);

                if count > 0 {
                    let commit = SuppressedHybridCommit {
                        key: key.clone(),
                        window_limit: hard_window_limit.expect("Window limit should be set"),
                        count,
                        declined_count: 0,
                    };

                    drop(state);
                    self.send_commit(commit).await?;

                    self.limiting_state.get(key).expect("Key should be present")
                } else {
                    state
                }
            }
        };

        let hard_window_limit = match hard_window_limit {
            Some(window_limit) => window_limit,
            None => {
                let Some(rate_limit) = rate_limit else {
                    let is_undefined =
                        matches!(state.deref(), SuppressedRedisLimitingState::Undefined);

                    if !is_undefined {
                        drop(state);
                        let mut state = self
                            .limiting_state
                            .get_mut(key)
                            .expect("Key should be present");
                        *state = SuppressedRedisLimitingState::Undefined;
                    }

                    return Ok(RateLimitDecision::Allowed);
                };

                ((*self.window_size_seconds as f64) * **rate_limit * *self.hard_limit_factor) as u64
            }
        };

        let soft_window_limit = (hard_window_limit as f64 / *self.hard_limit_factor) as u64;
        let new_ttl_ms =
            current_suppression_factor_ttl_ms.unwrap_or(*self.suppression_factor_cache_ms);

        let new_time_instant = Instant::now();

        if current_total_count
            .saturating_sub(current_declined_count)
            .saturating_add(check_count)
            > soft_window_limit
        {
            let current_suppression_factor = if current_total_count
                .saturating_sub(current_declined_count)
                >= hard_window_limit
            {
                1f64
            } else {
                current_suppression_factor
            };

            let should_allow = if current_suppression_factor == 0f64 {
                true
            } else if current_suppression_factor == 1f64 {
                false
            } else {
                random_bool(1f64 - current_suppression_factor)
            };

            let new_declined = if should_allow { 0 } else { increment };

            if let SuppressedRedisLimitingState::Suppressing {
                time_instant,
                suppression_factor,
                suppression_factor_ttl_ms,
                starting_count,
                count,
                declined_count,
                ..
            } = state.deref()
            {
                *mutex_lock(time_instant, "suppressing.time_instant")? = new_time_instant;
                *mutex_lock(suppression_factor_ttl_ms, "suppressing.ttl_ms")? = new_ttl_ms;
                *mutex_lock(suppression_factor, "suppressing.suppression_factor")? =
                    current_suppression_factor;
                *mutex_lock(starting_count, "suppressing.starting_count")? = current_total_count;
                count.store(increment, Ordering::Release);
                declined_count.store(new_declined, Ordering::Release);

                drop(state);
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
                    declined_count: AtomicU64::new(new_declined),
                };
            }

            return Ok(RateLimitDecision::Suppressed {
                suppression_factor: current_suppression_factor,
                is_allowed: should_allow,
            });
        }

        if let SuppressedRedisLimitingState::Accepting {
            window_limit,
            count,
            time_instant,
            starting_count,
            declined_count,
        } = state.deref()
        {
            *mutex_lock(window_limit, "accepting.window_limit")? = hard_window_limit;
            *mutex_lock(time_instant, "accepting.time_instant")? = new_time_instant;
            *mutex_lock(starting_count, "accepting.starting_count")? = current_total_count;
            *mutex_lock(declined_count, "accepting.declined_count")? = current_declined_count;
            count.store(increment, Ordering::Release);

            drop(state);
        } else {
            drop(state);
            let mut state = self
                .limiting_state
                .get_mut(key)
                .expect("Key should be present");

            *state = SuppressedRedisLimitingState::Accepting {
                window_limit: Mutex::new(hard_window_limit),
                time_instant: Mutex::new(new_time_instant),
                starting_count: Mutex::new(current_total_count),
                declined_count: Mutex::new(current_declined_count),
                count: AtomicU64::new(increment),
            };
        }

        Ok(RateLimitDecision::Allowed)
    }

    /// Evict expired buckets and update the total count.
    pub(crate) async fn cleanup(&self, stale_after_ms: u64) -> Result<(), TrypemaError> {
        self.redis_proxy.cleanup(stale_after_ms).await?;
        self.limiting_state.clear();
        Ok(())
    }

    async fn flush(&self) -> Result<(), TrypemaError> {
        let mut resets: Vec<RedisKey> = Vec::new();

        for state in self.limiting_state.iter() {
            let key = state.key();
            // TODO: only flush the keys that needs to be flushed, eg, an inactive key should be
            // ignored. We would compare last modified time with the current time.
            // If we don't have any count for both Accepting and Suppressing, we can skip it.

            if let SuppressedRedisLimitingState::Accepting { .. }
            | SuppressedRedisLimitingState::Suppressing { .. } = state.deref()
            {
                resets.push(key.clone());
            }
        }

        if resets.is_empty() {
            return Ok(());
        }

        let read_state_results = self.redis_proxy.batch_read_state(&resets).await?;

        let mut rng = |p: f64| rand::random_bool(p);

        for result in read_state_results {
            if let Err(err) = self
                .reset_single_state_from_read_result(result, 0, 0, None, &mut rng)
                .await
            {
                tracing::error!(error = ?err, "Failed to reset state from redis read result");
                continue;
            }
        }

        Ok(())
    } // end method flush
}
