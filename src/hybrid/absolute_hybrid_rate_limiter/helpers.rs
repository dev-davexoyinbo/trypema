use super::*;

#[derive(Debug)]
struct AcceptingTransition {
    commit: AbsoluteHybridCommit,
    previous_accept_limit: u64,
    retry_after_ms: u128,
    remaining_after_waiting: u64,
    committed_total: u64,
    last_synced: Instant,
}

#[derive(Debug)]
enum LocalAdmission {
    Allowed,
    Rejected(RateLimitDecision),
    Refresh,
    Exhausted(AcceptingTransition),
}

#[derive(Debug)]
pub(super) struct ResolvedRedisState {
    pub(super) key: RedisKey,
    pub(super) current_total: u64,
    pub(super) window_limit: Option<f64>,
    pub(super) oldest_bucket_ttl: Option<u64>,
    pub(super) oldest_bucket_count: Option<u64>,
    pub(super) state_revision: StateRevision,
}

impl AbsoluteHybridRateLimiter {
    fn evaluate_local_state(
        &self,
        key: &RedisKey,
        state: &AbsoluteRedisLimitingState,
        check_count: u64,
        increment: u64,
        mode: CallMode,
    ) -> Result<LocalAdmission, TrypemaError> {
        match state {
            AbsoluteRedisLimitingState::Undefined => Ok(LocalAdmission::Refresh),
            AbsoluteRedisLimitingState::Absent { last_synced } => {
                if mode == CallMode::ReadOnly && last_synced.elapsed() < self.allowed_lag {
                    return Ok(LocalAdmission::Allowed);
                }

                Ok(LocalAdmission::Refresh)
            }
            AbsoluteRedisLimitingState::Rejecting {
                time_instant,
                ttl_ms,
                count_after_release,
                last_synced,
                ..
            } => {
                // With the setting on, a read-only call also bounds the rejection cache by the
                // allowed lag. A recording call keeps the retry TTL rule and reads no clock here.
                if mode == CallMode::ReadOnly
                    && self.allowed_lag_applies_to_rejections
                    && mutex_lock(last_synced, "rejecting.last_synced")?.elapsed()
                        >= self.allowed_lag
                {
                    return Ok(LocalAdmission::Refresh);
                }

                let elapsed_ms = mutex_lock(time_instant, "rejecting.time_instant")?
                    .elapsed()
                    .as_millis();
                let ttl_ms = *mutex_lock(ttl_ms, "rejecting.ttl_ms")? as u128;

                if elapsed_ms >= ttl_ms {
                    return Ok(LocalAdmission::Refresh);
                }

                let remaining_after_waiting =
                    *mutex_lock(count_after_release, "rejecting.count_after_release")?;

                Ok(LocalAdmission::Rejected(RateLimitDecision::Rejected {
                    window_size: self.window_size,
                    retry_after: duration_from_milliseconds(ttl_ms.saturating_sub(elapsed_ms)),
                    remaining_after_waiting,
                }))
            }
            AbsoluteRedisLimitingState::Accepting {
                window_limit,
                accept_limit,
                starting_count,
                count,
                last_synced,
                oldest_bucket_ttl,
                oldest_bucket_count,
                last_modified,
                state_revision,
            } => {
                // A read-only call records no activity and answers only from local state that
                // is younger than the allowed lag. The recording fast path reads no clock here.
                if mode == CallMode::ReadOnly
                    && mutex_lock(last_synced, "accepting.last_synced")?.elapsed()
                        >= self.allowed_lag
                {
                    return Ok(LocalAdmission::Refresh);
                }

                let local_count = count.load(Ordering::Acquire);
                let accept_limit = *mutex_lock(accept_limit, "accepting.accept_limit")?;

                if check_count <= accept_limit.saturating_sub(local_count) {
                    if mode == CallMode::Recording {
                        *mutex_lock(last_modified, "accepting.last_modified")? = Instant::now();
                        count.fetch_add(increment, Ordering::AcqRel);
                    }

                    return Ok(LocalAdmission::Allowed);
                }

                let starting_count = *mutex_lock(starting_count, "accepting.starting_count")?;
                let committed_total = starting_count.saturating_add(local_count);
                let last_synced = *mutex_lock(last_synced, "accepting.last_synced")?;
                let elapsed_ms = last_synced.elapsed().as_millis();
                let retry_after_ms =
                    match *mutex_lock(oldest_bucket_ttl, "accepting.oldest_bucket_ttl")? {
                        Some(oldest_bucket_ttl) => {
                            (oldest_bucket_ttl as u128).saturating_sub(elapsed_ms)
                        }
                        None => self.window_size.as_milliseconds(),
                    };
                let remaining_after_waiting =
                    (*mutex_lock(oldest_bucket_count, "accepting.oldest_bucket_count")?)
                        .unwrap_or(committed_total);

                if local_count < accept_limit {
                    return Ok(LocalAdmission::Rejected(RateLimitDecision::Rejected {
                        window_size: self.window_size,
                        retry_after: duration_from_milliseconds(retry_after_ms),
                        remaining_after_waiting,
                    }));
                }

                Ok(LocalAdmission::Exhausted(AcceptingTransition {
                    commit: AbsoluteHybridCommit {
                        key: key.clone(),
                        window_limit: *mutex_lock(window_limit, "accepting.window_limit")?,
                        count: local_count,
                        state_revision: *mutex_lock(state_revision, "accepting.state_revision")?,
                    },
                    previous_accept_limit: accept_limit,
                    retry_after_ms,
                    remaining_after_waiting,
                    committed_total,
                    last_synced,
                }))
            }
        }
    }

    fn restore_accepting_state(
        &self,
        commit: &AbsoluteHybridCommit,
        previous_accept_limit: u64,
    ) -> Result<(), TrypemaError> {
        if let Entry::Occupied(mut entry) = self.limiting_state.entry(commit.key.clone())
            && let AbsoluteRedisLimitingState::Accepting {
                accept_limit,
                count,
                ..
            } = entry.get_mut()
        {
            count.store(commit.count, Ordering::Release);
            *mutex_lock(accept_limit, "accepting.accept_limit")? = previous_accept_limit;
        }

        Ok(())
    }

    pub(super) async fn flush_local_pending_for_lifecycle(
        &self,
        key: &RedisKey,
    ) -> Result<bool, TrypemaError> {
        let Some(state) = self.limiting_state.get(key) else {
            return Ok(false);
        };
        let has_pending = matches!(state.deref(), AbsoluteRedisLimitingState::Accepting { count, .. }
            if count.load(Ordering::Acquire) > 0);
        drop(state);

        if !has_pending {
            return Ok(false);
        }

        let frozen = match self.limiting_state.entry(key.clone()) {
            Entry::Occupied(mut entry) => match entry.get_mut() {
                AbsoluteRedisLimitingState::Accepting {
                    window_limit,
                    accept_limit,
                    count,
                    state_revision,
                    ..
                } => {
                    let local_count = count.load(Ordering::Acquire);
                    if local_count == 0 {
                        return Ok(false);
                    }

                    let previous_accept_limit =
                        *mutex_lock(accept_limit, "accepting.accept_limit")?;
                    let commit = AbsoluteHybridCommit {
                        key: key.clone(),
                        window_limit: *mutex_lock(window_limit, "accepting.window_limit")?,
                        count: local_count,
                        state_revision: *mutex_lock(state_revision, "accepting.state_revision")?,
                    };
                    count.store(0, Ordering::Release);
                    *mutex_lock(accept_limit, "accepting.accept_limit")? = 0;
                    Some((commit, previous_accept_limit))
                }
                AbsoluteRedisLimitingState::Undefined
                | AbsoluteRedisLimitingState::Absent { .. }
                | AbsoluteRedisLimitingState::Rejecting { .. } => None,
            },
            Entry::Vacant(_) => None,
        };

        let Some((commit, previous_accept_limit)) = frozen else {
            return Ok(false);
        };

        let _maintenance_guard = self.maintenance_lock.read().await;
        if let Err(err) = self
            .redis_proxy
            .batch_commit_state(std::slice::from_ref(&commit))
            .await
        {
            self.restore_accepting_state(&commit, previous_accept_limit)?;
            return Err(err);
        }

        Ok(true)
    }

    pub(super) async fn resolve_redis_state_and_commit(
        &self,
        read_state_result: AbsoluteHybridRedisProxyReadStateResult,
    ) -> Result<ResolvedRedisState, TrypemaError> {
        let AbsoluteHybridRedisProxyReadStateResult {
            key,
            current_total_count: redis_total,
            window_limit,
            oldest_bucket_ttl,
            oldest_bucket_count,
            state_revision,
        } = read_state_result;

        let mut current_total = redis_total;
        let mut window_limit = window_limit;

        let state = self.get_or_create_limiting_state(&key);

        match state.deref() {
            AbsoluteRedisLimitingState::Undefined | AbsoluteRedisLimitingState::Absent { .. } => {
                drop(state);
            }
            AbsoluteRedisLimitingState::Rejecting {
                committed_count,
                committed_at,
                state_revision: local_state_revision,
                ..
            } => {
                let window_size_ms = self.window_size.as_milliseconds();

                if *mutex_lock(local_state_revision, "rejecting.state_revision")? == state_revision
                    && committed_at.elapsed().as_millis() < window_size_ms
                {
                    current_total = current_total.max(*committed_count);
                }

                drop(state);
            }
            AbsoluteRedisLimitingState::Accepting { .. } => {
                drop(state);
                let frozen = match self.limiting_state.entry(key.clone()) {
                    Entry::Occupied(mut entry) => match entry.get_mut() {
                        AbsoluteRedisLimitingState::Accepting {
                            window_limit: local_window_limit,
                            accept_limit,
                            count,
                            state_revision,
                            ..
                        } => {
                            let local_count = count.load(Ordering::Acquire);
                            let previous_accept_limit =
                                *mutex_lock(accept_limit, "accepting.accept_limit")?;
                            let local_window_limit =
                                *mutex_lock(local_window_limit, "accepting.window_limit")?;
                            let local_state_revision =
                                *mutex_lock(state_revision, "accepting.state_revision")?;

                            count.store(0, Ordering::Release);
                            *mutex_lock(accept_limit, "accepting.accept_limit")? = 0;

                            Some((
                                local_count,
                                previous_accept_limit,
                                local_window_limit,
                                local_state_revision,
                            ))
                        }
                        AbsoluteRedisLimitingState::Undefined
                        | AbsoluteRedisLimitingState::Absent { .. }
                        | AbsoluteRedisLimitingState::Rejecting { .. } => None,
                    },
                    Entry::Vacant(_) => None,
                };

                if let Some((
                    local_count,
                    previous_accept_limit,
                    local_window_limit,
                    local_state_revision,
                )) = frozen
                {
                    window_limit.get_or_insert(local_window_limit);
                    current_total = current_total.saturating_add(local_count);

                    if local_count > 0 {
                        let commit = AbsoluteHybridCommit {
                            key: key.clone(),
                            window_limit: local_window_limit,
                            count: local_count,
                            state_revision: local_state_revision,
                        };

                        let _maintenance_guard = self.maintenance_lock.read().await;
                        if let Err(err) = self
                            .redis_proxy
                            .batch_commit_state(std::slice::from_ref(&commit))
                            .await
                        {
                            self.restore_accepting_state(&commit, previous_accept_limit)?;
                            return Err(err);
                        }
                    }
                }
            }
        }

        Ok(ResolvedRedisState {
            key,
            current_total,
            window_limit,
            oldest_bucket_ttl,
            oldest_bucket_count,
            state_revision,
        })
    }

    fn store_absent_state(&self, key: &RedisKey) {
        let absent = AbsoluteRedisLimitingState::Absent {
            last_synced: Instant::now(),
        };

        match self.limiting_state.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                *entry.get_mut() = absent;
            }
            Entry::Vacant(entry) => {
                entry.insert(absent);
            }
        }
    }

    fn store_rejecting_state(
        &self,
        state: &ResolvedRedisState,
        retry_after_ms: u64,
        remaining_after_waiting: u64,
        synced_at: Instant,
    ) -> Result<(), TrypemaError> {
        let time_instant = Instant::now();
        let current = self.get_or_create_limiting_state(&state.key);

        if let AbsoluteRedisLimitingState::Rejecting {
            time_instant: current_time_instant,
            ttl_ms,
            count_after_release,
            last_synced,
            state_revision: current_state_revision,
            ..
        } = current.deref()
        {
            // Refresh only the Redis-derived rejection interval. `committed_at`
            // must keep the time of the actual local commit; refreshing it here
            // would keep an expired committed count alive indefinitely.
            *mutex_lock(current_time_instant, "rejecting.time_instant")? = time_instant;
            *mutex_lock(last_synced, "rejecting.last_synced")? = synced_at;
            *mutex_lock(ttl_ms, "rejecting.ttl_ms")? = retry_after_ms;
            *mutex_lock(count_after_release, "rejecting.count_after_release")? =
                remaining_after_waiting;
            *mutex_lock(current_state_revision, "rejecting.state_revision")? = state.state_revision;
            return Ok(());
        }

        drop(current);

        let rejecting = AbsoluteRedisLimitingState::Rejecting {
            time_instant: Mutex::new(time_instant),
            ttl_ms: Mutex::new(retry_after_ms),
            count_after_release: Mutex::new(remaining_after_waiting),
            committed_count: state.current_total,
            committed_at: time_instant,
            last_synced: Mutex::new(synced_at),
            state_revision: Mutex::new(state.state_revision),
        };

        match self.limiting_state.entry(state.key.clone()) {
            Entry::Occupied(mut entry) => {
                *entry.get_mut() = rejecting;
            }
            Entry::Vacant(entry) => {
                entry.insert(rejecting);
            }
        }

        Ok(())
    }

    fn store_accepting_state(
        &self,
        state: &ResolvedRedisState,
        window_limit: f64,
        increment: u64,
        mode: CallMode,
    ) -> Result<(), TrypemaError> {
        let synced_at = Instant::now();
        let accept_limit = (window_limit as u64).saturating_sub(state.current_total);
        let current = self.get_or_create_limiting_state(&state.key);

        if let AbsoluteRedisLimitingState::Accepting {
            window_limit: current_window_limit,
            accept_limit: current_accept_limit,
            starting_count,
            count,
            last_synced,
            oldest_bucket_ttl,
            oldest_bucket_count,
            last_modified,
            state_revision,
        } = current.deref()
        {
            *mutex_lock(current_window_limit, "accepting.window_limit")? = window_limit;
            // Any pending local count was committed before this refresh. Keep it in the
            // inferred baseline instead of falling back to the pre-commit Redis total.
            *mutex_lock(starting_count, "accepting.starting_count")? = state.current_total;
            *mutex_lock(last_synced, "accepting.last_synced")? = synced_at;
            // A read-only refresh is not activity; it must not keep the key alive.
            if mode == CallMode::Recording {
                *mutex_lock(last_modified, "accepting.last_modified")? = synced_at;
            }
            *mutex_lock(oldest_bucket_ttl, "accepting.oldest_bucket_ttl")? =
                state.oldest_bucket_ttl;
            *mutex_lock(oldest_bucket_count, "accepting.oldest_bucket_count")? =
                state.oldest_bucket_count;
            count.store(increment, Ordering::Release);
            *mutex_lock(current_accept_limit, "accepting.accept_limit")? = accept_limit;
            *mutex_lock(state_revision, "accepting.state_revision")? = state.state_revision;
            return Ok(());
        }

        drop(current);

        let accepting = AbsoluteRedisLimitingState::Accepting {
            window_limit: Mutex::new(window_limit),
            accept_limit: Mutex::new(accept_limit),
            starting_count: Mutex::new(state.current_total),
            count: AtomicU64::new(increment),
            last_synced: Mutex::new(synced_at),
            oldest_bucket_ttl: Mutex::new(state.oldest_bucket_ttl),
            oldest_bucket_count: Mutex::new(state.oldest_bucket_count),
            last_modified: Mutex::new(synced_at),
            state_revision: Mutex::new(state.state_revision),
        };

        match self.limiting_state.entry(state.key.clone()) {
            Entry::Occupied(mut entry) => {
                *entry.get_mut() = accepting;
            }
            Entry::Vacant(entry) => {
                entry.insert(accepting);
            }
        }

        Ok(())
    }

    async fn read_redis_and_reset_state(
        &self,
        key: &RedisKey,
        check_count: u64,
        increment: u64,
        rate_limit: Option<&RateLimit>,
        mode: CallMode,
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
            mode,
        )
        .await
    }

    pub(super) async fn reset_single_state_from_read_result(
        &self,
        read_state_result: AbsoluteHybridRedisProxyReadStateResult,
        check_count: u64,
        increment: u64,
        rate_limit: Option<&RateLimit>,
        mode: CallMode,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let state = self
            .resolve_redis_state_and_commit(read_state_result)
            .await?;

        let window_limit = match state.window_limit {
            Some(window_limit) => window_limit,
            None => {
                let Some(rate_limit) = rate_limit else {
                    self.store_absent_state(&state.key);

                    return Ok(RateLimitDecision::Allowed);
                };

                (self.window_size.as_seconds() as f64) * rate_limit.as_per_second()
            }
        };

        if state.current_total.saturating_add(check_count) > window_limit as u64 {
            let retry_after_ms = state.oldest_bucket_ttl.unwrap_or(0);
            let remaining_after_waiting = state.oldest_bucket_count.unwrap_or(0);

            self.store_rejecting_state(
                &state,
                retry_after_ms,
                remaining_after_waiting,
                Instant::now(),
            )?;

            return Ok(RateLimitDecision::Rejected {
                window_size: self.window_size,
                retry_after: duration_from_milliseconds(u128::from(retry_after_ms)),
                remaining_after_waiting,
            });
        }

        self.store_accepting_state(&state, window_limit, increment, mode)?;

        Ok(RateLimitDecision::Allowed)
    }

    pub(super) async fn send_commit(
        &self,
        commit: AbsoluteHybridCommit,
    ) -> Result<(), TrypemaError> {
        self.commiter_sender
            .send(commit.into())
            .await
            .map_err(|err| TrypemaError::CustomError(format!("Failed to send commit: {err:?}")))?;

        Ok(())
    } // end method send_commit

    pub(super) async fn is_allowed_with_count_increment(
        &self,
        key: &RedisKey,
        check_count: u64,
        increment: u64,
        rate_limit: Option<&RateLimit>,
        mode: CallMode,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let state = self.get_or_create_limiting_state(key);

        match self.evaluate_local_state(key, state.deref(), check_count, increment, mode)? {
            LocalAdmission::Allowed => return Ok(RateLimitDecision::Allowed),
            LocalAdmission::Rejected(decision) => return Ok(decision),
            LocalAdmission::Refresh | LocalAdmission::Exhausted(_) => {}
        }

        drop(state);

        self.coordinate_local_transition(key, check_count, increment, rate_limit, mode)
            .await
    } // end method is_allowed_with_count_increment

    /// The local-state half of [`inc`](Self::inc) for a key with local state. `None` means the
    /// key has no local state or needs a Redis refresh or commit.
    pub(super) fn try_inc_from_local_state(
        &self,
        key: &str,
        count: u64,
    ) -> Result<Option<RateLimitDecision>, TrypemaError> {
        let Some(state) = self.limiting_state.get(key) else {
            return Ok(None);
        };

        match self.evaluate_local_state(
            state.key(),
            state.value(),
            count,
            count,
            CallMode::Recording,
        )? {
            LocalAdmission::Allowed => Ok(Some(RateLimitDecision::Allowed)),
            LocalAdmission::Rejected(decision) => Ok(Some(decision)),
            LocalAdmission::Refresh | LocalAdmission::Exhausted(_) => Ok(None),
        }
    } // end fn try_inc_from_local_state

    async fn coordinate_local_transition(
        &self,
        key: &RedisKey,
        check_count: u64,
        increment: u64,
        rate_limit: Option<&RateLimit>,
        mode: CallMode,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let reset_lock = self.get_or_create_reset_lock(key);
        let _reset_guard = reset_lock.lock().await;

        let state = self.get_or_create_limiting_state(key);

        match self.evaluate_local_state(key, state.deref(), check_count, increment, mode)? {
            LocalAdmission::Allowed => return Ok(RateLimitDecision::Allowed),
            LocalAdmission::Rejected(decision) => return Ok(decision),
            LocalAdmission::Refresh => {
                drop(state);
                return self
                    .read_redis_and_reset_state(key, check_count, increment, rate_limit, mode)
                    .await;
            }
            LocalAdmission::Exhausted(_) => drop(state),
        }

        let transition = match self.limiting_state.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let admission =
                    self.evaluate_local_state(key, entry.get(), check_count, increment, mode)?;
                match admission {
                    LocalAdmission::Allowed => return Ok(RateLimitDecision::Allowed),
                    LocalAdmission::Rejected(decision) => return Ok(decision),
                    LocalAdmission::Refresh => None,
                    LocalAdmission::Exhausted(transition) => {
                        let AbsoluteRedisLimitingState::Accepting {
                            accept_limit,
                            count,
                            ..
                        } = entry.get_mut()
                        else {
                            return Err(TrypemaError::CustomError(
                                "absolute hybrid state changed during exclusive revalidation"
                                    .to_string(),
                            ));
                        };

                        count.store(0, Ordering::Release);
                        *mutex_lock(accept_limit, "accepting.accept_limit")? = 0;
                        Some(transition)
                    }
                }
            }
            Entry::Vacant(_) => None,
        };

        let Some(transition) = transition else {
            return self
                .read_redis_and_reset_state(key, check_count, increment, rate_limit, mode)
                .await;
        };

        // With nothing pending, the local data already decides the rejection. A commit of 0
        // would still refresh Redis TTLs and activity metadata.
        if transition.commit.count > 0 {
            let _maintenance_guard = self.maintenance_lock.read().await;
            if let Err(err) = self
                .redis_proxy
                .batch_commit_state(std::slice::from_ref(&transition.commit))
                .await
            {
                self.restore_accepting_state(&transition.commit, transition.previous_accept_limit)?;
                return Err(err);
            }
        }

        let state = ResolvedRedisState {
            key: key.clone(),
            current_total: transition.committed_total,
            window_limit: Some(transition.commit.window_limit),
            oldest_bucket_ttl: Some(transition.retry_after_ms as u64),
            oldest_bucket_count: Some(transition.remaining_after_waiting),
            state_revision: transition.commit.state_revision,
        };

        // The rejection comes from local data, so it is as old as the Redis read behind that
        // data. A commit writes Redis but does not read what other instances changed.
        self.store_rejecting_state(
            &state,
            transition.retry_after_ms as u64,
            transition.remaining_after_waiting,
            transition.last_synced,
        )?;

        Ok(RateLimitDecision::Rejected {
            window_size: self.window_size,
            retry_after: duration_from_milliseconds(transition.retry_after_ms),
            remaining_after_waiting: transition.remaining_after_waiting,
        })
    }
}
