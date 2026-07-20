use super::*;

#[derive(Debug)]
struct AcceptingTransition {
    commit: AbsoluteHybridCommit,
    previous_accept_limit: u64,
    retry_after_ms: u128,
    remaining_after_waiting: u64,
    committed_total: u64,
}

#[derive(Debug)]
enum LocalAdmission {
    Allowed,
    Rejected(RateLimitDecision),
    Refresh,
    Exhausted(AcceptingTransition),
}

#[derive(Debug)]
struct ResolvedRedisState {
    key: RedisKey,
    redis_total: u64,
    current_total: u64,
    window_limit: Option<u64>,
    oldest_bucket_ttl: Option<u64>,
    oldest_bucket_count: Option<u64>,
}

impl AbsoluteHybridRateLimiter {
    fn evaluate_local_state(
        &self,
        key: &RedisKey,
        state: &AbsoluteRedisLimitingState,
        check_count: u64,
        increment: u64,
    ) -> Result<LocalAdmission, TrypemaError> {
        match state {
            AbsoluteRedisLimitingState::Undefined => Ok(LocalAdmission::Refresh),
            AbsoluteRedisLimitingState::Rejecting {
                time_instant,
                ttl_ms,
                count_after_release,
                ..
            } => {
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
                    window_size_seconds: *self.window_size_seconds,
                    retry_after_ms: ttl_ms.saturating_sub(elapsed_ms),
                    remaining_after_waiting,
                }))
            }
            AbsoluteRedisLimitingState::Accepting {
                window_limit,
                accept_limit,
                starting_count,
                count,
                time_instant,
                oldest_bucket_ttl,
                oldest_bucket_count,
                last_modified,
            } => {
                let local_count = count.load(Ordering::Acquire);
                let accept_limit = *mutex_lock(accept_limit, "accepting.accept_limit")?;

                if check_count <= accept_limit.saturating_sub(local_count) {
                    *mutex_lock(last_modified, "accepting.last_modified")? = Instant::now();
                    count.fetch_add(increment, Ordering::AcqRel);

                    return Ok(LocalAdmission::Allowed);
                }

                let starting_count = *mutex_lock(starting_count, "accepting.starting_count")?;
                let committed_total = starting_count.saturating_add(local_count);
                let oldest_bucket_ttl =
                    (*mutex_lock(oldest_bucket_ttl, "accepting.oldest_bucket_ttl")?)
                        .map(|ttl| ttl as u128)
                        .unwrap_or((*self.sync_interval_ms).min(*self.rate_group_size_ms) as u128);
                let elapsed_ms = mutex_lock(time_instant, "accepting.time_instant")?
                    .elapsed()
                    .as_millis();
                let retry_after_ms = oldest_bucket_ttl
                    .saturating_sub(elapsed_ms)
                    .max(*self.sync_interval_ms as u128);
                let remaining_after_waiting =
                    (*mutex_lock(oldest_bucket_count, "accepting.oldest_bucket_count")?)
                        .unwrap_or(committed_total);

                if local_count < accept_limit {
                    return Ok(LocalAdmission::Rejected(RateLimitDecision::Rejected {
                        window_size_seconds: *self.window_size_seconds,
                        retry_after_ms,
                        remaining_after_waiting,
                    }));
                }

                Ok(LocalAdmission::Exhausted(AcceptingTransition {
                    commit: AbsoluteHybridCommit {
                        key: key.clone(),
                        window_limit: *mutex_lock(window_limit, "accepting.window_limit")?,
                        count: local_count,
                    },
                    previous_accept_limit: accept_limit,
                    retry_after_ms,
                    remaining_after_waiting,
                    committed_total,
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

    async fn resolve_redis_state_and_commit(
        &self,
        read_state_result: AbsoluteHybridRedisProxyReadStateResult,
    ) -> Result<ResolvedRedisState, TrypemaError> {
        let AbsoluteHybridRedisProxyReadStateResult {
            key,
            current_total_count: redis_total,
            window_limit,
            oldest_bucket_ttl,
            oldest_bucket_count,
        } = read_state_result;

        let mut current_total = redis_total;
        let mut window_limit = window_limit;

        let state = self.get_or_create_limiting_state(&key);

        match state.deref() {
            AbsoluteRedisLimitingState::Undefined => {
                drop(state);
            }
            AbsoluteRedisLimitingState::Rejecting {
                committed_count,
                committed_at,
                ..
            } => {
                let window_size_ms = (*self.window_size_seconds as u128).saturating_mul(1_000);

                if committed_at.elapsed().as_millis() < window_size_ms {
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
                            ..
                        } => {
                            let local_count = count.load(Ordering::Acquire);
                            let previous_accept_limit =
                                *mutex_lock(accept_limit, "accepting.accept_limit")?;
                            let local_window_limit =
                                *mutex_lock(local_window_limit, "accepting.window_limit")?;

                            count.store(0, Ordering::Release);
                            *mutex_lock(accept_limit, "accepting.accept_limit")? = 0;

                            Some((local_count, previous_accept_limit, local_window_limit))
                        }
                        AbsoluteRedisLimitingState::Undefined
                        | AbsoluteRedisLimitingState::Rejecting { .. } => None,
                    },
                    Entry::Vacant(_) => None,
                };

                if let Some((local_count, previous_accept_limit, local_window_limit)) = frozen {
                    window_limit.get_or_insert(local_window_limit);
                    current_total = current_total.saturating_add(local_count);

                    if local_count > 0 {
                        let commit = AbsoluteHybridCommit {
                            key: key.clone(),
                            window_limit: local_window_limit,
                            count: local_count,
                        };

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
            redis_total,
            current_total,
            window_limit,
            oldest_bucket_ttl,
            oldest_bucket_count,
        })
    }

    fn reset_to_undefined(&self, key: &RedisKey) {
        let state = self.get_or_create_limiting_state(key);

        if matches!(state.deref(), AbsoluteRedisLimitingState::Undefined) {
            return;
        }

        drop(state);

        match self.limiting_state.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                *entry.get_mut() = AbsoluteRedisLimitingState::Undefined;
            }
            Entry::Vacant(entry) => {
                entry.insert(AbsoluteRedisLimitingState::Undefined);
            }
        }
    }

    fn store_rejecting_state(
        &self,
        state: &ResolvedRedisState,
        retry_after_ms: u64,
        remaining_after_waiting: u64,
    ) -> Result<(), TrypemaError> {
        let time_instant = Instant::now();
        let current = self.get_or_create_limiting_state(&state.key);

        if let AbsoluteRedisLimitingState::Rejecting {
            time_instant: current_time_instant,
            ttl_ms,
            count_after_release,
            ..
        } = current.deref()
        {
            // Refresh only the Redis-derived rejection interval. `committed_at`
            // must keep the time of the actual local commit; refreshing it here
            // would keep an expired committed count alive indefinitely.
            *mutex_lock(current_time_instant, "rejecting.time_instant")? = time_instant;
            *mutex_lock(ttl_ms, "rejecting.ttl_ms")? = retry_after_ms;
            *mutex_lock(count_after_release, "rejecting.count_after_release")? =
                remaining_after_waiting;
            return Ok(());
        }

        drop(current);

        let rejecting = AbsoluteRedisLimitingState::Rejecting {
            time_instant: Mutex::new(time_instant),
            ttl_ms: Mutex::new(retry_after_ms),
            count_after_release: Mutex::new(remaining_after_waiting),
            committed_count: state.current_total,
            committed_at: time_instant,
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
        window_limit: u64,
        increment: u64,
    ) -> Result<(), TrypemaError> {
        let time_instant = Instant::now();
        let accept_limit = window_limit.saturating_sub(state.current_total);
        let current = self.get_or_create_limiting_state(&state.key);

        if let AbsoluteRedisLimitingState::Accepting {
            window_limit: current_window_limit,
            accept_limit: current_accept_limit,
            starting_count,
            count,
            time_instant: current_time_instant,
            oldest_bucket_ttl,
            oldest_bucket_count,
            last_modified,
        } = current.deref()
        {
            *mutex_lock(current_window_limit, "accepting.window_limit")? = window_limit;
            *mutex_lock(starting_count, "accepting.starting_count")? = state.redis_total;
            *mutex_lock(current_time_instant, "accepting.time_instant")? = time_instant;
            *mutex_lock(last_modified, "accepting.last_modified")? = time_instant;
            *mutex_lock(oldest_bucket_ttl, "accepting.oldest_bucket_ttl")? =
                state.oldest_bucket_ttl;
            *mutex_lock(oldest_bucket_count, "accepting.oldest_bucket_count")? =
                state.oldest_bucket_count;
            count.store(increment, Ordering::Release);
            *mutex_lock(current_accept_limit, "accepting.accept_limit")? = accept_limit;
            return Ok(());
        }

        drop(current);

        let accepting = AbsoluteRedisLimitingState::Accepting {
            window_limit: Mutex::new(window_limit),
            accept_limit: Mutex::new(accept_limit),
            starting_count: Mutex::new(state.redis_total),
            count: AtomicU64::new(increment),
            time_instant: Mutex::new(time_instant),
            oldest_bucket_ttl: Mutex::new(state.oldest_bucket_ttl),
            oldest_bucket_count: Mutex::new(state.oldest_bucket_count),
            last_modified: Mutex::new(time_instant),
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
        )
        .await
    }

    pub(super) async fn reset_single_state_from_read_result(
        &self,
        read_state_result: AbsoluteHybridRedisProxyReadStateResult,
        check_count: u64,
        increment: u64,
        rate_limit: Option<&RateLimit>,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let state = self
            .resolve_redis_state_and_commit(read_state_result)
            .await?;

        let window_limit = match state.window_limit {
            Some(window_limit) => window_limit,
            None => {
                let Some(rate_limit) = rate_limit else {
                    self.reset_to_undefined(&state.key);

                    return Ok(RateLimitDecision::Allowed);
                };

                ((*self.window_size_seconds as f64) * **rate_limit) as u64
            }
        };

        if state.current_total.saturating_add(check_count) > window_limit {
            let retry_after_ms = state
                .oldest_bucket_ttl
                .unwrap_or((*self.sync_interval_ms).min(*self.rate_group_size_ms))
                .max(*self.sync_interval_ms);
            let remaining_after_waiting = state.oldest_bucket_count.unwrap_or(0);

            self.store_rejecting_state(&state, retry_after_ms, remaining_after_waiting)?;

            return Ok(RateLimitDecision::Rejected {
                window_size_seconds: *self.window_size_seconds,
                retry_after_ms: retry_after_ms as u128,
                remaining_after_waiting,
            });
        }

        self.store_accepting_state(&state, window_limit, increment)?;

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
    ) -> Result<RateLimitDecision, TrypemaError> {
        let state = self.get_or_create_limiting_state(key);

        match self.evaluate_local_state(key, state.deref(), check_count, increment)? {
            LocalAdmission::Allowed => return Ok(RateLimitDecision::Allowed),
            LocalAdmission::Rejected(decision) => return Ok(decision),
            LocalAdmission::Refresh | LocalAdmission::Exhausted(_) => {}
        }

        drop(state);

        self.coordinate_local_transition(key, check_count, increment, rate_limit)
            .await
    } // end method is_allowed_with_count_increment

    async fn coordinate_local_transition(
        &self,
        key: &RedisKey,
        check_count: u64,
        increment: u64,
        rate_limit: Option<&RateLimit>,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let reset_lock = self.get_or_create_reset_lock(key);
        let _reset_guard = reset_lock.lock().await;

        let state = self.get_or_create_limiting_state(key);

        match self.evaluate_local_state(key, state.deref(), check_count, increment)? {
            LocalAdmission::Allowed => return Ok(RateLimitDecision::Allowed),
            LocalAdmission::Rejected(decision) => return Ok(decision),
            LocalAdmission::Refresh => {
                drop(state);
                return self
                    .read_redis_and_reset_state(key, check_count, increment, rate_limit)
                    .await;
            }
            LocalAdmission::Exhausted(_) => drop(state),
        }

        let transition = match self.limiting_state.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let admission =
                    self.evaluate_local_state(key, entry.get(), check_count, increment)?;
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
                .read_redis_and_reset_state(key, check_count, increment, rate_limit)
                .await;
        };

        if let Err(err) = self
            .redis_proxy
            .batch_commit_state(std::slice::from_ref(&transition.commit))
            .await
        {
            self.restore_accepting_state(&transition.commit, transition.previous_accept_limit)?;
            return Err(err);
        }

        let state = ResolvedRedisState {
            key: key.clone(),
            redis_total: transition.committed_total,
            current_total: transition.committed_total,
            window_limit: Some(transition.commit.window_limit),
            oldest_bucket_ttl: Some(transition.retry_after_ms as u64),
            oldest_bucket_count: Some(transition.remaining_after_waiting),
        };

        self.store_rejecting_state(
            &state,
            transition.retry_after_ms as u64,
            transition.remaining_after_waiting,
        )?;

        Ok(RateLimitDecision::Rejected {
            window_size_seconds: *self.window_size_seconds,
            retry_after_ms: transition.retry_after_ms,
            remaining_after_waiting: transition.remaining_after_waiting,
        })
    }
}
