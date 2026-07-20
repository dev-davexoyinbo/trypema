use super::*;

#[derive(Debug)]
struct FrozenLocalState {
    state: Option<SuppressedRedisLimitingState>,
    pending_count: u64,
    pending_declined_count: u64,
    hard_window_limit: Option<f64>,
    suppression_factor_ttl_ms: Option<u64>,
}

#[derive(Debug)]
struct ResolvedRedisState {
    key: RedisKey,
    current_total_count: u64,
    current_declined_count: u64,
    suppression_factor: f64,
    suppression_factor_ttl_ms: Option<u64>,
    hard_window_limit: Option<f64>,
}

#[derive(Debug)]
struct AcceptingState<'a> {
    hard_window_limit: &'a Mutex<f64>,
    starting_count: &'a Mutex<u64>,
    count: &'a AtomicU64,
    declined_count: &'a Mutex<u64>,
    last_modified: &'a Mutex<Instant>,
}

#[derive(Debug)]
struct SuppressingState<'a> {
    time_instant: &'a Mutex<Instant>,
    hard_window_limit: &'a Mutex<f64>,
    suppression_factor: &'a Mutex<f64>,
    suppression_factor_ttl_ms: &'a Mutex<u64>,
    starting_count: &'a Mutex<u64>,
    starting_declined_count: &'a Mutex<u64>,
    count: &'a AtomicU64,
    declined_count: &'a AtomicU64,
}

impl SuppressedHybridRateLimiter {
    // When None is returned, we coordinate a local transition.
    fn evaluate_local_state_and_increment(
        &self,
        state: &SuppressedRedisLimitingState,
        check_count: u64,
        increment: u64,
        random_bool: &mut impl FnMut(f64) -> bool,
    ) -> Result<Option<RateLimitDecision>, TrypemaError> {
        match state {
            SuppressedRedisLimitingState::Undefined => Ok(None),
            SuppressedRedisLimitingState::Accepting {
                hard_window_limit,
                starting_count,
                count,
                declined_count,
                last_modified,
            } => self.evaluate_accepting_state_and_increment(
                AcceptingState {
                    hard_window_limit,
                    starting_count,
                    count,
                    declined_count,
                    last_modified,
                },
                check_count,
                increment,
            ),
            SuppressedRedisLimitingState::Suppressing {
                time_instant,
                hard_window_limit,
                suppression_factor,
                suppression_factor_ttl_ms,
                starting_count,
                starting_declined_count,
                count,
                declined_count,
            } => self.evaluate_suppressing_state_and_increment(
                SuppressingState {
                    time_instant,
                    hard_window_limit,
                    suppression_factor,
                    suppression_factor_ttl_ms,
                    starting_count,
                    starting_declined_count,
                    count,
                    declined_count,
                },
                check_count,
                increment,
                random_bool,
            ),
        }
    } // end fn evaluate_local_state

    fn evaluate_accepting_state_and_increment(
        &self,
        state: AcceptingState<'_>,
        check_count: u64,
        increment: u64,
    ) -> Result<Option<RateLimitDecision>, TrypemaError> {
        let hard_window_limit =
            *mutex_lock(state.hard_window_limit, "accepting.hard_window_limit")?;
        let starting_count = *mutex_lock(state.starting_count, "accepting.starting_count")?;
        let declined_count = *mutex_lock(state.declined_count, "accepting.declined_count")?;
        let hard_window_capacity = hard_window_limit as u64;
        let soft_window_limit = (hard_window_limit / *self.hard_limit_factor) as u64;
        let forecasted_allowed_count = starting_count
            .saturating_add(state.count.load(Ordering::Acquire))
            .saturating_sub(declined_count)
            .saturating_add(check_count);

        if forecasted_allowed_count > soft_window_limit
            || forecasted_allowed_count == hard_window_capacity
        {
            return Ok(None);
        }

        *mutex_lock(state.last_modified, "accepting.last_modified")? = Instant::now();
        state.count.fetch_add(increment, Ordering::AcqRel);

        Ok(Some(RateLimitDecision::Allowed))
    } // end fn evaluate_accepting_state

    fn evaluate_suppressing_state_and_increment(
        &self,
        state: SuppressingState<'_>,
        check_count: u64,
        increment: u64,
        random_bool: &mut impl FnMut(f64) -> bool,
    ) -> Result<Option<RateLimitDecision>, TrypemaError> {
        let hard_window_limit =
            *mutex_lock(state.hard_window_limit, "suppressing.hard_window_limit")?;
        let suppression_factor =
            *mutex_lock(state.suppression_factor, "suppressing.suppression_factor")?;
        let elapsed_ms = mutex_lock(state.time_instant, "suppressing.time_instant")?
            .elapsed()
            .as_millis();
        let ttl_ms = *mutex_lock(state.suppression_factor_ttl_ms, "suppressing.ttl_ms")? as u128;
        let starting_count = *mutex_lock(state.starting_count, "suppressing.starting_count")?;
        let starting_declined_count = *mutex_lock(
            state.starting_declined_count,
            "suppressing.starting_declined_count",
        )?;

        let hard_window_capacity = hard_window_limit as u64;
        let soft_window_limit = (hard_window_limit / *self.hard_limit_factor) as u64;
        let forecasted_allowed_count = starting_count
            .saturating_add(state.count.load(Ordering::Acquire))
            .saturating_sub(
                starting_declined_count
                    .saturating_add(state.declined_count.load(Ordering::Acquire)),
            )
            .saturating_add(check_count);
        let reached_hard_window_limit = forecasted_allowed_count == hard_window_capacity;

        if forecasted_allowed_count <= soft_window_limit || reached_hard_window_limit {
            state.count.fetch_add(increment, Ordering::AcqRel);

            if reached_hard_window_limit {
                self.cache_full_suppression(&state)?;
            }

            return Ok(Some(RateLimitDecision::Allowed));
        }

        if forecasted_allowed_count > hard_window_capacity {
            self.cache_full_suppression(&state)?;
            state.count.fetch_add(increment, Ordering::AcqRel);
            state.declined_count.fetch_add(increment, Ordering::AcqRel);

            return Ok(Some(RateLimitDecision::Suppressed {
                suppression_factor: 1f64,
                is_allowed: false,
            }));
        }

        if elapsed_ms >= ttl_ms {
            return Ok(None);
        }

        let should_allow = if suppression_factor == 0f64 {
            true
        } else if suppression_factor == 1f64 {
            false
        } else {
            random_bool(1f64 - suppression_factor)
        };

        state.count.fetch_add(increment, Ordering::AcqRel);

        if !should_allow {
            state.declined_count.fetch_add(increment, Ordering::AcqRel);
        }

        Ok(Some(RateLimitDecision::Suppressed {
            suppression_factor,
            is_allowed: should_allow,
        }))
    } // end fn evaluate_suppressing_state

    fn cache_full_suppression(&self, state: &SuppressingState<'_>) -> Result<(), TrypemaError> {
        *mutex_lock(
            state.suppression_factor_ttl_ms,
            "suppressing.suppression_factor_ttl_ms",
        )? = *self.suppression_factor_cache_period;
        *mutex_lock(state.time_instant, "suppressing.time_instant")? = Instant::now();
        *mutex_lock(state.suppression_factor, "suppressing.suppression_factor")? = 1f64;

        Ok(())
    } // end fn cache_full_suppression

    pub(super) fn local_suppression_factor(
        &self,
        state: &SuppressedRedisLimitingState,
    ) -> Result<Option<f64>, TrypemaError> {
        match state {
            SuppressedRedisLimitingState::Suppressing {
                time_instant,
                suppression_factor_ttl_ms,
                suppression_factor,
                count,
                starting_count,
                starting_declined_count,
                hard_window_limit,
                declined_count,
            } => {
                let hard_window_limit =
                    *mutex_lock(hard_window_limit, "suppressing.hard_window_limit")?;
                let mut suppression_factor =
                    *mutex_lock(suppression_factor, "suppressing.suppression_factor")?;
                let elapsed_ms = mutex_lock(time_instant, "suppressing.time_instant")?
                    .elapsed()
                    .as_millis();
                let ttl_ms = *mutex_lock(suppression_factor_ttl_ms, "suppressing.ttl_ms")? as u128;
                let starting_count = *mutex_lock(starting_count, "suppressing.starting_count")?;
                let starting_declined_count = *mutex_lock(
                    starting_declined_count,
                    "suppressing.starting_declined_count",
                )?;

                let observed_count = starting_count.saturating_add(count.load(Ordering::Acquire));
                let accepted_count = observed_count.saturating_sub(
                    starting_declined_count.saturating_add(declined_count.load(Ordering::Acquire)),
                );

                if observed_count >= hard_window_limit as u64 {
                    suppression_factor = 1f64;
                } else if accepted_count < (hard_window_limit / *self.hard_limit_factor) as u64 {
                    suppression_factor = 0f64;
                }

                Ok((elapsed_ms < ttl_ms).then_some(suppression_factor))
            }

            SuppressedRedisLimitingState::Accepting {
                hard_window_limit,
                count,
                starting_count,
                ..
            } => {
                let hard_window_limit =
                    *mutex_lock(hard_window_limit, "accepting.hard_window_limit")?;
                let starting_count = *mutex_lock(starting_count, "accepting.starting_count")?;
                let observed_count = starting_count.saturating_add(count.load(Ordering::Acquire));

                let suppression_factor = if observed_count >= hard_window_limit as u64 {
                    1f64
                } else {
                    0f64
                };

                Ok(Some(suppression_factor))
            }

            SuppressedRedisLimitingState::Undefined => Ok(None),
        }
    } // end fn local_suppression_factor

    pub(super) fn local_snapshot(
        &self,
        state: &SuppressedRedisLimitingState,
    ) -> Result<Option<SuppressedRateLimitSnapshot>, TrypemaError> {
        let Some(suppression_factor) = self.local_suppression_factor(state)? else {
            return Ok(None);
        };

        let (total, total_declined) = match state {
            SuppressedRedisLimitingState::Accepting {
                starting_count,
                count,
                declined_count,
                ..
            } => {
                let starting_count = *mutex_lock(starting_count, "accepting.starting_count")?;
                let total = starting_count.saturating_add(count.load(Ordering::Acquire));
                let total_declined =
                    (*mutex_lock(declined_count, "accepting.declined_count")?).min(total);

                (total, total_declined)
            }
            SuppressedRedisLimitingState::Suppressing {
                starting_count,
                starting_declined_count,
                count,
                declined_count,
                ..
            } => {
                let starting_count = *mutex_lock(starting_count, "suppressing.starting_count")?;
                let starting_declined_count = *mutex_lock(
                    starting_declined_count,
                    "suppressing.starting_declined_count",
                )?;

                let total = starting_count.saturating_add(count.load(Ordering::Acquire));
                let total_declined = starting_declined_count
                    .saturating_add(declined_count.load(Ordering::Acquire))
                    .min(total);

                (total, total_declined)
            }
            SuppressedRedisLimitingState::Undefined => return Ok(None),
        };

        Ok(Some(SuppressedRateLimitSnapshot {
            total,
            total_declined,
            suppression_factor,
        }))
    } // end fn local_snapshot

    fn freeze_local_state(&self, key: &RedisKey) -> Result<FrozenLocalState, TrypemaError> {
        let state = match self.limiting_state.entry(key.clone()) {
            Entry::Occupied(mut entry) => Some(std::mem::replace(
                entry.get_mut(),
                SuppressedRedisLimitingState::Undefined,
            )),
            Entry::Vacant(_) => None,
        };

        let local_values = match state.as_ref() {
            Some(SuppressedRedisLimitingState::Accepting {
                hard_window_limit,
                count,
                ..
            }) => Ok((
                count.load(Ordering::Acquire),
                0,
                Some(*mutex_lock(
                    hard_window_limit,
                    "accepting.hard_window_limit",
                )?),
                None,
            )),
            Some(SuppressedRedisLimitingState::Suppressing {
                hard_window_limit,
                suppression_factor_ttl_ms,
                count,
                declined_count,
                ..
            }) => Ok((
                count.load(Ordering::Acquire),
                declined_count.load(Ordering::Acquire),
                Some(*mutex_lock(
                    hard_window_limit,
                    "suppressing.hard_window_limit",
                )?),
                Some(*mutex_lock(
                    suppression_factor_ttl_ms,
                    "suppressing.suppression_factor_ttl_ms",
                )?),
            )),
            Some(SuppressedRedisLimitingState::Undefined) | None => Ok((0, 0, None, None)),
        };

        let (pending_count, pending_declined_count, hard_window_limit, suppression_factor_ttl_ms) =
            match local_values {
                Ok(values) => values,
                Err(err) => {
                    if let Some(state) = state {
                        self.store_local_state(key, state);
                    }

                    return Err(err);
                }
            };

        Ok(FrozenLocalState {
            state,
            pending_count,
            pending_declined_count,
            hard_window_limit,
            suppression_factor_ttl_ms,
        })
    } // end fn freeze_local_state

    fn store_local_state(&self, key: &RedisKey, state: SuppressedRedisLimitingState) {
        match self.limiting_state.entry(key.clone()) {
            Entry::Occupied(mut entry) => *entry.get_mut() = state,
            Entry::Vacant(entry) => {
                entry.insert(state);
            }
        }
    } // end fn store_local_state

    async fn resolve_redis_state_and_commit(
        &self,
        read_state_result: SuppressedHybridRedisProxyReadStateResult,
    ) -> Result<ResolvedRedisState, TrypemaError> {
        let SuppressedHybridRedisProxyReadStateResult {
            key,
            current_total_count: redis_total_count,
            current_total_declined: redis_declined_count,
            suppression_factor,
            suppression_factor_ttl_ms,
            hard_window_limit,
        } = read_state_result;

        let frozen = self.freeze_local_state(&key)?;

        let current_total_count = redis_total_count.saturating_add(frozen.pending_count);
        let current_declined_count = redis_declined_count
            .saturating_add(frozen.pending_declined_count)
            .min(current_total_count);
        let hard_window_limit = hard_window_limit.or(frozen.hard_window_limit);
        let suppression_factor_ttl_ms =
            suppression_factor_ttl_ms.or(frozen.suppression_factor_ttl_ms);

        if frozen.pending_count > 0 {
            let Some(commit_hard_window_limit) = hard_window_limit else {
                if let Some(state) = frozen.state {
                    self.store_local_state(&key, state);
                }

                return Err(TrypemaError::CustomError(
                    "suppressed hybrid pending state has no hard window limit".to_string(),
                ));
            };

            let commit = SuppressedHybridCommit {
                key: key.clone(),
                hard_window_limit: commit_hard_window_limit,
                count: frozen.pending_count,
                declined_count: frozen.pending_declined_count.min(frozen.pending_count),
            };

            if let Err(err) = self
                .redis_proxy
                .batch_commit_state(std::slice::from_ref(&commit))
                .await
            {
                if let Some(state) = frozen.state {
                    self.store_local_state(&key, state);
                }

                return Err(err);
            }
        }

        Ok(ResolvedRedisState {
            key,
            current_total_count,
            current_declined_count,
            suppression_factor,
            suppression_factor_ttl_ms,
            hard_window_limit,
        })
    } // end fn resolve_redis_state_and_commit

    pub(super) async fn reset_single_state_from_read_result(
        &self,
        read_state_result: SuppressedHybridRedisProxyReadStateResult,
        check_count: u64,
        increment: u64,
        rate_limit: Option<&RateLimit>,
        random_bool: &mut impl FnMut(f64) -> bool,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let state = self
            .resolve_redis_state_and_commit(read_state_result)
            .await?;

        let hard_window_limit = match state.hard_window_limit {
            Some(hard_window_limit) => hard_window_limit,
            None => {
                let Some(rate_limit) = rate_limit else {
                    self.limiting_state.remove_if(&state.key, |_, state| {
                        matches!(state, SuppressedRedisLimitingState::Undefined)
                    });

                    return Ok(RateLimitDecision::Allowed);
                };

                *self.window_size as f64 * **rate_limit * *self.hard_limit_factor
            }
        };

        let hard_window_capacity = hard_window_limit as u64;
        let soft_window_limit = (hard_window_limit / *self.hard_limit_factor) as u64;
        let forecasted_allowed_count = state
            .current_total_count
            .saturating_sub(state.current_declined_count)
            .saturating_add(check_count);
        let reached_hard_window_limit = forecasted_allowed_count == hard_window_capacity;

        if forecasted_allowed_count > soft_window_limit || reached_hard_window_limit {
            let suppression_factor = if forecasted_allowed_count >= hard_window_capacity {
                1f64
            } else {
                state.suppression_factor
            };

            let should_allow = if reached_hard_window_limit || suppression_factor == 0f64 {
                true
            } else if suppression_factor == 1f64 {
                false
            } else {
                random_bool(1f64 - suppression_factor)
            };

            let declined_count = if should_allow { 0 } else { increment };

            let suppression_factor_ttl_ms = if suppression_factor == 1f64 {
                *self.suppression_factor_cache_period
            } else {
                state
                    .suppression_factor_ttl_ms
                    .unwrap_or(*self.suppression_factor_cache_period)
            };

            self.store_local_state(
                &state.key,
                SuppressedRedisLimitingState::Suppressing {
                    time_instant: Mutex::new(Instant::now()),
                    hard_window_limit: Mutex::new(hard_window_limit),
                    suppression_factor: Mutex::new(suppression_factor),
                    suppression_factor_ttl_ms: Mutex::new(suppression_factor_ttl_ms),
                    starting_count: Mutex::new(state.current_total_count),
                    starting_declined_count: Mutex::new(state.current_declined_count),
                    count: AtomicU64::new(increment),
                    declined_count: AtomicU64::new(declined_count),
                },
            );

            return if reached_hard_window_limit {
                Ok(RateLimitDecision::Allowed)
            } else {
                Ok(RateLimitDecision::Suppressed {
                    suppression_factor,
                    is_allowed: should_allow,
                })
            };
        }

        self.store_local_state(
            &state.key,
            SuppressedRedisLimitingState::Accepting {
                hard_window_limit: Mutex::new(hard_window_limit),
                last_modified: Mutex::new(Instant::now()),
                starting_count: Mutex::new(state.current_total_count),
                declined_count: Mutex::new(state.current_declined_count),
                count: AtomicU64::new(increment),
            },
        );

        Ok(RateLimitDecision::Allowed)
    } // end fn reset_single_state_from_read_result

    async fn coordinate_local_transition(
        &self,
        key: &RedisKey,
        check_count: u64,
        increment: u64,
        rate_limit: Option<&RateLimit>,
        random_bool: &mut impl FnMut(f64) -> bool,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let lock = self.get_or_create_reset_lock(key);
        let _guard = lock.lock().await;

        if let Some(state) = self.limiting_state.get(key)
            && let Some(decision) = self.evaluate_local_state_and_increment(
                state.deref(),
                check_count,
                increment,
                random_bool,
            )?
        {
            return Ok(decision);
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
    } // end fn coordinate_local_transition

    pub(crate) async fn inc_with_rng(
        &self,
        key: &RedisKey,
        increment: u64,
        rate_limit: Option<&RateLimit>,
        random_bool: &mut impl FnMut(f64) -> bool,
    ) -> Result<RateLimitDecision, TrypemaError> {
        self.send_epoch_change_if_needed();

        let state = self.get_or_create_limiting_state(key);

        if let Some(decision) = self.evaluate_local_state_and_increment(
            state.deref(),
            increment,
            increment,
            random_bool,
        )? {
            return Ok(decision);
        }

        drop(state);

        self.coordinate_local_transition(key, increment, increment, rate_limit, random_bool)
            .await
    } // end fn inc_with_rng

    pub(super) fn snapshot_pending_state(&self, key: &RedisKey) -> SuppressedHybridPendingState {
        self.limiting_state
            .get(key)
            .map_or_else(SuppressedHybridPendingState::default, |state| {
                match state.deref() {
                    SuppressedRedisLimitingState::Accepting { count, .. } => {
                        SuppressedHybridPendingState {
                            pending_count: count.load(Ordering::Acquire),
                            pending_declined_count: 0,
                        }
                    }
                    SuppressedRedisLimitingState::Suppressing {
                        count,
                        declined_count,
                        ..
                    } => SuppressedHybridPendingState {
                        pending_count: count.load(Ordering::Acquire),
                        pending_declined_count: declined_count.load(Ordering::Acquire),
                    },
                    SuppressedRedisLimitingState::Undefined => {
                        SuppressedHybridPendingState::default()
                    }
                }
            })
    } // end fn snapshot_pending_state

    pub(super) async fn reconcile_post_set_increments(
        &self,
        key: &RedisKey,
        hard_window_limit: f64,
        pending_state: SuppressedHybridPendingState,
    ) -> Result<(), TrypemaError> {
        let Some((_, state)) = self.limiting_state.remove(key) else {
            return Ok(());
        };

        let (actual_count, actual_declined_count) = match state {
            SuppressedRedisLimitingState::Accepting { count, .. } => (count.into_inner(), 0),
            SuppressedRedisLimitingState::Suppressing {
                count,
                declined_count,
                ..
            } => (count.into_inner(), declined_count.into_inner()),
            SuppressedRedisLimitingState::Undefined => return Ok(()),
        };

        let extra_count = actual_count.saturating_sub(pending_state.pending_count);

        if extra_count == 0 {
            return Ok(());
        }

        let extra_declined_count = actual_declined_count
            .saturating_sub(pending_state.pending_declined_count)
            .min(extra_count);

        let commit = SuppressedHybridCommit {
            key: key.clone(),
            hard_window_limit,
            count: extra_count,
            declined_count: extra_declined_count,
        };

        if let Err(err) = self
            .redis_proxy
            .batch_commit_state(std::slice::from_ref(&commit))
            .await
        {
            tracing::warn!(error = ?err, "direct post-set commit failed; queued for retry");
            self.send_commit(commit).await?;
        }

        Ok(())
    } // end fn reconcile_post_set_increments

    pub(super) async fn send_commit(
        &self,
        commit: SuppressedHybridCommit,
    ) -> Result<(), TrypemaError> {
        self.commiter_sender
            .send(commit.into())
            .await
            .map_err(|err| TrypemaError::CustomError(format!("Failed to send commit: {err:?}")))?;

        Ok(())
    } // end fn send_commit

    #[cfg(test)]
    pub(crate) fn local_state_count(&self) -> usize {
        self.limiting_state.len()
    }

    #[cfg(test)]
    pub(crate) fn install_set_if_test_hook(
        &self,
    ) -> Result<Arc<SuppressedHybridSetIfTestHook>, TrypemaError> {
        let hook = Arc::new(SuppressedHybridSetIfTestHook::default());
        *mutex_lock(&self.set_if_test_hook, "set_if_test_hook")? = Some(Arc::clone(&hook));

        Ok(hook)
    }

    pub(super) fn should_cleanup_local_state(
        state: &SuppressedRedisLimitingState,
        stale_after_ms: u64,
    ) -> bool {
        match state {
            SuppressedRedisLimitingState::Undefined => true,
            SuppressedRedisLimitingState::Accepting { last_modified, .. } => {
                let last_modified = match last_modified.lock() {
                    Ok(last_modified) => last_modified,
                    Err(err) => {
                        tracing::warn!("last_modified is poisoned: {err:?}");
                        return true;
                    }
                };

                last_modified.elapsed().as_millis() >= stale_after_ms as u128
            }
            SuppressedRedisLimitingState::Suppressing {
                time_instant,
                suppression_factor_ttl_ms,
                ..
            } => {
                let elapsed_ms = match time_instant.lock() {
                    Ok(time_instant) => time_instant.elapsed().as_millis(),
                    Err(err) => {
                        tracing::warn!("time_instant is poisoned: {err:?}");
                        return true;
                    }
                };
                let cache_ttl_ms = match suppression_factor_ttl_ms.lock() {
                    Ok(suppression_factor_ttl_ms) => *suppression_factor_ttl_ms as u128,
                    Err(err) => {
                        tracing::warn!("suppression_factor_ttl_ms is poisoned: {err:?}");
                        return true;
                    }
                };

                elapsed_ms.saturating_sub(cache_ttl_ms) >= stale_after_ms as u128
            }
        }
    }

    pub(super) fn is_stale_for_flush(
        &self,
        state: &SuppressedRedisLimitingState,
    ) -> Result<bool, TrypemaError> {
        let elapsed_ms = match state {
            SuppressedRedisLimitingState::Undefined => return Ok(false),
            SuppressedRedisLimitingState::Accepting { last_modified, .. } => {
                mutex_lock(last_modified, "accepting.last_modified")?
                    .elapsed()
                    .as_millis()
            }
            SuppressedRedisLimitingState::Suppressing { time_instant, .. } => {
                mutex_lock(time_instant, "suppressing.time_instant")?
                    .elapsed()
                    .as_millis()
            }
        };

        Ok(elapsed_ms > (*self.window_size * 1_000) as u128)
    } // end fn is_stale_for_flush

    pub(super) fn has_pending_counts(state: &SuppressedRedisLimitingState) -> bool {
        match state {
            SuppressedRedisLimitingState::Accepting { count, .. } => {
                count.load(Ordering::Acquire) > 0
            }
            SuppressedRedisLimitingState::Suppressing {
                count,
                declined_count,
                ..
            } => count.load(Ordering::Acquire) > 0 || declined_count.load(Ordering::Acquire) > 0,
            SuppressedRedisLimitingState::Undefined => false,
        }
    } // end fn has_pending_counts

    pub(super) fn collect_flush_candidates(&self) -> (Vec<RedisKey>, Vec<RedisKey>) {
        let mut resets: Vec<RedisKey> = Vec::new();
        let mut stale: Vec<RedisKey> = Vec::new();

        for state in self.limiting_state.iter() {
            let is_stale = match self.is_stale_for_flush(state.value()) {
                Ok(is_stale) => is_stale,
                Err(err) => {
                    tracing::warn!(error = ?err, "failed to inspect local state for flush");
                    continue;
                }
            };

            if is_stale {
                stale.push(state.key().clone());
            } else if Self::has_pending_counts(state.value()) {
                resets.push(state.key().clone());
            }
        }

        (resets, stale)
    } // end fn collect_flush_candidates

    pub(super) fn reset_stale_local_state(&self, key: &RedisKey) -> Result<(), TrypemaError> {
        let should_reset = self
            .limiting_state
            .get(key)
            .is_some_and(|state| self.is_stale_for_flush(state.value()).unwrap_or(false));

        if !should_reset {
            return Ok(());
        }

        if let Some(mut state) = self.limiting_state.get_mut(key)
            && self.is_stale_for_flush(state.value())?
        {
            *state = SuppressedRedisLimitingState::Undefined;
        }

        Ok(())
    } // end fn reset_stale_local_state
} // end impl
