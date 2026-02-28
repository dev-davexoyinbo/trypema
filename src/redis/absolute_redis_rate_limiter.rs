use std::{
    cell::RefCell,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use dashmap::DashMap;
use redis::{Script, aio::ConnectionManager};
use tokio::sync::mpsc;

use crate::{
    RateGroupSizeMs, RateLimit, RateLimitDecision, RedisKey, RedisKeyGenerator,
    RedisRateLimiterOptions, TrypemaError, WindowSizeSeconds,
    common::RateType,
    redis::{
        RedisRateLimiterSignal,
        absolute_redis_commiter::{
            AbsoluteRedisCommit, AbsoluteRedisCommitter, AbsoluteRedisCommitterOptions,
            AbsoluteRedisCommitterSignal,
        },
        absolute_redis_proxy::{AbsoluteRedisProxy, AbsoluteRedisProxyReadStateResult},
    },
};

const ABSOLUTE_CLEANUP_LUA: &str = r#"
    local time_array = redis.call("TIME")
    local timestamp_ms = tonumber(time_array[1]) * 1000 + math.floor(tonumber(time_array[2]) / 1000)

    local prefix = KEYS[1]
    local rate_type = KEYS[2]
    local active_entities_key = KEYS[3]

    local stale_after_ms = tonumber(ARGV[1]) or 0
    local hash_suffix = ARGV[2]
    local window_limit_suffix = ARGV[3]
    local total_count_suffix = ARGV[4]
    local active_keys_suffix = ARGV[5]
    local suppression_factor_key_suffix = ARGV[6]


    local active_entities = redis.call("ZRANGE", active_entities_key, "-inf", timestamp_ms - stale_after_ms, "BYSCORE")

    if #active_entities == 0 then
        return 
    end

    local remove_keys = {}

    local suffixes = {hash_suffix, window_limit_suffix, total_count_suffix, active_keys_suffix, suppression_factor_key_suffix}
    for i = 1, #active_entities do
        local entity = active_entities[i]

        for i = 1, #suffixes do
            table.insert(remove_keys, prefix .. ":" .. entity .. ":" .. rate_type .. ":" .. suffixes[i])
        end
    end

    if #remove_keys > 0 then
        redis.call("DEL", unpack(remove_keys))
        redis.call("ZREM", active_entities_key, unpack(active_entities))
    end

    return
"#;

#[derive(Debug)]
enum AbsoluteRedisLimitingState {
    Accepting {
        window_limit: RefCell<u64>,
        accept_limit: RefCell<u64>,
        count: AtomicU64,
        time_instant: RefCell<Instant>,
        last_rate_group_ttl: RefCell<Option<u64>>,
        last_rate_group_count: RefCell<Option<u64>>,
    },
    Undefined,
    Rejecting {
        time_instant: RefCell<Instant>,
        ttl_ms: RefCell<u64>,
        count_after_release: RefCell<u64>,
    },
}

/// A rate limiter backed by Redis.
#[derive(Debug)]
pub struct AbsoluteRedisRateLimiter {
    connection_manager: ConnectionManager,
    window_size_seconds: WindowSizeSeconds,
    window_size_ms: u128,
    rate_group_size_ms: RateGroupSizeMs,
    key_generator: RedisKeyGenerator,
    cleanup_script: Script,
    commiter_sender: mpsc::Sender<AbsoluteRedisCommitterSignal>,
    redis_proxy: AbsoluteRedisProxy,

    // ...
    limiting_state: DashMap<RedisKey, AbsoluteRedisLimitingState>,
}

impl AbsoluteRedisRateLimiter {
    pub(crate) fn new(options: RedisRateLimiterOptions) -> Arc<Self> {
        let prefix = options.prefix.unwrap_or_else(RedisKey::default_prefix);

        let (tx, rx) = tokio::sync::mpsc::channel::<RedisRateLimiterSignal>(1);

        let redis_proxy =
            AbsoluteRedisProxy::new(prefix.clone(), options.connection_manager.clone());

        let local_cache_duration_ms = 50u64;

        let commiter_sender = AbsoluteRedisCommitter::run(AbsoluteRedisCommitterOptions {
            local_cache_duration: Duration::from_millis(local_cache_duration_ms),
            channel_capacity: 8192,
            max_batch_size: 128,
            limiter_sender: tx,
            redis_proxy: redis_proxy.clone(),
        });

        let limiter = Self {
            connection_manager: options.connection_manager,
            window_size_ms: (*options.window_size_seconds as u128).saturating_mul(1000),
            window_size_seconds: options.window_size_seconds,
            rate_group_size_ms: options.rate_group_size_ms,
            key_generator: RedisKeyGenerator::new(prefix, RateType::Absolute),
            cleanup_script: Script::new(ABSOLUTE_CLEANUP_LUA),
            commiter_sender,
            redis_proxy,
            limiting_state: DashMap::new(),
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

        tokio::spawn(async move {
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
            .await?;

        self.reset_single_state_from_read_result(
            read_state_result,
            check_count,
            increment,
            rate_limit,
        )
        .await
    } // end method reset_state_from_redis_read_result

    async fn send_commit(&self, commit: AbsoluteRedisCommit) -> Result<(), TrypemaError> {
        self.commiter_sender
            .send(commit.into())
            .await
            .map_err(|err| TrypemaError::CustomError(format!("Failed to send commit: {err:?}")))?;

        Ok(())
    } // end method send_commit

    async fn reset_single_state_from_read_result(
        &self,
        read_state_result: AbsoluteRedisProxyReadStateResult,
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
                    current_window_limit = Some(*window_limit.borrow());
                }

                if count > 0 {
                    current_total_count += count;

                    let commit = AbsoluteRedisCommit {
                        key: key.clone(),
                        window_size_seconds: *self.window_size_seconds,
                        window_limit: current_window_limit.expect("Window limit should be set"),
                        rate_group_size_ms: *self.rate_group_size_ms,
                        count,
                    };

                    drop(state);
                    // TODO: fix the Send issue
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

                    if is_undefined {
                        *self
                            .limiting_state
                            .get_mut(key)
                            .expect("Key should be present") =
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
                .unwrap_or(self.window_size_ms as u64);
            let new_count_after_release = read_state_result.current_total_count;

            if let AbsoluteRedisLimitingState::Rejecting {
                time_instant,
                ttl_ms,
                count_after_release,
            } = state.deref()
            {
                time_instant.replace(new_time_instant);
                ttl_ms.replace(new_ttl_ms);
                count_after_release.replace(new_count_after_release);
            } else {
                drop(state);
                let mut state = self
                    .limiting_state
                    .get_mut(key)
                    .expect("Key should be present");

                *state = AbsoluteRedisLimitingState::Rejecting {
                    time_instant: RefCell::new(new_time_instant),
                    ttl_ms: RefCell::new(new_ttl_ms),
                    count_after_release: RefCell::new(new_count_after_release),
                };
            }

            return Ok(RateLimitDecision::Rejected {
                window_size_seconds: *self.window_size_seconds,
                retry_after_ms: self.window_size_ms,
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
            window_limit.replace(new_window_limit);
            accept_limit.replace(new_accept_limit);
            count.store(increment, Ordering::Relaxed);
            time_instant.replace(new_time_instant);
            last_rate_group_ttl.replace(read_state_result.last_rate_group_ttl);
            last_rate_group_count.replace(read_state_result.last_rate_group_count);
        } else {
            drop(state);
            let mut state = self
                .limiting_state
                .get_mut(key)
                .expect("Key should be present");

            *state = AbsoluteRedisLimitingState::Accepting {
                window_limit: RefCell::new(new_window_limit),
                accept_limit: RefCell::new(new_accept_limit),
                count: AtomicU64::new(increment),
                time_instant: RefCell::new(new_time_instant),
                last_rate_group_ttl: RefCell::new(read_state_result.last_rate_group_ttl),
                last_rate_group_count: RefCell::new(read_state_result.last_rate_group_count),
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
            && time_instant.elapsed().as_millis() < *ttl_ms as u128
        {
            return Ok(RateLimitDecision::Rejected {
                window_size_seconds: *self.window_size_seconds,
                retry_after_ms: (*ttl_ms as u128)
                    .saturating_sub(time_instant.elapsed().as_millis()),
                remaining_after_waiting: *count_after_release,
            });
        } else if let AbsoluteRedisLimitingState::Accepting {
            accept_limit,
            count,
            ..
        } = state_entry.deref()
        {
            if count.load(Ordering::Relaxed) + check_count <= *accept_limit {
                count.fetch_add(increment, Ordering::Relaxed);
                return Ok(RateLimitDecision::Allowed);
            }

            drop(state_entry);

            let permit = self.commiter_sender.reserve().await.map_err(|_| {
                TrypemaError::CustomError("Failed to reserve commiter sender".to_string())
            })?;

            let Some(mut state_entry) = self.limiting_state.get_mut(key) else {
                unreachable!("Key should be present");
            };

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

                if current_total_count > 0 {
                    let commit = AbsoluteRedisCommit {
                        key: key.clone(),
                        window_size_seconds: *self.window_size_seconds,
                        window_limit: *window_limit,
                        rate_group_size_ms: *self.rate_group_size_ms,
                        count: current_total_count,
                    };

                    permit.send(commit.into());
                }

                let last_rate_group_ttl: u128 = last_rate_group_ttl
                    .map(|el| el as u128)
                    .unwrap_or(self.window_size_ms);
                let elapsed = time_instant.elapsed().as_millis();
                let retry_after_ms = last_rate_group_ttl.saturating_sub(elapsed);
                let remaining_after_waiting = last_rate_group_count.unwrap_or(current_total_count);

                // If we can still increment by at least one, then we don't set the state to
                // rejecting
                if current_total_count >= *window_limit {
                    *state_entry = AbsoluteRedisLimitingState::Rejecting {
                        time_instant: Instant::now(),
                        ttl_ms: retry_after_ms as u64,
                        count_after_release: remaining_after_waiting,
                    };
                }

                return Ok(RateLimitDecision::Rejected {
                    window_size_seconds: *self.window_size_seconds,
                    retry_after_ms,
                    remaining_after_waiting,
                });
            }

            drop(state_entry);
            return self
                .reset_state_from_redis_read_result_and_get_decision(
                    key,
                    check_count,
                    increment,
                    rate_limit,
                )
                .await;
        }

        drop(state_entry);
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
        let mut connection_manager = self.connection_manager.clone();

        let _: () = self
            .cleanup_script
            .key(self.key_generator.prefix.to_string())
            .key(self.key_generator.rate_type.to_string())
            .key(self.key_generator.get_active_entities_key())
            .arg(stale_after_ms)
            .arg(self.key_generator.hash_key_suffix.to_string())
            .arg(self.key_generator.window_limit_key_suffix.to_string())
            .arg(self.key_generator.total_count_key_suffix.to_string())
            .arg(self.key_generator.active_keys_key_suffix.to_string())
            .arg(self.key_generator.suppression_factor_key_suffix.to_string())
            .invoke_async(&mut connection_manager)
            .await?;

        Ok(())
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
