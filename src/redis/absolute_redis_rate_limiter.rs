use std::{
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use dashmap::DashMap;
use redis::{Script, aio::ConnectionManager};
use tokio::sync::{RwLock, RwLockWriteGuard, mpsc};

use crate::{
    RateGroupSizeMs, RateLimit, RateLimitDecision, RedisKey, RedisKeyGenerator,
    RedisRateLimiterOptions, TrypemaError, WindowSizeSeconds,
    common::RateType,
    redis::{
        RedisRateLimiterSignal,
        absolute_redis_commiter::{
            AbsoluteRedisCommit, AbsoluteRedisCommitter, AbsoluteRedisCommitterOptions,
        },
        absolute_redis_proxy::{AbsoluteRedisProxy, AbsoluteRedisProxyReadStateResult},
    },
};

const ABSOLUTE_INC_LUA: &str = r#"
    local time_array = redis.call("TIME")
    local timestamp_ms = tonumber(time_array[1]) * 1000 + math.floor(tonumber(time_array[2]) / 1000)


    local hash_key = KEYS[1]
    local active_keys = KEYS[2]
    local window_limit_key = KEYS[3]
    local total_count_key = KEYS[4]
    local get_active_entities_key = KEYS[5]

    local entity = ARGV[1]
    local window_size_seconds = tonumber(ARGV[2])
    local window_limit = tonumber(ARGV[3])
    local rate_group_size_ms = tonumber(ARGV[4])
    local count = tonumber(ARGV[5])

    redis.call("ZADD", get_active_entities_key, timestamp_ms, entity)


    local total_count = tonumber(redis.call("GET", total_count_key)) or 0

    if total_count + count > window_limit then
        local to_remove_keys = redis.call("ZRANGE", active_keys, "-inf", timestamp_ms - window_size_seconds * 1000, "BYSCORE")

        if #to_remove_keys > 0 then
            local to_remove = redis.call("HMGET", hash_key, unpack(to_remove_keys))
            redis.call("HDEL", hash_key, unpack(to_remove_keys))

            local remove_sum = 0

            for i = 1, #to_remove do
                local value = tonumber(to_remove[i])
                if value then
                    remove_sum = remove_sum + value
                end
            end

            total_count = redis.call("DECRBY", total_count_key, remove_sum)
            redis.call("ZREM", active_keys, unpack(to_remove_keys))
        end
    end

    if total_count + count > window_limit then
        local oldest_hash_fields = redis.call("ZRANGE", active_keys, 0, 0, "WITHSCORES")

        if #oldest_hash_fields == 0 then
            return {"rejected", 0, 0}
        end

        local oldest_hash_field = oldest_hash_fields[1]
        local oldest_hash_field_group_timestamp = tonumber(oldest_hash_fields[2])
        local oldest_hash_field_ttl = (window_size_seconds * 1000) - timestamp_ms + oldest_hash_field_group_timestamp
        local oldest_count = tonumber(redis.call("HGET", hash_key, oldest_hash_field)) or 0

        return {"rejected", oldest_hash_field_ttl, oldest_count}
    end

    local latest_hash_field_entry = redis.call("ZRANGE", active_keys, 0, 0, "REV", "WITHSCORES")
    if #latest_hash_field_entry > 0 then
        local latest_hash_field = latest_hash_field_entry[1]
        local latest_hash_field_group_timestamp = tonumber(latest_hash_field_entry[2])
        local latest_hash_field_age_ms = timestamp_ms - latest_hash_field_group_timestamp

        if latest_hash_field_age_ms > 0 and latest_hash_field_age_ms < rate_group_size_ms then
            timestamp_ms = tonumber(latest_hash_field)
        end
    end

    local hash_field = tostring(timestamp_ms)

    local new_count = redis.call("HINCRBY", hash_key, hash_field, count)
    redis.call("INCRBY", total_count_key, count)

    if new_count == count then
        redis.call("ZADD", active_keys, timestamp_ms, hash_field)
        redis.call("SET", window_limit_key, window_limit)
    end

    redis.call("EXPIRE", window_limit_key, window_size_seconds)

    return {"allowed", 0, 0}
"#;

const ABSOLUTE_IS_ALLOWED_LUA: &str = r#"
    local time_array = redis.call("TIME")
    local timestamp_ms = tonumber(time_array[1]) * 1000 + math.floor(tonumber(time_array[2]) / 1000)


    local hash_key = KEYS[1]
    local active_keys = KEYS[2]
    local window_limit_key = KEYS[3]
    local total_count_key = KEYS[4]

    local window_size_seconds = tonumber(ARGV[1])
    local rate_group_size_ms = tonumber(ARGV[2])

    local window_limit = tonumber(redis.call("GET", window_limit_key))
    if window_limit == nil then
        return {"allowed", 0, 0}
    end


    local total_count = tonumber(redis.call("GET", total_count_key)) or 0

    if total_count >= window_limit then
        local to_remove_keys = redis.call("ZRANGE", active_keys, "-inf", timestamp_ms - window_size_seconds * 1000, "BYSCORE")

        if #to_remove_keys > 0 then
            local to_remove = redis.call("HMGET", hash_key, unpack(to_remove_keys))
            redis.call("HDEL", hash_key, unpack(to_remove_keys))

            local remove_sum = 0

            for i = 1, #to_remove do
                local value = tonumber(to_remove[i])
                if value then
                    remove_sum = remove_sum + value
                end
            end

            total_count = redis.call("DECRBY", total_count_key, remove_sum)
            redis.call("ZREM", active_keys, unpack(to_remove_keys))
        end
    end


    if total_count >= window_limit then
        local oldest_hash_fields = redis.call("ZRANGE", active_keys, 0, 0, "WITHSCORES")

        if #oldest_hash_fields == 0 then
            return {"rejected", 0, 0}
        end

        local oldest_hash_field = oldest_hash_fields[1]
        local oldest_hash_field_group_timestamp = tonumber(oldest_hash_fields[2])
        local oldest_hash_field_ttl = (window_size_seconds * 1000) - timestamp_ms + oldest_hash_field_group_timestamp
        local oldest_count = tonumber(redis.call("HGET", hash_key, oldest_hash_field)) or 0

        return {"rejected", oldest_hash_field_ttl, oldest_count}
    end

    return {"allowed", 0, 0}
"#;

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
        window_limit: u64,
        accept_limit: u64,
        count: AtomicU64,
        time_instant: Instant,
        last_rate_group_ttl: Option<u64>,
        last_rate_group_count: Option<u64>,
    },
    Undefined {
        time_instant: Instant,
    },
    Rejecting {
        time_instant: Instant,
        ttl_ms: u64,
        release_time_instant: Instant,
        count_after_release: u64,
    },
}

/// A rate limiter backed by Redis.
#[derive(Clone, Debug)]
pub struct AbsoluteRedisRateLimiter {
    connection_manager: ConnectionManager,
    window_size_seconds: WindowSizeSeconds,
    rate_group_size_ms: RateGroupSizeMs,
    key_generator: RedisKeyGenerator,
    inc_script: Script,
    is_allowed_script: Script,
    cleanup_script: Script,
    commiter_sender: mpsc::Sender<AbsoluteRedisCommit>,
    redis_proxy: AbsoluteRedisProxy,

    // ...
    limiting_state: DashMap<RedisKey, Arc<RwLock<AbsoluteRedisLimitingState>>>,
    local_cache_duration_ms: u64,
}

impl AbsoluteRedisRateLimiter {
    pub(crate) fn new(options: RedisRateLimiterOptions) -> Arc<Self> {
        let prefix = options.prefix.unwrap_or_else(RedisKey::default_prefix);

        let (tx, rx) = tokio::sync::mpsc::channel::<RedisRateLimiterSignal>(4);

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
            window_size_seconds: options.window_size_seconds,
            rate_group_size_ms: options.rate_group_size_ms,
            key_generator: RedisKeyGenerator::new(prefix, RateType::Absolute),
            inc_script: Script::new(ABSOLUTE_INC_LUA),
            is_allowed_script: Script::new(ABSOLUTE_IS_ALLOWED_LUA),
            cleanup_script: Script::new(ABSOLUTE_CLEANUP_LUA),
            commiter_sender,
            redis_proxy,
            limiting_state: DashMap::new(),
            local_cache_duration_ms,
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
                        limiter.flush().await;
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
            .is_allowed_with_count_increment(key, count, count)
            .await?;

        if !matches!(decision, RateLimitDecision::Allowed) {
            return Ok(decision);
        }

        let state_entry = self.limiting_state.get(key).expect("Key should be present");

        if let AbsoluteRedisLimitingState::Undefined { .. } = state_entry.read().await.deref() {
            let mut state = state_entry.write().await;
            let window_limit = *self.window_size_seconds as f64 * **rate_limit;

            *state = AbsoluteRedisLimitingState::Accepting {
                window_limit: window_limit as u64,
                accept_limit: window_limit as u64,
                count: AtomicU64::new(count),
                time_instant: Instant::now(),
                last_rate_group_ttl: None,
                last_rate_group_count: None,
            };
        }

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
        return self.is_allowed_with_count_increment(key, 1, 0).await;
    } // end method is_allowed

    async fn reset_state_from_redis_read_result_and_get_decision(
        &self,
        key: &RedisKey,
        check_count: u64,
        increment: u64,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let state_entry = match self.limiting_state.get(key) {
            Some(state) => state,
            None => {
                self.limiting_state.entry(key.clone()).or_insert_with(|| {
                    Arc::new(RwLock::new(AbsoluteRedisLimitingState::Undefined {
                        time_instant: Instant::now(),
                    }))
                });

                self.limiting_state.get(key).expect("Key should be present")
            }
        };

        let state = state_entry.write().await;

        let read_state_result = self
            .redis_proxy
            .read_state(key, *self.window_size_seconds as u128 * 1000)
            .await?;

        self.reset_single_state_from_read_result(state, read_state_result, check_count, increment)
            .await
    } // end method reset_state_from_redis_read_result

    async fn reset_single_state_from_read_result(
        &self,
        mut state: RwLockWriteGuard<'_, AbsoluteRedisLimitingState>,
        read_state_result: AbsoluteRedisProxyReadStateResult,
        check_count: u64,
        increment: u64,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let Some(window_limit) = read_state_result.window_limit else {
            *state = AbsoluteRedisLimitingState::Undefined {
                time_instant: Instant::now(),
            };

            return Ok(RateLimitDecision::Allowed);
        };

        if read_state_result.current_total_count + check_count > window_limit
            && let Some(retry_after_ms) = read_state_result.last_rate_group_ttl
            && let Some(remaining_after_waiting) = read_state_result.last_rate_group_count
        {
            *state = AbsoluteRedisLimitingState::Rejecting {
                time_instant: Instant::now(),
                release_time_instant: Instant::now() + Duration::from_millis(retry_after_ms),
                ttl_ms: retry_after_ms,
                count_after_release: remaining_after_waiting,
            };

            return Ok(RateLimitDecision::Rejected {
                window_size_seconds: *self.window_size_seconds,
                retry_after_ms: retry_after_ms as u128,
                remaining_after_waiting,
            });
        } else if read_state_result.current_total_count + check_count <= window_limit {
            *state = AbsoluteRedisLimitingState::Accepting {
                window_limit,
                accept_limit: window_limit - read_state_result.current_total_count,
                count: AtomicU64::new(increment),
                time_instant: Instant::now(),
                last_rate_group_ttl: read_state_result.last_rate_group_ttl,
                last_rate_group_count: read_state_result.last_rate_group_count,
            };

            return Ok(RateLimitDecision::Allowed);
        }

        *state = AbsoluteRedisLimitingState::Undefined {
            time_instant: Instant::now(),
        };

        Ok(RateLimitDecision::Allowed)
    }

    async fn is_allowed_with_count_increment(
        &self,
        key: &RedisKey,
        check_count: u64,
        increment: u64,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let state_entry = match self.limiting_state.get(key) {
            Some(state) => state,
            None => {
                self.limiting_state.entry(key.clone()).or_insert_with(|| {
                    Arc::new(RwLock::new(AbsoluteRedisLimitingState::Undefined {
                        time_instant: Instant::now(),
                    }))
                });

                self.limiting_state.get(key).expect("Key should be present")
            }
        };

        let state_lock = state_entry.read().await;

        if let AbsoluteRedisLimitingState::Accepting {
            window_limit,
            accept_limit,
            count,
            time_instant,
            last_rate_group_ttl,
            last_rate_group_count,
        } = state_lock.deref()
        {
            let elapsed = time_instant.elapsed().as_millis();
            let last_rate_group_ttl: Option<u64> = *last_rate_group_ttl;
            let last_rate_group_count: Option<u64> = *last_rate_group_count;
            let current_total_count = count.load(Ordering::Relaxed);
            let window_limit = *window_limit;

            if count.load(Ordering::Relaxed) + check_count <= *accept_limit {
                if elapsed < self.local_cache_duration_ms as u128 {
                    count.fetch_add(increment, Ordering::Relaxed);
                    return Ok(RateLimitDecision::Allowed);
                } else {
                    drop(state_lock);

                    let commit = AbsoluteRedisCommit {
                        key: key.clone(),
                        window_size_seconds: *self.window_size_seconds,
                        window_limit,
                        rate_group_size_ms: *self.rate_group_size_ms,
                        count: current_total_count,
                    };

                    self.commiter_sender.send(commit).await.map_err(|e| {
                        TrypemaError::CustomError(format!(
                            "Failed to send commit signal to absolute Redis rate limiter commiter: {e}"
                        ))
                    })?;

                    return self
                        .reset_state_from_redis_read_result_and_get_decision(
                            key,
                            check_count,
                            increment,
                        )
                        .await;
                }
            }

            drop(state_lock);
            let mut state = state_entry.write().await;

            if let Some(last_rate_group_ttl) = last_rate_group_ttl
                && let Some(last_rate_group_count) = last_rate_group_count
                && elapsed < last_rate_group_ttl as u128
            {
                let retry_after_ms = (last_rate_group_ttl as u128).saturating_sub(elapsed);
                let remaining_after_waiting = last_rate_group_count;
                let window_size_seconds = *self.window_size_seconds;

                let commit = AbsoluteRedisCommit {
                    key: key.clone(),
                    window_size_seconds: *self.window_size_seconds,
                    window_limit,
                    rate_group_size_ms: *self.rate_group_size_ms,
                    count: current_total_count,
                };

                self.commiter_sender.send(commit).await.map_err(|e| {
                    TrypemaError::CustomError(format!(
                        "Failed to send commit signal to absolute Redis rate limiter commiter: {e}"
                    ))
                })?;

                *state = AbsoluteRedisLimitingState::Rejecting {
                    time_instant: Instant::now(),
                    release_time_instant: Instant::now()
                        + Duration::from_millis(retry_after_ms as u64),
                    ttl_ms: last_rate_group_ttl,
                    count_after_release: remaining_after_waiting,
                };

                return Ok(RateLimitDecision::Rejected {
                    window_size_seconds,
                    retry_after_ms,
                    remaining_after_waiting,
                });
            }

            drop(state);

            return self
                .reset_state_from_redis_read_result_and_get_decision(key, check_count, increment)
                .await;
        } else if let AbsoluteRedisLimitingState::Rejecting {
            time_instant,
            ttl_ms,
            release_time_instant,
            count_after_release,
        } = state_lock.deref()
        {
            if release_time_instant.elapsed().as_millis() == 0 {
                return Ok(RateLimitDecision::Rejected {
                    window_size_seconds: *self.window_size_seconds,
                    retry_after_ms: (*ttl_ms as u128)
                        .saturating_sub(time_instant.elapsed().as_millis()),
                    remaining_after_waiting: *count_after_release,
                });
            }

            drop(state_lock);
            return self
                .reset_state_from_redis_read_result_and_get_decision(key, check_count, increment)
                .await;
        } else {
            drop(state_lock);
            return self
                .reset_state_from_redis_read_result_and_get_decision(key, check_count, increment)
                .await;
        }
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

    async fn flush(&self) {
        let mut resets: Vec<RedisKey> = Vec::new();

        for state in self.limiting_state.iter() {
            let key = state.key();
            let state_lock = state.value().read().await;

            match state_lock.deref() {
                AbsoluteRedisLimitingState::Undefined { .. }
                | AbsoluteRedisLimitingState::Rejecting { .. } => {}
                AbsoluteRedisLimitingState::Accepting {
                    window_limit,
                    accept_limit,
                    count,
                    ..
                } => {
                    let commit = AbsoluteRedisCommit {
                        key: key.clone(),
                        window_size_seconds: *self.window_size_seconds,
                        window_limit: *window_limit,
                        rate_group_size_ms: *self.rate_group_size_ms,
                        count: count.load(Ordering::Relaxed),
                    };

                    if let Err(err) = self.commiter_sender.send(commit).await {
                        tracing::error!(error = ?err, "Failed to send commit signal to Redis rate limiter");
                        continue;
                    }

                    resets.push(key.clone());
                }
            }
        }

        // TODO: batch reset
        for key in resets {
            self.reset_state_from_redis_read_result_and_get_decision(&key, 0, 0)
                .await
                .unwrap();
        }
    } // end method flush
}
