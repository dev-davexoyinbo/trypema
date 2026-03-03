use redis::{Script, aio::ConnectionManager};

use crate::{
    TrypemaError,
    common::RateType,
    hybrid::RedisProxyCommitter,
    redis::{RedisKey, RedisKeyGenerator},
};

const CLEANUP_LUA: &str = r#"
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

const LUA_HELPERS: &str = r#"
    local function now_ms()
        local time_array = redis.call("TIME")
        return tonumber(time_array[1]) * 1000 + math.floor(tonumber(time_array[2]) / 1000)
    end

    local function cleanup_expired_keys(hash_key, active_keys, total_count_key, timestamp_ms, window_size_seconds)
        local to_remove_keys = redis.call("ZRANGE", active_keys, "-inf", timestamp_ms - window_size_seconds * 1000, "BYSCORE")

        if #to_remove_keys == 0 then
            return
        end

        local to_remove = redis.call("HMGET", hash_key, unpack(to_remove_keys))
        redis.call("HDEL", hash_key, unpack(to_remove_keys))

        local remove_sum = 0

        for i = 1, #to_remove do
            remove_sum = remove_sum + (tonumber(to_remove[i]) or 0)
        end

        redis.call("DECRBY", total_count_key, remove_sum)
        redis.call("ZREM", active_keys, unpack(to_remove_keys))
    end

    local function calculate_total_in_last_second(hash_key, active_keys, timestamp_ms)
        local active_keys_in_1s = redis.call("ZRANGE", active_keys, "+inf", timestamp_ms - 1000, "BYSCORE", "REV")

        local total_in_last_second = 0

        if #active_keys_in_1s > 0 then
            local values = redis.call("HMGET", hash_key, unpack(active_keys_in_1s))
            for i = 1, #values do
                total_in_last_second = total_in_last_second + (tonumber(values[i]) or 0)
            end
        end

        return total_in_last_second
    end

    local function calculate_suppression_factor(hash_key, active_keys, total_count, total_in_last_second, timestamp_ms, window_size_seconds, window_limit, hard_limit_factor)
        if window_limit == nil then
            return 0
        end

        if total_count >= window_limit then
            return 1
        elseif total_count < window_limit / hard_limit_factor then
            return 0
        end

        local average_rate_in_window = total_count / window_size_seconds
        local perceived_rate_limit = average_rate_in_window

        if total_in_last_second > average_rate_in_window then
            perceived_rate_limit = total_in_last_second
        end

        local rate_limit = window_limit / window_size_seconds / hard_limit_factor

        return 1 - (rate_limit / perceived_rate_limit)
    end
"#;

const COMMIT_STATE_SCRIPT: &str = r#"
    local timestamp_ms = now_ms()

    local hash_key = KEYS[1]
    local active_keys = KEYS[2]
    local window_limit_key = KEYS[3]
    local total_count_key = KEYS[4]
    local get_active_entities_key = KEYS[5]
    local suppression_factor_key = KEYS[6]

    local entity = ARGV[1]
    local window_size_seconds = tonumber(ARGV[2])
    local window_limit_to_consider = tonumber(ARGV[3])
    local rate_group_size_ms = tonumber(ARGV[4])
    local suppression_factor_cache_ms = tonumber(ARGV[5])
    local hard_limit_factor = tonumber(ARGV[6])
    local count = tonumber(ARGV[7])

    local window_limit = tonumber(redis.call("GET", window_limit_key)) or window_limit_to_consider

    redis.call("ZADD", get_active_entities_key, timestamp_ms, entity)

    cleanup_expired_keys(hash_key, active_keys, total_count_key, timestamp_ms, window_size_seconds)

    local latest_hash_field_entry = redis.call("ZRANGE", active_keys, 0, 0, "REV", "WITHSCORES")
    if #latest_hash_field_entry > 0 then
        local latest_hash_field = latest_hash_field_entry[1]
        local latest_hash_field_group_timestamp = tonumber(latest_hash_field_entry[2])
        local latest_hash_field_age_ms = timestamp_ms - latest_hash_field_group_timestamp

        if latest_hash_field_age_ms >= 0 and latest_hash_field_age_ms < rate_group_size_ms then
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
"#;

const READ_STATE_SCRIPT: &str = r#"
    local timestamp_ms = now_ms()

    local hash_key = KEYS[1]
    local active_keys = KEYS[2]
    local window_limit_key = KEYS[3]
    local total_count_key = KEYS[4]
    local get_active_entities_key = KEYS[5]
    local suppression_factor_key = KEYS[6]

    local entity = ARGV[1]
    local window_size_seconds = tonumber(ARGV[2])
    local suppression_factor_cache_ms = tonumber(ARGV[5])
    local hard_limit_factor = tonumber(ARGV[6])

    local window_limit = tonumber(redis.call("GET", window_limit_key))

    redis.call("ZADD", get_active_entities_key, timestamp_ms, entity)

    cleanup_expired_keys(hash_key, active_keys, total_count_key, timestamp_ms, window_size_seconds)

    local total_count = tonumber(redis.call("GET", total_count_key)) or 0

    local suppression_factor = tonumber(redis.call("GET", suppression_factor_key))

    local total_in_last_second = calculate_total_in_last_second(hash_key, active_keys, timestamp_ms);

    if suppression_factor == nil or suppression_factor < 0 or suppression_factor > 1 then
        suppression_factor = calculate_suppression_factor(hash_key, active_keys, total_count, total_in_last_second, timestamp_ms, window_size_seconds, window_limit, hard_limit_factor)

        redis.call("SET", suppression_factor_key, suppression_factor, "PX", suppression_factor_cache_ms)
    end

    return {entity, tostring(suppression_factor), total_count, total_in_last_second, window_limit or -1 }
"#;

#[derive(Debug)]
pub(crate) struct SuppressedHybridCommit {
    pub key: RedisKey,
    pub window_size_seconds: u64,
    pub window_limit: u64,
    pub rate_group_size_ms: u64,
    pub count: u64,
}

#[derive(Debug)]
pub(crate) struct SuppressedHybridRedisProxyReadStateResult {
    pub key: RedisKey,
    pub current_total_count: u64,
    pub last_second_count: u64,
    pub suppression_factor: f64,
    pub window_limit: Option<u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct SuppressedHybridRedisProxy {
    key_generator: RedisKeyGenerator,
    read_state_script: Script,
    commit_state_script: Script,
    cleanup_script: Script,
    connection_manager: ConnectionManager,
}

impl SuppressedHybridRedisProxy {
    pub(crate) fn new(prefix: RedisKey, connection_manager: ConnectionManager) -> Self {
        Self {
            key_generator: RedisKeyGenerator::new(prefix, RateType::Suppressed),
            read_state_script: Script::new(&format!("{}\n{}", LUA_HELPERS, READ_STATE_SCRIPT)),
            commit_state_script: Script::new(&format!("{}\n{}", LUA_HELPERS, COMMIT_STATE_SCRIPT)),
            cleanup_script: Script::new(CLEANUP_LUA),
            connection_manager,
        }
    }

    pub(crate) async fn read_state(
        self: &SuppressedHybridRedisProxy,
        key: &RedisKey,
        window_size_ms: u128,
    ) -> Result<SuppressedHybridRedisProxyReadStateResult, TrypemaError> {
        let mut connection_manager = self.connection_manager.clone();

        let res: (String, f64, u64, u64, i64) = self
            .read_state_script
            .key(self.key_generator.get_hash_key(key))
            .key(self.key_generator.get_active_keys(key))
            .key(self.key_generator.get_window_limit_key(key))
            .key(self.key_generator.get_total_count_key(key))
            .arg(key.as_str())
            .arg(window_size_ms)
            .invoke_async(&mut connection_manager)
            .await?;

        Ok(map_redis_read_result_to_state(res))
    } // end method read_state

    #[inline]
    fn build_commit_pipeline(
        &self,
        commits: &[SuppressedHybridCommit],
        should_load_script: bool,
    ) -> redis::Pipeline {
        let mut pipe = redis::Pipeline::new();
        if should_load_script {
            pipe.load_script(&self.commit_state_script).ignore();
        }

        for commit in commits {
            pipe.invoke_script(
                self.commit_state_script
                    .key(self.key_generator.get_hash_key(&commit.key))
                    .key(self.key_generator.get_active_keys(&commit.key))
                    .key(self.key_generator.get_window_limit_key(&commit.key))
                    .key(self.key_generator.get_total_count_key(&commit.key))
                    .key(self.key_generator.get_active_entities_key())
                    .arg(commit.key.as_str())
                    .arg(commit.window_size_seconds)
                    .arg(commit.window_limit)
                    .arg(commit.rate_group_size_ms)
                    .arg(commit.count),
            );
        }

        pipe
    }

    pub(crate) async fn batch_read_state(
        self: &SuppressedHybridRedisProxy,
        keys: &Vec<RedisKey>,
        window_size_ms: u128,
    ) -> Result<Vec<SuppressedHybridRedisProxyReadStateResult>, TrypemaError> {
        let mut connection_manager = self.connection_manager.clone();

        let pipe = self.build_read_pipeline(keys, window_size_ms, false);

        let results = match pipe
            .query_async::<Vec<(String, f64, u64, u64, i64)>>(&mut connection_manager)
            .await
        {
            Ok(results) => results,
            Err(err) => {
                if err.kind() != redis::ErrorKind::Server(redis::ServerErrorKind::NoScript) {
                    tracing::error!("redis.read.error, error executing pipeline: {:?}", err);
                    return Err(TrypemaError::RedisError(err));
                }

                let pipe = self.build_read_pipeline(keys, window_size_ms, true);

                match pipe
                    .query_async::<Vec<(String, f64, u64, u64, i64)>>(&mut connection_manager)
                    .await
                {
                    Ok(results) => results,
                    Err(err) => {
                        tracing::error!("redis.read.error, error executing pipeline: {:?}", err);
                        return Err(TrypemaError::RedisError(err));
                    }
                }
            }
        };

        let results: Vec<SuppressedHybridRedisProxyReadStateResult> = results
            .into_iter()
            .map(map_redis_read_result_to_state)
            .collect();

        Ok(results)
    } // end method batch_commit_state

    #[inline]
    fn build_read_pipeline(
        &self,
        keys: &Vec<RedisKey>,
        window_size_ms: u128,
        should_load_script: bool,
    ) -> redis::Pipeline {
        let mut pipe = redis::Pipeline::new();
        if should_load_script {
            pipe.load_script(&self.read_state_script).ignore();
        }

        for key in keys {
            pipe.invoke_script(
                self.read_state_script
                    .key(self.key_generator.get_hash_key(key))
                    .key(self.key_generator.get_active_keys(key))
                    .key(self.key_generator.get_window_limit_key(key))
                    .key(self.key_generator.get_total_count_key(key))
                    .arg(key.as_str())
                    .arg(window_size_ms),
            );
        }

        pipe
    }

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
}

#[async_trait::async_trait]
impl RedisProxyCommitter<SuppressedHybridCommit> for SuppressedHybridRedisProxy {
    async fn batch_commit_state(
        self: &SuppressedHybridRedisProxy,
        commits: &[SuppressedHybridCommit],
    ) -> Result<(), TrypemaError> {
        let mut connection_manager = self.connection_manager.clone();

        let pipe = self.build_commit_pipeline(commits, false);

        let _: () = match pipe.query_async(&mut connection_manager).await {
            Ok(results) => results,
            Err(err) => {
                if err.kind() != redis::ErrorKind::Server(redis::ServerErrorKind::NoScript) {
                    tracing::error!("redis.commit.error, error executing pipeline: {:?}", err);
                    return Err(TrypemaError::RedisError(err));
                }

                let pipe = self.build_commit_pipeline(commits, true);

                match pipe.query_async::<()>(&mut connection_manager).await {
                    Ok(results) => results,
                    Err(err) => {
                        tracing::error!("redis.commit.error, error executing pipeline: {:?}", err);
                        return Err(TrypemaError::RedisError(err));
                    }
                }
            }
        };

        Ok(())
    } // end method batch_commit_state
}

fn map_redis_read_result_to_state(
    (entity, suppression_factor, current_total_count, last_second_count, window_limit): (
        String,
        f64,
        u64,
        u64,
        i64,
    ),
) -> SuppressedHybridRedisProxyReadStateResult {
    SuppressedHybridRedisProxyReadStateResult {
        key: RedisKey::from(entity),
        current_total_count,
        last_second_count,
        suppression_factor,
        window_limit: if window_limit < 0 {
            None
        } else {
            Some(window_limit as u64)
        },
    }
}
