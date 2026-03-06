use redis::{Script, aio::ConnectionManager};

use crate::{
    RateGroupSizeMs, TrypemaError, WindowSizeSeconds,
    common::RateType,
    hybrid::RedisProxyCommitter,
    redis::{RedisKey, RedisKeyGenerator},
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

const COMMIT_STATE_SCRIPT: &str = r#"
    local time_array = redis.call("TIME")
    local timestamp_ms = tonumber(time_array[1]) * 1000 + math.floor(tonumber(time_array[2]) / 1000)

    local hash_key = KEYS[1]
    local active_keys = KEYS[2]
    local window_limit_key = KEYS[3]
    local total_count_key = KEYS[4]
    local active_entities_key = KEYS[5]

    local entity = ARGV[1]
    local window_size_seconds = tonumber(ARGV[2])
    local window_limit = tonumber(ARGV[3])
    local rate_group_size_ms = tonumber(ARGV[4])
    local count = tonumber(ARGV[5])


    -- evict expired buckets
    local to_remove_keys = redis.call("ZRANGE", active_keys, "-inf", timestamp_ms - window_size_seconds * 1000, "BYSCORE")
    if #to_remove_keys > 0 then
        local to_remove = redis.call("HMGET", hash_key, unpack(to_remove_keys))
        redis.call("HDEL", hash_key, unpack(to_remove_keys))

        local remove_sum = 0

        for i = 1, #to_remove do
            remove_sum = remove_sum + (tonumber(to_remove[i]) or 0)
        end

        redis.call("DECRBY", total_count_key, remove_sum)
        redis.call("ZREM", active_keys, unpack(to_remove_keys))
    end

    --group bucketing
    local latest_hash_field_entry = redis.call("ZRANGE", active_keys, 0, 0, "REV", "WITHSCORES")
    if #latest_hash_field_entry > 0 then
        local age_ms = timestamp_ms - tonumber(latest_hash_field_entry[2])
        
        if age_ms > 0 and age_ms < rate_group_size_ms then
            timestamp_ms = tonumber(latest_hash_field_entry[1])
        end
    end

    local hash_field = tostring(timestamp_ms)
    local new_count = redis.call("HINCRBY", hash_key, hash_field, count)
    local total_count = redis.call("INCRBY", total_count_key, count)

    if new_count == count then
        redis.call("ZADD", active_keys, timestamp_ms, hash_field)
        redis.call("SET", window_limit_key, window_limit)
    end


    local oldest_hash_fields = redis.call("ZRANGE", active_keys, 0, 0, "WITHSCORES")
    local oldest_ttl = nil
    local oldest_count = nil

    if #oldest_hash_fields > 0 then
        oldest_count = tonumber(redis.call("HGET", hash_key, oldest_hash_fields[1])) or 0
        oldest_ttl = (window_size_seconds * 1000) - timestamp_ms + (tonumber(oldest_hash_fields[2]) or 0)
    end

    redis.call("EXPIRE", window_limit_key, window_size_seconds)
    redis.call("ZADD", active_entities_key, timestamp_ms, entity)
"#;

const READ_STATE_SCRIPT: &str = r#"
    local time_array = redis.call("TIME")
    local timestamp_ms = tonumber(time_array[1]) * 1000 + math.floor(tonumber(time_array[2]) / 1000)

    local hash_key = KEYS[1]
    local active_keys = KEYS[2]
    local window_limit_key = KEYS[3]
    local total_count_key = KEYS[4]

    local entity = ARGV[1]
    local window_size_ms = tonumber(ARGV[2])

    local res = redis.call("MGET", window_limit_key, total_count_key)

    local window_limit = tonumber(res[1])
    local total_count = tonumber(res[2]) or 0

    -- evict expired buckets
    local to_remove_keys = redis.call("ZRANGE", active_keys, "-inf", timestamp_ms - window_size_ms, "BYSCORE")
    if #to_remove_keys > 0 then
        local to_remove = redis.call("HMGET", hash_key, unpack(to_remove_keys))
        redis.call("HDEL", hash_key, unpack(to_remove_keys))

        local remove_sum = 0

        for i = 1, #to_remove do
            remove_sum = remove_sum + (tonumber(to_remove[i]) or 0)
        end

        total_count = redis.call("DECRBY", total_count_key, remove_sum)
        redis.call("ZREM", active_keys, unpack(to_remove_keys))
    end

    local oldest_hash_fields = redis.call("ZRANGE", active_keys, 0, 0, "WITHSCORES")
    local oldest_ttl = nil
    local oldest_count = nil

    if #oldest_hash_fields > 0 then
        oldest_count = tonumber(redis.call("HGET", hash_key, oldest_hash_fields[1])) or 0
        oldest_ttl = window_size_ms - timestamp_ms + (tonumber(oldest_hash_fields[2]) or 0)
    end

    return {entity, total_count, window_limit or -1, oldest_ttl or -1, oldest_count or -1}
"#;

#[derive(Debug)]
pub(crate) struct AbsoluteHybridCommit {
    pub key: RedisKey,
    pub window_limit: u64,
    pub count: u64,
}

#[derive(Debug)]
pub(crate) struct AbsoluteHybridRedisProxyReadStateResult {
    pub key: RedisKey,
    pub current_total_count: u64,
    pub window_limit: Option<u64>,
    pub last_rate_group_ttl: Option<u64>,
    pub last_rate_group_count: Option<u64>,
}

pub(crate) struct AbsoluteHybridRedisProxyOptions {
    pub prefix: RedisKey,
    pub connection_manager: ConnectionManager,
    pub window_size_seconds: WindowSizeSeconds,
    pub rate_group_size_ms: RateGroupSizeMs,
}

#[derive(Clone, Debug)]
pub(crate) struct AbsoluteHybridRedisProxy {
    key_generator: RedisKeyGenerator,
    read_state_script: Script,
    commit_state_script: Script,
    cleanup_script: Script,
    connection_manager: ConnectionManager,
    read_chunk_size: usize,
    window_size_seconds: WindowSizeSeconds,
    rate_group_size_ms: RateGroupSizeMs,
    window_size_ms: u128,
}

impl AbsoluteHybridRedisProxy {
    pub(crate) fn new(options: AbsoluteHybridRedisProxyOptions) -> Self {
        let AbsoluteHybridRedisProxyOptions {
            prefix,
            connection_manager,
            window_size_seconds,
            rate_group_size_ms,
        } = options;

        let window_size_ms = *window_size_seconds as u128 * 1000;

        Self {
            key_generator: RedisKeyGenerator::new(prefix, RateType::HybridAbsolute),
            read_state_script: Script::new(READ_STATE_SCRIPT),
            commit_state_script: Script::new(COMMIT_STATE_SCRIPT),
            cleanup_script: Script::new(ABSOLUTE_CLEANUP_LUA),
            connection_manager,
            read_chunk_size: 100,
            window_size_seconds,
            window_size_ms,
            rate_group_size_ms,
        }
    }

    pub(crate) async fn read_state(
        self: &AbsoluteHybridRedisProxy,
        key: &RedisKey,
    ) -> Result<AbsoluteHybridRedisProxyReadStateResult, TrypemaError> {
        let mut connection_manager = self.connection_manager.clone();

        let res: (String, u64, i64, i64, i64) = self
            .read_state_script
            .key(self.key_generator.get_hash_key(key))
            .key(self.key_generator.get_active_keys(key))
            .key(self.key_generator.get_window_limit_key(key))
            .key(self.key_generator.get_total_count_key(key))
            .arg(key.as_str())
            .arg(self.window_size_ms)
            .invoke_async(&mut connection_manager)
            .await?;

        Ok(map_redis_read_result_to_state(res))
    } // end method read_state

    #[inline]
    fn build_commit_pipeline(
        &self,
        commits: &[AbsoluteHybridCommit],
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
                    .arg(*self.window_size_seconds)
                    .arg(commit.window_limit)
                    .arg(*self.rate_group_size_ms)
                    .arg(commit.count),
            );
        }

        pipe
    }

    pub(crate) async fn batch_read_state(
        self: &AbsoluteHybridRedisProxy,
        keys: &[RedisKey],
    ) -> Result<Vec<AbsoluteHybridRedisProxyReadStateResult>, TrypemaError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut connection_manager = self.connection_manager.clone();

        let chunk_size = self.read_chunk_size.max(1);
        let mut all_results: Vec<AbsoluteHybridRedisProxyReadStateResult> =
            Vec::with_capacity(keys.len());

        for chunk in keys.chunks(chunk_size) {
            let pipe = self.build_read_pipeline(chunk, false);

            let results = match pipe
                .query_async::<Vec<(String, u64, i64, i64, i64)>>(&mut connection_manager)
                .await
            {
                Ok(results) => results,
                Err(err) => {
                    if err.kind() != redis::ErrorKind::Server(redis::ServerErrorKind::NoScript) {
                        tracing::error!("redis.read.error, error executing pipeline: {:?}", err);
                        return Err(TrypemaError::RedisError(err));
                    }

                    let pipe = self.build_read_pipeline(chunk, true);

                    match pipe
                        .query_async::<Vec<(String, u64, i64, i64, i64)>>(&mut connection_manager)
                        .await
                    {
                        Ok(results) => results,
                        Err(err) => {
                            tracing::error!(
                                "redis.read.error, error executing pipeline: {:?}",
                                err
                            );
                            return Err(TrypemaError::RedisError(err));
                        }
                    }
                }
            };

            all_results.extend(results.into_iter().map(map_redis_read_result_to_state));
        }

        Ok(all_results)
    } // end method batch_commit_state

    #[inline]
    fn build_read_pipeline(&self, keys: &[RedisKey], should_load_script: bool) -> redis::Pipeline {
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
                    .arg(self.window_size_ms),
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
impl RedisProxyCommitter<AbsoluteHybridCommit> for AbsoluteHybridRedisProxy {
    async fn batch_commit_state(
        self: &AbsoluteHybridRedisProxy,
        commits: &[AbsoluteHybridCommit],
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
    (entity, total_count, window_limit, oldest_ttl, oldest_count): (String, u64, i64, i64, i64),
) -> AbsoluteHybridRedisProxyReadStateResult {
    fn map_negative_to_none(value: i64) -> Option<u64> {
        if value < 0 { None } else { Some(value as u64) }
    }

    AbsoluteHybridRedisProxyReadStateResult {
        key: RedisKey::from(entity),
        current_total_count: total_count,
        window_limit: map_negative_to_none(window_limit),
        last_rate_group_ttl: map_negative_to_none(oldest_ttl),
        last_rate_group_count: map_negative_to_none(oldest_count),
    }
}
