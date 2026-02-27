use redis::{Script, aio::ConnectionManager};

use crate::{
    TrypemaError,
    common::RateType,
    redis::{RedisKey, RedisKeyGenerator},
};

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

    if #oldest_hash_fields == 0 then
        oldest_count = tonumber(redis.call("HGET", hash_key, oldest_hash_fields[1])) or 0
        oldest_ttl = window_size_ms - timestamp_ms + (tonumber(oldest_hash_fields[2]) or 0)
    end

    redis.call("EXPIRE", window_limit_key, window_size_seconds)
    redis.call("ZADD", active_entities_key, timestamp_ms, entity)

    {entity, total_count, window_limit, oldest_ttl, oldest_count}
"#;

const READ_STATE_SCRIPT: &str = r#"
    local time_array = redis.call("TIME")
    local timestamp_ms = tonumber(time_array[1]) * 1000 + math.floor(tonumber(time_array[2]) / 1000)

    -- keys
    local hash_key = KEYS[1]
    local active_keys = KEYS[2]
    local window_limit_key = KEYS[3]
    local total_count_key = KEYS[4]

    -- args
    local entity = ARGV[1]
    local window_size_ms = tonumber(ARGV[2])

    local res = redis.call("MGET", window_limit_key, total_count_key)

    local window_limit = tonumber(res[1])
    local total_count = tonumber(res[2]) or 0

    local oldest_hash_fields = redis.call("ZRANGE", active_keys, 0, 0, "WITHSCORES")
    local oldest_ttl = nil
    local oldest_count = nil

    if #oldest_hash_fields == 0 then
        oldest_count = tonumber(redis.call("HGET", hash_key, oldest_hash_fields[1])) or 0
        oldest_ttl = window_size_ms - timestamp_ms + (tonumber(oldest_hash_fields[2]) or 0)
    end


    {entity, total_count, window_limit, oldest_ttl, oldest_count}

"#;

pub(crate) struct AbsoluteRedisProxyReadStateResult {
    pub key: RedisKey,
    pub current_total_count: u64,
    pub window_limit: Option<u64>,
    pub last_rate_group_ttl: Option<u64>,
    pub last_rate_group_count: Option<u64>,
}

pub(crate) struct AbsoluteRedisProxyCommitStateResult {
    pub key: RedisKey,
    pub current_total_count: u64,
    pub window_limit: u64,
    pub last_rate_group_ttl: Option<u64>,
    pub last_rate_group_count: Option<u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct AbsoluteRedisProxy {
    key_generator: RedisKeyGenerator,
    read_state_script: Script,
    commit_state_script: Script,
    connection_manager: ConnectionManager,
}

impl AbsoluteRedisProxy {
    pub(crate) fn new(prefix: RedisKey, connection_manager: ConnectionManager) -> Self {
        Self {
            key_generator: RedisKeyGenerator::new(prefix, RateType::Absolute),
            read_state_script: Script::new(READ_STATE_SCRIPT),
            commit_state_script: Script::new(COMMIT_STATE_SCRIPT),
            connection_manager,
        }
    }

    pub(crate) async fn read_state(
        self: &AbsoluteRedisProxy,
        key: &RedisKey,
        window_size_ms: u128,
    ) -> Result<AbsoluteRedisProxyReadStateResult, TrypemaError> {
        let mut connection_manager = self.connection_manager.clone();

        let (entity, total_count, window_limit, oldest_ttl, oldest_count): (
            String,
            u64,
            Option<u64>,
            Option<u64>,
            Option<u64>,
        ) = self
            .read_state_script
            .key(self.key_generator.get_hash_key(key))
            .key(self.key_generator.get_active_keys(key))
            .key(self.key_generator.get_window_limit_key(key))
            .key(self.key_generator.get_total_count_key(key))
            .arg(key.as_str())
            .arg(window_size_ms)
            .invoke_async(&mut connection_manager)
            .await?;

        Ok(AbsoluteRedisProxyReadStateResult {
            key: RedisKey::from(entity),
            current_total_count: total_count,
            window_limit,
            last_rate_group_ttl: oldest_ttl,
            last_rate_group_count: oldest_count,
        })
    } // end method read_state

    pub(crate) async fn commit_state(
        self: &AbsoluteRedisProxy,
        key: &RedisKey,
        window_size_seconds: u64,
        window_limit: u64,
        rate_group_size_ms: u64,
        count: u64,
    ) -> Result<AbsoluteRedisProxyCommitStateResult, TrypemaError> {
        let mut connection_manager = self.connection_manager.clone();

        let (entity, total_count, window_limit, oldest_ttl, oldest_count): (
            String,
            u64,
            u64,
            Option<u64>,
            Option<u64>,
        ) = self
            .commit_state_script
            .key(self.key_generator.get_hash_key(key))
            .key(self.key_generator.get_active_keys(key))
            .key(self.key_generator.get_window_limit_key(key))
            .key(self.key_generator.get_total_count_key(key))
            .key(self.key_generator.get_active_entities_key())
            .arg(key.as_str())
            .arg(window_size_seconds)
            .arg(window_limit)
            .arg(rate_group_size_ms)
            .arg(count)
            .invoke_async(&mut connection_manager)
            .await?;

        Ok(AbsoluteRedisProxyCommitStateResult {
            key: RedisKey::from(entity),
            current_total_count: total_count,
            window_limit,
            last_rate_group_ttl: oldest_ttl,
            last_rate_group_count: oldest_count,
        })
    }
}
