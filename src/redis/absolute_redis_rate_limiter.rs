use redis::{Script, aio::ConnectionManager};

use crate::{
    RateGroupSizeMs, RateLimit, RateLimitDecision, RedisKey, RedisKeyGenerator,
    RedisRateLimiterOptions, TrypemaError, WindowSizeSeconds, common::RateType,
};

const ABSOLUTE_CHECK_LUA: &str = r#"
    local time_array = redis.call("TIME")
    local timestamp_ms = tonumber(time_array[1]) * 1000 + math.floor(tonumber(time_array[2]) / 1000)


    local hash_key = KEYS[1]
    local active_keys = KEYS[2]
    local window_limit_key = KEYS[3]
    local total_count_key = KEYS[4]

    local window_size_ms = tonumber(ARGV[1])
    local count = tonumber(ARGV[2])

    local window_limit = tonumber(redis.call("GET", window_limit_key))
    if window_limit == nil then
        return {"allowed", 0, 0}
    end


    local total_count = tonumber(redis.call("GET", total_count_key)) or 0

    if total_count + count > window_limit then
        local oldest_hash_fields = redis.call("ZRANGE", active_keys, 0, 0, "WITHSCORES")

        if #oldest_hash_fields == 0 then
            return {"rejected", tostring(timestamp_ms), 0, 0}
        end

        local oldest_count = tonumber(redis.call("HGET", hash_key, oldest_hash_fields[1])) or 0
        local ttl = window_size_ms - timestamp_ms + (tonumber(oldest_hash_fields[2]) or 0)

        return {"rejected",tostring(timestamp_ms), ttl, oldest_count}
    end

    return {"allowed", tostring(timestamp_ms), 0, 0}
"#;

const ABSOLUTE_COMMIT_LUA: &str = r#"
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
    local timestamp_ms = tonumber(ARGV[6])


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
    redis.call("INCRBY", total_count_key, count)

    if new_count == count then
        redis.call("ZADD", active_keys, timestamp_ms, hash_field)
        redis.call("SET", window_limit_key, window_limit)
    end

    redis.call("EXPIRE", window_limit_key, window_size_seconds)
    redis.call("ZADD", get_active_entities_key, timestamp_ms, entity)

    return 1
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

/// A rate limiter backed by Redis.
#[derive(Clone, Debug)]
pub struct AbsoluteRedisRateLimiter {
    connection_manager: ConnectionManager,
    window_size_seconds: WindowSizeSeconds,
    window_size_ms: u128,
    rate_group_size_ms: RateGroupSizeMs,
    key_generator: RedisKeyGenerator,
    check_script: Script,
    commit_script: Script,
    cleanup_script: Script,
}

impl AbsoluteRedisRateLimiter {
    pub(crate) fn new(options: RedisRateLimiterOptions) -> Self {
        let prefix = options.prefix.unwrap_or_else(RedisKey::default_prefix);

        Self {
            connection_manager: options.connection_manager,
            window_size_seconds: options.window_size_seconds,
            window_size_ms: *options.window_size_seconds as u128 * 1000,
            rate_group_size_ms: options.rate_group_size_ms,
            key_generator: RedisKeyGenerator::new(prefix, RateType::Absolute),
            check_script: Script::new(ABSOLUTE_CHECK_LUA),
            commit_script: Script::new(ABSOLUTE_COMMIT_LUA),
            cleanup_script: Script::new(ABSOLUTE_CLEANUP_LUA),
        }
    } // end method with_rate_type

    /// Check admission and, if allowed, increment the observed count for `key`.
    pub async fn inc(
        &self,
        key: &RedisKey,
        rate_limit: &RateLimit,
        count: u64,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let window_limit = *self.window_size_seconds as f64 * **rate_limit;
        let mut connection_manager = self.connection_manager.clone();

        let (result, timestamp_ms, retry_after_ms, remaining_after_waiting): (
            String,
            String,
            u128,
            u64,
        ) = self
            .check_script
            .key(self.key_generator.get_hash_key(key))
            .key(self.key_generator.get_active_keys(key))
            .key(self.key_generator.get_window_limit_key(key))
            .key(self.key_generator.get_total_count_key(key))
            .arg(self.window_size_ms)
            .arg(window_limit)
            .arg(*self.rate_group_size_ms)
            .arg(count)
            .invoke_async(&mut connection_manager)
            .await?;

        // .check_script
        // .key(self.key_generator.get_hash_key(key))
        // .key(self.key_generator.get_active_keys(key))
        // .key(self.key_generator.get_window_limit_key(key))
        // .key(self.key_generator.get_total_count_key(key))
        // .key(self.key_generator.get_active_entities_key())
        // .arg(key.to_string())
        // .arg(*self.window_size_seconds)
        // .arg(window_limit)
        // .arg(*self.rate_group_size_ms)
        // .arg(count)
        // .invoke_async(&mut connection_manager)
        // .await?;

        match result.as_str() {
            "allowed" => Ok(RateLimitDecision::Allowed),
            "rejected" => Ok(RateLimitDecision::Rejected {
                window_size_seconds: *self.window_size_seconds,
                retry_after_ms,
                remaining_after_waiting,
            }),
            _ => Err(TrypemaError::UnexpectedRedisScriptResult {
                operation: "absolute.inc",
                key: key.to_string(),
                result,
            }),
        }
    } // end method inc

    /// Determine whether `key` is currently allowed.
    ///
    /// Returns [`RateLimitDecision::Allowed`] if the current sliding window total
    /// is below the window limit, otherwise returns [`RateLimitDecision::Rejected`]
    /// with a best-effort `retry_after_ms`.
    ///
    /// This method performs lazy eviction of expired buckets for the key.
    pub async fn is_allowed(&self, key: &RedisKey) -> Result<RateLimitDecision, TrypemaError> {
        let mut connection_manager = self.connection_manager.clone();

        let (result, retry_after_ms, remaining_after_waiting): (String, u128, u64) = self
            .commit_script
            .key(self.key_generator.get_hash_key(key))
            .key(self.key_generator.get_active_keys(key))
            .key(self.key_generator.get_window_limit_key(key))
            .key(self.key_generator.get_total_count_key(key))
            .arg(*self.window_size_seconds)
            .arg(*self.rate_group_size_ms)
            .invoke_async(&mut connection_manager)
            .await?;

        match result.as_str() {
            "allowed" => Ok(RateLimitDecision::Allowed),
            "rejected" => Ok(RateLimitDecision::Rejected {
                window_size_seconds: *self.window_size_seconds,
                retry_after_ms,
                remaining_after_waiting,
            }),
            _ => Err(TrypemaError::UnexpectedRedisScriptResult {
                operation: "absolute.is_allowed",
                key: key.to_string(),
                result,
            }),
        }
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
