use redis::{Script, aio::ConnectionManager};

use crate::{
    HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision, RedisKey,
    RedisRateLimiterOptions, TrypemaError, WindowSizeSeconds,
    common::{RateType, SuppressionFactorCacheMs},
    redis::RedisKeyGenerator,
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
    local total_declined_suffix = ARGV[7]
    local hash_declined_suffix = ARGV[8]


    local active_entities = redis.call("ZRANGE", active_entities_key, "-inf", timestamp_ms - stale_after_ms, "BYSCORE")

    if #active_entities == 0 then
        return 
    end

    local remove_keys = {}

    local suffixes = {hash_suffix, window_limit_suffix, total_count_suffix, active_keys_suffix, suppression_factor_key_suffix, total_declined_suffix, hash_declined_suffix}
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

    local function cleanup_expired_keys(hash_key, hash_declined_key, active_keys, total_count_key, total_declined_key, timestamp_ms, window_size_seconds)
        local to_remove_keys = redis.call("ZRANGE", active_keys, "-inf", timestamp_ms - window_size_seconds * 1000, "BYSCORE")

        if #to_remove_keys == 0 then
            return
        end

        local to_remove_counts = redis.call("HMGET", hash_key, unpack(to_remove_keys))
        local to_remove_declines = redis.call("HMGET", hash_declined_key, unpack(to_remove_keys))

        redis.call("HDEL", hash_key, unpack(to_remove_keys))
        redis.call("HDEL", hash_declined_key, unpack(to_remove_keys))

        local remove_count_sum = 0
        local remove_declined_sum = 0

        for i = 1, #to_remove_counts do
            remove_count_sum = remove_count_sum + (tonumber(to_remove_counts[i]) or 0)
            remove_declined_sum = remove_declined_sum + (tonumber(to_remove_declines[i]) or 0)
        end

        redis.call("DECRBY", total_count_key, remove_count_sum)
        redis.call("DECRBY", total_declined_key, remove_declined_sum)
        redis.call("ZREM", active_keys, unpack(to_remove_keys))
    end

    local function calculate_suppression_factor(hash_key, active_keys, total_count, total_declined, timestamp_ms, window_size_seconds, window_limit, hard_limit_factor)
        if window_limit == nil then
            return 0
        end

        if (total_count - total_declined) <= window_limit / hard_limit_factor then
            return 0
        elseif total_count > window_limit then
            return 1
        end

        local active_keys_in_1s = redis.call("ZRANGE", active_keys, "+inf", timestamp_ms - 1000, "BYSCORE", "REV")
        local total_in_last_second = 0

        if #active_keys_in_1s > 0 then
            local values = redis.call("HMGET", hash_key, unpack(active_keys_in_1s))
            for i = 1, #values do
                total_in_last_second = total_in_last_second + (tonumber(values[i]) or 0)
            end
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

const SUPPRESSED_INC_LUA: &str = r#"
    local timestamp_ms = now_ms()

    local hash_key = KEYS[1]
    local active_keys = KEYS[2]
    local window_limit_key = KEYS[3]
    local total_count_key = KEYS[4]
    local get_active_entities_key = KEYS[5]
    local suppression_factor_key = KEYS[6]
    local total_declined_key = KEYS[7]
    local hash_declined_key = KEYS[8]

    local entity = ARGV[1]
    local window_size_seconds = tonumber(ARGV[2])
    local window_limit_to_consider = tonumber(ARGV[3])
    local rate_group_size_ms = tonumber(ARGV[4])
    local suppression_factor_cache_ms = tonumber(ARGV[5])
    local hard_limit_factor = tonumber(ARGV[6])
    local count = tonumber(ARGV[7])

    local window_limit = tonumber(redis.call("GET", window_limit_key)) or window_limit_to_consider

    redis.call("ZADD", get_active_entities_key, timestamp_ms, entity)

    cleanup_expired_keys(hash_key, hash_declined_key, active_keys, total_count_key, total_declined_key, timestamp_ms, window_size_seconds)

    local total_count = tonumber(redis.call("GET", total_count_key)) or 0
    local total_declined = tonumber(redis.call("GET", total_declined_key)) or 0

    local suppression_factor = tonumber(redis.call("GET", suppression_factor_key))

    if suppression_factor == nil or suppression_factor < 0 or suppression_factor > 1 then
        suppression_factor = calculate_suppression_factor(hash_key, active_keys, total_count, total_declined, timestamp_ms, window_size_seconds, window_limit, hard_limit_factor)

        redis.call("SET", suppression_factor_key, suppression_factor, "PX", suppression_factor_cache_ms)
    end


    local should_allow = true

    if suppression_factor == 0 then
        should_allow = true
    elseif suppression_factor == 1 then
        should_allow = false
    else
        should_allow = math.random() < (1 - suppression_factor)
    end

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

    if not should_allow then
        redis.call("HINCRBY", hash_declined_key, hash_field, count)
        redis.call("INCRBY", total_declined_key, count)
    end

    if new_count == count then
        redis.call("ZADD", active_keys, timestamp_ms, hash_field)
        redis.call("SET", window_limit_key, window_limit)
    end

    redis.call("EXPIRE", window_limit_key, window_size_seconds)

    if total_count - total_declined <= window_limit / hard_limit_factor then
        return {"allowed", 0, 0}
    elseif total_count > window_limit then
        return {"suppressed", "1", "0"}
    else
        return {"suppressed", tostring(suppression_factor), should_allow and "1" or "0"}
    end
"#;

const SUPPRESSED_GET_FACTOR_LUA: &str = r#"
    local timestamp_ms = now_ms()

    local hash_key = KEYS[1]
    local active_keys = KEYS[2]
    local window_limit_key = KEYS[3]
    local total_count_key = KEYS[4]
    local get_active_entities_key = KEYS[5]
    local suppression_factor_key = KEYS[6]
    local total_declined_key = KEYS[7]
    local hash_declined_key = KEYS[8]

    local entity = ARGV[1]
    local window_size_seconds = tonumber(ARGV[2])
    local rate_group_size_ms = tonumber(ARGV[3])
    local suppression_factor_cache_ms = tonumber(ARGV[4])
    local hard_limit_factor = tonumber(ARGV[5])

    redis.call("ZADD", get_active_entities_key, timestamp_ms, entity)

    cleanup_expired_keys(hash_key, hash_declined_key, active_keys, total_count_key, total_declined_key, timestamp_ms, window_size_seconds)

    local total_count = tonumber(redis.call("GET", total_count_key)) or 0
    local total_declined = tonumber(redis.call("GET", total_declined_key)) or 0

    local suppression_factor = tonumber(redis.call("GET", suppression_factor_key))

    if suppression_factor == nil or suppression_factor < 0 or suppression_factor > 1 then
        local window_limit = tonumber(redis.call("GET", window_limit_key)) 
        if window_limit == nil then
            suppression_factor = 0
        else
            suppression_factor = calculate_suppression_factor(hash_key, active_keys, total_count, total_declined, timestamp_ms, window_size_seconds, window_limit, hard_limit_factor)
        end

        redis.call("SET", suppression_factor_key, suppression_factor, "PX", suppression_factor_cache_ms)
    end

    return tostring(suppression_factor)
"#;

/// A rate limiter backed by Redis.
#[derive(Clone, Debug)]
pub struct SuppressedRedisRateLimiter {
    connection_manager: ConnectionManager,
    key_generator: RedisKeyGenerator,
    hard_limit_factor: HardLimitFactor,
    rate_group_size_ms: RateGroupSizeMs,
    window_size_seconds: WindowSizeSeconds,
    suppression_factor_cache_ms: SuppressionFactorCacheMs,
    inc_script: Script,
    cleanup_script: Script,
    suppression_factor_script: Script,
}

impl SuppressedRedisRateLimiter {
    pub(crate) fn new(options: RedisRateLimiterOptions) -> Self {
        let prefix = options.prefix.unwrap_or_else(RedisKey::default_prefix);
        let key_generator = RedisKeyGenerator::new(prefix, RateType::Suppressed);

        Self {
            connection_manager: options.connection_manager,
            window_size_seconds: options.window_size_seconds,
            rate_group_size_ms: options.rate_group_size_ms,
            hard_limit_factor: options.hard_limit_factor,
            suppression_factor_cache_ms: options.suppression_factor_cache_ms,
            key_generator,
            inc_script: Script::new(&format!("{}\n{}", LUA_HELPERS, SUPPRESSED_INC_LUA)),
            cleanup_script: Script::new(CLEANUP_LUA),
            suppression_factor_script: Script::new(&format!(
                "{}\n{}",
                LUA_HELPERS, SUPPRESSED_GET_FACTOR_LUA
            )),
        }
    }

    /// Check admission and increment counters for `key`.
    ///
    /// The suppressed strategy always increments the total observed counter. If the call is
    /// denied (`is_allowed: false`), it also increments the declined counter.
    pub async fn inc(
        &self,
        key: &RedisKey,
        rate_limit: &RateLimit,
        count: u64,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let window_limit =
            *self.window_size_seconds as f64 * **rate_limit * *self.hard_limit_factor;
        let mut connection_manager = self.connection_manager.clone();

        let (result, suppression_factor, should_allow): (String, f64, u8) = self
            .inc_script
            .key(self.key_generator.get_hash_key(key))
            .key(self.key_generator.get_active_keys(key))
            .key(self.key_generator.get_window_limit_key(key))
            .key(self.key_generator.get_total_count_key(key))
            .key(self.key_generator.get_active_entities_key())
            .key(self.key_generator.get_suppression_factor_key(key))
            .key(self.key_generator.get_total_declined_key(key))
            .key(self.key_generator.get_hash_declined_key(key))
            .arg(key.to_string())
            .arg(*self.window_size_seconds)
            .arg(window_limit)
            .arg(*self.rate_group_size_ms)
            .arg(*self.suppression_factor_cache_ms)
            .arg(*self.hard_limit_factor)
            .arg(count)
            .invoke_async(&mut connection_manager)
            .await?;

        match result.as_str() {
            "allowed" => Ok(RateLimitDecision::Allowed),
            "suppressed" => Ok(RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: should_allow == 1,
            }),
            _ => Err(TrypemaError::UnexpectedRedisScriptResult {
                operation: "suppressed.inc",
                key: key.to_string(),
                result,
            }),
        }
    } // end method inc

    /// Get the current suppression factor for `key`.
    ///
    /// This is useful for exporting metrics or debugging why calls are being suppressed.
    ///
    /// If a cached value exists in Redis, it is returned. Otherwise, this recomputes the
    /// suppression factor and writes it back to Redis with a TTL.
    ///
    /// If the cached value is outside `[0.0, 1.0]`, this recomputes and overwrites it.
    pub async fn get_suppression_factor(&self, key: &RedisKey) -> Result<f64, TrypemaError> {
        let mut connection_manager = self.connection_manager.clone();

        let suppression_factor: f64 = self
            .suppression_factor_script
            .key(self.key_generator.get_hash_key(key))
            .key(self.key_generator.get_active_keys(key))
            .key(self.key_generator.get_window_limit_key(key))
            .key(self.key_generator.get_total_count_key(key))
            .key(self.key_generator.get_active_entities_key())
            .key(self.key_generator.get_suppression_factor_key(key))
            .key(self.key_generator.get_total_declined_key(key))
            .key(self.key_generator.get_hash_declined_key(key))
            .arg(key.to_string())
            .arg(*self.window_size_seconds)
            .arg(*self.rate_group_size_ms)
            .arg(*self.suppression_factor_cache_ms)
            .arg(*self.hard_limit_factor)
            .invoke_async(&mut connection_manager)
            .await?;

        Ok(suppression_factor)
    } // end method calculate_suppression_factor

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
            .arg(self.key_generator.total_declined_key_suffix.to_string())
            .arg(self.key_generator.hash_declined_key_suffix.to_string())
            .invoke_async(&mut connection_manager)
            .await?;

        Ok(())
    }
}
