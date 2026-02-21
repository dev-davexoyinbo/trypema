use redis::{Script, aio::ConnectionManager};

use crate::{
    HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision, RedisKey,
    RedisRateLimiterOptions, TrypemaError, WindowSizeSeconds,
    common::{RateType, SuppressionFactorCacheMs},
    redis::RedisKeyGenerator,
};

/// A rate limiter backed by Redis.
#[derive(Clone, Debug)]
pub struct SuppressedRedisRateLimiter {
    connection_manager: ConnectionManager,
    key_generator: RedisKeyGenerator,
    hard_limit_factor: HardLimitFactor,
    rate_group_size_ms: RateGroupSizeMs,
    window_size_seconds: WindowSizeSeconds,
    suppression_factor_cache_ms: SuppressionFactorCacheMs,
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
        let script = redis::Script::new(
            r#"
            local time_array = redis.call("TIME")
            local timestamp_ms = tonumber(time_array[1]) * 1000 + math.floor(tonumber(time_array[2]) / 1000)

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

            local to_remove_keys = redis.call("ZRANGE", active_keys, "-inf", timestamp_ms - window_size_seconds * 1000, "BYSCORE")

            if #to_remove_keys > 0 then
                local to_remove = redis.call("HMGET", hash_key, unpack(to_remove_keys))
                redis.call("HDEL", hash_key, unpack(to_remove_keys))

                local remove_sum = 0
                local decline_sum = 0

                for i = 1, #to_remove do
                    local value = cjson.decode(to_remove[i])
                    sum_count = tonumber(value.count)
                    sum_declined = tonumber(value.declined)

                    if sum_count then
                        remove_sum = remove_sum + sum_count
                    end

                    if sum_declined then
                        decline_sum = decline_sum + sum_declined
                    end
                end


                redis.call("HINCRBY", total_count_key, "count", -1 * remove_sum)
                redis.call("HINCRBY", total_count_key, "declined", -1 * decline_sum)
                redis.call("ZREM", active_keys, unpack(to_remove_keys))
            end

            local total_count = tonumber(redis.call("HGET", total_count_key, "count")) or 0

            local suppression_factor = tonumber(redis.call("GET", suppression_factor_key))

            if suppression_factor == nil or suppression_factor < 0 or suppression_factor > 1 then
                if total_count >= window_limit then
                    suppression_factor = 1
                elseif total_count < window_limit / hard_limit_factor then
                    suppression_factor = 0
                else
                    local active_keys_in_1s = redis.call("ZRANGE", active_keys, "+inf", timestamp_ms - 1000, "BYSCORE", "REV")
                    local total_in_last_second = 0


                    if #active_keys_in_1s > 0 then
                        local values = redis.call("HMGET", hash_key, unpack(active_keys_in_1s))
                        for i = 1, #values do
                            local value = tonumber(cjson.decode(values[i]).count)

                            if value then
                                total_in_last_second = total_in_last_second + value
                            end
                        end
                    end

                    local average_rate_in_window = total_count / window_size_seconds
                    local perceived_rate_limit = average_rate_in_window

                    if total_in_last_second > average_rate_in_window then
                        perceived_rate_limit = total_in_last_second
                    end

                    local rate_limit = window_limit / window_size_seconds / hard_limit_factor

                    suppression_factor = 1 - (rate_limit / perceived_rate_limit)
                end

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

                if latest_hash_field_age_ms > 0 and latest_hash_field_age_ms < rate_group_size_ms then
                    timestamp_ms = tonumber(latest_hash_field)
                end
            end

            local hash_field = tostring(timestamp_ms)

            local raw_prev = redis.call("HGET", hash_key, hash_field)
            local prev_value = raw_prev and cjson.decode(raw_prev) or {count = 0, declined = 0}

            local new_count = (tonumber(prev_value.count) or 0) + count
            local new_declined = tonumber(prev_value.declined) or 0

            if not should_allow then
                new_declined = new_declined + count
            end

            redis.call("HSET", hash_key, hash_field, cjson.encode({count = new_count, declined = new_declined}))
            redis.call("HINCRBY", total_count_key, "count", count)
            if not should_allow then
                redis.call("HINCRBY", total_count_key, "declined", count)
            end

            if new_count == count then
               redis.call("ZADD", active_keys, timestamp_ms, hash_field)
               redis.call("SET", window_limit_key, window_limit)
            end

            redis.call("EXPIRE", window_limit_key, window_size_seconds)

            if suppression_factor == 0 then
                return {"allowed", 0, 0}
            else
                return {"suppressed", tostring(suppression_factor), should_allow and "1" or "0"}
            end
        "#,
        );

        let window_limit =
            *self.window_size_seconds as f64 * **rate_limit * *self.hard_limit_factor;
        let mut connection_manager = self.connection_manager.clone();

        let (result, suppression_factor, should_allow): (String, f64, u8) = script
            .key(self.key_generator.get_hash_key(key))
            .key(self.key_generator.get_active_keys(key))
            .key(self.key_generator.get_window_limit_key(key))
            .key(self.key_generator.get_total_count_key(key))
            .key(self.key_generator.get_active_entities_key())
            .key(self.key_generator.get_suppression_factor_key(key))
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
                operation: "absolute.inc",
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
        let script = Script::new(
            r#"
            local time_array = redis.call("TIME")
            local timestamp_ms = tonumber(time_array[1]) * 1000 + math.floor(tonumber(time_array[2]) / 1000)

            local hash_key = KEYS[1]
            local active_keys = KEYS[2]
            local window_limit_key = KEYS[3]
            local total_count_key = KEYS[4]
            local get_active_entities_key = KEYS[5]
            local suppression_factor_key = KEYS[6]

            local entity = ARGV[1]
            local window_size_seconds = tonumber(ARGV[2])
            local rate_group_size_ms = tonumber(ARGV[3])
            local suppression_factor_cache_ms = tonumber(ARGV[4])
            local hard_limit_factor = tonumber(ARGV[5])

            redis.call("ZADD", get_active_entities_key, timestamp_ms, entity)

            local to_remove_keys = redis.call("ZRANGE", active_keys, "-inf", timestamp_ms - window_size_seconds * 1000, "BYSCORE")

            if #to_remove_keys > 0 then
                local to_remove = redis.call("HMGET", hash_key, unpack(to_remove_keys))
                redis.call("HDEL", hash_key, unpack(to_remove_keys))

                local remove_sum = 0
                local decline_sum = 0

                for i = 1, #to_remove do
                    local value = cjson.decode(to_remove[i])
                    sum_count = tonumber(value.count)
                    sum_declined = tonumber(value.declined)

                    if sum_count then
                        remove_sum = remove_sum + sum_count
                    end

                    if sum_declined then
                        decline_sum = decline_sum + sum_declined
                    end
                end


                redis.call("HINCRBY", total_count_key, "count", -1 * remove_sum)
                redis.call("HINCRBY", total_count_key, "declined", -1 * decline_sum)
                redis.call("ZREM", active_keys, unpack(to_remove_keys))
            end

            local total_count = tonumber(redis.call("HGET", total_count_key, "count")) or 0

            local suppression_factor = tonumber(redis.call("GET", suppression_factor_key))

            if suppression_factor == nil or suppression_factor < 0 or suppression_factor > 1 then
                local window_limit = tonumber(redis.call("GET", window_limit_key)) 
                if window_limit == nil then
                    suppression_factor = 0
                elseif total_count >= window_limit then
                    suppression_factor = 1
                elseif total_count < window_limit / hard_limit_factor then
                    suppression_factor = 0
                else
                    local active_keys_in_1s = redis.call("ZRANGE", active_keys, "+inf", timestamp_ms - 1000, "BYSCORE", "REV")
                    local total_in_last_second = 0

                    if #active_keys_in_1s > 0 then
                        local values = redis.call("HMGET", hash_key, unpack(active_keys_in_1s))
                        for i = 1, #values do
                            local value = tonumber(cjson.decode(values[i]).count)
                            if value then
                                total_in_last_second = total_in_last_second + value
                            end
                        end
                    end

                    local average_rate_in_window = total_count / window_size_seconds
                    local perceived_rate_limit = average_rate_in_window

                    if total_in_last_second > average_rate_in_window then
                        perceived_rate_limit = total_in_last_second
                    end

                    local rate_limit = window_limit / window_size_seconds / hard_limit_factor

                    suppression_factor = 1 - (rate_limit / perceived_rate_limit)
                end

                redis.call("SET", suppression_factor_key, suppression_factor, "PX", suppression_factor_cache_ms)
            end

            return tostring(suppression_factor)
        "#,
        );

        let mut connection_manager = self.connection_manager.clone();

        let suppression_factor: f64 = script
            .key(self.key_generator.get_hash_key(key))
            .key(self.key_generator.get_active_keys(key))
            .key(self.key_generator.get_window_limit_key(key))
            .key(self.key_generator.get_total_count_key(key))
            .key(self.key_generator.get_active_entities_key())
            .key(self.key_generator.get_suppression_factor_key(key))
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
        let script = redis::Script::new(
            r#"
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
        "#,
        );

        let mut connection_manager = self.connection_manager.clone();

        let _: () = script
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
