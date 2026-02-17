use redis::aio::ConnectionManager;

use crate::{
    RateGroupSizeMs, RateLimit, RateLimitDecision, RedisKey, RedisKeyGenerator,
    RedisRateLimiterOptions, TrypemaError, WindowSizeSeconds, common::RateType,
};

/// A rate limiter backed by Redis.
pub struct AbsoluteRedisRateLimiter {
    connection_manager: ConnectionManager,
    prefix: RedisKey,
    window_size_seconds: WindowSizeSeconds,
    rate_group_size_ms: RateGroupSizeMs,
    key_generator: RedisKeyGenerator,
}

impl AbsoluteRedisRateLimiter {
    pub(crate) fn new(options: RedisRateLimiterOptions) -> Self {
        let prefix = options.prefix.unwrap_or_else(RedisKey::default_prefix);

        Self {
            connection_manager: options.connection_manager,
            prefix: prefix.clone(),
            window_size_seconds: options.window_size_seconds,
            rate_group_size_ms: options.rate_group_size_ms,
            key_generator: RedisKeyGenerator::new(prefix, RateType::Absolute),
        }
    }

    /// Check admission and, if allowed, increment the observed count for `key`.
    pub async fn inc(
        &self,
        key: &RedisKey,
        rate_limit: &RateLimit,
        count: u64,
    ) -> Result<RateLimitDecision, TrypemaError> {
        // TODO: cleanup the active keys set

        let script = redis::Script::new(
            r#"
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

                redis.call("DECRBY", total_count_key, remove_sum)
                redis.call("ZREM", active_keys, unpack(to_remove_keys))
            end

            local total_count = tonumber(redis.call("GET", total_count_key)) or 0

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
        "#,
        );

        let window_limit = *self.window_size_seconds as f64 * **rate_limit;
        let mut connection_manager = self.connection_manager.clone();

        let (result, retry_after_ms, remaining_after_waiting): (String, u64, u64) = script
            .key(self.key_generator.get_hash_key(key))
            .key(self.key_generator.get_active_keys(key))
            .key(self.key_generator.get_window_limit_key(key))
            .key(self.key_generator.get_total_count_key(key))
            .key(self.key_generator.get_active_entities_key())
            .arg(key.to_string())
            .arg(*self.window_size_seconds)
            .arg(window_limit)
            .arg(*self.rate_group_size_ms)
            .arg(count)
            .invoke_async(&mut connection_manager)
            .await?;

        match result.as_str() {
            "allowed" => Ok(RateLimitDecision::Allowed),
            "rejected" => Ok(RateLimitDecision::Rejected {
                window_size_seconds: *self.window_size_seconds,
                retry_after_ms,
                remaining_after_waiting,
            }),
            _ => unreachable!("unexpected result from Redis script: {result}"),
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
        // TODO: cleanup the active keys set

        // At least a version of Redis that is 7.4.0 or higher is needed
        let script = redis::Script::new(
            r#"
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

                redis.call("DECRBY", total_count_key, remove_sum)
                redis.call("ZREM", active_keys, unpack(to_remove_keys))
            end

            local total_count = tonumber(redis.call("GET", total_count_key)) or 0

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
        "#,
        );

        let mut connection_manager = self.connection_manager.clone();

        let (result, retry_after_ms, remaining_after_waiting): (String, u64, u64) = script
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
            _ => unreachable!("unexpected result from Redis script: {result}"),
        }
    }

    /// Evict expired buckets and update the total count.
    pub(crate) async fn cleanup(&self, stale_after_ms: u64) -> Result<bool, TrypemaError> {
        let script = redis::Script::new(
            r#"
            local time_array = redis.call("TIME")
            local timestamp_ms = tonumber(time_array[1]) * 1000 + math.floor(tonumber(time_array[2]) / 1000)

            local active_entities_key = KEYS[1]
            local stale_after_ms = tonumber(ARGV[1]) or 0

            local active_entities = redis.call("ZRANGEBYSCORE", active_entities_key, "-inf", timestamp_ms - stale_after_ms)

            for i = 1, #active_entities do
                local entity = active_entities[i]

                redis.call("ZREM", active_entities_key, entity)
                redis.call("HDEL", KEYS[2], entity)
            end


            return true
        "#,
        );

        let mut connection_manager = self.connection_manager.clone();

        let ok: bool = script
            .key(self.key_generator.get_active_entities_key())
            .arg(stale_after_ms)
            .invoke_async(&mut connection_manager)
            .await?;

        Ok(ok)
    } // end method cleanup
}
