use crate::{
    RateGroupSizeMs, RateLimit, RateLimitDecision, RedisKey, RedisRateLimiterOptions, TrypemaError,
    WindowSizeSeconds,
};

/// A rate limiter backed by Redis.
pub struct AbsoluteRedisRateLimiter {
    connection_manager: ConnectionManager,
    prefix: RedisKey,
    window_size_seconds: WindowSizeSeconds,
    rate_group_size_ms: RateGroupSizeMs,
}

impl AbsoluteRedisRateLimiter {
    pub(crate) fn new(options: RedisRateLimiterOptions) -> Self {
        Self {
            connection_manager: options.connection_manager,
            prefix: options.prefix.unwrap_or_else(RedisKey::default_prefix),
            window_size_seconds: options.window_size_seconds,
            rate_group_size_ms: options.rate_group_size_ms,
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

        // At least a version of Redis that is 7.4.0 or higher is needed
        let mut script = redis::Script::new(
            r#"
            local time_array = redis.call("TIME")
            local timestamp_ms = tonumber(time_array[1]) * 1000 + math.floor(tonumber(time_array[2]) / 1000)


            local hash_key = KEYS[1]
            local active_keys = KEYS[2]
            local window_limit_key = KEYS[3]

            local window_size_seconds = tonumber(ARGV[1])
            local window_limit = tonumber(ARGV[2])
            local rate_group_size_ms = tonumber(ARGV[3])
            local count = tonumber(ARGV[4])

            redis.call("ZREMRANGEBYSCORE", active_keys, "-inf", timestamp_ms - window_size_seconds * 1000)

            local values = redis.call("HVALS", hash_key)
            local sum = 0
            for i = 1, #values do
                local value = values[i]
                if value then
                    sum = sum + tonumber(value)
                end
            end

            if sum >= window_limit then
                local oldest_hash_fields = redis.call("ZRANGE", active_keys, 0, 0)

                if #oldest_hash_fields == 0 then
                    return {"rejected", 0, 0}
                end

                local oldest_hash_field = oldest_hash_fields[1]
                local oldest_hash_field_ttl = redis.call("PTTL", oldest_hash_field) or 0
                local oldest_count = tonumber(redis.call("HGET", hash_key, oldest_hash_field)) or 0

                return {"rejected", oldest_hash_field_ttl, oldest_count}
            end

            local latest_hash_field = redis.call("ZRANGE", active_keys, 0, 0, "REV")[1]
            if latest_hash_field then
                local latest_hash_field_ttl = redis.call("PTTL", latest_hash_field)
                if latest_hash_field_ttl > 0 and window_size_seconds * 1000 - latest_hash_field_ttl < rate_group_size_ms then
                    timestamp_ms = tonumber(latest_hash_field)
                end
            end

            local hash_field = tostring(timestamp_ms)
            local new_count = redis.call("HINCRBY", hash_key, hash_field, count)

            if new_count == count then
               redis.call("HEXPIRE", hash_key, window_size_seconds, "FIELDS", hash_field)
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
            .key(self.get_hash_key(key))
            .key(self.get_active_keys(key))
            .key(self.get_window_limit_key(key))
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
        let mut script = redis::Script::new(
            r#"
            local time_array = redis.call("TIME")
            local timestamp_ms = tonumber(time_array[1]) * 1000 + math.floor(tonumber(time_array[2]) / 1000)


            local hash_key = KEYS[1]
            local active_keys = KEYS[2]
            local window_limit_key = KEYS[3]

            local window_size_seconds = tonumber(ARGV[1])
            local rate_group_size_ms = tonumber(ARGV[2])

            redis.call("ZREMRANGEBYSCORE", active_keys, "-inf", timestamp_ms - window_size_seconds * 1000)

            local window_limit = tonumber(redis.call("GET", window_limit_key))
            if window_limit == nil then
                return {"accepted", 0, 0}
            end

            local values = redis.call("HVALS", hash_key)
            local sum = 0
            for i = 1, #values do
                local value = values[i]
                if value then
                    sum = sum + tonumber(value)
                end
            end

            if sum >= window_limit then
                local oldest_hash_fields = redis.call("ZRANGE", active_keys, 0, 0)

                if #oldest_hash_fields == 0 then
                    return {"rejected", 0, 0}
                end

                local oldest_hash_field = oldest_hash_fields[1]
                local oldest_hash_field_ttl = redis.call("PTTL", oldest_hash_field) or 0
                local oldest_count = tonumber(redis.call("HGET", hash_key, oldest_hash_field)) or 0

                return {"rejected", oldest_hash_field_ttl, oldest_count}
            end

            return {"allowed", 0, 0}
        "#,
        );

        let mut connection_manager = self.connection_manager.clone();

        let (result, retry_after_ms, remaining_after_waiting): (String, u64, u64) = script
            .key(self.get_hash_key(key))
            .key(self.get_active_keys(key))
            .key(self.get_window_limit_key(key))
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

    /// Determine whether `key` is currently allowed.
    fn get_hash_key(&self, key: &RedisKey) -> String {
        format!("{}:{}:absolute:h", *self.prefix, **key)
    } // end method get_hash_key

    fn get_window_limit_key(&self, key: &RedisKey) -> String {
        format!("{}:{}:absolute:w", *self.prefix, **key)
    } // end method get_window_limit_key

    fn get_active_keys(&self, key: &RedisKey) -> String {
        format!("{}:{}:absolute:a", *self.prefix, **key)
    } // end method get_active_keys
}
