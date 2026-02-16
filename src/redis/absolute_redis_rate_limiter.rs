use crate::{
    RateGroupSizeMs, RateLimit, RateLimitDecision, RedisKey, RedisRateLimiterOptions, TrypemaError,
    WindowSizeSeconds,
};

/// A rate limiter backed by Redis.
pub struct AbsoluteRedisRateLimiter {
    redis_client: redis::Client,
    prefix: RedisKey,
    window_size_seconds: WindowSizeSeconds,
    rate_group_size_ms: RateGroupSizeMs,
}

impl AbsoluteRedisRateLimiter {
    pub(crate) fn new(options: RedisRateLimiterOptions) -> Self {
        Self {
            redis_client: options.redis_client,
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
        let mut connection = self.redis_client.get_connection()?;

        // At least a version of Redis that is 7.4.0 or higher is needed
        let mut script = redis::Script::new(
            r#"
            local hash_key = KEYS[1]
            local latest_key = KEYS[2]

            local window_size_seconds = tonumber(ARGV[1])
            local window_limit = tonumber(ARGV[2])
            local rate_group_size_ms = tonumber(ARGV[3])
            local count = tonumber(ARGV[4])

            local values = redis.call("HVALS", hash_key)
            local sum = 0
            for i = 1, #values do
                local value = values[i]
                if value then
                    sum = sum + tonumber(value)
                end
            end

            if sum >= window_limit then
                return {"rejected"}
            end

            local time_array = redis.call("TIME")
            local timestamp_ms = tonumber(time_array[1]) * 1000 + math.floor(tonumber(time_array[2]) / 1000)

            local latest_hash_field = redis.call("GET", latest_key)
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
               redis.call("SET", latest_key, hash_field)
               redis.call("EXPIRE", latest_key, window_size_seconds)
            end

            return {"allowed"}
        "#,
        );

        let window_limit = *self.window_size_seconds as f64 * **rate_limit;

        let [result] = script
            .key(self.get_hash_key(key))
            .key(self.get_latest_key(key))
            .arg(*self.window_size_seconds)
            .arg(window_limit)
            .arg(*self.rate_group_size_ms)
            .arg(count)
            .invoke_async(&mut connection)
            .await?;

        match result {
            "allowed" => Ok(RateLimitDecision::Allowed),
            "rejected" => Ok(RateLimitDecision::Rejected {
                window_size_seconds: *self.window_size_seconds,
                retry_after_ms: 0,
                remaining_after_waiting: 0,
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
    pub fn is_allowed(&self, _key: &RedisKey) -> Result<RateLimitDecision, TrypemaError> {
        todo!("AbsoluteRedisRateLimiter::is_allowed");
    }

    /// Determine whether `key` is currently allowed.
    fn get_hash_key(&self, key: &RedisKey) -> String {
        format!("{}:{}:absolute:h", *self.prefix, **key)
    } // end method get_hash_key

    fn get_latest_key(&self, key: &RedisKey) -> String {
        format!("{}:{}:absolute:l", *self.prefix, **key)
    } // end method get_latest_key
}
