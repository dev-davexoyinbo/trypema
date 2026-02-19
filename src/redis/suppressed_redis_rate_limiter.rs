use redis::{Script, aio::ConnectionManager};

use crate::{
    AbsoluteRedisRateLimiter, HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision,
    RedisKey, RedisRateLimiterOptions, TrypemaError, WindowSizeSeconds, common::RateType,
};

/// A rate limiter backed by Redis.
pub struct SuppressedRedisRateLimiter {
    connection_manager: ConnectionManager,

    accepted_limiter: AbsoluteRedisRateLimiter,
    observed_limiter: AbsoluteRedisRateLimiter,
    hard_limit_factor: HardLimitFactor,
    rate_group_size_ms: RateGroupSizeMs,
    window_size_seconds: WindowSizeSeconds,
}

impl SuppressedRedisRateLimiter {
    pub(crate) fn new(options: RedisRateLimiterOptions) -> Self {
        let accepted_limiter =
            AbsoluteRedisRateLimiter::with_rate_type(RateType::Suppressed, options.clone());
        let observed_limiter =
            AbsoluteRedisRateLimiter::with_rate_type(RateType::SuppressedObserved, options.clone());

        Self {
            connection_manager: options.connection_manager,
            window_size_seconds: options.window_size_seconds,
            rate_group_size_ms: options.rate_group_size_ms,
            accepted_limiter,
            observed_limiter,
            hard_limit_factor: options.hard_limit_factor,
        }
    }

    /// Check admission and, if allowed, increment the observed count for `key`.
    pub async fn inc(
        &self,
        key: &RedisKey,
        rate_limit: &RateLimit,
        count: u64,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let mut rng = |p: f64| rand::random_bool(p);
        self.inc_with_rng(key, rate_limit, count, &mut rng).await
    } // end method inc

    pub(crate) async fn inc_with_rng(
        &self,
        key: &RedisKey,
        rate_limit: &RateLimit,
        count: u64,
        random_bool: &mut impl FnMut(f64) -> bool,
    ) -> Result<RateLimitDecision, TrypemaError> {
        // Always track observed usage; this limiter uses a max rate to avoid rejecting.
        self.observed_limiter
            .inc(key, &RateLimit::max(), count)
            .await?;

        let suppression_factor = self
            .compute_or_calculate_suppression_factor(key)
            .await?
            .min(1f64);

        if suppression_factor <= 0f64 {
            return self
                .accepted_limiter
                .inc(key, &self.get_hard_limit(rate_limit), count)
                .await;
        }

        let should_allow = random_bool(1f64 - suppression_factor);

        if !should_allow {
            return Ok(RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: false,
            });
        }

        let decision = self
            .accepted_limiter
            .inc(key, &self.get_hard_limit(rate_limit), count)
            .await?;

        let decision = match decision {
            RateLimitDecision::Allowed => RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: true,
            },
            // This is a hard limit rejection, so we can't suppress.
            RateLimitDecision::Rejected { .. } => decision,
            RateLimitDecision::Suppressed { .. } => {
                unreachable!("AbsoluteLocalRateLimiter::inc: should not be suppressed")
            }
        };

        Ok(decision)
    }

    async fn compute_or_calculate_suppression_factor(
        &self,
        key: &RedisKey,
    ) -> Result<f64, TrypemaError> {
        let script = Script::new(
            r#"
            local time_array = redis.call("TIME")
            local now_ms = tonumber(time_array[1]) * 1000 + math.floor(tonumber(time_array[2]) / 1000)

            local accepted_total_count_key = KEYS[1]
            local accepted_active_keys_key = KEYS[2]
            local accepted_window_limit_key = KEYS[3]
            local suppressed_factor_key = KEYS[4]

            local window_size_seconds = tonumber(ARGV[1])
            local rate_group_size_ms = tonumber(ARGV[2])

            local suppression_factor = tonumber(redis.call("GET", suppressed_factor_key))
            if suppression_factor ~= nil then
                return suppression_factor
            end

            local window_limit = tonumber(redis.call("GET", accepted_window_limit_key)) or 0
            local accepted_total_count = tonumber(redis.call("GET", accepted_total_count_key)) or 0
            local active_keys_count = tonumber(redis.call("ZCARD", accepted_active_keys_key)) or 0

            if window_limit == 0 or accepted_total_count < window_limit or active_keys_count == 0 then
                redis.call("SET", suppressed_factor_key, 0, PX, rate_group_size_ms)
                return 0
            end

            local accepted_active_keys_in_1s = redis.call("ZRANGE", accepted_active_keys_key, +inf, now_ms - 1000, "BYSCORE", "REV")
            local total_in_last_second = 0

            if #accepted_active_keys_in_1s > 0 then
                local values = redis.call("HMGET", accepted_active_keys_key, unpack(accepted_active_keys_in_1s))
                for i = 1, #values do
                    local value = tonumber(values[i])
                    if value then
                        total_in_last_second = total_in_last_second + value
                    end
                end
            end

            local oldest_hash_field_timestamp = tonumber(accepted_active_keys_in_1s[1])

            local average_rate_in_window = accepted_total / window_size_seconds
            local perceived_rate_limit = average_rate_in_window

            if total_in_last_second > average_rate_in_window then
                perceived_rate_limit = total_in_last_second
            end

            suppression_factor = 1 - (perceived_rate_limit / window_limit)
            suppression_factor_exp = rate_group_size_ms - now_ms + oldest_hash_field_timestamp

            if suppression_factor_exp <= 0 then
                suppression_factor_exp = 1000
            end

            redis.call("SET", suppressed_factor_key, suppression_factor, PX, suppression_factor_exp)

            return suppression_factor
        "#,
        );

        let mut connection_manager = self.connection_manager.clone();

        let suppression_factor: f64 = script
            .key(
                self.accepted_limiter
                    .key_generator()
                    .get_total_count_key(key),
            )
            .key(self.accepted_limiter.key_generator().get_active_keys(key))
            .key(
                self.accepted_limiter
                    .key_generator()
                    .get_window_limit_key(key),
            )
            .key(
                self.observed_limiter
                    .key_generator()
                    .get_suppression_factor_key(key),
            )
            .arg(*self.window_size_seconds)
            .arg(*self.rate_group_size_ms)
            .invoke_async(&mut connection_manager)
            .await?;

        Ok(suppression_factor)
    } // end method calculate_suppression_factor

    #[inline]
    fn get_hard_limit(&self, rate_limit: &RateLimit) -> RateLimit {
        // if hard limit factor is always > 0 and rate limit is always > 0, this is safe
        let Ok(val) = RateLimit::try_from(*self.hard_limit_factor * **rate_limit) else {
            unreachable!(
                "SuppressedLocalRateLimiter::get_hard_limit: hard_limit_factor is always > 0"
            );
        };

        val
    } // end method get_hard_limit

    pub(crate) async fn cleanup(&self, stale_after_ms: u64) -> Result<(), TrypemaError> {
        self.accepted_limiter.cleanup(stale_after_ms).await?;
        self.observed_limiter.cleanup(stale_after_ms).await
    } // end method cleanup
}
