use redis::{Script, aio::ConnectionManager};

use crate::{
    HistoryPreservation, RateGroupSizeMs, RateLimit, RateLimitComparator, RateLimitDecision,
    RedisKey, RedisKeyGenerator, RedisRateLimiterOptions, TrypemaError, WindowSizeSeconds,
    common::{HistoryUpdateMode, RateType},
    redis::scripts::{
        ABSOLUTE_CLEANUP_LUA, ABSOLUTE_GET_TOTAL_LUA, ABSOLUTE_INC_LUA, ABSOLUTE_IS_ALLOWED_LUA,
        ABSOLUTE_SET_IF_LUA, absolute_lua_script,
    },
};

/// Strict sliding-window rate limiter backed by Redis.
///
/// Provides the same deterministic admission semantics as
/// [`AbsoluteLocalRateLimiter`](crate::local::AbsoluteLocalRateLimiter), but stores
/// all state in Redis so limits are shared across processes and servers.
///
/// # Implementation
///
/// Every `inc()` and `is_allowed()` call executes an atomic Lua script against Redis.
/// Within a single script execution, Redis guarantees atomicity — there are no
/// TOCTOU (time-of-check-to-time-of-use) races between reading and updating state
/// for a single key.
///
/// Timestamps are obtained from Redis server time, avoiding client clock skew issues.
///
/// # Data Model
///
/// For a key `K` with prefix `P`:
/// - `P:K:absolute:h` — Hash of `timestamp_ms → count` (sliding window buckets)
/// - `P:K:absolute:a` — Sorted set of active bucket timestamps (for efficient eviction)
/// - `P:K:absolute:w` — Window limit string (set on first call, refreshed with `EXPIRE`)
/// - `P:K:absolute:t` — Total count across all active buckets
/// - `P:active_entities` — Sorted set of all active keys (used by cleanup)
///
/// # Semantics
///
/// - Rate limits are **sticky**: the first `inc()` call for a key stores the window limit;
///   subsequent calls use the stored limit.
/// - Rejected increments are **not** recorded (the count is only added on `Allowed`).
/// - Overall rate limiting across multiple clients is **best-effort** (not linearisable).
#[derive(Clone, Debug)]
pub struct AbsoluteRedisRateLimiter {
    connection_manager: ConnectionManager,
    window_size_seconds: WindowSizeSeconds,
    rate_group_size_ms: RateGroupSizeMs,
    key_generator: RedisKeyGenerator,
    inc_script: Script,
    is_allowed_script: Script,
    get_total_script: Script,
    set_if_script: Script,
    cleanup_script: Script,
}

impl AbsoluteRedisRateLimiter {
    pub(crate) fn new(options: RedisRateLimiterOptions) -> Self {
        let prefix = options.prefix.unwrap_or_else(RedisKey::default_prefix);

        Self {
            connection_manager: options.connection_manager,
            window_size_seconds: options.window_size_seconds,
            rate_group_size_ms: options.rate_group_size_ms,
            key_generator: RedisKeyGenerator::new(prefix, RateType::Absolute),
            inc_script: absolute_lua_script(ABSOLUTE_INC_LUA),
            is_allowed_script: absolute_lua_script(ABSOLUTE_IS_ALLOWED_LUA),
            get_total_script: absolute_lua_script(ABSOLUTE_GET_TOTAL_LUA),
            set_if_script: absolute_lua_script(ABSOLUTE_SET_IF_LUA),
            cleanup_script: absolute_lua_script(ABSOLUTE_CLEANUP_LUA),
        }
    } // end method with_rate_type

    /// Check admission and, if allowed, atomically record the increment for `key`.
    ///
    /// Executes an atomic Lua script that:
    /// 1. Evicts expired buckets (lazy cleanup)
    /// 2. Checks if `total + count > window_limit`
    /// 3. If under the window limit: records the increment and returns `Allowed`
    /// 4. If over the window limit: returns `Rejected` with best-effort backoff hints
    ///
    /// # Arguments
    ///
    /// - `key`: Validated [`RedisKey`] identifying the rate-limited resource
    /// - `rate_limit`: Per-second rate limit (sticky — stored on first call per key)
    /// - `count`: Amount to increment (typically `1`)
    ///
    /// # Returns
    ///
    /// - `Ok(Allowed)` — under limit, increment recorded
    /// - `Ok(Rejected { .. })` — over limit, increment **not** recorded
    /// - `Err(TrypemaError)` — Redis connectivity or script error
    ///
    /// # Examples
    ///
    /// ```
    /// # trypema::__doctest_helpers::with_redis_rate_limiter(|rl| async move {
    /// use trypema::{RateLimit, RateLimitDecision};
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from(trypema::__doctest_helpers::unique_key()).unwrap();
    /// let rate = RateLimit::try_from(10.0).unwrap();
    /// assert!(matches!(
    ///     rl.redis().absolute().inc(&key, &rate, 1).await.unwrap(),
    ///     RateLimitDecision::Allowed
    /// ));
    /// # });
    /// ```
    pub async fn inc(
        &self,
        key: &RedisKey,
        rate_limit: &RateLimit,
        count: u64,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let mut connection_manager = self.connection_manager.clone();

        let window_limit = *self.window_size_seconds as f64 * **rate_limit;

        let (result, retry_after_ms, remaining_after_waiting): (String, u128, u64) = self
            .inc_script
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
            _ => Err(TrypemaError::UnexpectedRedisScriptResult {
                operation: "absolute.inc",
                key: key.to_string(),
                result,
            }),
        }
    } // end method inc

    /// Determine whether `key` is currently allowed (read-only).
    ///
    /// Returns [`RateLimitDecision::Allowed`] if the current sliding window total
    /// is below the window limit, otherwise returns [`RateLimitDecision::Rejected`]
    /// with a best-effort `retry_after_ms`. Does not record an increment.
    ///
    /// # Examples
    ///
    /// ```
    /// # trypema::__doctest_helpers::with_redis_rate_limiter(|rl| async move {
    /// use trypema::{RateLimit, RateLimitDecision};
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from(trypema::__doctest_helpers::unique_key()).unwrap();
    /// // Unknown key → always allowed
    /// assert!(matches!(
    ///     rl.redis().absolute().is_allowed(&key).await.unwrap(),
    ///     RateLimitDecision::Allowed
    /// ));
    /// # });
    /// ```
    pub async fn is_allowed(&self, key: &RedisKey) -> Result<RateLimitDecision, TrypemaError> {
        let mut connection_manager = self.connection_manager.clone();

        let (result, retry_after_ms, remaining_after_waiting): (String, u128, u64) = self
            .is_allowed_script
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
