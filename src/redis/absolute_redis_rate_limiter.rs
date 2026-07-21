use redis::{Script, aio::ConnectionManager};

use crate::{
    BucketSize, ConditionalSetOutcome, HistoryPreservation, RateLimit, RateLimitComparator,
    RateLimitDecision, TrypemaError, WindowSize,
    common::{HistoryUpdateMode, RateType, duration_from_milliseconds},
    redis::{
        RedisKey, RedisKeyGenerator,
        redis_rate_limiter_provider::RedisRateLimiterConfig,
        scripts::{
            ABSOLUTE_CLEANUP_LUA, ABSOLUTE_GET_TOTAL_LUA, ABSOLUTE_INC_LUA,
            ABSOLUTE_IS_ALLOWED_LUA, ABSOLUTE_SET_IF_LUA, absolute_lua_script,
        },
    },
};

/// Sliding-window allow/reject limiter backed by Redis.
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
    window_size: WindowSize,
    bucket_size: BucketSize,
    key_generator: RedisKeyGenerator,
    inc_script: Script,
    is_allowed_script: Script,
    get_total_script: Script,
    set_if_script: Script,
    cleanup_script: Script,
}

impl AbsoluteRedisRateLimiter {
    pub(crate) fn new(options: RedisRateLimiterConfig) -> Self {
        let prefix = options.prefix.unwrap_or_else(RedisKey::default_prefix);

        Self {
            connection_manager: options.connection_manager,
            window_size: options.provider.window_size,
            bucket_size: options.provider.bucket_size,
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
    /// ```no_run
    /// # use trypema::{RateLimiterBuilder, redis::RedisRateLimiterProvider};
    /// # async fn example(connection_manager: trypema::redis::ConnectionManager) {
    /// let rl = RedisRateLimiterProvider::builder(connection_manager).build().unwrap();
    /// use trypema::{RateLimit, RateLimitDecision};
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from("user_123").unwrap();
    /// let rate = RateLimit::per_second(10.0).unwrap();
    /// assert!(matches!(
    ///     rl.absolute().inc(&key, &rate, 1).await.unwrap(),
    ///     RateLimitDecision::Allowed
    /// ));
    /// # }
    /// ```
    pub async fn inc(
        &self,
        key: &RedisKey,
        rate_limit: &RateLimit,
        count: u64,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let mut connection_manager = self.connection_manager.clone();

        let window_limit = self.window_size.as_seconds() as f64 * rate_limit.as_per_second();

        let (result, retry_after_ms, remaining_after_waiting): (String, u128, u64) = self
            .inc_script
            .key(self.key_generator.get_hash_key(key))
            .key(self.key_generator.get_active_keys(key))
            .key(self.key_generator.get_window_limit_key(key))
            .key(self.key_generator.get_total_count_key(key))
            .key(self.key_generator.get_active_entities_key())
            .arg(key.to_string())
            .arg(self.window_size.as_seconds())
            .arg(window_limit)
            .arg(self.bucket_size.as_milliseconds())
            .arg(count)
            .invoke_async(&mut connection_manager)
            .await?;

        match result.as_str() {
            "allowed" => Ok(RateLimitDecision::Allowed),
            "rejected" => Ok(RateLimitDecision::Rejected {
                window_size: self.window_size,
                retry_after: duration_from_milliseconds(retry_after_ms),
                remaining_after_waiting,
            }),
            _ => Err(TrypemaError::UnexpectedRedisScriptResult {
                operation: "absolute.inc",
                key: key.to_string(),
                result,
            }),
        }
    } // end method inc

    /// Determine whether `key` is currently allowed without recording an increment.
    ///
    /// Returns [`RateLimitDecision::Allowed`] if the current sliding window total
    /// is below the window limit, otherwise returns [`RateLimitDecision::Rejected`]
    /// with a best-effort `retry_after`. Does not record an increment.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use trypema::{RateLimiterBuilder, redis::RedisRateLimiterProvider};
    /// # async fn example(connection_manager: trypema::redis::ConnectionManager) {
    /// let rl = RedisRateLimiterProvider::builder(connection_manager).build().unwrap();
    /// use trypema::{RateLimit, RateLimitDecision};
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from("user_123").unwrap();
    /// // Unknown key → always allowed
    /// assert!(matches!(
    ///     rl.absolute().is_allowed(&key).await.unwrap(),
    ///     RateLimitDecision::Allowed
    /// ));
    /// # }
    /// ```
    pub async fn is_allowed(&self, key: &RedisKey) -> Result<RateLimitDecision, TrypemaError> {
        let mut connection_manager = self.connection_manager.clone();

        let (result, retry_after_ms, remaining_after_waiting): (String, u128, u64) = self
            .is_allowed_script
            .key(self.key_generator.get_hash_key(key))
            .key(self.key_generator.get_active_keys(key))
            .key(self.key_generator.get_window_limit_key(key))
            .key(self.key_generator.get_total_count_key(key))
            .arg(self.window_size.as_seconds())
            .invoke_async(&mut connection_manager)
            .await?;

        match result.as_str() {
            "allowed" => Ok(RateLimitDecision::Allowed),
            "rejected" => Ok(RateLimitDecision::Rejected {
                window_size: self.window_size,
                retry_after: duration_from_milliseconds(retry_after_ms),
                remaining_after_waiting,
            }),
            _ => Err(TrypemaError::UnexpectedRedisScriptResult {
                operation: "absolute.is_allowed",
                key: key.to_string(),
                result,
            }),
        }
    }

    /// Current live window total for `key`.
    ///
    /// Executes an atomic Lua script that evicts expired buckets and returns the
    /// resulting window total. Unlike the hybrid variant there is no local state,
    /// so the result is exactly the shared Redis total at execution time.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use trypema::{RateLimiterBuilder, redis::RedisRateLimiterProvider};
    /// # async fn example(connection_manager: trypema::redis::ConnectionManager) {
    /// let rl = RedisRateLimiterProvider::builder(connection_manager).build().unwrap();
    /// use trypema::RateLimit;
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from("user_123").unwrap();
    /// assert_eq!(rl.absolute().get(&key).await.unwrap(), 0);
    ///
    /// let rate = RateLimit::per_second(10.0).unwrap();
    /// rl.absolute().inc(&key, &rate, 3).await.unwrap();
    /// assert_eq!(rl.absolute().get(&key).await.unwrap(), 3);
    /// # }
    /// ```
    pub async fn get(&self, key: &RedisKey) -> Result<u64, TrypemaError> {
        let mut connection_manager = self.connection_manager.clone();

        let total: u64 = self
            .get_total_script
            .key(self.key_generator.get_hash_key(key))
            .key(self.key_generator.get_active_keys(key))
            .key(self.key_generator.get_total_count_key(key))
            .key(self.key_generator.get_active_entities_key())
            .key(self.key_generator.get_window_limit_key(key))
            .arg(key.to_string())
            .arg(self.window_size.as_seconds())
            .invoke_async(&mut connection_manager)
            .await?;

        Ok(total)
    } // end method get

    async fn set_if_with_history_mode(
        &self,
        key: &RedisKey,
        rate_limit: &RateLimit,
        comparator: RateLimitComparator,
        count: u64,
        mode: HistoryUpdateMode,
    ) -> Result<(u64, u64), TrypemaError> {
        let mut connection_manager = self.connection_manager.clone();
        let (comparator_op, comparator_operand) = comparator.redis_args();

        let (new_total, old_total, _changed): (u64, u64, u64) = self
            .set_if_script
            .key(self.key_generator.get_hash_key(key))
            .key(self.key_generator.get_active_keys(key))
            .key(self.key_generator.get_window_limit_key(key))
            .key(self.key_generator.get_total_count_key(key))
            .key(self.key_generator.get_active_entities_key())
            .arg(key.to_string())
            .arg(self.window_size.as_seconds())
            .arg(self.window_size.as_seconds() as f64 * rate_limit.as_per_second())
            .arg(comparator_op)
            .arg(comparator_operand)
            .arg(count)
            .arg(mode.redis_arg())
            .arg(0_u64)
            .invoke_async(&mut connection_manager)
            .await?;

        Ok((new_total, old_total))
    }

    /// Conditionally replace the window total for `key` (atomic on Redis).
    ///
    /// Executes an atomic Lua script that computes the live total without writing,
    /// evaluates `comparator`, and — on a match — prunes expired buckets and replaces
    /// the window contents with a single current-timestamp bucket holding `count`.
    /// On a match the key's window limit is (re)defined as
    /// `window_size × rate_limit` and its TTL refreshed. A comparator miss
    /// performs no Redis writes. A matched `count` of zero removes every per-entity
    /// Redis key and its active-entity membership.
    ///
    /// Every step happens inside one script execution, so unlike the hybrid variant
    /// there is no local state to fold and no sync lag: the comparator always sees
    /// the exact shared total.
    ///
    /// # Arguments
    ///
    /// - `key`: Validated [`RedisKey`] identifying the rate-limited resource
    /// - `rate_limit`: Per-second rate limit used to (re)define the window limit
    /// - `comparator`: Guard evaluated against the current window total
    /// - `count`: The total to write when the guard matches
    ///
    /// # Returns
    ///
    /// A [`ConditionalSetOutcome`] describing whether the comparator matched and the totals
    /// before and after the operation.
    ///
    /// # Priming Idiom
    ///
    /// `set_if(key, rate, RateLimitComparator::Lt(count), count)` raises the window
    /// total to at least `count` and never lowers it — idempotent and safe to retry,
    /// e.g. for seeding a quota window from an external usage store.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use trypema::{RateLimiterBuilder, redis::RedisRateLimiterProvider};
    /// # async fn example(connection_manager: trypema::redis::ConnectionManager) {
    /// let rl = RedisRateLimiterProvider::builder(connection_manager).build().unwrap();
    /// use trypema::{RateLimit, RateLimitComparator};
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from("user_123").unwrap();
    /// let rate = RateLimit::per_second(10.0).unwrap();
    ///
    /// // Prime the window to 40.
    /// let outcome = rl
    ///     .absolute()
    ///     .set_if(&key, &rate, RateLimitComparator::Lt(40), 40)
    ///     .await
    ///     .unwrap();
    /// assert!(outcome.matched);
    /// assert_eq!((outcome.current_total, outcome.previous_total), (40, 0));
    ///
    /// // Re-priming is a no-op: the guard no longer matches.
    /// let outcome = rl
    ///     .absolute()
    ///     .set_if(&key, &rate, RateLimitComparator::Lt(40), 40)
    ///     .await
    ///     .unwrap();
    /// assert!(!outcome.matched);
    /// assert_eq!((outcome.current_total, outcome.previous_total), (40, 40));
    /// # }
    /// ```
    pub async fn set_if(
        &self,
        key: &RedisKey,
        rate_limit: &RateLimit,
        comparator: RateLimitComparator,
        count: u64,
    ) -> Result<ConditionalSetOutcome, TrypemaError> {
        let (current_total, previous_total) = self
            .set_if_with_history_mode(
                key,
                rate_limit,
                comparator,
                count,
                HistoryUpdateMode::Replace,
            )
            .await?;

        Ok(ConditionalSetOutcome {
            matched: comparator.matches(previous_total),
            previous_total,
            current_total,
        })
    } // end method set_if

    /// Conditionally set the total while retaining the selected side of the Redis
    /// sliding-window history.
    pub async fn set_if_preserve_history(
        &self,
        key: &RedisKey,
        rate_limit: &RateLimit,
        comparator: RateLimitComparator,
        count: u64,
        preservation: HistoryPreservation,
    ) -> Result<ConditionalSetOutcome, TrypemaError> {
        let (current_total, previous_total) = self
            .set_if_with_history_mode(
                key,
                rate_limit,
                comparator,
                count,
                HistoryUpdateMode::Preserve(preservation),
            )
            .await?;

        Ok(ConditionalSetOutcome {
            matched: comparator.matches(previous_total),
            previous_total,
            current_total,
        })
    } // end method set_if_preserve_history

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
