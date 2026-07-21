use redis::{Script, aio::ConnectionManager};

use crate::{
    BucketSize, ConditionalSetOutcome, HardLimitFactor, HistoryPreservation, RateLimit,
    RateLimitComparator, RateLimitDecision, SuppressedRateLimitSnapshot, TrypemaError, WindowSize,
    common::{HistoryUpdateMode, RateType, SuppressionFactorCachePeriod},
    redis::{
        RedisKey, RedisKeyGenerator,
        redis_rate_limiter_provider::RedisRateLimiterConfig,
        scripts::{
            SUPPRESSED_CLEANUP_LUA, SUPPRESSED_GET_FACTOR_LUA, SUPPRESSED_GET_STATE_LUA,
            SUPPRESSED_INC_LUA, SUPPRESSED_SET_IF_LUA, lua_script, suppressed_lua_script,
        },
    },
};

/// Probabilistic suppression rate limiter backed by Redis.
///
/// Provides the same probabilistic suppression semantics as
/// [`SuppressedLocalRateLimiter`](crate::local::SuppressedLocalRateLimiter), but stores
/// all state in Redis so limits are shared across processes and servers.
///
/// # Implementation
///
/// Every `inc()` and `get_suppression_factor()` call executes an atomic Lua script
/// against Redis. The scripts handle bucket eviction, suppression factor computation,
/// and the probabilistic admission decision in a single atomic operation.
///
/// # Data Model
///
/// For a key `K` with prefix `P`:
/// - `P:K:suppressed:h` — Hash of `timestamp_ms → count` (total observed per bucket)
/// - `P:K:suppressed:hd` — Hash of `timestamp_ms → declined_count` (declined per bucket)
/// - `P:K:suppressed:a` — Sorted set of active bucket timestamps
/// - `P:K:suppressed:w` — Hard window limit (set on first call)
/// - `P:K:suppressed:t` — Total observed count across all buckets
/// - `P:K:suppressed:d` — Total declined count across all buckets
/// - `P:K:suppressed:sf` — Cached suppression factor (string with `PX` TTL)
/// - `P:active_entities` — Sorted set of all active keys (used by cleanup)
///
/// # Tracking
///
/// The suppressed strategy always increments the total observed counter. If a call is
/// denied (`is_allowed: false`), it also increments the declined counter. This allows
/// deriving accepted usage as: `accepted = observed - declined`.
#[derive(Clone, Debug)]
pub struct SuppressedRedisRateLimiter {
    connection_manager: ConnectionManager,
    key_generator: RedisKeyGenerator,
    hard_limit_factor: HardLimitFactor,
    bucket_size: BucketSize,
    window_size: WindowSize,
    suppression_factor_cache_period: SuppressionFactorCachePeriod,
    inc_script: Script,
    cleanup_script: Script,
    suppression_factor_script: Script,
    get_state_script: Script,
    set_if_script: Script,
}

impl SuppressedRedisRateLimiter {
    pub(crate) fn new(options: RedisRateLimiterConfig) -> Self {
        let prefix = options.prefix.unwrap_or_else(RedisKey::default_prefix);
        let key_generator = RedisKeyGenerator::new(prefix, RateType::Suppressed);

        Self {
            connection_manager: options.connection_manager,
            window_size: options.provider.window_size,
            bucket_size: options.provider.bucket_size,
            hard_limit_factor: options.provider.hard_limit_factor,
            suppression_factor_cache_period: options.provider.suppression_factor_cache_period,
            key_generator,
            inc_script: suppressed_lua_script(SUPPRESSED_INC_LUA),
            cleanup_script: lua_script(SUPPRESSED_CLEANUP_LUA),
            suppression_factor_script: suppressed_lua_script(SUPPRESSED_GET_FACTOR_LUA),
            get_state_script: suppressed_lua_script(SUPPRESSED_GET_STATE_LUA),
            set_if_script: lua_script(SUPPRESSED_SET_IF_LUA),
        }
    }

    /// Check admission and increment counters for `key` using probabilistic suppression.
    ///
    /// Executes an atomic Lua script that:
    /// 1. Evicts expired buckets (lazy cleanup)
    /// 2. Computes or retrieves cached suppression factor
    /// 3. Probabilistically decides admission
    /// 4. Records the increment (always) and declined count (if denied)
    ///
    /// # Arguments
    ///
    /// - `key`: Validated [`RedisKey`] identifying the rate-limited resource
    /// - `rate_limit`: Per-second rate limit (sticky — stored on first call per key)
    /// - `count`: Amount to increment (typically `1`)
    ///
    /// # Returns
    ///
    /// - `Ok(Allowed)` — the increment remains within soft capacity or reaches the hard limit
    ///   exactly
    /// - `Ok(Suppressed { is_allowed, suppression_factor })` — the forecasted accepted total is
    ///   above soft capacity without landing exactly on the hard limit; check `is_allowed`
    /// - `Err(TrypemaError)` — Redis connectivity or script error
    ///
    /// The total observed counter is **always** incremented, regardless of the decision. If the
    /// increment reaches the hard limit exactly, it is admitted and a factor of `1.0` is cached
    /// for subsequent calls. If `is_allowed` is `false`, the declined counter is also incremented.
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
    /// // Under limit → Allowed
    /// assert!(matches!(
    ///     rl.suppressed().inc(&key, &rate, 1).await.unwrap(),
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
        let hard_window_limit = self.window_size.as_seconds() as f64
            * rate_limit.as_per_second()
            * self.hard_limit_factor.as_multiplier();

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
            .arg(self.window_size.as_seconds())
            .arg(hard_window_limit)
            .arg(self.bucket_size.as_milliseconds())
            .arg(self.suppression_factor_cache_period.as_milliseconds())
            .arg(self.hard_limit_factor.as_multiplier())
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
            .key(self.key_generator.get_suppression_factor_key(key))
            .key(self.key_generator.get_total_declined_key(key))
            .key(self.key_generator.get_hash_declined_key(key))
            .arg(key.to_string())
            .arg(self.window_size.as_seconds())
            .arg(
                self.window_size.as_seconds() as f64
                    * rate_limit.as_per_second()
                    * self.hard_limit_factor.as_multiplier(),
            )
            .arg(comparator_op)
            .arg(comparator_operand)
            .arg(count)
            .arg(mode.redis_arg())
            .arg(0_u64)
            .arg(0_u64)
            .invoke_async(&mut connection_manager)
            .await?;

        Ok((new_total, old_total))
    }

    /// Get the current suppression factor for `key`.
    ///
    /// Returns a value in the range `[0.0, 1.0]`:
    /// - `0.0` — no suppression (below capacity or key not found)
    /// - `0.0 < sf < 1.0` — partial suppression (at capacity)
    /// - `1.0` — full suppression (cached at the hard boundary or on a forecast above it)
    ///
    /// This method is read-only with respect to request counts — it does not record any
    /// increment. It is useful for exporting metrics, building dashboards, or debugging
    /// why calls are being suppressed.
    ///
    /// **Caching:** If a cached value exists in Redis (set via `SET ... PX`), it is returned
    /// directly. Otherwise, this recomputes the factor via the same algorithm used in `inc()`
    /// and writes it back to Redis with a `suppression_factor_cache_period` TTL. If the cached
    /// value is outside `[0.0, 1.0]`, it is treated as stale and recomputed.
    /// Unknown keys return `0.0` without creating cache or active-entity state. Evicting
    /// expired history invalidates any factor cached from that history.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use trypema::{RateLimiterBuilder, redis::RedisRateLimiterProvider};
    /// # async fn example(connection_manager: trypema::redis::ConnectionManager) {
    /// let rl = RedisRateLimiterProvider::builder(connection_manager).build().unwrap();
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from("user_123").unwrap();
    /// // No state yet → 0.0 (no suppression)
    /// assert_eq!(rl.suppressed().get_suppression_factor(&key).await.unwrap(), 0.0);
    /// # }
    /// ```
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
            .arg(self.window_size.as_seconds())
            .arg(self.bucket_size.as_milliseconds())
            .arg(self.suppression_factor_cache_period.as_milliseconds())
            .arg(self.hard_limit_factor.as_multiplier())
            .invoke_async(&mut connection_manager)
            .await?;

        Ok(suppression_factor)
    } // end method calculate_suppression_factor

    /// Current live window state for `key`.
    ///
    /// Executes an atomic Lua script that evicts expired buckets (keeping the declined counters
    /// in step) and returns the observed total, declined total, and current suppression factor.
    /// The observed total includes accepted and declined calls, matching the counter that
    /// suppression decisions are based on.
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
    /// let snapshot = rl.suppressed().get(&key).await.unwrap();
    /// assert_eq!(snapshot.total, 0);
    /// assert_eq!(snapshot.total_declined, 0);
    /// assert_eq!(snapshot.suppression_factor, 0.0);
    ///
    /// let rate = RateLimit::per_second(10.0).unwrap();
    /// rl.suppressed().inc(&key, &rate, 3).await.unwrap();
    /// assert_eq!(rl.suppressed().get(&key).await.unwrap().total, 3);
    /// # }
    /// ```
    pub async fn get(&self, key: &RedisKey) -> Result<SuppressedRateLimitSnapshot, TrypemaError> {
        let mut connection_manager = self.connection_manager.clone();

        let (total, total_declined, suppression_factor): (u64, u64, f64) = self
            .get_state_script
            .key(self.key_generator.get_hash_key(key))
            .key(self.key_generator.get_active_keys(key))
            .key(self.key_generator.get_total_count_key(key))
            .key(self.key_generator.get_active_entities_key())
            .key(self.key_generator.get_total_declined_key(key))
            .key(self.key_generator.get_hash_declined_key(key))
            .key(self.key_generator.get_window_limit_key(key))
            .key(self.key_generator.get_suppression_factor_key(key))
            .arg(key.to_string())
            .arg(self.window_size.as_seconds())
            .arg(self.suppression_factor_cache_period.as_milliseconds())
            .arg(self.hard_limit_factor.as_multiplier())
            .invoke_async(&mut connection_manager)
            .await?;

        Ok(SuppressedRateLimitSnapshot {
            total,
            total_declined,
            suppression_factor,
        })
    } // end method get

    /// Conditionally replace the window total for `key` (atomic on Redis).
    ///
    /// Executes an atomic Lua script that computes the live total without writing,
    /// evaluates `comparator`, and — on a match — prunes expired buckets and replaces
    /// the window contents with a single current-timestamp bucket holding `count`,
    /// with **no declines** recorded against it. On a match the key's hard window
    /// limit is (re)defined as `window_size × rate_limit × hard_limit_factor`
    /// and the cached suppression factor is deleted so it is recomputed from the new
    /// state on the next call. A comparator miss leaves history, limit, TTL, and
    /// cached suppression metadata untouched. A matched `count` of zero removes all
    /// count, decline, limit, cache, history, and active-entity state for the key.
    ///
    /// # Arguments
    ///
    /// - `key`: Validated [`RedisKey`] identifying the rate-limited resource
    /// - `rate_limit`: Per-second rate limit used to (re)define the hard window limit
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
    /// total to at least `count` and never lowers it — idempotent and safe to retry.
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
    ///     .suppressed()
    ///     .set_if(&key, &rate, RateLimitComparator::Lt(40), 40)
    ///     .await
    ///     .unwrap();
    /// assert!(outcome.matched);
    /// assert_eq!((outcome.current_total, outcome.previous_total), (40, 0));
    ///
    /// // Re-priming is a no-op: the guard no longer matches.
    /// let outcome = rl
    ///     .suppressed()
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

    /// Conditionally set the observed total while retaining the selected side of
    /// the Redis sliding-window history.
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
