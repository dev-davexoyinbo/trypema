use std::{sync::atomic::Ordering, time::Instant};

use dashmap::DashMap;

use crate::{
    LocalRateLimiterOptions,
    common::{
        InstantRate, RateGroupSizeMs, RateLimit, RateLimitDecision, RateLimitSeries,
        WindowSizeSeconds,
    },
};

/// Strict sliding-window rate limiter for in-process use.
///
/// Provides deterministic rate limiting with per-key state maintained in memory.
/// Uses a sliding time window to track request counts and enforce limits.
///
/// # Algorithm
///
/// 1. **Window capacity:** `window_size_seconds × rate_limit`
/// 2. **Admission check:** Sum all bucket counts within the window
/// 3. **Decision:** Allow if `total < capacity`, reject otherwise
/// 4. **Increment:** If allowed, add count to current (or coalesced) bucket
///
/// # Thread Safety
///
/// - Uses [`DashMap`](dashmap::DashMap) for concurrent key access
/// - Uses atomics for per-bucket counters
/// - Safe for multi-threaded use without external synchronization
///
/// # Semantics & Limitations
///
/// **Sticky rate limits:**
/// - The first call for a key stores the rate limit
/// - Subsequent calls for the same key do not update it
/// - Rationale: Avoids races where concurrent calls specify different limits
///
/// **Best-effort concurrency:**
/// - Admission check and increment are not atomic across calls
/// - Multiple threads can observe "allowed" simultaneously
/// - All may proceed, causing temporary overshoot
/// - This is **expected behavior**, not a bug
///
/// **Conservative eviction:**
/// - Uses `Instant::elapsed().as_secs()` (whole-second truncation)
/// - A 1-second window may effectively require ~2 seconds for full expiration
/// - Trades precision for performance
///
/// **Memory growth:**
/// - Keys are not automatically removed
/// - Unbounded key cardinality will grow memory
/// - Use `run_cleanup_loop()` to periodically remove stale keys
///
/// **Lazy eviction:**
/// - Expired buckets are only removed when `is_allowed()` or `inc()` is called
/// - Stale buckets remain in memory until accessed or cleanup runs
///
/// # Performance
///
/// - **Admission check:** O(buckets_in_window) - typically < 10 buckets
/// - **Increment:** O(1) if coalesced, O(log n) to add new bucket
/// - **Memory:** ~50-200 bytes per key (depends on bucket count)
///
/// # Examples
///
/// ```no_run
/// use trypema::{HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision, RateLimiter, RateLimiterOptions, WindowSizeSeconds};
/// use trypema::local::LocalRateLimiterOptions;
/// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
/// # use trypema::redis::RedisRateLimiterOptions;
/// #
/// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
/// # fn options() -> RateLimiterOptions {
/// #     RateLimiterOptions {
/// #         local: LocalRateLimiterOptions {
/// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
/// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
/// #             hard_limit_factor: HardLimitFactor::default(),
/// #         },
/// #         redis: RedisRateLimiterOptions {
/// #             connection_manager: todo!(),
/// #             prefix: None,
/// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
/// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
/// #             hard_limit_factor: HardLimitFactor::default(),
/// #         },
/// #     }
/// # }
/// #
/// # #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
/// # fn options() -> RateLimiterOptions {
/// #     RateLimiterOptions {
/// #         local: LocalRateLimiterOptions {
/// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
/// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
/// #             hard_limit_factor: HardLimitFactor::default(),
/// #         },
/// #     }
/// # }
///
/// let rl = RateLimiter::new(options());
/// let limiter = rl.local().absolute();
///
/// let rate = RateLimit::try_from(10.0).unwrap(); // 10 req/s
///
/// // First request for key: allowed and rate limit stored
/// assert!(matches!(
///     limiter.inc("user_123", &rate, 1),
///     RateLimitDecision::Allowed
/// ));
///
/// // Check without incrementing
/// assert!(matches!(
///     limiter.is_allowed("user_123"),
///     RateLimitDecision::Allowed
/// ));
/// ```
pub struct AbsoluteLocalRateLimiter {
    window_size_seconds: WindowSizeSeconds,
    rate_group_size_ms: RateGroupSizeMs,
    series: DashMap<String, RateLimitSeries>,
}

impl AbsoluteLocalRateLimiter {
    pub(crate) fn new(options: LocalRateLimiterOptions) -> Self {
        Self {
            window_size_seconds: options.window_size_seconds,
            rate_group_size_ms: options.rate_group_size_ms,
            series: DashMap::new(),
        }
    } // end constructor

    pub(crate) fn series(&self) -> &DashMap<String, RateLimitSeries> {
        &self.series
    }

    /// Check admission and, if allowed, record the increment for `key`.
    ///
    /// This is the primary method for rate limiting. It performs an admission check
    /// and, if allowed, records the increment in the key's state.
    ///
    /// # Arguments
    ///
    /// - `key`: Unique identifier for the rate-limited resource (e.g., `"user_123"`, `"api_endpoint"`)
    /// - `rate_limit`: Per-second rate limit. **Sticky:** stored on first call, ignored on subsequent calls
    /// - `count`: Amount to increment (typically `1` for single requests, or batch size)
    ///
    /// # Returns
    ///
    /// - [`RateLimitDecision::Allowed`]: Request admitted, increment recorded
    /// - [`RateLimitDecision::Rejected`]: Over limit, increment **not** recorded
    ///
    /// # Behavior
    ///
    /// 1. Check current window usage via `is_allowed(key)`
    /// 2. If over limit, return `Rejected` (no state change)
    /// 3. If allowed:
    ///    - Check if recent bucket exists within `rate_group_size_ms`
    ///    - If yes: add count to existing bucket (coalescing)
    ///    - If no: create new bucket with count
    ///    - Return `Allowed`
    ///
    /// # Concurrency
    ///
    /// **Not atomic across calls.** Under concurrent load:
    /// - Multiple threads may observe `Allowed` simultaneously
    /// - All may proceed and increment, causing temporary overshoot
    /// - This is **expected** and by design for performance
    ///
    /// For strict enforcement, use external synchronization (e.g., per-key locks).
    ///
    /// # Bucket Coalescing
    ///
    /// Increments within `rate_group_size_ms` of the most recent bucket are merged
    /// into that bucket. This reduces memory usage and improves performance.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use trypema::{HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision, RateLimiter, RateLimiterOptions, WindowSizeSeconds};
    /// # use trypema::local::LocalRateLimiterOptions;
    /// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    /// # use trypema::redis::RedisRateLimiterOptions;
    /// #
    /// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    /// # fn options() -> RateLimiterOptions {
    /// #     RateLimiterOptions {
    /// #         local: LocalRateLimiterOptions {
    /// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
    /// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
    /// #             hard_limit_factor: HardLimitFactor::default(),
    /// #         },
    /// #         redis: RedisRateLimiterOptions {
    /// #             connection_manager: todo!(),
    /// #             prefix: None,
    /// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
    /// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
    /// #             hard_limit_factor: HardLimitFactor::default(),
    /// #         },
    /// #     }
    /// # }
    /// #
    /// # #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
    /// # fn options() -> RateLimiterOptions {
    /// #     RateLimiterOptions {
    /// #         local: LocalRateLimiterOptions {
    /// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
    /// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
    /// #             hard_limit_factor: HardLimitFactor::default(),
    /// #         },
    /// #     }
    /// # }
    /// #
    /// # let rl = RateLimiter::new(options());
    /// # let limiter = rl.local().absolute();
    /// let rate = RateLimit::try_from(10.0).unwrap();
    ///
    /// // Single request
    /// match limiter.inc("user_123", &rate, 1) {
    ///     RateLimitDecision::Allowed => println!("Request allowed"),
    ///     RateLimitDecision::Rejected { retry_after_ms, .. } => {
    ///         println!("Rate limited, retry in {}ms", retry_after_ms);
    ///     }
    ///     _ => unreachable!(),
    /// }
    ///
    /// // Batch of 10 requests
    /// match limiter.inc("user_456", &rate, 10) {
    ///     RateLimitDecision::Allowed => println!("Batch allowed"),
    ///     RateLimitDecision::Rejected { .. } => println!("Batch rejected"),
    ///     _ => unreachable!(),
    /// }
    /// ```
    pub fn inc(&self, key: &str, rate_limit: &RateLimit, count: u64) -> RateLimitDecision {
        let is_allowed = self.is_allowed(key);

        if !matches!(is_allowed, RateLimitDecision::Allowed) {
            return is_allowed;
        }

        if !self.series.contains_key(key) {
            self.series
                .entry(key.to_string())
                .or_insert_with(|| RateLimitSeries::new(*rate_limit));
        }

        let Some(rate_limit_series) = self.series.get(key) else {
            unreachable!("AbsoluteLocalRateLimiter::inc: key should be in map");
        };

        if let Some(last_entry) = rate_limit_series.series.back()
            && last_entry.timestamp.elapsed().as_millis() <= *self.rate_group_size_ms as u128
        {
            last_entry.count.fetch_add(count, Ordering::Relaxed);
            rate_limit_series.total.fetch_add(count, Ordering::Relaxed);
        } else {
            drop(rate_limit_series);

            let mut rate_limit_series = self
                .series
                .entry(key.to_string())
                .or_insert_with(|| RateLimitSeries::new(*rate_limit));

            rate_limit_series.series.push_back(InstantRate {
                count: count.into(),
                timestamp: Instant::now(),
            });

            rate_limit_series.total.fetch_add(count, Ordering::Relaxed);
        }

        RateLimitDecision::Allowed
    } // end method inc

    /// Check if `key` is currently under its rate limit (read-only).
    ///
    /// Performs an admission check **without** recording an increment. Useful for
    /// previewing whether a request would be allowed before doing expensive work.
    ///
    /// # Arguments
    ///
    /// - `key`: Unique identifier for the rate-limited resource
    ///
    /// # Returns
    ///
    /// - [`RateLimitDecision::Allowed`]: Key is under limit
    /// - [`RateLimitDecision::Rejected`]: Key is over limit, includes backoff hints
    ///
    /// # Behavior
    ///
    /// 1. If key doesn't exist, return `Allowed` (no state yet)
    /// 2. Perform lazy eviction of expired buckets
    /// 3. Sum remaining bucket counts
    /// 4. Compare against `window_capacity = window_size_seconds × rate_limit`
    /// 5. Return decision with metadata if rejected
    ///
    /// # Side Effects
    ///
    /// - **Lazy eviction:** Removes expired buckets from key's state
    /// - **No increment:** Does not modify counters (read-only check)
    ///
    /// # Use Cases
    ///
    /// - **Preview:** Check before expensive operations
    /// - **Metrics:** Sample rate limit status without affecting state
    /// - **Testing:** Verify rate limit behavior
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use trypema::{HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision, RateLimiter, RateLimiterOptions, WindowSizeSeconds};
    /// # use trypema::local::LocalRateLimiterOptions;
    /// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    /// # use trypema::redis::RedisRateLimiterOptions;
    /// #
    /// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    /// # fn options() -> RateLimiterOptions {
    /// #     RateLimiterOptions {
    /// #         local: LocalRateLimiterOptions {
    /// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
    /// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
    /// #             hard_limit_factor: HardLimitFactor::default(),
    /// #         },
    /// #         redis: RedisRateLimiterOptions {
    /// #             connection_manager: todo!(),
    /// #             prefix: None,
    /// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
    /// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
    /// #             hard_limit_factor: HardLimitFactor::default(),
    /// #         },
    /// #     }
    /// # }
    /// #
    /// # #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
    /// # fn options() -> RateLimiterOptions {
    /// #     RateLimiterOptions {
    /// #         local: LocalRateLimiterOptions {
    /// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
    /// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
    /// #             hard_limit_factor: HardLimitFactor::default(),
    /// #         },
    /// #     }
    /// # }
    /// #
    /// # let rl = RateLimiter::new(options());
    /// # let limiter = rl.local().absolute();
    /// # let rate = RateLimit::try_from(10.0).unwrap();
    /// // Check before expensive operation
    /// match limiter.is_allowed("user_123") {
    ///     RateLimitDecision::Allowed => {
    ///         // Do expensive work, then record
    ///         // expensive_operation();
    ///         limiter.inc("user_123", &rate, 1);
    ///     }
    ///     RateLimitDecision::Rejected { retry_after_ms, .. } => {
    ///         println!("Rate limited, retry in {}ms", retry_after_ms);
    ///     }
    ///     _ => unreachable!(),
    /// }
    ///
    /// // Preview for metrics (doesn't affect state)
    /// if matches!(limiter.is_allowed("api_endpoint"), RateLimitDecision::Rejected { .. }) {
    ///     // metrics.increment("rate_limit.at_capacity");
    /// }
    /// ```
    pub fn is_allowed(&self, key: &str) -> RateLimitDecision {
        let Some(rate_limit) = self.series.get(key) else {
            return RateLimitDecision::Allowed;
        };

        let rate_limit = match rate_limit.series.front() {
            None => rate_limit,
            Some(instant_rate)
                if instant_rate.timestamp.elapsed().as_secs() <= *self.window_size_seconds =>
            {
                rate_limit
            }
            Some(_) => {
                drop(rate_limit);

                let Some(mut rate_limit) = self.series.get_mut(key) else {
                    return RateLimitDecision::Allowed;
                };

                while let Some(instant_rate) = rate_limit.series.front()
                    && instant_rate.timestamp.elapsed().as_secs() > *self.window_size_seconds
                {
                    rate_limit.total.fetch_sub(
                        instant_rate.count.load(Ordering::Relaxed),
                        Ordering::Relaxed,
                    );

                    rate_limit.series.pop_front();
                }

                drop(rate_limit);

                let Some(rate_limit) = self.series.get(key) else {
                    return RateLimitDecision::Allowed;
                };

                rate_limit
            }
        };

        let window_limit = *self.window_size_seconds as f64 * *rate_limit.limit;

        if rate_limit.total.load(Ordering::Relaxed) < window_limit as u64 {
            return RateLimitDecision::Allowed;
        }

        let (retry_after_ms, remaining_after_waiting) = match rate_limit.series.front() {
            None => (0, 0),
            Some(instant_rate) => {
                let window_ms = self.window_size_seconds.saturating_mul(1000);
                let elapsed_ms = instant_rate.timestamp.elapsed().as_millis();
                let elapsed_ms = u64::try_from(elapsed_ms).unwrap_or(u64::MAX);
                let retry_after_ms = window_ms.saturating_sub(elapsed_ms);

                let current_total = rate_limit.total.load(Ordering::Relaxed);
                let oldest_count = instant_rate.count.load(Ordering::Relaxed);
                let remaining_after_waiting = current_total.saturating_sub(oldest_count);

                (retry_after_ms, remaining_after_waiting)
            }
        };

        RateLimitDecision::Rejected {
            window_size_seconds: *self.window_size_seconds,
            retry_after_ms,
            remaining_after_waiting,
        }
    } // end method is_allowed

    pub(crate) fn cleanup(&self, stale_after_ms: u64) {
        self.series.retain(
            |_, rate_limit_series| match rate_limit_series.series.back() {
                None => false,
                Some(instant_rate)
                    if instant_rate.timestamp.elapsed().as_millis() > stale_after_ms as u128 =>
                {
                    false
                }
                Some(_) => true,
            },
        );
    } // end method cleanup
} // end of impl
