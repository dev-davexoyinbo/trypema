use std::{sync::atomic::Ordering, time::Instant};

use dashmap::DashMap;

use crate::{
    common::{HardLimitFactor, RateGroupSizeMs, RateLimit, WindowSizeSeconds},
    AbsoluteLocalRateLimiter, LocalRateLimiterOptions, RateLimitDecision,
};

/// Probabilistic rate limiter with dual tracking for graceful degradation.
///
/// The suppressed strategy tracks two separate rate series:
/// - **Observed limiter:** Tracks all calls (including suppressed ones)
/// - **Accepted limiter:** Tracks only calls admitted by the strategy
///
/// This enables probabilistic suppression: when the observed rate exceeds the target,
/// the strategy probabilistically denies some requests to keep the accepted rate near
/// the configured limit.
///
/// # Algorithm
///
/// 1. **Record observed:** Every call increments the observed limiter (unbounded)
/// 2. **Check capacity:** If `accepted_usage < window_capacity`, bypass suppression → `Allowed`
/// 3. **Calculate suppression:** `suppression_factor = 1.0 - (perceived_rate / rate_limit)`
/// 4. **Probabilistic decision:** Random choice based on `1.0 - suppression_factor`
/// 5. **Hard cutoff:** If `accepted_usage >= hard_limit`, unconditionally reject → `Rejected`
///
/// # Suppression Factor
///
/// ```text
/// suppression_factor = 1.0 - (perceived_rate / rate_limit)
///
/// - 0.0: No suppression (below target rate)
/// - 0.3: Suppress 30% of requests
/// - 0.7: Suppress 70% of requests
/// - 1.0: Suppress all requests (full suppression)
/// ```
///
/// **Perceived rate** is `max(average_window_rate, last_second_rate)`.
///
/// # Three Operating Regimes
///
/// ## 1. Below Capacity (No Suppression)
///
/// `accepted_usage < window_capacity`
///
/// - Returns: [`RateLimitDecision::Allowed`]
/// - Behavior: All requests admitted (subject to hard cutoff)
///
/// ## 2. At Capacity (Probabilistic Suppression)
///
/// `window_capacity ≤ accepted_usage < hard_limit`
///
/// - Returns: [`RateLimitDecision::Suppressed { is_allowed, suppression_factor }`]
/// - Behavior: Probabilistically deny requests to maintain target rate
///
/// ## 3. Over Hard Limit (Hard Rejection)
///
/// `accepted_usage ≥ hard_limit`
///
/// - Returns: [`RateLimitDecision::Rejected`]
/// - Behavior: All requests unconditionally rejected (cannot be suppressed)
///
/// # Hard Limit
///
/// `hard_limit = rate_limit × hard_limit_factor`
///
/// - Acts as an absolute ceiling beyond which no requests are admitted
/// - Prevents runaway acceptance if suppression calculation fails
/// - Recommended: `hard_limit_factor = 1.5-2.0` for burst headroom
///
/// # Thread Safety
///
/// - Uses two independent [`AbsoluteLocalRateLimiter`] instances
/// - Uses [`DashMap`](dashmap::DashMap) for suppression factor cache
/// - Safe for concurrent use without external synchronization
///
/// # Inspiration
///
/// Based on [Ably's distributed rate limiting approach](https://ably.com/blog/distributed-rate-limiting-scale-your-platform),
/// which favors probabilistic suppression over hard cutoffs for better system behavior under load.
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
/// #             hard_limit_factor: HardLimitFactor::try_from(1.5).unwrap(),
/// #         },
/// #         redis: RedisRateLimiterOptions {
/// #             connection_manager: todo!(),
/// #             prefix: None,
/// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
/// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
/// #             hard_limit_factor: HardLimitFactor::try_from(1.5).unwrap(),
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
/// #             hard_limit_factor: HardLimitFactor::try_from(1.5).unwrap(),
/// #         },
/// #     }
/// # }
///
/// let rl = RateLimiter::new(options());
/// let limiter = rl.local().suppressed();
///
/// let rate = RateLimit::try_from(10.0).unwrap(); // 10 req/s target, 15 req/s hard limit
///
/// match limiter.inc("user_123", &rate, 1) {
///     RateLimitDecision::Allowed => {
///         // Below capacity, proceed normally
///     }
///     RateLimitDecision::Suppressed { is_allowed: true, suppression_factor } => {
///         // At capacity, this request allowed (but suppression active)
///         println!("Allowed with {}% suppression", suppression_factor * 100.0);
///     }
///     RateLimitDecision::Suppressed { is_allowed: false, suppression_factor } => {
///         // At capacity, this request suppressed
///         println!("Suppressed ({}% rate)", suppression_factor * 100.0);
///     }
///     RateLimitDecision::Rejected { .. } => {
///         // Over hard limit, unconditionally rejected
///     }
/// }
/// ```
pub struct SuppressedLocalRateLimiter {
    accepted_limiter: AbsoluteLocalRateLimiter,
    observed_limiter: AbsoluteLocalRateLimiter,
    suppression_factors: DashMap<String, (Instant, f64)>,
    hard_limit_factor: HardLimitFactor,
    rate_group_size_ms: RateGroupSizeMs,
    window_size_seconds: WindowSizeSeconds,
}

impl SuppressedLocalRateLimiter {
    pub(crate) fn new(options: LocalRateLimiterOptions) -> Self {
        let rate_group_size_ms = options.rate_group_size_ms;
        let window_size_seconds = options.window_size_seconds;
        let hard_limit_factor = options.hard_limit_factor;

        let accepted_limiter = AbsoluteLocalRateLimiter::new(options.clone());
        let observed_limiter = AbsoluteLocalRateLimiter::new(options);

        Self {
            accepted_limiter,
            observed_limiter,
            suppression_factors: DashMap::new(),
            hard_limit_factor,
            rate_group_size_ms,
            window_size_seconds,
        }
    } // end constructor

    /// Check admission with probabilistic suppression and record the increment.
    ///
    /// This is the primary method for the suppressed strategy. It always records the
    /// increment in the observed limiter, and conditionally records it in the accepted
    /// limiter based on suppression logic.
    ///
    /// # Arguments
    ///
    /// - `key`: Unique identifier for the rate-limited resource
    /// - `rate_limit`: Per-second rate limit. **Sticky:** stored on first call, ignored on subsequent calls
    /// - `count`: Amount to increment (typically `1` for single requests, or batch size)
    ///
    /// # Returns
    ///
    /// - [`RateLimitDecision::Allowed`]: Below capacity, no suppression active
    /// - [`RateLimitDecision::Suppressed`]: At capacity, probabilistic suppression active
    ///   - Check `is_allowed` field to determine if this specific call was admitted
    /// - [`RateLimitDecision::Rejected`]: Over hard limit, unconditionally rejected
    ///
    /// # Behavior
    ///
    /// 1. **Always record observed:** Increment observed limiter (unbounded)
    /// 2. **Calculate suppression factor** (cached per `rate_group_size_ms`)
    /// 3. **Decide:**
    ///    - If `suppression_factor ≤ 0.0`: Bypass suppression → try `accepted_limiter.inc()`
    ///    - Else: Probabilistic decision based on `1.0 - suppression_factor`
    /// 4. **If probabilistically denied:**
    ///    - Return `Suppressed { is_allowed: false }`
    ///    - Do **not** increment accepted limiter
    /// 5. **If probabilistically allowed:**
    ///    - Try `accepted_limiter.inc()` with hard limit
    ///    - If over hard limit → `Rejected`
    ///    - If under hard limit → `Suppressed { is_allowed: true }`
    ///
    /// # Concurrency
    ///
    /// Same as [`AbsoluteLocalRateLimiter::inc`]: not atomic across calls, temporary
    /// overshoot possible under high concurrency.
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
    /// #             hard_limit_factor: HardLimitFactor::try_from(1.5).unwrap(),
    /// #         },
    /// #         redis: RedisRateLimiterOptions {
    /// #             connection_manager: todo!(),
    /// #             prefix: None,
    /// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
    /// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
    /// #             hard_limit_factor: HardLimitFactor::try_from(1.5).unwrap(),
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
    /// #             hard_limit_factor: HardLimitFactor::try_from(1.5).unwrap(),
    /// #         },
    /// #     }
    /// # }
    /// #
    /// # let rl = RateLimiter::new(options());
    /// # let limiter = rl.local().suppressed();
    /// let rate = RateLimit::try_from(10.0).unwrap();
    ///
    /// match limiter.inc("user_123", &rate, 1) {
    ///     RateLimitDecision::Allowed => {
    ///         // Below capacity, proceed
    ///         println!("Request allowed");
    ///     }
    ///     RateLimitDecision::Suppressed { is_allowed, suppression_factor } => {
    ///         if is_allowed {
    ///             // At capacity but this request admitted
    ///             println!("Allowed with {}% suppression", suppression_factor * 100.0);
    ///             // Proceed, maybe with reduced priority
    ///         } else {
    ///             // At capacity and this request suppressed
    ///             println!("Suppressed ({}%)", suppression_factor * 100.0);
    ///             // Deny request, send 429
    ///         }
    ///     }
    ///     RateLimitDecision::Rejected { retry_after_ms, .. } => {
    ///         // Over hard limit, unconditionally rejected
    ///         println!("Hard limit exceeded, retry in {}ms", retry_after_ms);
    ///     }
    /// }
    /// ```
    pub fn inc(&self, key: &str, rate_limit: &RateLimit, count: u64) -> RateLimitDecision {
        let mut rng = |p: f64| rand::random_bool(p);
        self.inc_with_rng(key, rate_limit, count, &mut rng)
    }

    pub(crate) fn inc_with_rng(
        &self,
        key: &str,
        rate_limit: &RateLimit,
        count: u64,
        random_bool: &mut impl FnMut(f64) -> bool,
    ) -> RateLimitDecision {
        // Always track observed usage; this limiter uses a max rate to avoid rejecting.
        self.observed_limiter.inc(key, &RateLimit::max(), count);

        let mut suppression_factor = match self.suppression_factors.get(key) {
            None => self.calculate_suppression_factor(key).1,
            Some(val) if val.0.elapsed().as_millis() < *self.rate_group_size_ms as u128 => val.1,
            Some(val) => {
                drop(val);
                self.calculate_suppression_factor(key).1
            }
        };

        suppression_factor = suppression_factor.min(1f64);

        if suppression_factor <= 0f64 {
            return self
                .accepted_limiter
                .inc(key, &self.get_hard_limit(rate_limit), count);
        }

        let should_allow = random_bool(1f64 - suppression_factor);

        if !should_allow {
            return RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: false,
            };
        }

        let decision = self
            .accepted_limiter
            .inc(key, &self.get_hard_limit(rate_limit), count);

        match decision {
            RateLimitDecision::Allowed => RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: true,
            },
            // This is a hard limit rejection, so we can't suppress.
            RateLimitDecision::Rejected { .. } => decision,
            RateLimitDecision::Suppressed { .. } => {
                unreachable!("AbsoluteLocalRateLimiter::inc: should not be suppressed")
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn accepted_limiter(&self) -> &AbsoluteLocalRateLimiter {
        &self.accepted_limiter
    }

    #[cfg(test)]
    pub(crate) fn observed_limiter(&self) -> &AbsoluteLocalRateLimiter {
        &self.observed_limiter
    }

    #[cfg(test)]
    pub(crate) fn test_set_suppression_factor(&self, key: &str, at: Instant, value: f64) {
        self.suppression_factors
            .insert(key.to_string(), (at, value));
    }

    #[cfg(test)]
    pub(crate) fn test_get_suppression_factor(&self, key: &str) -> Option<(Instant, f64)> {
        self.suppression_factors.get(key).map(|v| *v)
    }

    #[inline(always)]
    fn persist_suppression_factor(&self, key: &str, value: f64) -> (Instant, f64) {
        let persist = (Instant::now(), value);
        self.suppression_factors.insert(key.to_string(), persist);
        persist
    }

    #[inline]
    fn calculate_suppression_factor(&self, key: &str) -> (Instant, f64) {
        let Some(series) = self.accepted_limiter.series().get(key) else {
            return self.persist_suppression_factor(key, 0f64);
        };

        if series.series.is_empty() {
            return self.persist_suppression_factor(key, 0f64);
        }

        let window_limit = *self.window_size_seconds as f64 * *series.limit;

        if series.total.load(Ordering::Relaxed) < window_limit as u64 {
            return self.persist_suppression_factor(key, 0f64);
        }

        let mut total_in_last_second = 0u64;

        for instant_rate in series.series.iter().rev() {
            if instant_rate.timestamp.elapsed().as_secs() > 1 {
                break;
            }

            total_in_last_second =
                total_in_last_second.saturating_add(instant_rate.count.load(Ordering::Relaxed));
        }

        let average_rate_in_window: f64 =
            series.total.load(Ordering::Relaxed) as f64 / *self.window_size_seconds as f64;

        let perceived_rate_limit = average_rate_in_window.max(total_in_last_second as f64);

        let suppression_factor = 1f64 - (perceived_rate_limit / *series.limit);

        self.persist_suppression_factor(key, suppression_factor)
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

    pub(crate) fn cleanup(&self, stale_after_ms: u64) {
        self.suppression_factors
            .retain(|_, (instant, _)| instant.elapsed().as_millis() < stale_after_ms as u128);
        self.accepted_limiter.cleanup(stale_after_ms);
        self.observed_limiter.cleanup(stale_after_ms);
    } // end method cleanup
} // end impl
