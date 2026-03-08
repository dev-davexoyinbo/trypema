use std::{
    collections::VecDeque,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use dashmap::DashMap;

use crate::{
    LocalRateLimiterOptions, RateGroupSizeMs, RateLimitDecision,
    common::{
        HardLimitFactor, InstantRate, RateLimit, SuppressionFactorCacheMs, WindowSizeSeconds,
    },
};

#[derive(Debug)]
pub(crate) struct RateLimitSeries {
    pub limit: f64,
    pub series: VecDeque<InstantRate>,
    pub total: AtomicU64,
    pub total_declined: AtomicU64,
}

impl RateLimitSeries {
    pub fn new(limit: f64) -> Self {
        Self {
            limit,
            series: VecDeque::new(),
            total: AtomicU64::new(0),
            total_declined: AtomicU64::new(0),
        }
    }
}

/// Probabilistic in-process rate limiter for graceful degradation.
///
/// This local suppressed limiter maintains a per-key sliding window of counters:
///
/// - `total`: the total number of calls observed for the key
/// - `declined`: the number of calls that were denied by suppression (`is_allowed: false`)
///
/// From these, you can derive "accepted" usage as `total - declined`.
///
/// # Algorithm
///
/// 1. **Compute suppression factor (cached):** see "Suppression Factor" below
/// 2. **Probabilistic decision:** if `0.0 < suppression_factor < 1.0`, allow with probability
///    `p = 1.0 - suppression_factor`
/// 3. **Record totals:** increments are tracked in the per-key sliding window; denied calls also
///    increment the declined counters
///
/// # Suppression Factor
///
/// ```text
/// suppression_factor = 1.0 - (rate_limit / perceived_rate)
///
/// - 0.0: No suppression (below target rate)
/// - 0.3: Suppress 30% of requests
/// - 0.7: Suppress 70% of requests
/// - 1.0: Suppress all requests (full suppression)
/// ```
///
/// **Perceived rate** is `max(average_window_rate, rate_in_last_1000ms)`.
///
/// `rate_in_last_1000ms` is computed using `Instant::elapsed().as_millis()` (whole-millisecond
/// truncation), so suppression reacts at ~1ms granularity.
///
/// # Three Operating Regimes
///
/// ## 1. Below Capacity (No Suppression)
///
/// If the observed window usage is below the window capacity, the suppression factor is `0.0`.
///
/// - Returns: [`RateLimitDecision::Allowed`]
/// - Behavior: All requests admitted
///
/// ## 2. At Capacity (Probabilistic Suppression)
///
/// When the observed usage is at/above capacity but below the hard cutoff, the limiter returns
/// [`RateLimitDecision::Suppressed`]. Callers must check `is_allowed`.
///
/// - Returns: [`RateLimitDecision::Suppressed { is_allowed, suppression_factor }`]
/// - Behavior: Probabilistically deny requests to maintain target rate
///
/// ## 3. Over Hard Limit (Full Suppression)
///
/// When the observed usage reaches the hard cutoff, the suppression factor becomes `1.0`.
///
/// - Returns: [`RateLimitDecision::Suppressed { is_allowed: false, suppression_factor: 1.0 }`]
/// - Behavior: All requests denied
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
/// - Uses [`DashMap`](dashmap::DashMap) and atomics; safe for concurrent use
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
/// use trypema::{HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision, RateLimiter, RateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds};
/// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
/// # use trypema::hybrid::SyncIntervalMs;
/// use trypema::local::LocalRateLimiterOptions;
/// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
/// # use trypema::redis::RedisRateLimiterOptions;
/// #
/// # #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
/// # fn options() -> RateLimiterOptions {
/// #     let window_size_seconds = WindowSizeSeconds::try_from(60).unwrap();
/// #     let rate_group_size_ms = RateGroupSizeMs::try_from(10).unwrap();
/// #     let hard_limit_factor = HardLimitFactor::try_from(1.5).unwrap();
/// #     let suppression_factor_cache_ms = SuppressionFactorCacheMs::default();
/// #     let sync_interval_ms = SyncIntervalMs::default();
/// #
/// #     RateLimiterOptions {
/// #         local: LocalRateLimiterOptions {
/// #             window_size_seconds,
/// #             rate_group_size_ms,
/// #             hard_limit_factor,
/// #             suppression_factor_cache_ms,
/// #         },
/// #         redis: RedisRateLimiterOptions {
/// #             connection_manager: todo!(),
/// #             prefix: None,
/// #             window_size_seconds,
/// #             rate_group_size_ms,
/// #             hard_limit_factor,
/// #             suppression_factor_cache_ms,
/// #             sync_interval_ms,
/// #         },
/// #     }
/// # }
/// #
/// # #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
/// # fn options() -> RateLimiterOptions {
/// #     let window_size_seconds = WindowSizeSeconds::try_from(60).unwrap();
/// #     let rate_group_size_ms = RateGroupSizeMs::try_from(10).unwrap();
/// #     let hard_limit_factor = HardLimitFactor::try_from(1.5).unwrap();
/// #     let suppression_factor_cache_ms = SuppressionFactorCacheMs::default();
/// #
/// #     RateLimiterOptions {
/// #         local: LocalRateLimiterOptions {
/// #             window_size_seconds,
/// #             rate_group_size_ms,
/// #             hard_limit_factor,
/// #             suppression_factor_cache_ms,
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
///     RateLimitDecision::Rejected { .. } => unreachable!("local suppressed limiter never rejects"),
/// }
/// ```
#[derive(Debug)]
pub struct SuppressedLocalRateLimiter {
    window_size_seconds: WindowSizeSeconds,
    window_size_ms: u128,
    window_duration: Duration,
    rate_group_size_ms: RateGroupSizeMs,
    series: DashMap<String, RateLimitSeries>,
    hard_limit_factor: HardLimitFactor,
    suppression_factor_cache_ms: SuppressionFactorCacheMs,
    suppression_factors: DashMap<String, (Instant, f64)>,
}

impl SuppressedLocalRateLimiter {
    pub(crate) fn new(options: LocalRateLimiterOptions) -> Self {
        Self {
            hard_limit_factor: options.hard_limit_factor,
            window_size_ms: (*options.window_size_seconds as u128).saturating_mul(1000),
            window_size_seconds: options.window_size_seconds,
            window_duration: Duration::from_secs(*options.window_size_seconds),
            suppression_factor_cache_ms: options.suppression_factor_cache_ms,
            rate_group_size_ms: options.rate_group_size_ms,
            series: DashMap::default(),
            suppression_factors: DashMap::default(),
        }
    } // end constructor

    /// Check admission and record the increment for `key` using probabilistic suppression.
    ///
    /// This is the primary method for the suppressed strategy. It always records the increment
    /// (both the total count and, if denied, the declined count), then returns an admission
    /// decision.
    ///
    /// # Arguments
    ///
    /// - `key`: Unique identifier for the rate-limited resource (e.g., `"user_123"`)
    /// - `rate_limit`: Per-second rate limit. **Sticky:** stored on first call, ignored on subsequent calls
    /// - `count`: Amount to increment (typically `1` for single requests, or batch size)
    ///
    /// # Returns
    ///
    /// - [`RateLimitDecision::Allowed`]: Below capacity, no suppression active
    /// - [`RateLimitDecision::Suppressed`]: At or above capacity. **Check `is_allowed`** for admission.
    ///   When over the hard limit, returns `Suppressed { is_allowed: false, suppression_factor: 1.0 }`.
    ///
    /// This method never returns [`RateLimitDecision::Rejected`].
    ///
    /// # Behaviour
    ///
    /// 1. The increment is **always** recorded in the sliding window (both allowed and denied calls)
    /// 2. If the forecasted accepted usage is below the soft window capacity, return `Allowed`
    /// 3. If above the hard limit (`rate_limit × hard_limit_factor`), return
    ///    `Suppressed { is_allowed: false, suppression_factor: 1.0 }`
    /// 4. Otherwise, compute (or retrieve cached) suppression factor and probabilistically decide
    ///
    /// # Concurrency
    ///
    /// Same best-effort semantics as the absolute strategy. Under concurrent load, multiple
    /// threads may proceed simultaneously, causing temporary overshoot. This is by design.
    pub fn inc(&self, key: &str, rate_limit: &RateLimit, count: u64) -> RateLimitDecision {
        let mut rng = |p: f64| rand::random_bool(p);
        self.inc_with_rng(key, rate_limit, count, &mut rng)
    }

    #[inline(always)]
    pub(crate) fn inc_with_rng(
        &self,
        key: &str,
        rate_limit: &RateLimit,
        count: u64,
        random_bool: &mut impl FnMut(f64) -> bool,
    ) -> RateLimitDecision {
        let rate_limit_series = match self.series.get(key) {
            Some(rate_limit_series) => rate_limit_series,
            None => {
                self.series.entry(key.to_string()).or_insert_with(|| {
                    RateLimitSeries::new(
                        **rate_limit * *self.hard_limit_factor * *self.window_size_seconds as f64,
                    )
                });

                let Some(rate_limit_series) = self.series.get(key) else {
                    unreachable!("AbsoluteLocalRateLimiter::inc: key should be in map");
                };

                rate_limit_series
            }
        };

        // delete expired buckets if necessary
        let rate_limit_series = match rate_limit_series.series.front() {
            None => rate_limit_series,
            Some(instant_rate)
                if instant_rate.timestamp.elapsed().as_millis() > self.window_size_ms =>
            {
                drop(rate_limit_series);
                let mut rate_limit_series =
                    self.series.get_mut(key).expect("Key should be present");
                let now = Instant::now();

                let split = rate_limit_series
                    .series
                    .partition_point(|r| now.duration_since(r.timestamp) > self.window_duration);

                let (removed_count, removed_declined) = rate_limit_series
                    .series
                    .drain(..split)
                    .fold((0u64, 0u64), |(count, declined), r| {
                        (
                            count + r.count.load(Ordering::Acquire),
                            declined + r.declined.load(Ordering::Acquire),
                        )
                    });

                rate_limit_series
                    .total
                    .fetch_sub(removed_count, Ordering::AcqRel);
                rate_limit_series
                    .total_declined
                    .fetch_sub(removed_declined, Ordering::AcqRel);

                drop(rate_limit_series);

                self.series.get(key).expect("Key should be present")
            }
            _ => rate_limit_series,
        };

        let hard_window_limit = rate_limit_series.limit;
        let soft_window_limit = hard_window_limit / *self.hard_limit_factor;

        let total = rate_limit_series.total.fetch_add(count, Ordering::AcqRel) + count;
        let total_declined = rate_limit_series.total_declined.load(Ordering::Acquire);
        let forcasted_allowed = total.saturating_sub(total_declined).saturating_add(count);
        let suppression_factor;
        let should_allow;

        let soft = soft_window_limit as u64;

        if forcasted_allowed < soft {
            should_allow = true;
            suppression_factor = 0f64;
        } else if forcasted_allowed == soft {
            // Exactly at soft limit: full suppression only when soft == hard (no headroom).
            if soft_window_limit == hard_window_limit {
                suppression_factor = 1f64;
                should_allow = false;
                rate_limit_series
                    .total_declined
                    .fetch_add(count, Ordering::AcqRel);
            } else {
                should_allow = true;
                suppression_factor = 0f64;
            }
        } else {
            suppression_factor = if forcasted_allowed as f64 >= hard_window_limit {
                1f64
            } else {
                self.get_suppression_factor_without_bucket_expire(key)
            };

            should_allow = if suppression_factor == 0f64 {
                true
            } else if suppression_factor == 1f64 {
                false
            } else {
                random_bool(1f64 - suppression_factor)
            };

            if !should_allow {
                rate_limit_series
                    .total_declined
                    .fetch_add(count, Ordering::AcqRel);
            }
        }

        if let Some(last_entry) = rate_limit_series.series.back()
            && last_entry.timestamp.elapsed().as_millis() <= *self.rate_group_size_ms as u128
        {
            last_entry.count.fetch_add(count, Ordering::AcqRel);
            if !should_allow {
                last_entry.declined.fetch_add(count, Ordering::AcqRel);
            }
        } else {
            drop(rate_limit_series);

            let Some(mut rate_limit_series) = self.series.get_mut(key) else {
                unreachable!("SuppressedLocalRateLimiter::inc: key should be in map");
            };

            rate_limit_series.series.push_back(InstantRate {
                count: count.into(),
                declined: AtomicU64::new(if should_allow { 0 } else { count }),
                timestamp: Instant::now(),
            });
        }

        if total.saturating_sub(total_declined).saturating_add(count) <= soft_window_limit as u64 {
            RateLimitDecision::Allowed
        } else {
            RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: should_allow,
            }
        }
    }

    fn remove_expired_buckets(&self, key: &str) {
        let Some(rate_limit_series) = self.series.get(key) else {
            return;
        };

        let Some(instant_rate) = rate_limit_series.series.front() else {
            return;
        };

        if instant_rate.timestamp.elapsed().as_millis() <= self.window_size_ms {
            return;
        }

        drop(rate_limit_series);

        let Some(mut rate_limit_series) = self.series.get_mut(key) else {
            return;
        };

        let now = Instant::now();

        let split = rate_limit_series
            .series
            .partition_point(|r| now.duration_since(r.timestamp) > self.window_duration);

        let (removed_count, removed_declined) =
            rate_limit_series
                .series
                .drain(..split)
                .fold((0u64, 0u64), |(count, declined), r| {
                    (
                        count + r.count.load(Ordering::Acquire),
                        declined + r.declined.load(Ordering::Acquire),
                    )
                });

        rate_limit_series
            .total
            .fetch_sub(removed_count, Ordering::AcqRel);
        rate_limit_series
            .total_declined
            .fetch_sub(removed_declined, Ordering::AcqRel);
    } // end method remove_expired_buckets

    /// Get the current suppression factor for `key`.
    ///
    /// Returns a value in the range `[0.0, 1.0]`:
    /// - `0.0` — no suppression (below capacity or key not found)
    /// - `0.0 < sf < 1.0` — partial suppression (at capacity)
    /// - `1.0` — full suppression (over hard limit)
    ///
    /// This method is read-only and does not record any increment. It is useful for
    /// exporting metrics, building dashboards, or debugging why calls are being suppressed.
    ///
    /// **Caching:** Returns the cached value if it was computed within `suppression_factor_cache_ms`.
    /// Otherwise, evicts expired buckets and recomputes the factor from the current sliding window.
    pub fn get_suppression_factor(&self, key: &str) -> f64 {
        self.remove_expired_buckets(key);
        self.get_suppression_factor_without_bucket_expire(key)
    } // end method get_suppression_factor

    fn get_suppression_factor_without_bucket_expire(&self, key: &str) -> f64 {
        let suppression_factor = match self.suppression_factors.get(key) {
            None => self.calculate_suppression_factor(key).1,
            Some(val)
                if val.0.elapsed().as_millis() < *self.suppression_factor_cache_ms as u128 =>
            {
                val.1
            }
            Some(val) => {
                drop(val);
                self.calculate_suppression_factor(key).1
            }
        };

        if suppression_factor < 0f64 {
            panic!(
                "SuppressedLocalRateLimiter::get_suppression_factor: negative suppression factor"
            );
        }

        if suppression_factor > 1f64 {
            panic!("SuppressedLocalRateLimiter::get_suppression_factor: suppression factor > 1");
        }

        suppression_factor
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

    fn calculate_suppression_factor(&self, key: &str) -> (Instant, f64) {
        let Some(series) = self.series.get(key) else {
            return self.persist_suppression_factor(key, 0f64);
        };

        if series.series.is_empty() {
            return self.persist_suppression_factor(key, 0f64);
        }

        let hard_window_limit = series.limit as u64;
        let soft_window_limit = (hard_window_limit as f64 / *self.hard_limit_factor) as u64;

        let total = series.total.load(Ordering::Acquire);
        let total_declined = series.total_declined.load(Ordering::Acquire);

        if total >= hard_window_limit {
            return self.persist_suppression_factor(key, 1f64);
        }

        let accepted = total.saturating_sub(total_declined);

        if accepted < soft_window_limit {
            return self.persist_suppression_factor(key, 0f64);
        }

        if accepted == soft_window_limit && soft_window_limit == hard_window_limit {
            return self.persist_suppression_factor(key, 1f64);
        }

        let mut total_in_last_second = 0u64;

        for instant_rate in series.series.iter().rev() {
            if instant_rate.timestamp.elapsed().as_millis() > 1000 {
                break;
            }

            total_in_last_second =
                total_in_last_second.saturating_add(instant_rate.count.load(Ordering::Acquire));
        }

        let average_rate_in_window: f64 = total as f64 / *self.window_size_seconds as f64;

        let perceived_rate_limit = average_rate_in_window.max(total_in_last_second as f64);

        let suppression_factor = 1f64
            - (soft_window_limit as f64 / *self.window_size_seconds as f64 / perceived_rate_limit);

        self.persist_suppression_factor(key, suppression_factor)
    } // end method calculate_suppression_factor

    pub(crate) fn cleanup(&self, stale_after_ms: u64) {
        self.suppression_factors
            .retain(|_, (instant, _)| instant.elapsed().as_millis() < stale_after_ms as u128);
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
} // end impl
