use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use ahash::RandomState;
use dashmap::DashMap;

use crate::{
    LocalRateLimiterOptions, RateGroupSizeMs, RateLimitDecision,
    common::{
        HardLimitFactor, InstantRate, RateLimit, RateLimitSeries, SuppressionFactorCacheMs,
        WindowSizeSeconds,
    },
};

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
/// #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
/// #         },
/// #         redis: RedisRateLimiterOptions {
/// #             connection_manager: todo!(),
/// #             prefix: None,
/// #             window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
/// #             rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
/// #             hard_limit_factor: HardLimitFactor::try_from(1.5).unwrap(),
/// #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
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
/// #             suppression_factor_cache_ms: SuppressionFactorCacheMs::default(),
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
    rate_group_size_ms: RateGroupSizeMs,
    series: DashMap<String, RateLimitSeries, RandomState>,
    hard_limit_factor: HardLimitFactor,
    suppression_factor_cache_ms: SuppressionFactorCacheMs,
    suppression_factors: DashMap<String, (Instant, f64), RandomState>,
}

impl SuppressedLocalRateLimiter {
    pub(crate) fn new(options: LocalRateLimiterOptions) -> Self {
        Self {
            hard_limit_factor: options.hard_limit_factor,
            window_size_seconds: options.window_size_seconds,
            suppression_factor_cache_ms: options.suppression_factor_cache_ms,
            rate_group_size_ms: options.rate_group_size_ms,
            series: DashMap::default(),
            suppression_factors: DashMap::default(),
        }
    } // end constructor

    /// inc
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
        if !self.series.contains_key(key) {
            self.series
                .entry(key.to_string())
                .or_insert_with(|| RateLimitSeries::new(self.get_hard_limit(rate_limit)));
        }

        let suppression_factor = self.get_suppression_factor(key);

        let should_allow = if suppression_factor == 0f64 {
            true
        } else if suppression_factor == 1f64 {
            false
        } else {
            random_bool(1f64 - suppression_factor)
        };

        let Some(rate_limit_series) = self.series.get(key) else {
            unreachable!("SuppressedLocalRateLimiter::inc: key should be in map");
        };

        if let Some(last_entry) = rate_limit_series.series.back()
            && last_entry.timestamp.elapsed().as_millis() <= *self.rate_group_size_ms as u128
        {
            last_entry.count.fetch_add(count, Ordering::Relaxed);
            rate_limit_series.total.fetch_add(count, Ordering::Relaxed);
            if !should_allow {
                last_entry.declined.fetch_add(count, Ordering::Relaxed);
                rate_limit_series
                    .total_declined
                    .fetch_add(count, Ordering::Relaxed);
            }
        } else {
            drop(rate_limit_series);

            let mut rate_limit_series = self
                .series
                .entry(key.to_string())
                .or_insert_with(|| RateLimitSeries::new(self.get_hard_limit(rate_limit)));

            rate_limit_series.series.push_back(InstantRate {
                count: count.into(),
                declined: AtomicU64::new(if should_allow { 0 } else { count }),
                timestamp: Instant::now(),
            });

            rate_limit_series.total.fetch_add(count, Ordering::Relaxed);

            if !should_allow {
                rate_limit_series
                    .total_declined
                    .fetch_add(count, Ordering::Relaxed);
            }
        }

        match suppression_factor {
            0f64 => RateLimitDecision::Allowed,
            _ => RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: should_allow,
            },
        }
    }

    fn remove_expired_buckets(&self, key: &str) {
        let Some(rate_limit_series) = self.series.get(key) else {
            return;
        };

        let Some(instant_rate) = rate_limit_series.series.front() else {
            return;
        };

        if instant_rate.timestamp.elapsed().as_millis() <= *self.window_size_seconds as u128 {
            return;
        }

        drop(rate_limit_series);

        let Some(mut rate_limit_series) = self.series.get_mut(key) else {
            return;
        };

        while let Some(instant_rate) = rate_limit_series.series.front()
            && instant_rate.timestamp.elapsed().as_millis()
                > (*self.window_size_seconds * 1000) as u128
        {
            rate_limit_series.total.fetch_sub(
                instant_rate.count.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            rate_limit_series.total_declined.fetch_sub(
                instant_rate.declined.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );

            rate_limit_series.series.pop_front();
        }
    } // end method remove_expired_buckets

    /// Get the current suppression factor for `key`.
    ///
    /// This is useful for exporting metrics or debugging why calls are being suppressed.
    ///
    /// Returns the cached value if it is still fresh, otherwise recomputes and refreshes
    /// the cache.
    pub fn get_suppression_factor(&self, key: &str) -> f64 {
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
        self.remove_expired_buckets(key);

        let Some(series) = self.series.get(key) else {
            return self.persist_suppression_factor(key, 0f64);
        };

        if series.series.is_empty() {
            return self.persist_suppression_factor(key, 0f64);
        }

        let hard_limit = series.limit;
        let rate_limit = self.get_rate_limit_from_hard_limit(&hard_limit);

        let window_limit = *self.window_size_seconds as f64 * *rate_limit;
        let hard_window_limit = *self.window_size_seconds as f64 * *hard_limit;

        if series.total.load(Ordering::Relaxed) >= hard_window_limit as u64 {
            return self.persist_suppression_factor(key, 1f64);
        }

        if series.total.load(Ordering::Relaxed) < window_limit as u64 {
            return self.persist_suppression_factor(key, 0f64);
        }

        let mut total_in_last_second = 0u64;

        for instant_rate in series.series.iter().rev() {
            if instant_rate.timestamp.elapsed().as_millis() > 1000 {
                break;
            }

            total_in_last_second =
                total_in_last_second.saturating_add(instant_rate.count.load(Ordering::Relaxed));
        }

        let average_rate_in_window: f64 =
            series.total.load(Ordering::Relaxed) as f64 / *self.window_size_seconds as f64;

        let perceived_rate_limit = average_rate_in_window.max(total_in_last_second as f64);

        let suppression_factor = 1f64 - (*rate_limit / perceived_rate_limit);

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

    #[inline]
    fn get_rate_limit_from_hard_limit(&self, hard_limit: &RateLimit) -> RateLimit {
        let Ok(val) = RateLimit::try_from(**hard_limit / *self.hard_limit_factor) else {
            unreachable!(
                "SuppressedLocalRateLimiter::get_rate_limit_from_hard_limit: hard_limit_factor is always > 0"
            );
        };

        val
    }

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
