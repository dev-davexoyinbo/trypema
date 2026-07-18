use std::{
    collections::VecDeque,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use dashmap::{DashMap, mapref::entry::Entry};

use crate::{
    HistoryPreservation, LocalRateLimiterOptions, RateGroupSizeMs, RateLimitComparator,
    RateLimitDecision, SuppressedRateLimitSnapshot,
    common::{
        Bucket, HardLimitFactor, HistoryUpdateMode, RandomState, RateLimit,
        SuppressionFactorCacheMs, WindowSizeSeconds,
    },
};

#[derive(Debug)]
pub(crate) struct RateLimitSeries {
    pub hard_window_limit: f64,
    pub buckets: VecDeque<Bucket>,
    pub total_count: AtomicU64,
    pub total_declined_count: AtomicU64,
}

impl RateLimitSeries {
    pub fn new(hard_window_limit: f64) -> Self {
        Self {
            hard_window_limit,
            buckets: VecDeque::new(),
            total_count: AtomicU64::new(0),
            total_declined_count: AtomicU64::new(0),
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
/// ## 2. Above Capacity (Probabilistic Suppression)
///
/// When the forecasted accepted usage is above capacity but below the hard cutoff, the limiter
/// returns [`RateLimitDecision::Suppressed`]. Callers must check `is_allowed`. The increment that
/// reaches capacity exactly is still [`RateLimitDecision::Allowed`].
///
/// - Returns: [`RateLimitDecision::Suppressed { is_allowed, suppression_factor }`]
/// - Behavior: Probabilistically deny requests to maintain target rate
///
/// ## 3. After the Hard Limit (Full Suppression)
///
/// The increment that reaches the hard cutoff exactly is admitted and caches a suppression factor
/// of `1.0`. Subsequent increments are fully suppressed while that factor remains applicable.
///
/// - Returns: [`RateLimitDecision::Suppressed { is_allowed: false, suppression_factor: 1.0 }`]
/// - Behavior: All requests denied
///
/// # Hard Limit
///
/// `hard_window_limit = rate_limit × window_size_seconds × hard_limit_factor`
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
/// ```
/// # let rl = trypema::__doctest_helpers::rate_limiter();
/// use trypema::{RateLimit, RateLimitDecision};
///
/// let limiter = rl.local().suppressed();
/// let rate = RateLimit::try_from(10.0).unwrap();
///
/// match limiter.inc("user_123", &rate, 1) {
///     RateLimitDecision::Allowed => {}
///     RateLimitDecision::Suppressed { is_allowed, suppression_factor } => {
///         println!("suppression: {suppression_factor:.2}, allowed: {is_allowed}");
///     }
///     RateLimitDecision::Rejected { .. } => unreachable!(),
/// }
/// ```
#[derive(Debug)]
pub struct SuppressedLocalRateLimiter {
    window_size_seconds: WindowSizeSeconds,
    window_size_ms: u128,
    window_duration: Duration,
    rate_group_size_ms: RateGroupSizeMs,
    pub(crate) series: DashMap<String, RateLimitSeries, RandomState>,
    hard_limit_factor: HardLimitFactor,
    suppression_factor_cache_ms: SuppressionFactorCacheMs,
    pub(crate) suppression_factors: DashMap<String, (Instant, f64), RandomState>,
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
    /// - [`RateLimitDecision::Allowed`]: The increment remains within soft capacity or reaches the
    ///   hard limit exactly
    /// - [`RateLimitDecision::Suppressed`]: The forecasted accepted total is above soft capacity
    ///   without landing exactly on the hard limit. **Check `is_allowed`** for admission. When over
    ///   the hard limit, returns `Suppressed { is_allowed: false, suppression_factor: 1.0 }`.
    ///
    /// This method never returns [`RateLimitDecision::Rejected`].
    ///
    /// # Behaviour
    ///
    /// 1. The increment is **always** recorded in the sliding window (both allowed and denied calls)
    /// 2. If the forecasted accepted usage is at or below the soft window capacity, return `Allowed`
    /// 3. If the increment reaches the hard limit exactly, admit it and cache full suppression for
    ///    subsequent calls
    /// 4. If above the hard limit (`window_size_seconds × rate_limit × hard_limit_factor`), return
    ///    `Suppressed { is_allowed: false, suppression_factor: 1.0 }`
    /// 5. Otherwise, compute (or retrieve cached) suppression factor and probabilistically decide
    ///
    /// # Concurrency
    ///
    /// Same best-effort semantics as the absolute strategy. Under concurrent load, multiple
    /// threads may proceed simultaneously, causing temporary overshoot. This is by design.
    ///
    /// # Examples
    ///
    /// ```
    /// # let rl = trypema::__doctest_helpers::rate_limiter();
    /// use trypema::{RateLimit, RateLimitDecision};
    ///
    /// let limiter = rl.local().suppressed();
    /// let rate = RateLimit::try_from(10.0).unwrap();
    ///
    /// // Under limit → Allowed
    /// assert!(matches!(limiter.inc("user_123", &rate, 1), RateLimitDecision::Allowed));
    /// ```
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
        let series = match self.series.get(key) {
            Some(series) => series,
            None => self
                .series
                .entry(key.to_string())
                .or_insert_with(|| {
                    RateLimitSeries::new(
                        **rate_limit * *self.hard_limit_factor * *self.window_size_seconds as f64,
                    )
                })
                .downgrade(),
        };

        // delete expired buckets if necessary
        let series = match series.buckets.front() {
            Some(oldest_bucket)
                if oldest_bucket.timestamp.elapsed().as_millis() > self.window_size_ms =>
            {
                drop(series);

                let mut series = self.series.entry(key.to_string()).or_insert_with(|| {
                    RateLimitSeries::new(
                        **rate_limit * *self.hard_limit_factor * *self.window_size_seconds as f64,
                    )
                });

                let (_, evicted) = Self::evict_expired(&mut series, self.window_duration);

                if evicted {
                    self.suppression_factors.remove(key);
                }

                series.downgrade()
            }
            _ => series,
        };

        let soft_window_limit = (series.hard_window_limit / *self.hard_limit_factor) as u64;
        let hard_window_limit = series.hard_window_limit as u64;
        let total_count = series.total_count.load(Ordering::Acquire);
        let total_declined_count = series.total_declined_count.load(Ordering::Acquire);
        let forecasted_allowed_count = total_count
            .saturating_sub(total_declined_count)
            .saturating_add(count);

        let reached_hard_window_limit = forecasted_allowed_count == hard_window_limit;
        let should_return_allowed =
            forecasted_allowed_count <= soft_window_limit || reached_hard_window_limit;

        let suppression_factor;
        let should_allow;

        if should_return_allowed {
            should_allow = true;

            if reached_hard_window_limit {
                // Admit the increment that reaches the hard limit, then cache full
                // suppression so the next increment is declined.
                suppression_factor = self.persist_suppression_factor(key, 1f64).1;
            } else {
                suppression_factor = 0f64;
            }
        } else {
            suppression_factor = if forecasted_allowed_count > hard_window_limit {
                self.persist_suppression_factor(key, 1f64).1
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
                series
                    .total_declined_count
                    .fetch_add(count, Ordering::AcqRel);
            }
        }

        series.total_count.fetch_add(count, Ordering::AcqRel);

        if let Some(latest_bucket) = series.buckets.back()
            && latest_bucket.timestamp.elapsed().as_millis() <= *self.rate_group_size_ms as u128
        {
            latest_bucket.count.fetch_add(count, Ordering::AcqRel);

            if !should_allow {
                latest_bucket
                    .declined_count
                    .fetch_add(count, Ordering::AcqRel);
            }
        } else {
            let hard_window_limit = series.hard_window_limit;
            drop(series);

            let bucket = Bucket {
                count: count.into(),
                declined_count: AtomicU64::new(if should_allow { 0 } else { count }),
                timestamp: Instant::now(),
            };

            match self.series.entry(key.to_string()) {
                Entry::Occupied(mut entry) => entry.get_mut().buckets.push_back(bucket),
                Entry::Vacant(entry) => {
                    let mut series = RateLimitSeries::new(hard_window_limit);

                    series.total_count.store(count, Ordering::Release);

                    if !should_allow {
                        series.total_declined_count.store(count, Ordering::Release);
                    }

                    series.buckets.push_back(bucket);
                    entry.insert(series);
                }
            }
        }

        if should_return_allowed {
            RateLimitDecision::Allowed
        } else {
            RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: should_allow,
            }
        }
    }

    fn remove_expired_buckets(&self, key: &str) {
        let Some(series) = self.series.get(key) else {
            return;
        };

        let Some(oldest_bucket) = series.buckets.front() else {
            return;
        };

        if oldest_bucket.timestamp.elapsed().as_millis() <= self.window_size_ms {
            return;
        }

        drop(series);

        let Some(mut series) = self.series.get_mut(key) else {
            return;
        };

        let (_, evicted) = Self::evict_expired(&mut series, self.window_duration);
        drop(series);

        if evicted {
            self.suppression_factors.remove(key);
        }
    } // end method remove_expired_buckets

    /// Get the current suppression factor for `key`.
    ///
    /// Returns a value in the range `[0.0, 1.0]`:
    /// - `0.0` — no suppression (below capacity or key not found)
    /// - `0.0 < sf < 1.0` — partial suppression (at capacity)
    /// - `1.0` — full suppression (cached at the hard boundary or on a forecast above it)
    ///
    /// This method is read-only and does not record any increment. It is useful for
    /// exporting metrics, building dashboards, or debugging why calls are being suppressed.
    ///
    /// **Caching:** Returns the cached value if it was computed within `suppression_factor_cache_ms`.
    /// Otherwise, evicts expired buckets and recomputes the factor from the current sliding window.
    ///
    /// # Examples
    ///
    /// ```
    /// # let rl = trypema::__doctest_helpers::rate_limiter();
    /// let limiter = rl.local().suppressed();
    ///
    /// // No state yet → 0.0 (no suppression)
    /// assert_eq!(limiter.get_suppression_factor("unknown_key"), 0.0);
    /// ```
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

        if series.buckets.is_empty() {
            return self.persist_suppression_factor(key, 0f64);
        }

        let soft_window_limit = (series.hard_window_limit / *self.hard_limit_factor) as u64;

        let total_count = series.total_count.load(Ordering::Acquire);
        let total_declined_count = series.total_declined_count.load(Ordering::Acquire);

        if total_count >= series.hard_window_limit as u64 {
            return self.persist_suppression_factor(key, 1f64);
        }

        let accepted_count = total_count.saturating_sub(total_declined_count);

        if accepted_count < soft_window_limit {
            return self.persist_suppression_factor(key, 0f64);
        }

        if accepted_count == soft_window_limit
            && soft_window_limit == series.hard_window_limit as u64
        {
            return self.persist_suppression_factor(key, 1f64);
        }

        let mut total_in_last_second = 0u64;

        for bucket in series.buckets.iter().rev() {
            if bucket.timestamp.elapsed().as_millis() > 1000 {
                break;
            }

            total_in_last_second =
                total_in_last_second.saturating_add(bucket.count.load(Ordering::Acquire));
        }

        let average_rate_in_window: f64 = total_count as f64 / *self.window_size_seconds as f64;

        let perceived_rate_limit = average_rate_in_window.max(total_in_last_second as f64);

        let suppression_factor = 1f64
            - (soft_window_limit as f64 / *self.window_size_seconds as f64 / perceived_rate_limit);

        self.persist_suppression_factor(key, suppression_factor)
    } // end method calculate_suppression_factor

    /// Current live window state for `key`.
    ///
    /// Evicts expired buckets first, then returns the observed total, declined total,
    /// and current suppression factor. The observed total includes accepted and declined
    /// calls, matching the counter that suppression decisions are based on. Unknown keys
    /// return [`SuppressedRateLimitSnapshot::default()`].
    ///
    /// # Examples
    ///
    /// ```
    /// # let rl = trypema::__doctest_helpers::rate_limiter();
    /// use trypema::RateLimit;
    ///
    /// let limiter = rl.local().suppressed();
    /// let snapshot = limiter.get("user_123");
    /// assert_eq!(snapshot.total, 0);
    /// assert_eq!(snapshot.total_declined, 0);
    /// assert_eq!(snapshot.suppression_factor, 0.0);
    ///
    /// let rate = RateLimit::try_from(10.0).unwrap();
    /// limiter.inc("user_123", &rate, 3);
    /// assert_eq!(limiter.get("user_123").total, 3);
    /// ```
    pub fn get(&self, key: &str) -> SuppressedRateLimitSnapshot {
        let Some(series) = self.series.get(key) else {
            return SuppressedRateLimitSnapshot::default();
        };

        let (total_count, contained_expired) =
            Self::inspect_live_total(&series, self.window_duration);

        if !contained_expired {
            let total_declined_count = series.total_declined_count.load(Ordering::Acquire);
            drop(series);

            return SuppressedRateLimitSnapshot {
                total: total_count,
                total_declined: total_declined_count,
                suppression_factor: self.get_suppression_factor_without_bucket_expire(key),
            };
        }

        drop(series);

        let Some(mut series) = self.series.get_mut(key) else {
            return SuppressedRateLimitSnapshot::default();
        };

        let (total_count, evicted) = Self::evict_expired(&mut series, self.window_duration);
        let total_declined_count = series.total_declined_count.load(Ordering::Acquire);

        drop(series);

        if evicted {
            self.suppression_factors.remove(key);
        }

        SuppressedRateLimitSnapshot {
            total: total_count,
            total_declined: total_declined_count,
            suppression_factor: self.get_suppression_factor_without_bucket_expire(key),
        }
    } // end method get

    fn evict_expired(series: &mut RateLimitSeries, window_duration: Duration) -> (u64, bool) {
        let now = Instant::now();
        let split = series
            .buckets
            .partition_point(|bucket| now.duration_since(bucket.timestamp) > window_duration);

        if split == 0 {
            return (series.total_count.load(Ordering::Acquire), false);
        }

        let (removed_count, removed_declined_count) =
            series
                .buckets
                .drain(..split)
                .fold((0u64, 0u64), |(count, declined_count), bucket| {
                    (
                        count + bucket.count.load(Ordering::Acquire),
                        declined_count + bucket.declined_count.load(Ordering::Acquire),
                    )
                });

        let total_count = series
            .total_count
            .fetch_sub(removed_count, Ordering::AcqRel)
            - removed_count;
        series
            .total_declined_count
            .fetch_sub(removed_declined_count, Ordering::AcqRel);

        (total_count, true)
    }

    fn inspect_live_total(series: &RateLimitSeries, window_duration: Duration) -> (u64, bool) {
        let now = Instant::now();
        let split = series
            .buckets
            .partition_point(|bucket| now.duration_since(bucket.timestamp) > window_duration);

        if split == 0 {
            return (series.total_count.load(Ordering::Acquire), split > 0);
        }

        let total_count = series
            .buckets
            .iter()
            .skip(split)
            .map(|bucket| bucket.count.load(Ordering::Acquire))
            .sum();

        (total_count, split > 0)
    }

    fn apply_history_update(
        series: &mut RateLimitSeries,
        count: u64,
        old_total: u64,
        mode: HistoryUpdateMode,
    ) {
        match mode {
            HistoryUpdateMode::Replace => {
                series.buckets.clear();
                series.buckets.push_back(Bucket {
                    count: count.into(),
                    declined_count: AtomicU64::new(0),
                    timestamp: Instant::now(),
                });
            }
            HistoryUpdateMode::Preserve(preservation) if count > old_total => {
                let delta = count - old_total;
                let bucket = match preservation {
                    HistoryPreservation::PreserveNewest => series.buckets.back(),
                    HistoryPreservation::PreserveOldest => series.buckets.front(),
                };

                if let Some(bucket) = bucket {
                    bucket.count.fetch_add(delta, Ordering::AcqRel);
                } else {
                    series.buckets.push_back(Bucket {
                        count: delta.into(),
                        declined_count: AtomicU64::new(0),
                        timestamp: Instant::now(),
                    });
                }
            }
            HistoryUpdateMode::Preserve(preservation) if count < old_total => {
                let mut to_remove = old_total - count;

                while to_remove > 0 {
                    let bucket = match preservation {
                        HistoryPreservation::PreserveNewest => series.buckets.front(),
                        HistoryPreservation::PreserveOldest => series.buckets.back(),
                    };

                    let Some(bucket) = bucket else {
                        break;
                    };

                    let bucket_count = bucket.count.load(Ordering::Acquire);

                    if bucket_count <= to_remove {
                        to_remove -= bucket_count;

                        match preservation {
                            HistoryPreservation::PreserveNewest => {
                                series.buckets.pop_front();
                            }

                            HistoryPreservation::PreserveOldest => {
                                series.buckets.pop_back();
                            }
                        }
                    } else {
                        let declined_count = bucket.declined_count.load(Ordering::Acquire);
                        let retained_count = bucket_count - to_remove;
                        let retained_declined_count =
                            ((declined_count as u128 * retained_count as u128)
                                / bucket_count as u128) as u64;

                        bucket.count.store(retained_count, Ordering::Release);
                        bucket
                            .declined_count
                            .store(retained_declined_count, Ordering::Release);

                        to_remove = 0;
                    }
                }
            }
            HistoryUpdateMode::Preserve(_) => {}
        }

        let total_declined_count = series
            .buckets
            .iter()
            .map(|bucket| bucket.declined_count.load(Ordering::Acquire))
            .sum();
        series.total_count.store(count, Ordering::Release);
        series
            .total_declined_count
            .store(total_declined_count, Ordering::Release);
    }

    fn set_if_with_history_mode(
        &self,
        key: &str,
        rate_limit: &RateLimit,
        comparator: RateLimitComparator,
        count: u64,
        mode: HistoryUpdateMode,
    ) -> (u64, u64) {
        let existing = match self.series.get(key) {
            Some(series) => {
                let (old_total, contains_expired) =
                    Self::inspect_live_total(&series, self.window_duration);

                if !comparator.matches(old_total) {
                    return (old_total, old_total);
                }

                let hard_window_limit =
                    **rate_limit * *self.hard_limit_factor * *self.window_size_seconds as f64;

                let unchanged = count > 0
                    && matches!(mode, HistoryUpdateMode::Preserve(_))
                    && !contains_expired
                    && old_total == count
                    && series.hard_window_limit == hard_window_limit;

                if unchanged {
                    return (old_total, old_total);
                }

                true
            }
            None => false,
        };

        if !existing && (!comparator.matches(0) || count == 0) {
            return (0, 0);
        }

        let (new_total, old_total) = match self.series.entry(key.to_string()) {
            Entry::Occupied(mut entry) => {
                let (old_total, contains_expired) =
                    Self::inspect_live_total(entry.get(), self.window_duration);

                if !comparator.matches(old_total) {
                    return (old_total, old_total);
                }

                if count == 0 {
                    entry.remove();
                    (0, old_total)
                } else {
                    let series = entry.get_mut();

                    if contains_expired {
                        Self::evict_expired(series, self.window_duration);
                    }

                    series.hard_window_limit =
                        **rate_limit * *self.hard_limit_factor * *self.window_size_seconds as f64;

                    Self::apply_history_update(series, count, old_total, mode);

                    (count, old_total)
                }
            }
            Entry::Vacant(entry) => {
                if !comparator.matches(0) || count == 0 {
                    return (0, 0);
                }

                let mut series = RateLimitSeries::new(
                    **rate_limit * *self.hard_limit_factor * *self.window_size_seconds as f64,
                );

                Self::apply_history_update(&mut series, count, 0, mode);
                entry.insert(series);

                (count, 0)
            }
        };

        self.suppression_factors.remove(key);

        (new_total, old_total)
    } // end fn set_if_with_history_mode

    /// Conditionally replace the window total for `key`.
    ///
    /// When `comparator` matches the key's current post-eviction window total, the
    /// window contents are replaced by a single current-timestamp bucket holding
    /// `count` with **no declines** recorded against it; otherwise nothing is
    /// written. On a match, the key's stored hard window limit is redefined from
    /// `rate_limit` (`window_size_seconds × rate_limit × hard_limit_factor`) — unlike
    /// `inc`, where the limit is sticky after the first call — and the cached
    /// suppression factor is dropped so it is recomputed from the new state on the
    /// next call. A comparator miss leaves both the limit and cache untouched. A
    /// matched `count` of zero removes both the series and cached factor completely.
    ///
    /// # Arguments
    ///
    /// - `key`: Unique identifier for the rate-limited resource
    /// - `rate_limit`: Per-second rate limit; redefines the stored hard limit
    /// - `comparator`: Guard evaluated against the current window total
    /// - `count`: The total to write when the guard matches
    ///
    /// # Returns
    ///
    /// `(new_total, old_total)` — `old_total` is the post-eviction total the
    /// comparator was evaluated against; `new_total` is `count` when the guard
    /// matched, `old_total` otherwise.
    ///
    /// # Priming Idiom
    ///
    /// `set_if(key, rate, RateLimitComparator::Lt(count), count)` raises the window
    /// total to at least `count` and never lowers it — idempotent and safe to retry.
    ///
    /// # Examples
    ///
    /// ```
    /// # let rl = trypema::__doctest_helpers::rate_limiter();
    /// use trypema::{RateLimit, RateLimitComparator};
    ///
    /// let limiter = rl.local().suppressed();
    /// let rate = RateLimit::try_from(10.0).unwrap();
    ///
    /// // Prime the window to 40.
    /// let (new_total, old_total) =
    ///     limiter.set_if("user_123", &rate, RateLimitComparator::Lt(40), 40);
    /// assert_eq!((new_total, old_total), (40, 0));
    ///
    /// // Re-priming is a no-op: the guard no longer matches.
    /// let (new_total, old_total) =
    ///     limiter.set_if("user_123", &rate, RateLimitComparator::Lt(40), 40);
    /// assert_eq!((new_total, old_total), (40, 40));
    /// ```
    pub fn set_if(
        &self,
        key: &str,
        rate_limit: &RateLimit,
        comparator: RateLimitComparator,
        count: u64,
    ) -> (u64, u64) {
        self.set_if_with_history_mode(
            key,
            rate_limit,
            comparator,
            count,
            HistoryUpdateMode::Replace,
        )
    } // end method set_if

    /// Conditionally set the observed total while retaining the selected side of
    /// the existing sliding-window history.
    pub fn set_if_preserve_history(
        &self,
        key: &str,
        rate_limit: &RateLimit,
        comparator: RateLimitComparator,
        count: u64,
        preservation: HistoryPreservation,
    ) -> (u64, u64) {
        self.set_if_with_history_mode(
            key,
            rate_limit,
            comparator,
            count,
            HistoryUpdateMode::Preserve(preservation),
        )
    } // end method set_if_preserve_history

    pub(crate) fn cleanup(&self, stale_after_ms: u64) {
        self.suppression_factors
            .retain(|_, (instant, _)| instant.elapsed().as_millis() < stale_after_ms as u128);
        self.series.retain(|_, series| match series.buckets.back() {
            None => false,
            Some(latest_bucket)
                if latest_bucket.timestamp.elapsed().as_millis() > stale_after_ms as u128 =>
            {
                false
            }
            Some(_) => true,
        });
    } // end method cleanup
} // end impl
