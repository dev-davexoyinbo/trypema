use std::{
    collections::VecDeque,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use ahash::RandomState;
use dashmap::DashMap;

use crate::{
    HistoryPreservation, LocalRateLimiterOptions, RateLimitComparator,
    common::{
        HistoryUpdateMode, InstantRate, RateGroupSizeMs, RateLimit, RateLimitDecision,
        WindowSizeSeconds,
    },
};

#[derive(Debug)]
pub(crate) struct RateLimitSeries {
    pub limit: RateLimit,
    pub series: VecDeque<InstantRate>,
    pub total: AtomicU64,
}

impl RateLimitSeries {
    pub fn new(limit: RateLimit) -> Self {
        Self {
            limit,
            series: VecDeque::new(),
            total: AtomicU64::new(0),
        }
    }
}

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
/// **Eviction granularity:**
/// - Uses `Instant::elapsed().as_millis()` (whole-millisecond truncation)
/// - Buckets expire close to `window_size_seconds` (lazy eviction may delay removal until next call)
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
/// - **Admission check:** O(buckets_in_window) — typically < 10 buckets
/// - **Increment:** O(1) amortised (coalesced into existing bucket or appended via `push_back`)
/// - **Memory:** ~50–200 bytes per key (depends on bucket count)
///
/// # Examples
///
/// ```
/// # let rl = trypema::__doctest_helpers::rate_limiter();
/// use trypema::{RateLimit, RateLimitDecision};
///
/// let limiter = rl.local().absolute();
/// let rate = RateLimit::try_from(10.0).unwrap();
///
/// assert!(matches!(limiter.inc("user_123", &rate, 1), RateLimitDecision::Allowed));
/// assert!(matches!(limiter.is_allowed("user_123"), RateLimitDecision::Allowed));
/// ```
#[derive(Debug)]
pub struct AbsoluteLocalRateLimiter {
    window_size_seconds: WindowSizeSeconds,
    window_size_ms: u128,
    window_duration: Duration,
    rate_group_size_ms: RateGroupSizeMs,
    series: DashMap<String, RateLimitSeries, RandomState>,
}

impl AbsoluteLocalRateLimiter {
    pub(crate) fn new(options: LocalRateLimiterOptions) -> Self {
        Self {
            window_size_ms: (*options.window_size_seconds as u128).saturating_mul(1000),
            window_duration: Duration::from_secs(*options.window_size_seconds),
            window_size_seconds: options.window_size_seconds,
            rate_group_size_ms: options.rate_group_size_ms,
            series: DashMap::default(),
        }
    } // end constructor

    #[cfg(test)]
    pub(crate) fn series(&self) -> &DashMap<String, RateLimitSeries, RandomState> {
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
    /// ```
    /// # let rl = trypema::__doctest_helpers::rate_limiter();
    /// use trypema::{RateLimit, RateLimitDecision};
    ///
    /// let limiter = rl.local().absolute();
    /// let rate = RateLimit::try_from(10.0).unwrap();
    ///
    /// // Single request
    /// assert!(matches!(limiter.inc("user_123", &rate, 1), RateLimitDecision::Allowed));
    ///
    /// // Batch of 10
    /// assert!(matches!(limiter.inc("user_456", &rate, 10), RateLimitDecision::Allowed));
    /// ```
    pub fn inc(&self, key: &str, rate_limit: &RateLimit, count: u64) -> RateLimitDecision {
        let is_allowed = self.is_allowed(key);

        if !matches!(is_allowed, RateLimitDecision::Allowed) {
            return is_allowed;
        }

        let series = match self.series.get(key) {
            Some(series) => series,
            None => self
                .series
                .entry(key.to_string())
                .or_insert_with(|| RateLimitSeries::new(*rate_limit))
                .downgrade(),
        };

        if let Some(last_entry) = series.series.back()
            && last_entry.timestamp.elapsed().as_millis() <= *self.rate_group_size_ms as u128
        {
            last_entry.count.fetch_add(count, Ordering::Relaxed);
            series.total.fetch_add(count, Ordering::Relaxed);
        } else {
            drop(series);

            let mut rate_limit_series = self
                .series
                .entry(key.to_string())
                .or_insert_with(|| RateLimitSeries::new(*rate_limit));

            rate_limit_series.series.push_back(InstantRate {
                count: count.into(),
                timestamp: Instant::now(),
                declined: AtomicU64::new(0),
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
    /// ```
    /// # let rl = trypema::__doctest_helpers::rate_limiter();
    /// use trypema::{RateLimit, RateLimitDecision};
    ///
    /// let limiter = rl.local().absolute();
    /// let rate = RateLimit::try_from(10.0).unwrap();
    ///
    /// // Unknown key → always allowed
    /// assert!(matches!(limiter.is_allowed("new_key"), RateLimitDecision::Allowed));
    ///
    /// // Check before recording
    /// if matches!(limiter.is_allowed("user_123"), RateLimitDecision::Allowed) {
    ///     limiter.inc("user_123", &rate, 1);
    /// }
    /// ```
    pub fn is_allowed(&self, key: &str) -> RateLimitDecision {
        let Some(series) = self.series.get(key) else {
            return RateLimitDecision::Allowed;
        };

        let total_count = series.total.load(Ordering::Relaxed);
        let window_limit = (*self.window_size_seconds as f64 * *series.limit) as u64;

        if total_count < window_limit {
            return RateLimitDecision::Allowed;
        }

        let (retry_after_ms, remaining_after_waiting) = match series.series.front() {
            None => (0, 0),
            Some(instant_rate)
                if instant_rate.timestamp.elapsed().as_millis() <= self.window_size_ms =>
            {
                let elapsed_ms = instant_rate.timestamp.elapsed().as_millis();
                (
                    self.window_size_ms.saturating_sub(elapsed_ms),
                    instant_rate.count.load(Ordering::Relaxed),
                )
            }
            Some(_) => {
                drop(series);

                let Some(mut series) = self.series.get_mut(key) else {
                    return RateLimitDecision::Allowed;
                };

                let total_count = Self::evict_expired(&mut series, self.window_duration);

                if total_count < window_limit {
                    return RateLimitDecision::Allowed;
                }

                let (elapsed_ms, count) = series
                    .series
                    .front()
                    .map(|i| {
                        (
                            self.window_size_ms
                                .saturating_sub(i.timestamp.elapsed().as_millis()),
                            i.count.load(Ordering::Relaxed),
                        )
                    })
                    .unwrap_or((0, 0));

                (elapsed_ms, count)
            }
        };

        RateLimitDecision::Rejected {
            window_size_seconds: *self.window_size_seconds,
            retry_after_ms,
            remaining_after_waiting,
        }
    } // end method is_allowed

    /// Evict buckets older than the window from `series`, keeping `total` consistent.
    fn evict_expired(series: &mut RateLimitSeries, window_duration: Duration) -> u64 {
        let now = Instant::now();

        let split = series
            .series
            .partition_point(|r| now.duration_since(r.timestamp) > window_duration);

        if split == 0 {
            return 0;
        }

        let drained = series
            .series
            .drain(..split)
            .map(|r| r.count.load(Ordering::Relaxed))
            .sum::<u64>();

        let prev = series.total.fetch_sub(drained, Ordering::Relaxed);

        prev - drained
    } // end fn evict_expired

    fn live_total(&self, key: &str) -> u64 {
        let Some(series) = self.series.get(key) else {
            return 0;
        };

        let mut total = series.total.load(Ordering::Relaxed);

        match series.series.front() {
            None => {}
            // check if the oldest value is still within the window size ms
            Some(instant_rate)
                if instant_rate.timestamp.elapsed().as_millis() <= self.window_size_ms => {}
            Some(_) => {
                drop(series);

                let Some(mut series) = self.series.get_mut(key) else {
                    return 0;
                };

                total = Self::evict_expired(&mut series, self.window_duration);
            }
        }

        total
    }

    fn set_if_with_history_mode(
        &self,
        key: &str,
        rate_limit: &RateLimit,
        comparator: RateLimitComparator,
        count: u64,
        mode: HistoryUpdateMode,
    ) -> (u64, u64) {
        let old_total = self.live_total(key);

        if !comparator.matches(old_total) || (old_total == 0 && count == 0) {
            return (old_total, old_total);
        }

        if count == 0 {
            self.series.remove(key);

            return (0, old_total);
        }

        let mut series = self
            .series
            .entry(key.to_string())
            .or_insert_with(|| RateLimitSeries::new(*rate_limit));

        let old_total = series.total.load(Ordering::Relaxed);
        series.limit = *rate_limit;

        // preservation of history doesn't need the history to be updated in this case
        if old_total == count && matches!(mode, HistoryUpdateMode::Preserve(_)) {
            return (old_total, old_total);
        }

        match mode {
            HistoryUpdateMode::Replace => {
                series.series.clear();

                if count > 0 {
                    series.series.push_back(InstantRate {
                        count: count.into(),
                        timestamp: Instant::now(),
                        declined: AtomicU64::new(0),
                    });
                }
            }
            HistoryUpdateMode::Preserve(preservation) if count > old_total => {
                let delta = count - old_total;
                let bucket = match preservation {
                    HistoryPreservation::PreserveNewest => series.series.back(),
                    HistoryPreservation::PreserveOldest => series.series.front(),
                };

                if let Some(bucket) = bucket {
                    bucket.count.fetch_add(delta, Ordering::Relaxed);
                } else {
                    series.series.push_back(InstantRate {
                        count: delta.into(),
                        timestamp: Instant::now(),
                        declined: AtomicU64::new(0),
                    });
                }
            }
            HistoryUpdateMode::Preserve(preservation) if count < old_total => {
                let mut to_remove = old_total - count;

                while to_remove > 0 {
                    let bucket_count = match preservation {
                        HistoryPreservation::PreserveNewest => series
                            .series
                            .front()
                            .map(|bucket| bucket.count.load(Ordering::Relaxed)),
                        HistoryPreservation::PreserveOldest => series
                            .series
                            .back()
                            .map(|bucket| bucket.count.load(Ordering::Relaxed)),
                    };

                    let Some(bucket_count) = bucket_count else {
                        break;
                    };

                    if bucket_count <= to_remove {
                        to_remove -= bucket_count;
                        match preservation {
                            HistoryPreservation::PreserveNewest => {
                                series.series.pop_front();
                            }
                            HistoryPreservation::PreserveOldest => {
                                series.series.pop_back();
                            }
                        }
                    } else {
                        let bucket = match preservation {
                            HistoryPreservation::PreserveNewest => {
                                series.series.front().expect("bucket should exist")
                            }
                            HistoryPreservation::PreserveOldest => {
                                series.series.back().expect("bucket should exist")
                            }
                        };
                        bucket.count.fetch_sub(to_remove, Ordering::Relaxed);
                        to_remove = 0;
                    }
                }
            }
            HistoryUpdateMode::Preserve(_) => {}
        }

        series.total.store(count, Ordering::Relaxed);

        (count, old_total)
    }

    /// Current window total for `key` (read-only from the caller's perspective).
    ///
    /// Ignores expired buckets and returns the sum of live bucket counts without
    /// mutating the stored history. Unknown keys return `0`.
    ///
    /// # Examples
    ///
    /// ```
    /// # let rl = trypema::__doctest_helpers::rate_limiter();
    /// use trypema::RateLimit;
    ///
    /// let limiter = rl.local().absolute();
    /// assert_eq!(limiter.get("user_123"), 0);
    ///
    /// let rate = RateLimit::try_from(10.0).unwrap();
    /// limiter.inc("user_123", &rate, 3);
    /// assert_eq!(limiter.get("user_123"), 3);
    /// ```
    pub fn get(&self, key: &str) -> u64 {
        self.live_total(key)
        // let Some(series) = self.series.get(key) else {
        //     return 0;
        // };
        //
        // let (total, _contains_expired, _should_cleanup) =
        //     Self::live_total(&series, &self.window_duration);
        //
        // total
    } // end method get

    /// Conditionally replace the window total for `key`.
    ///
    /// When `comparator` matches the key's current window total, the
    /// window contents are replaced by a single current-timestamp bucket holding
    /// `count` and the stored rate limit is redefined to `rate_limit`; otherwise
    /// nothing is written. Unlike `inc`, a matched call can therefore replace a
    /// previously stored limit.
    ///
    /// The entire operation runs while holding the key's map entry, so it is
    /// consistent with respect to other `set_if` calls; interleaving with concurrent
    /// `inc` calls follows the limiter's usual best-effort concurrency model.
    ///
    /// # Arguments
    ///
    /// - `key`: Unique identifier for the rate-limited resource
    /// - `rate_limit`: Per-second rate limit; redefines the stored limit
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
    /// let limiter = rl.local().absolute();
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

    /// Conditionally set the current total while retaining the selected side of
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
