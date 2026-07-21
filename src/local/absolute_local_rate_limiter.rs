use std::{
    collections::VecDeque,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use dashmap::{DashMap, mapref::entry::Entry};

use crate::{
    ConditionalSetOutcome, HistoryPreservation, RateLimitComparator,
    builder::ProviderConfig,
    common::{
        Bucket, BucketSize, HistoryUpdateMode, RandomState, RateLimit, RateLimitDecision,
        WindowSize, duration_from_milliseconds,
    },
};

#[derive(Debug)]
pub(crate) struct RateLimitSeries {
    pub window_limit: f64,
    pub buckets: VecDeque<Bucket>,
    pub total_count: AtomicU64,
}

impl RateLimitSeries {
    pub fn new(window_limit: f64) -> Self {
        Self {
            window_limit,
            buckets: VecDeque::new(),
            total_count: AtomicU64::new(0),
        }
    }
}

/// Sliding-window allow/reject rate limiter for in-process use.
///
/// Provides deterministic rate limiting with per-key state maintained in memory.
/// Uses a sliding time window to track request counts and enforce limits.
///
/// # Algorithm
///
/// 1. **Window capacity:** `window_size × rate_limit`
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
/// - The first call for a key stores its computed window limit
/// - Subsequent calls for the same key do not update that stored window limit
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
/// - Buckets expire close to `window_size` (lazy eviction may delay removal until next call)
///
/// **Lazy eviction:**
/// - Reads and admission operations may remove expired buckets
/// - Provider builders start stale-key cleanup by default
///
/// # Examples
///
/// ```
/// # use trypema::{RateLimiterBuilder, local::LocalRateLimiterProvider};
/// # let rl = LocalRateLimiterProvider::builder().disable_cleanup().build().unwrap();
/// use trypema::{RateLimit, RateLimitDecision};
///
/// let limiter = rl.absolute();
/// let rate = RateLimit::per_second(10.0).unwrap();
///
/// assert!(matches!(limiter.inc("user_123", &rate, 1), RateLimitDecision::Allowed));
/// assert!(matches!(limiter.is_allowed("user_123"), RateLimitDecision::Allowed));
/// ```
#[derive(Debug)]
pub struct AbsoluteLocalRateLimiter {
    window_size: WindowSize,
    window_size_ms: u128,
    window_duration: Duration,
    bucket_size: BucketSize,
    series: DashMap<String, RateLimitSeries, RandomState>,
}

impl AbsoluteLocalRateLimiter {
    pub(crate) fn new(options: ProviderConfig) -> Self {
        Self {
            window_size_ms: options.window_size.as_milliseconds(),
            window_duration: Duration::from_secs(options.window_size.as_seconds()),
            window_size: options.window_size,
            bucket_size: options.bucket_size,
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
    ///    - Check if recent bucket exists within `bucket_size`
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
    /// Increments within `bucket_size` of the most recent bucket are merged
    /// into that bucket. This reduces memory usage and improves performance.
    ///
    /// # Examples
    ///
    /// ```
    /// # use trypema::{RateLimiterBuilder, local::LocalRateLimiterProvider};
    /// # let rl = LocalRateLimiterProvider::builder().disable_cleanup().build().unwrap();
    /// use trypema::{RateLimit, RateLimitDecision};
    ///
    /// let limiter = rl.absolute();
    /// let rate = RateLimit::per_second(10.0).unwrap();
    ///
    /// // Single request
    /// assert!(matches!(limiter.inc("user_123", &rate, 1), RateLimitDecision::Allowed));
    ///
    /// // Batch of 10
    /// assert!(matches!(limiter.inc("user_456", &rate, 10), RateLimitDecision::Allowed));
    /// ```
    pub fn inc(&self, key: &str, rate_limit: &RateLimit, count: u64) -> RateLimitDecision {
        let decision = self.is_allowed(key);

        if !matches!(decision, RateLimitDecision::Allowed) {
            return decision;
        }

        let series = match self.series.get(key) {
            Some(series) => series,
            None => self
                .series
                .entry(key.to_string())
                .or_insert_with(|| {
                    RateLimitSeries::new(
                        rate_limit.as_per_second() * self.window_size.as_seconds() as f64,
                    )
                })
                .downgrade(),
        };

        if let Some(latest_bucket) = series.buckets.back()
            && latest_bucket.timestamp.elapsed().as_millis()
                <= self.bucket_size.as_milliseconds() as u128
        {
            latest_bucket.count.fetch_add(count, Ordering::AcqRel);
            series.total_count.fetch_add(count, Ordering::AcqRel);
        } else {
            drop(series);

            let mut series = self.series.entry(key.to_string()).or_insert_with(|| {
                RateLimitSeries::new(
                    rate_limit.as_per_second() * self.window_size.as_seconds() as f64,
                )
            });

            series.buckets.push_back(Bucket {
                count: count.into(),
                timestamp: Instant::now(),
                declined_count: AtomicU64::new(0),
            });

            series.total_count.fetch_add(count, Ordering::AcqRel);
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
    /// 4. Compare against `window_capacity = window_size × rate_limit`
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
    /// # use trypema::{RateLimiterBuilder, local::LocalRateLimiterProvider};
    /// # let rl = LocalRateLimiterProvider::builder().disable_cleanup().build().unwrap();
    /// use trypema::{RateLimit, RateLimitDecision};
    ///
    /// let limiter = rl.absolute();
    /// let rate = RateLimit::per_second(10.0).unwrap();
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

        let total_count = series.total_count.load(Ordering::Acquire);

        if total_count < series.window_limit as u64 {
            return RateLimitDecision::Allowed;
        }

        let (retry_after_ms, remaining_after_waiting) = match series.buckets.front() {
            None => (0, 0),
            Some(oldest_bucket)
                if oldest_bucket.timestamp.elapsed().as_millis() <= self.window_size_ms =>
            {
                let elapsed_ms = oldest_bucket.timestamp.elapsed().as_millis();
                (
                    self.window_size_ms.saturating_sub(elapsed_ms),
                    oldest_bucket.count.load(Ordering::Acquire),
                )
            }
            Some(_) => {
                drop(series);

                let Some(mut series) = self.series.get_mut(key) else {
                    return RateLimitDecision::Allowed;
                };

                let total_count = Self::evict_expired(&mut series, self.window_duration);

                if total_count < series.window_limit as u64 {
                    return RateLimitDecision::Allowed;
                }

                let (elapsed_ms, count) = series
                    .buckets
                    .front()
                    .map(|bucket| {
                        (
                            self.window_size_ms
                                .saturating_sub(bucket.timestamp.elapsed().as_millis()),
                            bucket.count.load(Ordering::Acquire),
                        )
                    })
                    .unwrap_or((0, 0));

                (elapsed_ms, count)
            }
        };

        RateLimitDecision::Rejected {
            window_size: self.window_size,
            retry_after: duration_from_milliseconds(retry_after_ms),
            remaining_after_waiting,
        }
    } // end method is_allowed

    /// Evict buckets older than the window from `series`, keeping `total` consistent.
    fn evict_expired(series: &mut RateLimitSeries, window_duration: Duration) -> u64 {
        let now = Instant::now();

        let split = series
            .buckets
            .partition_point(|bucket| now.duration_since(bucket.timestamp) > window_duration);

        if split == 0 {
            return series.total_count.load(Ordering::Acquire);
        }

        let drained = series
            .buckets
            .drain(..split)
            .map(|bucket| bucket.count.load(Ordering::Acquire))
            .sum::<u64>();

        let prev = series.total_count.fetch_sub(drained, Ordering::AcqRel);

        prev - drained
    } // end fn evict_expired

    fn inspect_live_total(series: &RateLimitSeries, window_duration: Duration) -> (u64, bool) {
        let now = Instant::now();
        let split = series
            .buckets
            .partition_point(|bucket| now.duration_since(bucket.timestamp) > window_duration);

        if split == 0 {
            return (series.total_count.load(Ordering::Acquire), false);
        }

        let total_count = series
            .buckets
            .iter()
            .skip(split)
            .map(|bucket| bucket.count.load(Ordering::Acquire))
            .sum();

        (total_count, true)
    }

    fn live_total(&self, key: &str) -> u64 {
        let Some(series) = self.series.get(key) else {
            return 0;
        };

        let (total, contains_expired) = Self::inspect_live_total(&series, self.window_duration);
        if !contains_expired {
            return total;
        }

        drop(series);

        let Some(mut series) = self.series.get_mut(key) else {
            return 0;
        };

        Self::evict_expired(&mut series, self.window_duration)
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

                let window_limit =
                    rate_limit.as_per_second() * self.window_size.as_seconds() as f64;

                let unchanged = count > 0
                    && matches!(mode, HistoryUpdateMode::Preserve(_))
                    && !contains_expired
                    && old_total == count
                    && series.window_limit == window_limit;

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

        match self.series.entry(key.to_string()) {
            Entry::Occupied(mut entry) => {
                let (old_total, contains_expired) =
                    Self::inspect_live_total(entry.get(), self.window_duration);

                if !comparator.matches(old_total) {
                    return (old_total, old_total);
                }

                if count == 0 {
                    entry.remove();
                    return (0, old_total);
                }

                let series = entry.get_mut();

                if contains_expired {
                    Self::evict_expired(series, self.window_duration);
                }

                series.window_limit =
                    rate_limit.as_per_second() * self.window_size.as_seconds() as f64;

                if old_total == count && matches!(mode, HistoryUpdateMode::Preserve(_)) {
                    return (old_total, old_total);
                }

                Self::apply_history_update(series, count, old_total, mode);

                (count, old_total)
            }
            Entry::Vacant(entry) => {
                if !comparator.matches(0) || count == 0 {
                    return (0, 0);
                }

                let mut series = RateLimitSeries::new(
                    rate_limit.as_per_second() * self.window_size.as_seconds() as f64,
                );

                Self::apply_history_update(&mut series, count, 0, mode);
                entry.insert(series);

                (count, 0)
            }
        }
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
                    timestamp: Instant::now(),
                    declined_count: AtomicU64::new(0),
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
                        timestamp: Instant::now(),
                        declined_count: AtomicU64::new(0),
                    });
                }
            }
            HistoryUpdateMode::Preserve(preservation) if count < old_total => {
                let mut to_remove = old_total - count;
                let mut bucket;
                let mut bucket_count;

                while to_remove > 0 {
                    bucket = match preservation {
                        HistoryPreservation::PreserveNewest => series.buckets.front(),
                        HistoryPreservation::PreserveOldest => series.buckets.back(),
                    };

                    let Some(bucket) = bucket else {
                        break;
                    };

                    bucket_count = bucket.count.load(Ordering::Acquire);

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
                        bucket.count.fetch_sub(to_remove, Ordering::AcqRel);
                        to_remove = 0;
                    }
                }
            }
            HistoryUpdateMode::Preserve(_) => {}
        }

        series.total_count.store(count, Ordering::Release);
    }

    /// Current live window total for `key`.
    ///
    /// Evicts expired buckets when necessary and returns the sum of live bucket
    /// counts. Unknown keys return `0` without inserting state.
    ///
    /// # Examples
    ///
    /// ```
    /// # use trypema::{RateLimiterBuilder, local::LocalRateLimiterProvider};
    /// # let rl = LocalRateLimiterProvider::builder().disable_cleanup().build().unwrap();
    /// use trypema::RateLimit;
    ///
    /// let limiter = rl.absolute();
    /// assert_eq!(limiter.get("user_123"), 0);
    ///
    /// let rate = RateLimit::per_second(10.0).unwrap();
    /// limiter.inc("user_123", &rate, 3);
    /// assert_eq!(limiter.get("user_123"), 3);
    /// ```
    pub fn get(&self, key: &str) -> u64 {
        self.live_total(key)
    } // end method get

    /// Conditionally replace the window total for `key`.
    ///
    /// When `comparator` matches the key's current window total, the
    /// window contents are replaced by a single current-timestamp bucket holding
    /// `count` and the stored window limit is recomputed from `rate_limit`;
    /// otherwise nothing is written. Unlike `inc`, a matched call can therefore
    /// replace a previously stored limit.
    ///
    /// The entire operation runs while holding the key's map entry, so it is
    /// consistent with respect to other `set_if` calls; interleaving with concurrent
    /// `inc` calls follows the limiter's usual best-effort concurrency model.
    ///
    /// # Arguments
    ///
    /// - `key`: Unique identifier for the rate-limited resource
    /// - `rate_limit`: Per-second rate limit used to redefine the stored window limit
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
    /// ```
    /// # use trypema::{RateLimiterBuilder, local::LocalRateLimiterProvider};
    /// # let rl = LocalRateLimiterProvider::builder().disable_cleanup().build().unwrap();
    /// use trypema::{RateLimit, RateLimitComparator};
    ///
    /// let limiter = rl.absolute();
    /// let rate = RateLimit::per_second(10.0).unwrap();
    ///
    /// // Prime the window to 40.
    /// let outcome = limiter.set_if("user_123", &rate, RateLimitComparator::Lt(40), 40);
    /// assert!(outcome.matched);
    /// assert_eq!((outcome.current_total, outcome.previous_total), (40, 0));
    ///
    /// // Re-priming is a no-op: the guard no longer matches.
    /// let outcome = limiter.set_if("user_123", &rate, RateLimitComparator::Lt(40), 40);
    /// assert!(!outcome.matched);
    /// assert_eq!((outcome.current_total, outcome.previous_total), (40, 40));
    /// ```
    pub fn set_if(
        &self,
        key: &str,
        rate_limit: &RateLimit,
        comparator: RateLimitComparator,
        count: u64,
    ) -> ConditionalSetOutcome {
        let (current_total, previous_total) = self.set_if_with_history_mode(
            key,
            rate_limit,
            comparator,
            count,
            HistoryUpdateMode::Replace,
        );

        ConditionalSetOutcome {
            matched: comparator.matches(previous_total),
            previous_total,
            current_total,
        }
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
    ) -> ConditionalSetOutcome {
        let (current_total, previous_total) = self.set_if_with_history_mode(
            key,
            rate_limit,
            comparator,
            count,
            HistoryUpdateMode::Preserve(preservation),
        );

        ConditionalSetOutcome {
            matched: comparator.matches(previous_total),
            previous_total,
            current_total,
        }
    } // end method set_if_preserve_history

    pub(crate) fn cleanup(&self, stale_after_ms: u64) {
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
