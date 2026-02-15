use std::{sync::atomic::Ordering, time::Instant};

use dashmap::DashMap;

use crate::{
    LocalRateLimiterOptions,
    common::{InstantRate, RateLimit, RateLimitDecision, RateLimitSeries, WindowSizeSeconds},
};

/// Local, per-key rate limiter with a sliding time window.
///
/// This implementation is designed for multi-threaded use.
/// Internally it uses a [`dashmap::DashMap`] for concurrent access and atomics
/// for per-key counters.
pub struct AbsoluteLocalRateLimiter {
    window_size_seconds: WindowSizeSeconds,
    rate_group_size_ms: u16,
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

    /// Check admission and, if allowed, increment the observed count for `key`.
    ///
    /// - `rate_limit`: per-second limit for `key`. This is stored the first time
    ///   the key is seen; subsequent calls for the same key do not update it.
    /// - `count`: amount to add (typically `1`).
    ///
    /// If the key is currently over limit, this returns [`RateLimitDecision::Rejected`]
    /// and does not apply the increment.
    ///
    /// Increments close together in time may be coalesced based on
    /// `rate_group_size_ms`.
    pub fn inc(&self, key: &str, rate_limit: RateLimit, count: u64) -> RateLimitDecision {
        let is_allowed = self.is_allowed(key);

        if !matches!(is_allowed, RateLimitDecision::Allowed) {
            return is_allowed;
        }

        if !self.series.contains_key(key) {
            self.series
                .entry(key.to_string())
                .or_insert_with(|| RateLimitSeries::new(rate_limit));
        }

        let Some(rate_limit_series) = self.series.get(key) else {
            unreachable!("AbsoluteLocalRateLimiter::inc: key should be in map");
        };

        if let Some(last_entry) = rate_limit_series.series.back()
            && last_entry.timestamp.elapsed().as_millis() <= self.rate_group_size_ms as u128
        {
            last_entry.count.fetch_add(count, Ordering::Relaxed);
            rate_limit_series.total.fetch_add(count, Ordering::Relaxed);
        } else {
            drop(rate_limit_series);

            let mut rate_limit_series = self
                .series
                .entry(key.to_string())
                .or_insert_with(|| RateLimitSeries::new(rate_limit));

            rate_limit_series.series.push_back(InstantRate {
                count: count.into(),
                timestamp: Instant::now(),
            });

            rate_limit_series.total.fetch_add(count, Ordering::Relaxed);
        }

        RateLimitDecision::Allowed
    } // end method inc

    /// Determine whether `key` is currently allowed.
    ///
    /// Returns [`RateLimitDecision::Allowed`] if the current sliding window total
    /// is below the window limit, otherwise returns [`RateLimitDecision::Rejected`]
    /// with a best-effort `retry_after_ms`.
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
} // end of impl
