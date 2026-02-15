use std::{sync::atomic::Ordering, time::Instant};

use dashmap::DashMap;

use crate::{
    AbsoluteLocalRateLimiter, LocalRateLimiterOptions, RateLimitDecision,
    common::{RateGroupSizeMs, RateLimit, WindowSizeSeconds},
};

/// Placeholder for an alternative local rate limiter strategy.
///
/// This type exists to reserve the API surface while additional strategies and
/// providers are implemented.
pub struct SuppressedLocalRateLimiter {
    accepted_limiter: AbsoluteLocalRateLimiter,
    observed_limiter: AbsoluteLocalRateLimiter,
    suppression_factors: DashMap<String, (Instant, f64)>,
    hard_limit_factor: f64,
    rate_group_size_ms: RateGroupSizeMs,
    window_size_seconds: WindowSizeSeconds,
}

impl SuppressedLocalRateLimiter {
    pub(crate) fn new(options: LocalRateLimiterOptions) -> Self {
        let rate_group_size_ms = options.rate_group_size_ms;
        let window_size_seconds = options.window_size_seconds;

        let accepted_limiter = AbsoluteLocalRateLimiter::new(options.clone());
        let observed_limiter = AbsoluteLocalRateLimiter::new(options);

        Self {
            accepted_limiter,
            observed_limiter,
            suppression_factors: DashMap::new(),
            hard_limit_factor: 3f64,
            rate_group_size_ms,
            window_size_seconds,
        }
    } // end constructor

    /// Check admission and, if allowed, increment the observed count for `key`.
    ///
    /// This is a stub implementation that does not actually apply the increment.
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
    pub fn inc(&self, key: &str, rate_limit: &RateLimit, count: u64) -> RateLimitDecision {
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

        let should_allow = rand::random_bool((1f64 - suppression_factor) as f64);

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
        let Ok(val) = RateLimit::try_from(self.hard_limit_factor * **rate_limit) else {
            unreachable!(
                "AbsoluteLocalRateLimiter::get_hard_limit: hard_limit_factor is always > 0"
            );
        };

        val
    } // end method get_hard_limit
} // end impl    
