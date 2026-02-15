use std::{sync::atomic::Ordering, time::Instant};

use dashmap::DashMap;

use crate::{
    AbsoluteLocalRateLimiter, LocalRateLimiterOptions, RateLimitDecision,
    common::{HardLimitFactor, RateGroupSizeMs, RateLimit, WindowSizeSeconds},
};

/// Local strategy that can probabilistically suppress work while tracking both
/// observed and accepted rates.
///
/// This limiter maintains two internal limiters:
/// - `observed_limiter`: tracks all calls (including suppressed ones)
/// - `accepted_limiter`: tracks only calls admitted by this strategy
///
/// It returns [`RateLimitDecision::Suppressed`] when operating in a regime where
/// suppression may occur. When `is_allowed` is `false`, the call is denied without
/// incrementing the accepted series.
///
/// A hard cutoff is enforced using `hard_limit_factor`: if accepted usage would exceed
/// `rate_limit * hard_limit_factor`, the decision is returned as
/// [`RateLimitDecision::Rejected`] (a hard rejection that cannot be "suppressed").
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

    /// Check admission and, if allowed, increment the observed count for `key`.
    ///
    /// - `rate_limit`: per-second limit for `key`. This is stored the first time
    ///   the key is seen; subsequent calls for the same key do not update it.
    /// - `count`: amount to add (typically `1`).
    ///
    /// Return values:
    /// - [`RateLimitDecision::Allowed`]: suppression is bypassed and the increment is applied.
    /// - [`RateLimitDecision::Suppressed`]: suppression is in effect; check `is_allowed`.
    /// - [`RateLimitDecision::Rejected`]: hard cutoff was hit; the increment is not applied
    ///   to the accepted series.
    ///
    /// Increments close together in time may be coalesced based on
    /// `rate_group_size_ms`.
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
} // end impl
