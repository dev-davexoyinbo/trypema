use std::{cmp, time::Instant};

use dashmap::DashMap;

use crate::{AbsoluteLocalRateLimiter, LocalRateLimiterOptions, RateLimitDecision};

/// Placeholder for an alternative local rate limiter strategy.
///
/// This type exists to reserve the API surface while additional strategies and
/// providers are implemented.
pub struct SuppressedLocalRateLimiter {
    accepted_limiter: AbsoluteLocalRateLimiter,
    observed_limiter: AbsoluteLocalRateLimiter,
    suppression_factors: DashMap<String, (Instant, f32)>,
    hard_limit_factor: f32,
    rate_group_size_ms: u16,
}

impl SuppressedLocalRateLimiter {
    pub(crate) fn new(options: LocalRateLimiterOptions) -> Self {
        let rate_group_size_ms = options.rate_group_size_ms;
        let accepted_limiter = AbsoluteLocalRateLimiter::new(options.clone());
        let observed_limiter = AbsoluteLocalRateLimiter::new(options);

        Self {
            accepted_limiter,
            observed_limiter,
            suppression_factors: DashMap::new(),
            hard_limit_factor: 3f32,
            rate_group_size_ms,
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
    pub fn inc(&self, key: &str, rate_limit: u64, count: u64) -> RateLimitDecision {
        self.observed_limiter.inc(key, u64::MAX, count);

        let mut suppression_factor = match self.suppression_factors.get(key) {
            None => self.calculate_suppression_factor(key).1,
            Some(val) if val.0.elapsed().as_millis() < self.rate_group_size_ms as u128 => val.1,
            Some(val) => {
                drop(val);
                self.calculate_suppression_factor(key).1
            }
        };

        suppression_factor = suppression_factor.min(1f32);

        if suppression_factor <= 0f32 {
            return self
                .accepted_limiter
                .inc(key, self.get_hard_limit(rate_limit), count);
        }

        let should_allow = rand::random_bool((1f32 - suppression_factor) as f64);

        if !should_allow {
            return RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: false,
            };
        }

        let decision = self
            .accepted_limiter
            .inc(key, self.get_hard_limit(rate_limit), count);

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

    fn calculate_suppression_factor(&self, key: &str) -> (Instant, f32) {
        todo!("SuppressedLocalRateLimiter::calculate_suppression_factor: implement");
    } // end method calculate_suppression_factor

    #[inline]
    fn get_hard_limit(&self, rate_limit: u64) -> u64 {
        (self.hard_limit_factor * rate_limit as f32) as u64
    }
} // end impl    
