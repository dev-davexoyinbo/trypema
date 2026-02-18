use std::time::Instant;

use dashmap::DashMap;
use redis::aio::ConnectionManager;

use crate::{
    AbsoluteRedisRateLimiter, HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision,
    RedisKey, RedisKeyGenerator, RedisRateLimiterOptions, TrypemaError, WindowSizeSeconds,
    common::RateType,
};

/// A rate limiter backed by Redis.
pub struct SuppressedRedisRateLimiter {
    connection_manager: ConnectionManager,

    accepted_limiter: AbsoluteRedisRateLimiter,
    observed_limiter: AbsoluteRedisRateLimiter,
    hard_limit_factor: HardLimitFactor,
    rate_group_size_ms: RateGroupSizeMs,
    window_size_seconds: WindowSizeSeconds,
}

impl SuppressedRedisRateLimiter {
    pub(crate) fn new(options: RedisRateLimiterOptions) -> Self {
        let accepted_limiter =
            AbsoluteRedisRateLimiter::with_rate_type(RateType::Suppressed, options.clone());
        let observed_limiter =
            AbsoluteRedisRateLimiter::with_rate_type(RateType::SuppressedObserved, options.clone());

        Self {
            connection_manager: options.connection_manager,
            window_size_seconds: options.window_size_seconds,
            rate_group_size_ms: options.rate_group_size_ms,
            accepted_limiter,
            observed_limiter,
            hard_limit_factor: options.hard_limit_factor,
        }
    }

    /// Check admission and, if allowed, increment the observed count for `key`.
    pub async fn inc(
        &self,
        key: &RedisKey,
        rate_limit: &RateLimit,
        count: u64,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let mut rng = |p: f64| rand::random_bool(p);
        self.inc_with_rng(key, rate_limit, count, &mut rng).await
    } // end method inc

    pub(crate) async fn inc_with_rng(
        &self,
        key: &RedisKey,
        rate_limit: &RateLimit,
        count: u64,
        random_bool: &mut impl FnMut(f64) -> bool,
    ) -> Result<RateLimitDecision, TrypemaError> {
        // Always track observed usage; this limiter uses a max rate to avoid rejecting.
        self.observed_limiter
            .inc(key, &RateLimit::max(), count)
            .await?;

        // TODO: get suppression factor from redis

        let suppression_factor = self.compute_or_calculate_suppression_factor(key)?.min(1f64);

        if suppression_factor <= 0f64 {
            return self
                .accepted_limiter
                .inc(key, &self.get_hard_limit(rate_limit), count)
                .await;
        }

        let should_allow = random_bool(1f64 - suppression_factor);

        if !should_allow {
            return Ok(RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: false,
            });
        }

        let decision = self
            .accepted_limiter
            .inc(key, &self.get_hard_limit(rate_limit), count)
            .await?;

        let decision = match decision {
            RateLimitDecision::Allowed => RateLimitDecision::Suppressed {
                suppression_factor,
                is_allowed: true,
            },
            // This is a hard limit rejection, so we can't suppress.
            RateLimitDecision::Rejected { .. } => decision,
            RateLimitDecision::Suppressed { .. } => {
                unreachable!("AbsoluteLocalRateLimiter::inc: should not be suppressed")
            }
        };

        Ok(decision)
    }

    #[inline]
    fn compute_or_calculate_suppression_factor(&self, key: &RedisKey) -> Result<f64, TrypemaError> {
        // let Some(series) = self.accepted_limiter.series().get(key) else {
        //     return self.persist_suppression_factor(key, 0f64);
        // };
        //
        // if series.series.is_empty() {
        //     return self.persist_suppression_factor(key, 0f64);
        // }
        //
        // let window_limit = *self.window_size_seconds as f64 * *series.limit;
        //
        // if series.total.load(Ordering::Relaxed) < window_limit as u64 {
        //     return self.persist_suppression_factor(key, 0f64);
        // }
        //
        // let mut total_in_last_second = 0u64;
        //
        // for instant_rate in series.series.iter().rev() {
        //     if instant_rate.timestamp.elapsed().as_secs() > 1 {
        //         break;
        //     }
        //
        //     total_in_last_second =
        //         total_in_last_second.saturating_add(instant_rate.count.load(Ordering::Relaxed));
        // }
        //
        // let average_rate_in_window: f64 =
        //     series.total.load(Ordering::Relaxed) as f64 / *self.window_size_seconds as f64;
        //
        // let perceived_rate_limit = average_rate_in_window.max(total_in_last_second as f64);
        //
        // let suppression_factor = 1f64 - (perceived_rate_limit / *series.limit);
        //
        // self.persist_suppression_factor(key, suppression_factor)

        todo!()
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

    pub(crate) async fn cleanup(&self, stale_after_ms: u64) -> Result<(), TrypemaError> {
        // TODO: cleanup suppression factors
        self.accepted_limiter.cleanup(stale_after_ms).await?;
        self.observed_limiter.cleanup(stale_after_ms).await
    } // end method cleanup
}
