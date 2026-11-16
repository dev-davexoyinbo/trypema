//! Test-only accessors and trait impls of crate types, so the crate types carry no test code.

use dashmap::DashMap;

use crate::{
    common::{ConditionalSetOutcome, RandomState},
    local::{AbsoluteLocalRateLimiter, absolute_local_rate_limiter::RateLimitSeries},
};

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
use std::sync::Arc;

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
use crate::{
    TrypemaError,
    hybrid::{AbsoluteHybridRateLimiter, SuppressedHybridRateLimiter},
    redis::{RedisKey, RedisKeyGenerator, mutex_lock},
};

impl PartialEq<(u64, u64)> for ConditionalSetOutcome {
    fn eq(&self, other: &(u64, u64)) -> bool {
        (self.current_total, self.previous_total) == *other
    }
} // end impl

impl AbsoluteLocalRateLimiter {
    pub(crate) fn series(&self) -> &DashMap<String, RateLimitSeries, RandomState> {
        &self.series
    }
} // end impl

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
impl RedisKeyGenerator {
    /// All Redis keys associated with a single entity key.
    ///
    /// Includes every key the generator can produce for `key` regardless of rate type.
    /// Keys that are not written by a particular rate type (e.g. `hd`/`d`/`sf` for absolute
    /// limiters) will simply not exist, so `!exists` is trivially true in cleanup checks.
    pub(crate) fn get_all_entity_keys(&self, key: &RedisKey) -> Vec<String> {
        vec![
            self.get_hash_key(key),
            self.get_hash_declined_key(key),
            self.get_active_keys(key),
            self.get_window_limit_key(key),
            self.get_total_count_key(key),
            self.get_total_declined_key(key),
            self.get_suppression_factor_key(key),
        ]
    }
} // end impl

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
impl AbsoluteHybridRateLimiter {
    pub(crate) fn local_state_count(&self) -> usize {
        self.limiting_state.len()
    }

    pub(crate) fn reset_lock_count(&self) -> usize {
        self.reset_locks.len()
    }
} // end impl

/// Pauses a suppressed hybrid `set_if` after its pending-state snapshot, until `resume`.
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
#[derive(Debug, Default)]
pub(crate) struct SuppressedHybridSetIfTestHook {
    pub snapshot_taken: tokio::sync::Notify,
    pub resume: tokio::sync::Notify,
}

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
impl SuppressedHybridRateLimiter {
    pub(crate) fn local_state_count(&self) -> usize {
        self.limiting_state.len()
    }

    pub(crate) fn reset_lock_count(&self) -> usize {
        self.reset_locks.len()
    }

    pub(crate) fn install_set_if_test_hook(
        &self,
    ) -> Result<Arc<SuppressedHybridSetIfTestHook>, TrypemaError> {
        let hook = Arc::new(SuppressedHybridSetIfTestHook::default());
        *mutex_lock(&self.set_if_test_hook, "set_if_test_hook")? = Some(Arc::clone(&hook));

        Ok(hook)
    }
} // end impl
