use std::ops::{Deref, DerefMut};
use std::time::Duration;

use crate::TrypemaError;

/// How often each hybrid rate limiter advances its epoch (used to detect activity periods).
pub(crate) const EPOCH_CHANGE_INTERVAL: Duration = Duration::from_secs(15);

/// Computes the inactivity-detection sleep duration for the Redis committer.
///
/// The committer waits at least this long without observing activity before marking itself
/// inactive. The interval is floored at 30 s and scales with the configured `sync_interval`
/// (at `10×`), then halved so the inactivity check fires roughly mid-cycle:
///
/// ```text
/// max(30_000 ms, sync_interval × 10) / 2
/// ```
pub(crate) fn committer_inactivity_sleep(sync_interval: Duration) -> Duration {
    let min_ms = (EPOCH_CHANGE_INTERVAL * 2).as_millis() as u64;
    let multiplier: u64 = 10;
    Duration::from_millis(min_ms.max(sync_interval.as_millis() as u64 * multiplier)) / 2
}

/// A signal to the Redis rate limiter to flush its local cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum RedisRateLimiterSignal {
    /// Flush the local cache.
    Flush,
}

/// Sync interval (milliseconds) for the hybrid provider's background flush.
///
/// The hybrid provider batches local increments and periodically commits them to Redis
/// via a background actor (the `RedisCommitter`). This value controls
/// how often that flush occurs.
///
/// # Trade-offs
///
/// - **Shorter interval** (5–10ms): less lag between local and Redis state, at the cost of
///   more frequent Redis writes.
/// - **Longer interval** (50–100ms): fewer Redis writes, but admission decisions may be
///   based on stale state for longer.
///
/// # Recommendation
///
/// Start with **10ms** (the default). It is generally recommended to keep
/// `sync_interval_ms` ≤ `rate_group_size_ms`.
///
/// # Validation
///
/// Must be ≥ 1. A value of `0` returns an error.
///
/// # Examples
///
/// ```
/// use trypema::hybrid::SyncIntervalMs;
///
/// // Default: 10ms
/// let interval = SyncIntervalMs::default();
/// assert_eq!(*interval, 10);
///
/// // Custom: 50ms for reduced Redis writes
/// let interval = SyncIntervalMs::try_from(50).unwrap();
///
/// // Invalid: 0ms
/// assert!(SyncIntervalMs::try_from(0).is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct SyncIntervalMs(u64);

impl Default for SyncIntervalMs {
    /// Returns a sync interval of 10 ms.
    fn default() -> Self {
        Self(10)
    }
}

impl Deref for SyncIntervalMs {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for SyncIntervalMs {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl TryFrom<u64> for SyncIntervalMs {
    type Error = TrypemaError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        if value == 0 {
            Err(TrypemaError::InvalidRateGroupSizeMs(
                "Sync interval must be greater than 0".to_string(),
            ))
        } else {
            Ok(Self(value))
        }
    }
}
