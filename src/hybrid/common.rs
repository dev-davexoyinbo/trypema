use std::ops::{Deref, DerefMut};
use std::time::Duration;

use crate::{
    TrypemaError,
    common::{MILLISECONDS_PER_SECOND, SECONDS_PER_HOUR, SECONDS_PER_MINUTE, checked_duration},
};

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

/// Sync interval stored in milliseconds for the hybrid provider's background flush.
///
/// The hybrid provider batches local increments and periodically commits them to Redis via a
/// background actor. This value controls how often that flush occurs.
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
/// `sync_interval` ≤ `bucket_size`.
///
/// # Validation
///
/// Must be ≥ 1. A value of `0` returns an error.
///
/// # Examples
///
/// ```
/// use trypema::hybrid::SyncInterval;
///
/// let interval = SyncInterval::milliseconds(10).unwrap();
/// assert_eq!(*interval, 10);
///
/// let interval = SyncInterval::milliseconds(50).unwrap();
/// let interval = SyncInterval::milliseconds_or_panic(75);
///
/// // Invalid: 0ms
/// assert!(SyncInterval::milliseconds(0).is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct SyncInterval(u64);

impl Default for SyncInterval {
    /// Returns a sync interval of 10 ms.
    fn default() -> Self {
        Self(10)
    }
}

impl SyncInterval {
    /// Create a sync interval expressed in milliseconds.
    pub fn milliseconds(value: u64) -> Result<Self, TrypemaError> {
        Self::from_milliseconds(value, 1)
    }

    /// Create a sync interval expressed in milliseconds.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero.
    pub fn milliseconds_or_panic(value: u64) -> Self {
        Self::milliseconds(value).unwrap()
    }

    /// Create a sync interval expressed in seconds.
    pub fn seconds(value: u64) -> Result<Self, TrypemaError> {
        Self::from_milliseconds(value, MILLISECONDS_PER_SECOND)
    }

    /// Create a sync interval expressed in seconds.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero or conversion overflows.
    pub fn seconds_or_panic(value: u64) -> Self {
        Self::seconds(value).unwrap()
    }

    /// Create a sync interval expressed in minutes.
    pub fn minutes(value: u64) -> Result<Self, TrypemaError> {
        Self::from_milliseconds(value, MILLISECONDS_PER_SECOND * SECONDS_PER_MINUTE)
    }

    /// Create a sync interval expressed in minutes.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero or conversion overflows.
    pub fn minutes_or_panic(value: u64) -> Self {
        Self::minutes(value).unwrap()
    }

    /// Create a sync interval expressed in hours.
    pub fn hours(value: u64) -> Result<Self, TrypemaError> {
        Self::from_milliseconds(value, MILLISECONDS_PER_SECOND * SECONDS_PER_HOUR)
    }

    /// Create a sync interval expressed in hours.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero or conversion overflows.
    pub fn hours_or_panic(value: u64) -> Self {
        Self::hours(value).unwrap()
    }

    fn from_milliseconds(value: u64, multiplier: u64) -> Result<Self, TrypemaError> {
        checked_duration(
            value,
            multiplier,
            "sync interval",
            TrypemaError::InvalidSyncInterval,
        )
        .map(Self)
    }
}

impl Deref for SyncInterval {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for SyncInterval {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
