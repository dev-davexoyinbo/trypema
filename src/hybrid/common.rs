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

/// Whether a hybrid call records activity or only reads state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CallMode {
    /// `inc`, `try_inc`, and the flush refresh. They record activity and never use the allowed
    /// lag.
    Recording,
    /// `is_allowed`, `get_estimate`, and `get_suppression_factor`. They record no activity and
    /// answer from local state only while it is younger than the allowed lag.
    ReadOnly,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct StateRevision {
    pub namespace: u64,
    pub key: u64,
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
/// assert_eq!(interval.as_milliseconds(), 10);
/// assert_eq!(interval.as_seconds(), 0.01);
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
    /// Return synchronization interval in milliseconds.
    pub fn as_milliseconds(self) -> u64 {
        self.0
    }

    /// Return the synchronization interval in seconds.
    pub fn as_seconds(self) -> f64 {
        self.as_period(MILLISECONDS_PER_SECOND)
    }

    /// Return the synchronization interval in minutes.
    pub fn as_minutes(self) -> f64 {
        self.as_period(MILLISECONDS_PER_SECOND * SECONDS_PER_MINUTE)
    }

    /// Return the synchronization interval in hours.
    pub fn as_hours(self) -> f64 {
        self.as_period(MILLISECONDS_PER_SECOND * SECONDS_PER_HOUR)
    }

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

    fn as_period(self, period_milliseconds: u64) -> f64 {
        (self.0 as f64) / (period_milliseconds as f64)
    }
}

/// Maximum age of the local state that answers a hybrid read-only call.
///
/// `is_allowed` and `get_estimate` on the absolute hybrid limiter, and `get_suppression_factor`
/// and `get_estimate` on the suppressed hybrid limiter, answer from local state. When an
/// accepting state or the local entry of a key with no stored limit is older than this lag, the
/// call reads Redis again before it answers. Concurrent calls for the same key share one Redis
/// read. By default the lag does not apply to a cached rejection, which answers until its retry
/// TTL ends (up to one window). The builder setting `allowed_lag_applies_to_rejections(true)`
/// applies the lag to read-only answers from a cached rejection as well. The lag does not apply
/// to a cached suppression state, which answers until its cache period ends.
///
/// A read-only call records no activity. A key in accepting state that is only checked becomes
/// stale, and cleanup removes it. A key with a cached rejection or suppression state that is
/// checked after the cache ends gets a new cache from Redis each time, so cleanup keeps it while
/// it is checked.
///
/// `inc` does not use this lag. Its pending count reaches Redis at the next flush, as set by
/// [`SyncInterval`].
///
/// # Trade-offs
///
/// - **Shorter lag**: read-only answers see increments from other instances sooner, at the cost
///   of more Redis reads for keys that are checked often.
/// - **Longer lag**: fewer Redis reads, but a read-only answer can be older.
///
/// # Validation
///
/// Must be ≥ 1 millisecond. A value of `0` returns an error.
///
/// # Examples
///
/// ```
/// use trypema::hybrid::AllowedLag;
///
/// let lag = AllowedLag::default();
/// assert_eq!(lag.as_milliseconds(), 1_000);
///
/// let lag = AllowedLag::milliseconds(200).unwrap();
/// assert_eq!(lag.as_seconds(), 0.2);
///
/// let lag = AllowedLag::seconds_or_panic(5);
/// assert_eq!(lag.as_milliseconds(), 5_000);
///
/// // Invalid: 0ms
/// assert!(AllowedLag::milliseconds(0).is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct AllowedLag(u64);

impl Default for AllowedLag {
    /// Returns an allowed lag of 1 second.
    fn default() -> Self {
        Self(MILLISECONDS_PER_SECOND)
    }
}

impl AllowedLag {
    /// Return the allowed lag in milliseconds.
    pub fn as_milliseconds(self) -> u64 {
        self.0
    }

    /// Return the allowed lag in seconds.
    pub fn as_seconds(self) -> f64 {
        self.as_period(MILLISECONDS_PER_SECOND)
    }

    /// Return the allowed lag in minutes.
    pub fn as_minutes(self) -> f64 {
        self.as_period(MILLISECONDS_PER_SECOND * SECONDS_PER_MINUTE)
    }

    /// Return the allowed lag in hours.
    pub fn as_hours(self) -> f64 {
        self.as_period(MILLISECONDS_PER_SECOND * SECONDS_PER_HOUR)
    }

    /// Create an allowed lag expressed in milliseconds.
    pub fn milliseconds(value: u64) -> Result<Self, TrypemaError> {
        Self::from_milliseconds(value, 1)
    }

    /// Create an allowed lag expressed in milliseconds.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero.
    pub fn milliseconds_or_panic(value: u64) -> Self {
        Self::milliseconds(value).unwrap()
    }

    /// Create an allowed lag expressed in seconds.
    pub fn seconds(value: u64) -> Result<Self, TrypemaError> {
        Self::from_milliseconds(value, MILLISECONDS_PER_SECOND)
    }

    /// Create an allowed lag expressed in seconds.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero or conversion overflows.
    pub fn seconds_or_panic(value: u64) -> Self {
        Self::seconds(value).unwrap()
    }

    /// Create an allowed lag expressed in minutes.
    pub fn minutes(value: u64) -> Result<Self, TrypemaError> {
        Self::from_milliseconds(value, MILLISECONDS_PER_SECOND * SECONDS_PER_MINUTE)
    }

    /// Create an allowed lag expressed in minutes.
    ///
    /// # Panics
    ///
    /// Panics when `value` is zero or conversion overflows.
    pub fn minutes_or_panic(value: u64) -> Self {
        Self::minutes(value).unwrap()
    }

    /// Create an allowed lag expressed in hours.
    pub fn hours(value: u64) -> Result<Self, TrypemaError> {
        Self::from_milliseconds(value, MILLISECONDS_PER_SECOND * SECONDS_PER_HOUR)
    }

    /// Create an allowed lag expressed in hours.
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
            "allowed lag",
            TrypemaError::InvalidAllowedLag,
        )
        .map(Self)
    }

    fn as_period(self, period_milliseconds: u64) -> f64 {
        (self.0 as f64) / (period_milliseconds as f64)
    }
}
