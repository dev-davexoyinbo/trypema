use std::ops::{Deref, DerefMut};

use crate::TrypemaError;

/// A signal to the Redis rate limiter to flush its local cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum RedisRateLimiterSignal {
    /// Flush the local cache.
    Flush,
}

/// Sync interval (milliseconds) for the hybrid provider's background flush.
///
/// The hybrid provider batches local increments and periodically commits them to Redis.
/// Smaller values reduce sync lag (but increase Redis write frequency); larger values reduce Redis
/// write frequency (but increase how stale Redis state can be between flushes).
///
/// It is generally recommended to keep `sync_interval_ms` <= `rate_group_size_ms`.
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
