use std::ops::{Deref, DerefMut};

use crate::TrypemaError;

/// A signal to the Redis rate limiter to flush its local cache.
pub enum RedisRateLimiterSignal {
    /// Flush the local cache.
    Flush,
}

///. It is suggested that the sync interval be lesser than the rate group size.
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
