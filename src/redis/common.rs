use std::ops::{Deref, DerefMut};

use crate::TrypemaError;

/// A validated newtype for Redis keys.
///
/// This is a string with the following constraints:
/// - Must not be empty
/// - Must not contain colons
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct RedisKey(String);

impl RedisKey {
    /// Create a new default prefix.
    pub fn default_prefix() -> Self {
        Self("trypema".to_string())
    }
}

impl Deref for RedisKey {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RedisKey {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl TryFrom<String> for RedisKey {
    type Error = TrypemaError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() {
            Err(TrypemaError::InvalidRedisKey(
                "Redis key must not be empty".to_string(),
            ))
        } else if value.len() > 255 {
            Err(TrypemaError::InvalidRedisKey(
                "Redis key must not be longer than 255 characters".to_string(),
            ))
        } else if value.contains(":") {
            Err(TrypemaError::InvalidRedisKey(
                "Redis key must not contain colons".to_string(),
            ))
        } else {
            Ok(Self(value))
        }
    }
}
