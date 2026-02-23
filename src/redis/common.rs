use std::ops::{Deref, DerefMut};

use crate::{TrypemaError, common::RateType};

/// A validated newtype for Redis keys.
///
/// This is a string with the following constraints:
/// - Must not be empty
/// - Must not contain colons
#[derive(Debug, Clone, PartialEq, PartialOrd, Hash, Eq)]
pub struct RedisKey(String);
// TODO: make RedisKey Arc<str> to avoid cloning

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

#[derive(Clone, Debug)]
pub(crate) struct RedisKeyGenerator {
    pub prefix: RedisKey,
    pub rate_type: RateType,
    pub active_entities_key_suffix: String,
    pub hash_key_suffix: String,
    pub window_limit_key_suffix: String,
    pub total_count_key_suffix: String,
    pub active_keys_key_suffix: String,
    pub suppression_factor_key_suffix: String,
}

impl RedisKeyGenerator {
    pub(crate) fn new(prefix: RedisKey, rate_type: RateType) -> Self {
        Self {
            prefix,
            rate_type,
            active_entities_key_suffix: "active_entities".to_string(),
            hash_key_suffix: "h".to_string(),
            window_limit_key_suffix: "w".to_string(),
            total_count_key_suffix: "t".to_string(),
            active_keys_key_suffix: "a".to_string(),
            suppression_factor_key_suffix: "sf".to_string(),
        }
    }

    fn get_key_with_suffix(&self, key: &RedisKey, suffix: &str) -> String {
        format!("{}:{}:{}:{}", *self.prefix, **key, self.rate_type, suffix)
    }

    pub(crate) fn get_hash_key(&self, key: &RedisKey) -> String {
        self.get_key_with_suffix(key, &self.hash_key_suffix)
    }

    pub(crate) fn get_window_limit_key(&self, key: &RedisKey) -> String {
        self.get_key_with_suffix(key, &self.window_limit_key_suffix)
    }

    pub(crate) fn get_total_count_key(&self, key: &RedisKey) -> String {
        self.get_key_with_suffix(key, &self.total_count_key_suffix)
    }

    pub(crate) fn get_active_keys(&self, key: &RedisKey) -> String {
        self.get_key_with_suffix(key, &self.active_keys_key_suffix)
    }

    pub(crate) fn get_active_entities_key(&self) -> String {
        format!("{}:{}", *self.prefix, self.active_entities_key_suffix)
    }

    pub(crate) fn get_suppression_factor_key(&self, key: &RedisKey) -> String {
        self.get_key_with_suffix(key, &self.suppression_factor_key_suffix)
    }
}
