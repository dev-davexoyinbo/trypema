use std::{
    hash::Hash,
    ops::{Deref, DerefMut},
    sync::Mutex,
};

use dashmap::{DashMap, mapref::one::RefMut, try_result::TryResult};

use crate::{TrypemaError, common::RateType};

/// A validated newtype for Redis rate limiting keys.
///
/// All Redis and hybrid provider operations require keys wrapped in this type. Validation
/// is performed at construction time via `TryFrom<String>`.
///
/// # Validation Rules
///
/// - **Must not be empty** — an empty key has no semantic meaning
/// - **Must be ≤ 255 bytes** — prevents excessively long Redis keys
/// - **Must not contain `:` (colon)** — colons are used internally as key separators
///   (e.g., `{prefix}:{user_key}:{rate_type}:{suffix}`)
///
/// # Examples
///
/// ```
/// use trypema::redis::RedisKey;
///
/// // Valid keys
/// let key = RedisKey::try_from("user_123".to_string()).unwrap();
/// let key = RedisKey::try_from("api_v2_endpoint".to_string()).unwrap();
///
/// // Invalid: contains ':'
/// assert!(RedisKey::try_from("user:123".to_string()).is_err());
///
/// // Invalid: empty string
/// assert!(RedisKey::try_from("".to_string()).is_err());
///
/// // Invalid: too long (> 255 bytes)
/// assert!(RedisKey::try_from("a".repeat(256)).is_err());
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Hash, Eq)]
pub struct RedisKey(String);

impl RedisKey {
    /// Create a new default prefix.
    pub fn default_prefix() -> Self {
        Self("trypema".to_string())
    }

    pub(crate) fn from(value: String) -> Self {
        Self(value)
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
    pub hash_declined_key_suffix: String,
    pub window_limit_key_suffix: String,
    pub total_count_key_suffix: String,
    pub total_declined_key_suffix: String,
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
            hash_declined_key_suffix: "hd".to_string(),
            window_limit_key_suffix: "w".to_string(),
            total_count_key_suffix: "t".to_string(),
            total_declined_key_suffix: "d".to_string(),
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

    pub(crate) fn get_hash_declined_key(&self, key: &RedisKey) -> String {
        self.get_key_with_suffix(key, &self.hash_declined_key_suffix)
    }

    pub(crate) fn get_window_limit_key(&self, key: &RedisKey) -> String {
        self.get_key_with_suffix(key, &self.window_limit_key_suffix)
    }

    pub(crate) fn get_total_count_key(&self, key: &RedisKey) -> String {
        self.get_key_with_suffix(key, &self.total_count_key_suffix)
    }

    pub(crate) fn get_total_declined_key(&self, key: &RedisKey) -> String {
        self.get_key_with_suffix(key, &self.total_declined_key_suffix)
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

pub(crate) fn mutex_lock<'a, T>(
    m: &'a Mutex<T>,
    what: &'static str,
) -> Result<std::sync::MutexGuard<'a, T>, TrypemaError> {
    m.lock()
        .map_err(|_| TrypemaError::CustomError(format!("mutex poisoned: {what}")))
}

pub(crate) const _GET_MUT_TIMEOUT_MS: u64 = 3;

pub(crate) async fn _try_get_mut_async_with_timeout<'a, K, V>(
    map: &'a DashMap<K, V>,
    key: &K,
    timeout_ms: u64,
) -> Option<RefMut<'a, K, V>>
where
    K: Eq + Hash,
{
    let start = std::time::Instant::now();
    loop {
        match map.try_get_mut(key) {
            TryResult::Present(v) => return Some(v),
            TryResult::Absent => return None,
            TryResult::Locked => {
                if start.elapsed().as_millis() >= timeout_ms as u128 {
                    return None;
                }
                crate::runtime::_yield_now().await;
            }
        }
    }
}
