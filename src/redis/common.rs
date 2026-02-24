use std::{
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use redis::{Client, aio::ConnectionManager};

use crate::{TrypemaError, common::RateType};

/// A wrapper for a vector of [`redis::aio::ConnectionManager`]s.
#[derive(Debug)]
pub struct TrypemaRedisClient {
    connection_managers: Arc<Vec<ConnectionManager>>,
    track_index: AtomicUsize,
}

impl TrypemaRedisClient {
    /// Create a new [`RedisClient`] from a single [`redis::aio::ConnectionManager`].
    pub async fn default_from_client(client: Client) -> Result<Self, TrypemaError> {
        Self::from_client(client, 1).await
    }

    /// Create a new [`RedisClient`] from a vector of [`redis::aio::ConnectionManager`]s.
    pub async fn from_client(
        client: Client,
        connection_count: usize,
    ) -> Result<Self, TrypemaError> {
        if connection_count == 0 {
            return Err(TrypemaError::InvalidRedisClientConnectionCount(
                "connection count must be > 0".to_string(),
            ));
        }

        let mut connection_managers = Vec::with_capacity(connection_count);

        for _ in 0..connection_count {
            connection_managers.push(client.get_connection_manager().await?);
        }

        Ok(Self {
            connection_managers: Arc::new(connection_managers),
            track_index: AtomicUsize::new(0),
        })
    }

    /// Get a [`redis::aio::ConnectionManager`] from the client.
    pub(crate) fn get(&self) -> ConnectionManager {
        let index = self.track_index.fetch_add(1, Ordering::Relaxed);
        self.connection_managers[index % self.connection_managers.len()].clone()
    } // end method get
} // end impl RedisClient

impl Clone for TrypemaRedisClient {
    fn clone(&self) -> Self {
        Self {
            connection_managers: self.connection_managers.clone(),
            track_index: AtomicUsize::new(0),
        }
    }
}

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
