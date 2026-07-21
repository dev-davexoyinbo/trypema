use redis::{Script, aio::ConnectionManager};

use crate::{
    BucketSize, RateLimitComparator, TrypemaError, WindowSize,
    common::{HistoryUpdateMode, RateType},
    hybrid::RedisProxyCommitter,
    redis::{
        RedisKey, RedisKeyGenerator,
        scripts::{
            ABSOLUTE_CLEANUP_LUA, ABSOLUTE_HYBRID_COMMIT_STATE_LUA, ABSOLUTE_HYBRID_READ_STATE_LUA,
            ABSOLUTE_SET_IF_LUA, absolute_lua_script,
        },
    },
};

#[derive(Debug)]
pub(crate) struct AbsoluteHybridCommit {
    pub key: RedisKey,
    pub window_limit: u64,
    pub count: u64,
}

#[derive(Debug)]
pub(crate) struct AbsoluteHybridRedisProxyReadStateResult {
    pub key: RedisKey,
    pub current_total_count: u64,
    pub window_limit: Option<u64>,
    pub oldest_bucket_ttl: Option<u64>,
    pub oldest_bucket_count: Option<u64>,
}

pub(crate) struct AbsoluteHybridRedisProxyOptions {
    pub prefix: RedisKey,
    pub connection_manager: ConnectionManager,
    pub window_size: WindowSize,
    pub bucket_size: BucketSize,
}

#[derive(Clone, Debug)]
pub(crate) struct AbsoluteHybridRedisProxy {
    key_generator: RedisKeyGenerator,
    read_state_script: Script,
    commit_state_script: Script,
    set_if_script: Script,
    cleanup_script: Script,
    connection_manager: ConnectionManager,
    read_chunk_size: usize,
    window_size: WindowSize,
    bucket_size: BucketSize,
    window_size_ms: u128,
}

impl AbsoluteHybridRedisProxy {
    pub(crate) fn new(options: AbsoluteHybridRedisProxyOptions) -> Self {
        let AbsoluteHybridRedisProxyOptions {
            prefix,
            connection_manager,
            window_size,
            bucket_size,
        } = options;

        Self {
            key_generator: RedisKeyGenerator::new(prefix, RateType::HybridAbsolute),
            read_state_script: absolute_lua_script(ABSOLUTE_HYBRID_READ_STATE_LUA),
            commit_state_script: absolute_lua_script(ABSOLUTE_HYBRID_COMMIT_STATE_LUA),
            set_if_script: absolute_lua_script(ABSOLUTE_SET_IF_LUA),
            cleanup_script: absolute_lua_script(ABSOLUTE_CLEANUP_LUA),
            connection_manager,
            read_chunk_size: 100,
            window_size_ms: window_size.as_milliseconds(),
            window_size,
            bucket_size,
        }
    }

    pub(crate) async fn read_state(
        self: &AbsoluteHybridRedisProxy,
        key: &RedisKey,
    ) -> Result<AbsoluteHybridRedisProxyReadStateResult, TrypemaError> {
        let mut connection_manager = self.connection_manager.clone();

        let res: (String, u64, i64, i64, i64) = self
            .read_state_script
            .key(self.key_generator.get_hash_key(key))
            .key(self.key_generator.get_active_keys(key))
            .key(self.key_generator.get_window_limit_key(key))
            .key(self.key_generator.get_total_count_key(key))
            .key(self.key_generator.get_active_entities_key())
            .arg(key.as_str())
            .arg(self.window_size_ms)
            .invoke_async(&mut connection_manager)
            .await?;

        Ok(map_redis_read_result_to_state(res))
    } // end method read_state

    #[inline]
    fn build_commit_pipeline(
        &self,
        commits: &[AbsoluteHybridCommit],
        should_load_script: bool,
    ) -> redis::Pipeline {
        let mut pipe = redis::Pipeline::new();
        if should_load_script {
            pipe.load_script(&self.commit_state_script).ignore();
        }

        for commit in commits {
            pipe.invoke_script(
                self.commit_state_script
                    .key(self.key_generator.get_hash_key(&commit.key))
                    .key(self.key_generator.get_active_keys(&commit.key))
                    .key(self.key_generator.get_window_limit_key(&commit.key))
                    .key(self.key_generator.get_total_count_key(&commit.key))
                    .key(self.key_generator.get_active_entities_key())
                    .arg(commit.key.as_str())
                    .arg(self.window_size.as_seconds())
                    .arg(commit.window_limit)
                    .arg(self.bucket_size.as_milliseconds())
                    .arg(commit.count),
            );
        }

        pipe
    }

    pub(crate) async fn batch_read_state(
        self: &AbsoluteHybridRedisProxy,
        keys: &[RedisKey],
    ) -> Result<Vec<AbsoluteHybridRedisProxyReadStateResult>, TrypemaError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut connection_manager = self.connection_manager.clone();

        let chunk_size = self.read_chunk_size.max(1);
        let mut all_results: Vec<AbsoluteHybridRedisProxyReadStateResult> =
            Vec::with_capacity(keys.len());

        for chunk in keys.chunks(chunk_size) {
            let pipe = self.build_read_pipeline(chunk, false);

            let results = match pipe
                .query_async::<Vec<(String, u64, i64, i64, i64)>>(&mut connection_manager)
                .await
            {
                Ok(results) => results,
                Err(err) => {
                    if err.kind() != redis::ErrorKind::Server(redis::ServerErrorKind::NoScript) {
                        tracing::error!("redis.read.error, error executing pipeline: {:?}", err);
                        return Err(TrypemaError::RedisError(err));
                    }

                    let pipe = self.build_read_pipeline(chunk, true);

                    match pipe
                        .query_async::<Vec<(String, u64, i64, i64, i64)>>(&mut connection_manager)
                        .await
                    {
                        Ok(results) => results,
                        Err(err) => {
                            tracing::error!(
                                "redis.read.error, error executing pipeline: {:?}",
                                err
                            );
                            return Err(TrypemaError::RedisError(err));
                        }
                    }
                }
            };

            all_results.extend(results.into_iter().map(map_redis_read_result_to_state));
        }

        Ok(all_results)
    } // end method batch_commit_state

    #[inline]
    fn build_read_pipeline(&self, keys: &[RedisKey], should_load_script: bool) -> redis::Pipeline {
        let mut pipe = redis::Pipeline::new();
        if should_load_script {
            pipe.load_script(&self.read_state_script).ignore();
        }

        for key in keys {
            pipe.invoke_script(
                self.read_state_script
                    .key(self.key_generator.get_hash_key(key))
                    .key(self.key_generator.get_active_keys(key))
                    .key(self.key_generator.get_window_limit_key(key))
                    .key(self.key_generator.get_total_count_key(key))
                    .key(self.key_generator.get_active_entities_key())
                    .arg(key.as_str())
                    .arg(self.window_size_ms),
            );
        }

        pipe
    }

    /// Conditionally replace the window total for `key`.
    ///
    /// Atomically computes the logical live total without writing and evaluates the
    /// comparator. On a match, the script prunes expired buckets and applies the
    /// requested history mode; `window_limit` is (re)written and its TTL refreshed.
    /// A miss performs no writes.
    ///
    /// Returns `(new_total, old_total)` where `old_total` is the post-eviction total
    /// the comparator was evaluated against and `new_total` is the total after the
    /// operation (`count` when matched, `old_total` otherwise).
    pub(crate) async fn set_if(
        &self,
        key: &RedisKey,
        window_limit: u64,
        comparator: RateLimitComparator,
        count: u64,
        mode: HistoryUpdateMode,
        pending_count: u64,
    ) -> Result<(u64, u64, bool), TrypemaError> {
        let mut connection_manager = self.connection_manager.clone();

        let (comparator_op, comparator_operand) = comparator.redis_args();

        let (new_total, old_total, changed): (u64, u64, u64) = self
            .set_if_script
            .key(self.key_generator.get_hash_key(key))
            .key(self.key_generator.get_active_keys(key))
            .key(self.key_generator.get_window_limit_key(key))
            .key(self.key_generator.get_total_count_key(key))
            .key(self.key_generator.get_active_entities_key())
            .arg(key.as_str())
            .arg(self.window_size.as_seconds())
            .arg(window_limit)
            .arg(comparator_op)
            .arg(comparator_operand)
            .arg(count)
            .arg(mode.redis_arg())
            .arg(pending_count)
            .invoke_async(&mut connection_manager)
            .await?;

        Ok((new_total, old_total, changed != 0))
    } // end method set_if

    /// Evict expired buckets and update the total count.
    pub(crate) async fn cleanup(&self, stale_after_ms: u64) -> Result<(), TrypemaError> {
        let mut connection_manager = self.connection_manager.clone();

        let _: () = self
            .cleanup_script
            .key(self.key_generator.prefix.to_string())
            .key(self.key_generator.rate_type.to_string())
            .key(self.key_generator.get_active_entities_key())
            .arg(stale_after_ms)
            .arg(self.key_generator.hash_key_suffix.to_string())
            .arg(self.key_generator.window_limit_key_suffix.to_string())
            .arg(self.key_generator.total_count_key_suffix.to_string())
            .arg(self.key_generator.active_keys_key_suffix.to_string())
            .arg(self.key_generator.suppression_factor_key_suffix.to_string())
            .invoke_async(&mut connection_manager)
            .await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl RedisProxyCommitter<AbsoluteHybridCommit> for AbsoluteHybridRedisProxy {
    async fn batch_commit_state(
        self: &AbsoluteHybridRedisProxy,
        commits: &[AbsoluteHybridCommit],
    ) -> Result<(), TrypemaError> {
        let mut connection_manager = self.connection_manager.clone();

        let pipe = self.build_commit_pipeline(commits, false);

        let _: () = match pipe.query_async(&mut connection_manager).await {
            Ok(results) => results,
            Err(err) => {
                if err.kind() != redis::ErrorKind::Server(redis::ServerErrorKind::NoScript) {
                    tracing::error!("redis.commit.error, error executing pipeline: {:?}", err);
                    return Err(TrypemaError::RedisError(err));
                }

                let pipe = self.build_commit_pipeline(commits, true);

                match pipe.query_async::<()>(&mut connection_manager).await {
                    Ok(results) => results,
                    Err(err) => {
                        tracing::error!("redis.commit.error, error executing pipeline: {:?}", err);
                        return Err(TrypemaError::RedisError(err));
                    }
                }
            }
        };

        Ok(())
    } // end method batch_commit_state
}

fn map_redis_read_result_to_state(
    (entity, total_count, window_limit, oldest_ttl, oldest_count): (String, u64, i64, i64, i64),
) -> AbsoluteHybridRedisProxyReadStateResult {
    fn map_negative_to_none(value: i64) -> Option<u64> {
        if value < 0 { None } else { Some(value as u64) }
    }

    AbsoluteHybridRedisProxyReadStateResult {
        key: RedisKey::from(entity),
        current_total_count: total_count,
        window_limit: map_negative_to_none(window_limit),
        oldest_bucket_ttl: map_negative_to_none(oldest_ttl),
        oldest_bucket_count: map_negative_to_none(oldest_count),
    }
}
