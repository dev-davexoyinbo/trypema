use redis::{Script, aio::ConnectionManager};

use crate::{
    BucketSize, RateLimitComparator, TrypemaError, WindowSize,
    common::{HistoryUpdateMode, RateType},
    hybrid::RedisProxyCommitter,
    hybrid::common::StateRevision,
    redis::{
        RedisKey, RedisKeyGenerator,
        scripts::{
            ABSOLUTE_CLEANUP_LUA, ABSOLUTE_HYBRID_CLEAR_LUA, ABSOLUTE_HYBRID_COMMIT_STATE_LUA,
            ABSOLUTE_HYBRID_DELETE_LUA, ABSOLUTE_HYBRID_READ_STATE_LUA,
            ABSOLUTE_HYBRID_SET_RATE_LIMIT_LUA, ABSOLUTE_SET_IF_LUA, absolute_lua_script,
        },
    },
};

#[derive(Debug)]
pub(crate) struct AbsoluteHybridCommit {
    pub key: RedisKey,
    pub window_limit: f64,
    pub count: u64,
    pub state_revision: StateRevision,
}

#[derive(Debug)]
pub(crate) struct AbsoluteHybridRedisProxyReadStateResult {
    pub key: RedisKey,
    pub current_total_count: u64,
    pub window_limit: Option<f64>,
    pub oldest_bucket_ttl: Option<u64>,
    pub oldest_bucket_count: Option<u64>,
    pub state_revision: StateRevision,
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
    set_rate_limit_script: Script,
    delete_script: Script,
    clear_script: Script,
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
            set_rate_limit_script: absolute_lua_script(ABSOLUTE_HYBRID_SET_RATE_LIMIT_LUA),
            delete_script: absolute_lua_script(ABSOLUTE_HYBRID_DELETE_LUA),
            clear_script: absolute_lua_script(ABSOLUTE_HYBRID_CLEAR_LUA),
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

        let res: (String, u64, String, i64, i64, u64, u64) = self
            .read_state_script
            .key(self.key_generator.get_hash_key(key))
            .key(self.key_generator.get_active_keys(key))
            .key(self.key_generator.get_window_limit_key(key))
            .key(self.key_generator.get_total_count_key(key))
            .key(self.key_generator.get_active_entities_key())
            .key(self.key_generator.get_state_revision_key())
            .key(self.key_generator.get_key_state_revisions_key())
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
                    .key(self.key_generator.get_state_revision_key())
                    .key(self.key_generator.get_key_state_revisions_key())
                    .arg(commit.key.as_str())
                    .arg(self.window_size.as_seconds())
                    .arg(commit.window_limit)
                    .arg(self.bucket_size.as_milliseconds())
                    .arg(commit.count)
                    .arg(commit.state_revision.namespace)
                    .arg(commit.state_revision.key),
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
                .query_async::<Vec<(String, u64, String, i64, i64, u64, u64)>>(
                    &mut connection_manager,
                )
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
                        .query_async::<Vec<(String, u64, String, i64, i64, u64, u64)>>(
                            &mut connection_manager,
                        )
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
                    .key(self.key_generator.get_state_revision_key())
                    .key(self.key_generator.get_key_state_revisions_key())
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
        window_limit: f64,
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

    pub(crate) async fn set_rate_limit(
        &self,
        key: &RedisKey,
        window_limit: f64,
    ) -> Result<Option<(f64, bool)>, TrypemaError> {
        let mut connection_manager = self.connection_manager.clone();

        let (status, previous, changed): (String, String, u8) = self
            .set_rate_limit_script
            .key(self.key_generator.get_window_limit_key(key))
            .key(self.key_generator.get_active_entities_key())
            .key(self.key_generator.get_key_state_revisions_key())
            .arg(key.as_str())
            .arg(self.window_size.as_seconds())
            .arg(window_limit)
            .invoke_async(&mut connection_manager)
            .await?;

        match status.as_str() {
            "missing" => Ok(None),
            "found" => previous
                .parse::<f64>()
                .map(|previous| Some((previous, changed == 1)))
                .map_err(|_| {
                    TrypemaError::CustomError(
                        "invalid stored absolute hybrid window limit".to_string(),
                    )
                }),
            "invalid" => Err(TrypemaError::CustomError(
                "invalid stored absolute hybrid window limit".to_string(),
            )),
            _ => Err(TrypemaError::UnexpectedRedisScriptResult {
                operation: "absolute_hybrid.set_rate_limit",
                key: key.to_string(),
                result: status,
            }),
        }
    }

    pub(crate) async fn delete(
        &self,
        key: &RedisKey,
        pending_count: u64,
        cached_committed_count: u64,
        state_revision: StateRevision,
        local_existed: bool,
    ) -> Result<Option<u64>, TrypemaError> {
        let mut connection_manager = self.connection_manager.clone();

        let (existed, total_count): (u8, u64) = self
            .delete_script
            .key(self.key_generator.get_hash_key(key))
            .key(self.key_generator.get_active_keys(key))
            .key(self.key_generator.get_window_limit_key(key))
            .key(self.key_generator.get_total_count_key(key))
            .key(self.key_generator.get_active_entities_key())
            .key(self.key_generator.get_key_state_revisions_key())
            .key(self.key_generator.get_state_revision_key())
            .arg(key.as_str())
            .arg(self.window_size.as_seconds())
            .arg(pending_count)
            .arg(cached_committed_count)
            .arg(state_revision.namespace)
            .arg(state_revision.key)
            .arg(u8::from(local_existed))
            .invoke_async(&mut connection_manager)
            .await?;

        Ok((existed == 1).then_some(total_count))
    }

    pub(crate) async fn clear(&self) -> Result<(), TrypemaError> {
        let mut connection_manager = self.connection_manager.clone();

        let _: () = self
            .clear_script
            .key(self.key_generator.get_active_entities_key())
            .key(self.key_generator.get_state_revision_key())
            .key(self.key_generator.get_key_state_revisions_key())
            .arg(self.key_generator.prefix.to_string())
            .arg(self.key_generator.rate_type.to_string())
            .arg(self.key_generator.hash_key_suffix.to_string())
            .arg(self.key_generator.active_keys_key_suffix.to_string())
            .arg(self.key_generator.window_limit_key_suffix.to_string())
            .arg(self.key_generator.total_count_key_suffix.to_string())
            .invoke_async(&mut connection_manager)
            .await?;

        Ok(())
    }

    /// Evict expired buckets and update the total count.
    pub(crate) async fn cleanup(&self, stale_after_ms: u64) -> Result<(), TrypemaError> {
        let mut connection_manager = self.connection_manager.clone();

        // One script call removes a limited number of stale keys. The first call sets the
        // cutoff of the pass, so that new activity cannot make the pass longer.
        let mut cutoff_ms = 0u64;

        loop {
            cutoff_ms = self
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
                .arg(cutoff_ms)
                .invoke_async(&mut connection_manager)
                .await?;

            if cutoff_ms == 0 {
                return Ok(());
            }
        }
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
    (
        entity,
        total_count,
        window_limit,
        oldest_ttl,
        oldest_count,
        state_revision,
        key_state_revision,
    ): (String, u64, String, i64, i64, u64, u64),
) -> AbsoluteHybridRedisProxyReadStateResult {
    fn map_negative_to_none(value: i64) -> Option<u64> {
        if value < 0 { None } else { Some(value as u64) }
    }

    let window_limit = window_limit
        .parse::<f64>()
        .ok()
        .filter(|window_limit| *window_limit >= 0.0);

    AbsoluteHybridRedisProxyReadStateResult {
        key: RedisKey::from(entity),
        current_total_count: total_count,
        window_limit,
        oldest_bucket_ttl: map_negative_to_none(oldest_ttl),
        oldest_bucket_count: map_negative_to_none(oldest_count),
        state_revision: StateRevision {
            namespace: state_revision,
            key: key_state_revision,
        },
    }
}
