use redis::{Script, aio::ConnectionManager};

use crate::{
    BucketSize, HardLimitFactor, RateLimitComparator, SuppressionFactorCachePeriod, TrypemaError,
    WindowSize,
    common::{HistoryUpdateMode, RateType},
    hybrid::RedisProxyCommitter,
    redis::{
        RedisKey, RedisKeyGenerator,
        scripts::{
            SUPPRESSED_CLEANUP_LUA, SUPPRESSED_HYBRID_COMMIT_STATE_LUA,
            SUPPRESSED_HYBRID_READ_STATE_LUA, SUPPRESSED_SET_IF_LUA, lua_script,
            suppressed_lua_script,
        },
    },
};

#[derive(Debug)]
pub(crate) struct SuppressedHybridCommit {
    pub key: RedisKey,
    pub hard_window_limit: f64,
    pub count: u64,
    pub declined_count: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SuppressedHybridPendingState {
    pub pending_count: u64,
    pub pending_declined_count: u64,
}

#[derive(Debug)]
pub(crate) struct SuppressedHybridRedisProxyReadStateResult {
    pub key: RedisKey,
    pub current_total_count: u64,
    pub current_total_declined: u64,
    pub suppression_factor: f64,
    pub suppression_factor_ttl_ms: Option<u64>,
    pub hard_window_limit: Option<f64>,
}

#[derive(Clone, Debug)]
pub(crate) struct SuppressedHybridRedisProxy {
    key_generator: RedisKeyGenerator,
    read_state_script: Script,
    commit_state_script: Script,
    set_if_script: Script,
    cleanup_script: Script,
    connection_manager: ConnectionManager,
    hard_limit_factor: HardLimitFactor,
    suppression_factor_cache_period: SuppressionFactorCachePeriod,
    bucket_size: BucketSize,
    window_size: WindowSize,
    read_chunk_size: usize,
}

pub(crate) struct SuppressedHybridRedisProxyOptions {
    pub hard_limit_factor: HardLimitFactor,
    pub suppression_factor_cache_period: SuppressionFactorCachePeriod,
    pub bucket_size: BucketSize,
    pub window_size: WindowSize,
    pub prefix: RedisKey,
    pub connection_manager: ConnectionManager,
}

impl SuppressedHybridRedisProxy {
    pub(crate) fn new(
        SuppressedHybridRedisProxyOptions {
            prefix,
            connection_manager,
            hard_limit_factor,
            suppression_factor_cache_period,
            bucket_size,
            window_size,
        }: SuppressedHybridRedisProxyOptions,
    ) -> Self {
        Self {
            key_generator: RedisKeyGenerator::new(prefix, RateType::HybridSuppressed),
            read_state_script: suppressed_lua_script(SUPPRESSED_HYBRID_READ_STATE_LUA),
            commit_state_script: suppressed_lua_script(SUPPRESSED_HYBRID_COMMIT_STATE_LUA),
            set_if_script: lua_script(SUPPRESSED_SET_IF_LUA),
            cleanup_script: lua_script(SUPPRESSED_CLEANUP_LUA),
            hard_limit_factor,
            suppression_factor_cache_period,
            connection_manager,
            bucket_size,
            window_size,
            read_chunk_size: 100,
        }
    }

    pub(crate) async fn read_state(
        self: &SuppressedHybridRedisProxy,
        key: &RedisKey,
    ) -> Result<SuppressedHybridRedisProxyReadStateResult, TrypemaError> {
        let mut connection_manager = self.connection_manager.clone();

        let res: (String, f64, u64, u64, f64, i64) = self
            .read_state_script
            .key(self.key_generator.get_hash_key(key))
            .key(self.key_generator.get_active_keys(key))
            .key(self.key_generator.get_window_limit_key(key))
            .key(self.key_generator.get_total_count_key(key))
            .key(self.key_generator.get_active_entities_key())
            .key(self.key_generator.get_suppression_factor_key(key))
            .key(self.key_generator.get_total_declined_key(key))
            .key(self.key_generator.get_hash_declined_key(key))
            .arg(key.as_str())
            .arg(*self.window_size)
            .arg(*self.suppression_factor_cache_period)
            .arg(*self.hard_limit_factor)
            .invoke_async(&mut connection_manager)
            .await?;

        Ok(map_redis_read_result_to_state(res))
    } // end method read_state

    #[inline]
    fn build_commit_pipeline(
        &self,
        commits: &[SuppressedHybridCommit],
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
                    .key(self.key_generator.get_suppression_factor_key(&commit.key))
                    .key(self.key_generator.get_total_declined_key(&commit.key))
                    .key(self.key_generator.get_hash_declined_key(&commit.key))
                    .arg(commit.key.as_str())
                    .arg(*self.window_size)
                    .arg(commit.hard_window_limit)
                    .arg(*self.bucket_size)
                    .arg(*self.suppression_factor_cache_period)
                    .arg(*self.hard_limit_factor)
                    .arg(commit.count)
                    .arg(commit.declined_count),
            );
        }

        pipe
    }

    pub(crate) async fn batch_read_state(
        self: &SuppressedHybridRedisProxy,
        keys: &[RedisKey],
    ) -> Result<Vec<SuppressedHybridRedisProxyReadStateResult>, TrypemaError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut connection_manager = self.connection_manager.clone();

        let chunk_size = self.read_chunk_size.max(1);
        let mut all_results: Vec<SuppressedHybridRedisProxyReadStateResult> =
            Vec::with_capacity(keys.len());

        for chunk in keys.chunks(chunk_size) {
            let pipe = self.build_read_pipeline(chunk, false);

            let results = match pipe
                .query_async::<Vec<(String, f64, u64, u64, f64, i64)>>(&mut connection_manager)
                .await
            {
                Ok(results) => results,
                Err(err) => {
                    if err.kind() != redis::ErrorKind::Server(redis::ServerErrorKind::NoScript) {
                        tracing::error!("redis.read.error, error executing pipeline: {:?}", err);
                        eprintln!("redis.read.error, error executing pipeline: {:?}", err);
                        return Err(TrypemaError::RedisError(err));
                    }

                    let pipe = self.build_read_pipeline(chunk, true);

                    match pipe
                        .query_async::<Vec<(String, f64, u64, u64, f64, i64)>>(
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
                            eprintln!("redis.read.error, error executing pipeline: {:?}", err);
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
                    .key(self.key_generator.get_suppression_factor_key(key))
                    .key(self.key_generator.get_total_declined_key(key))
                    .key(self.key_generator.get_hash_declined_key(key))
                    .arg(key.as_str())
                    .arg(*self.window_size)
                    .arg(*self.suppression_factor_cache_period)
                    .arg(*self.hard_limit_factor),
            );
        }

        pipe
    }

    /// Conditionally replace the window total for `key`.
    ///
    /// Runs the shared suppressed `set_if` script: compute the logical live total
    /// without writing, evaluate the comparator, and apply the requested history mode
    /// only on a match. Matched replacement resets declined counters; preservation
    /// retains proportional declines. The hard window limit is (re)written and suppression
    /// cache invalidated only when state changes. A miss performs no writes. Returns
    /// `(new_total, old_total, changed)`.
    pub(crate) async fn set_if(
        &self,
        key: &RedisKey,
        hard_window_limit: f64,
        comparator: RateLimitComparator,
        count: u64,
        mode: HistoryUpdateMode,
        pending: SuppressedHybridPendingState,
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
            .key(self.key_generator.get_suppression_factor_key(key))
            .key(self.key_generator.get_total_declined_key(key))
            .key(self.key_generator.get_hash_declined_key(key))
            .arg(key.as_str())
            .arg(*self.window_size)
            .arg(hard_window_limit)
            .arg(comparator_op)
            .arg(comparator_operand)
            .arg(count)
            .arg(mode.redis_arg())
            .arg(pending.pending_count)
            .arg(pending.pending_declined_count)
            .invoke_async(&mut connection_manager)
            .await?;

        Ok((new_total, old_total, changed != 0))
    } // end method set_if

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
            .arg(self.key_generator.total_declined_key_suffix.to_string())
            .arg(self.key_generator.hash_declined_key_suffix.to_string())
            .invoke_async(&mut connection_manager)
            .await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl RedisProxyCommitter<SuppressedHybridCommit> for SuppressedHybridRedisProxy {
    async fn batch_commit_state(
        self: &SuppressedHybridRedisProxy,
        commits: &[SuppressedHybridCommit],
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
        suppression_factor,
        current_total_count,
        current_total_declined,
        hard_window_limit,
        suppression_factor_ttl_ms,
    ): (String, f64, u64, u64, f64, i64),
) -> SuppressedHybridRedisProxyReadStateResult {
    SuppressedHybridRedisProxyReadStateResult {
        key: RedisKey::from(entity),
        current_total_count,
        current_total_declined,
        suppression_factor,
        hard_window_limit: if hard_window_limit < 0.0 {
            None
        } else {
            Some(hard_window_limit)
        },
        suppression_factor_ttl_ms: if suppression_factor_ttl_ms < 0 {
            None
        } else {
            Some(suppression_factor_ttl_ms as u64)
        },
    }
}
