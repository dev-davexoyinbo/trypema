use async_channel::Sender;
use redis::{RedisError, Script, aio::ConnectionManager};

use crate::{RateGroupSizeMs, WindowSizeSeconds, redis::RedisKey};

const ABSOLUTE_COMMIT_LUA: &str = r#"
    local hash_key = KEYS[1]
    local active_keys = KEYS[2]
    local window_limit_key = KEYS[3]
    local total_count_key = KEYS[4]
    local active_entities_key = KEYS[5]

    local entity = ARGV[1]
    local window_size_seconds = tonumber(ARGV[2])
    local window_limit = tonumber(ARGV[3])
    local rate_group_size_ms = tonumber(ARGV[4])
    local count = tonumber(ARGV[5])
    local timestamp_ms = tonumber(ARGV[6])


    -- evict expired buckets
    local to_remove_keys = redis.call("ZRANGE", active_keys, "-inf", timestamp_ms - window_size_seconds * 1000, "BYSCORE")
    if #to_remove_keys > 0 then
        local to_remove = redis.call("HMGET", hash_key, unpack(to_remove_keys))
        redis.call("HDEL", hash_key, unpack(to_remove_keys))

        local remove_sum = 0

        for i = 1, #to_remove do
            remove_sum = remove_sum + (tonumber(to_remove[i]) or 0)
        end

        redis.call("DECRBY", total_count_key, remove_sum)
        redis.call("ZREM", active_keys, unpack(to_remove_keys))
    end

    --group bucketing
    local latest_hash_field_entry = redis.call("ZRANGE", active_keys, 0, 0, "REV", "WITHSCORES")
    if #latest_hash_field_entry > 0 then
        local age_ms = timestamp_ms - tonumber(latest_hash_field_entry[2])
        
        if age_ms > 0 and age_ms < rate_group_size_ms then
            timestamp_ms = tonumber(latest_hash_field_entry[1])
        end
    end

    local hash_field = tostring(timestamp_ms)
    local new_count = redis.call("HINCRBY", hash_key, hash_field, count)
    redis.call("INCRBY", total_count_key, count)

    if new_count == count then
        redis.call("ZADD", active_keys, timestamp_ms, hash_field)
        redis.call("SET", window_limit_key, window_limit)
    end

    redis.call("EXPIRE", window_limit_key, window_size_seconds)
    redis.call("ZADD", active_entities_key, timestamp_ms, entity)

    return 1
"#;

pub(crate) struct AbsoluteRedisCommit {
    // keys
    pub hash_key: String,
    pub active_keys_key: String,
    pub window_limit_key: String,
    pub total_count_key: String,
    pub active_entities_key: String,
    // args
    pub entity_key: RedisKey,
    pub window_size_seconds: WindowSizeSeconds,
    pub window_limit: f64,
    pub rate_group_size_ms: RateGroupSizeMs,
    pub count: u64,
    pub timestamp_ms: String,
}

pub(crate) struct AbsoluteRedisRateCommitter {} // end struct AbsoluteRedisRateCommitter

impl AbsoluteRedisRateCommitter {
    pub(crate) fn run(
        mut connection_manager: ConnectionManager,
        channel_size: usize,
        max_batch_size: usize,
    ) -> Sender<AbsoluteRedisCommit> {
        let (sender, receiver) = async_channel::bounded(channel_size);

        tokio::spawn(async move {
            let mut batch = Vec::with_capacity(max_batch_size);
            let script = Script::new(ABSOLUTE_COMMIT_LUA);

            while let Ok(val) = receiver.recv().await {
                batch.push(val);

                while batch.len() < max_batch_size {
                    match receiver.try_recv() {
                        Ok(val) => batch.push(val),
                        Err(_) => break,
                    }
                }

                // eprintln!("batch size: {}", batch.len());

                let pipe = Self::build_pipeline(&batch, &script, false);

                if let Err::<(), RedisError>(err) = pipe.query_async(&mut connection_manager).await
                {
                    if err.kind() != redis::ErrorKind::Server(redis::ServerErrorKind::NoScript) {
                        tracing::error!("redis.commit.error, error executing pipeline: {:?}", err);
                        continue;
                    }

                    let pipe = Self::build_pipeline(&batch, &script, true);

                    if let Err::<(), _>(err) = pipe.query_async(&mut connection_manager).await {
                        tracing::error!("redis.commit.error, error executing pipeline: {:?}", err);
                        continue;
                    }
                }

                batch.clear();
            }
        });

        sender
    }

    #[inline]
    fn build_pipeline(
        commits: &Vec<AbsoluteRedisCommit>,
        script: &Script,
        should_load_script: bool,
    ) -> redis::Pipeline {
        let mut pipe = redis::Pipeline::new();
        if should_load_script {
            pipe.load_script(script).ignore();
        }

        for commit in commits {
            pipe.invoke_script(
                script
                    .key(commit.hash_key.as_str())
                    .key(commit.active_keys_key.as_str())
                    .key(commit.window_limit_key.as_str())
                    .key(commit.total_count_key.as_str())
                    .key(commit.active_entities_key.as_str())
                    .arg(&*commit.entity_key)
                    .arg(*commit.window_size_seconds)
                    .arg(commit.window_limit)
                    .arg(*commit.rate_group_size_ms)
                    .arg(commit.count)
                    .arg(commit.timestamp_ms.as_str()),
            )
            .ignore();
        }

        pipe
    }
}
