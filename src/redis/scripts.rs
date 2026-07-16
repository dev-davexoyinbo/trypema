//! Redis Lua scripts used by the Redis and hybrid rate limiters.
//!
//! Keeping the scripts in one module makes their atomic state transitions easier to audit and
//! prevents provider implementations from mixing Redis commands with Rust orchestration.

use redis::Script;

/// Helpers shared by every Redis rate-limiter script.
const COMMON_LUA_HELPERS: &str = r#"
    local function now_ms()
        local time_array = redis.call("TIME")
        return tonumber(time_array[1]) * 1000 + math.floor(tonumber(time_array[2]) / 1000)
    end

    local function expired_bucket_keys(active_keys, timestamp_ms, window_size_ms)
        local expired_before = "(" .. tostring(timestamp_ms - window_size_ms)
        return redis.call("ZRANGE", active_keys, "-inf", expired_before, "BYSCORE")
    end

    local function sum_values(values)
        local total = 0
        for i = 1, #values do
            total = total + (tonumber(values[i]) or 0)
        end
        return total
    end

    local function comparator_matches(value, comparator_op, comparator_operand)
        if comparator_op == "nil" then
            return true
        elseif comparator_op == "eq" then
            return value == comparator_operand
        elseif comparator_op == "lt" then
            return value < comparator_operand
        elseif comparator_op == "gt" then
            return value > comparator_operand
        elseif comparator_op == "ne" then
            return value ~= comparator_operand
        end
        return nil
    end

    local function valid_history_mode(history_mode)
        return history_mode == "replace"
            or history_mode == "preserve_newest"
            or history_mode == "preserve_oldest"
    end

    local function write_bucket_timestamp(active_keys, timestamp_ms, rate_group_size_ms)
        local latest = redis.call("ZRANGE", active_keys, 0, 0, "REV", "WITHSCORES")

        if #latest == 0 then
            return timestamp_ms
        end

        local latest_timestamp_ms = tonumber(latest[2])
        local age_ms = timestamp_ms - latest_timestamp_ms

        if age_ms >= 0 and age_ms < rate_group_size_ms then
            return tonumber(latest[1])
        end

        return timestamp_ms
    end

    local function oldest_bucket_metadata(hash_key, active_keys, timestamp_ms, window_size_ms)
        local oldest = redis.call("ZRANGE", active_keys, 0, 0, "WITHSCORES")
        if #oldest == 0 then
            return nil, nil
        end

        local count = tonumber(redis.call("HGET", hash_key, oldest[1])) or 0
        local retry_after_ms = window_size_ms - timestamp_ms + (tonumber(oldest[2]) or 0)
        return retry_after_ms, count
    end

"#;

/// Helpers shared by absolute Redis and hybrid scripts.
const ABSOLUTE_LUA_HELPERS: &str = r#"
    local function cleanup_expired_absolute(hash_key, active_keys, total_count_key, timestamp_ms, window_size_ms, delete_empty_total)
        local expired_keys = expired_bucket_keys(active_keys, timestamp_ms, window_size_ms)
        local total_count = tonumber(redis.call("GET", total_count_key)) or 0

        if #expired_keys == 0 then
            return total_count, false
        end

        local expired_counts = redis.call("HMGET", hash_key, unpack(expired_keys))
        local expired_sum = sum_values(expired_counts)
        redis.call("HDEL", hash_key, unpack(expired_keys))
        redis.call("ZREM", active_keys, unpack(expired_keys))

        if expired_sum > 0 or not delete_empty_total then
            total_count = redis.call("DECRBY", total_count_key, expired_sum)
            if delete_empty_total and total_count <= 0 then
                redis.call("DEL", total_count_key)
                total_count = 0
            end
        end

        return total_count, true
    end
"#;
// Absolute rate-limiter scripts.

pub(crate) const ABSOLUTE_INC_LUA: &str = r#"
    local timestamp_ms = now_ms()
    local hash_key = KEYS[1]
    local active_keys = KEYS[2]
    local window_limit_key = KEYS[3]
    local total_count_key = KEYS[4]
    local active_entities_key = KEYS[5]

    local entity = ARGV[1]
    local window_size_seconds = tonumber(ARGV[2])
    local requested_window_limit = tonumber(ARGV[3])
    local window_limit = tonumber(redis.call("GET", window_limit_key)) or requested_window_limit
    local window_capacity = math.floor(window_limit)
    local rate_group_size_ms = tonumber(ARGV[4])
    local count = tonumber(ARGV[5])

    redis.call("ZADD", active_entities_key, timestamp_ms, entity)


    local total_count = tonumber(redis.call("GET", total_count_key)) or 0

    if total_count + count > window_capacity then
        total_count = cleanup_expired_absolute(
            hash_key,
            active_keys,
            total_count_key,
            timestamp_ms,
            window_size_seconds * 1000,
            false
        )
    end

    if total_count + count > window_capacity then
        local retry_after_ms, oldest_count = oldest_bucket_metadata(
            hash_key,
            active_keys,
            timestamp_ms,
            window_size_seconds * 1000
        )

        if retry_after_ms == nil then
            return {"rejected", 0, 0}
        end

        return {"rejected", retry_after_ms, oldest_count}
    end

    timestamp_ms = write_bucket_timestamp(active_keys, timestamp_ms, rate_group_size_ms)

    local hash_field = tostring(timestamp_ms)

    local new_count = redis.call("HINCRBY", hash_key, hash_field, count)
    redis.call("INCRBY", total_count_key, count)

    if new_count == count then
        redis.call("ZADD", active_keys, timestamp_ms, hash_field)
        redis.call("SET", window_limit_key, window_limit)
    end

    redis.call("EXPIRE", window_limit_key, window_size_seconds)

    return {"allowed", 0, 0}
"#;

pub(crate) const ABSOLUTE_IS_ALLOWED_LUA: &str = r#"
    local timestamp_ms = now_ms()
    local hash_key = KEYS[1]
    local active_keys = KEYS[2]
    local window_limit_key = KEYS[3]
    local total_count_key = KEYS[4]

    local window_size_seconds = tonumber(ARGV[1])
    local window_limit = tonumber(redis.call("GET", window_limit_key))

    if window_limit == nil then
        return {"allowed", 0, 0}
    end

    local window_capacity = math.floor(window_limit)
    local total_count = tonumber(redis.call("GET", total_count_key)) or 0

    if total_count >= window_capacity then
        total_count = cleanup_expired_absolute(
            hash_key,
            active_keys,
            total_count_key,
            timestamp_ms,
            window_size_seconds * 1000,
            false
        )
    end

    if total_count >= window_capacity then
        local retry_after_ms, oldest_count = oldest_bucket_metadata(
            hash_key,
            active_keys,
            timestamp_ms,
            window_size_seconds * 1000
        )

        if retry_after_ms == nil then
            return {"rejected", 0, 0}
        end

        return {"rejected", retry_after_ms, oldest_count}
    end

    return {"allowed", 0, 0}
"#;

"#;
"#;

"#;

/// Build a script with the common helper prelude.
pub(crate) fn lua_script(body: &str) -> Script {
    Script::new(&format!("{COMMON_LUA_HELPERS}\n{body}"))
}

/// Build an absolute script with the common and absolute helper preludes.
pub(crate) fn absolute_lua_script(body: &str) -> Script {
    Script::new(&format!(
        "{COMMON_LUA_HELPERS}\n{ABSOLUTE_LUA_HELPERS}\n{body}"
    ))
}

/// Build a suppressed script with the common and suppressed helper preludes.
pub(crate) fn suppressed_lua_script(body: &str) -> Script {
    Script::new(&format!(
        "{COMMON_LUA_HELPERS}\n{SUPPRESSED_LUA_HELPERS}\n{body}"
    ))
}
