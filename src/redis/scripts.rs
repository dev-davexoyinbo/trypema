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

        if age_ms >= 0 and age_ms <= rate_group_size_ms then
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

    local function cleanup_stale_entities(prefix, rate_type, active_entities_key, timestamp_ms, stale_after_ms, suffixes)
        local stale_entities = redis.call(
            "ZRANGE",
            active_entities_key,
            "-inf",
            timestamp_ms - stale_after_ms,
            "BYSCORE"
        )

        if #stale_entities == 0 then
            return
        end

        local remove_keys = {}

        for i = 1, #stale_entities do
            local entity = stale_entities[i]

            for j = 1, #suffixes do
                table.insert(
                    remove_keys,
                    prefix .. ":" .. entity .. ":" .. rate_type .. ":" .. suffixes[j]
                )
            end
        end

        redis.call("DEL", unpack(remove_keys))
        redis.call("ZREM", active_entities_key, unpack(stale_entities))
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

pub(crate) const ABSOLUTE_CLEANUP_LUA: &str = r#"
    local timestamp_ms = now_ms()

    local prefix = KEYS[1]
    local rate_type = KEYS[2]
    local active_entities_key = KEYS[3]

    local stale_after_ms = tonumber(ARGV[1]) or 0
    local hash_suffix = ARGV[2]
    local window_limit_suffix = ARGV[3]
    local total_count_suffix = ARGV[4]
    local active_keys_suffix = ARGV[5]
    local suppression_factor_key_suffix = ARGV[6]


    local suffixes = {hash_suffix, window_limit_suffix, total_count_suffix, active_keys_suffix, suppression_factor_key_suffix}

    cleanup_stale_entities(
        prefix,
        rate_type,
        active_entities_key,
        timestamp_ms,
        stale_after_ms,
        suffixes
    )
"#;

/// Read an absolute sliding window's live total, evicting expired buckets first.
///
/// Existing entities are marked active so cleanup tracking stays consistent; unknown reads
/// remain absent.
pub(crate) const ABSOLUTE_GET_TOTAL_LUA: &str = r#"
    local timestamp_ms = now_ms()

    local hash_key = KEYS[1]
    local active_keys = KEYS[2]
    local total_count_key = KEYS[3]
    local active_entities_key = KEYS[4]
    local window_limit_key = KEYS[5]

    local entity = ARGV[1]
    local window_size_seconds = tonumber(ARGV[2])

    local total_count = cleanup_expired_absolute(
        hash_key,
        active_keys,
        total_count_key,
        timestamp_ms,
        window_size_seconds * 1000,
        true
    )

    local entity_exists = redis.call("EXISTS", hash_key, active_keys, total_count_key, window_limit_key) > 0

    if entity_exists then
        if total_count < 0 then
            redis.call("SET", total_count_key, 0)
        end

        redis.call("ZADD", active_entities_key, timestamp_ms, entity)
    end

    if total_count < 0 then
        total_count = 0
    end

    return total_count
"#;

/// Conditionally update an absolute sliding window's total and history.
///
/// Shared by the Redis and hybrid absolute limiters. Comparator misses perform no writes, and
/// hybrid callers may include pending local counts in the atomic comparison.
pub(crate) const ABSOLUTE_SET_IF_LUA: &str = r#"
    local timestamp_ms = now_ms()

    local hash_key = KEYS[1]
    local active_keys = KEYS[2]
    local window_limit_key = KEYS[3]
    local total_count_key = KEYS[4]
    local active_entities_key = KEYS[5]

    local entity = ARGV[1]
    local window_size_seconds = tonumber(ARGV[2])
    local window_limit = tonumber(ARGV[3])
    local comparator_op = ARGV[4]
    local comparator_operand = tonumber(ARGV[5])
    local count = tonumber(ARGV[6])
    local history_mode = ARGV[7]
    local pending_count = tonumber(ARGV[8]) or 0

    local expired_keys = expired_bucket_keys(active_keys, timestamp_ms, window_size_seconds * 1000)
    local expired_sum = 0

    if #expired_keys > 0 then
        local expired_counts = redis.call("HMGET", hash_key, unpack(expired_keys))
        expired_sum = sum_values(expired_counts)
    end

    local redis_total = (tonumber(redis.call("GET", total_count_key)) or 0) - expired_sum

    if redis_total < 0 then
        redis_total = 0
    end

    local old_total = redis_total + pending_count
    local matched = comparator_matches(old_total, comparator_op, comparator_operand)

    if matched == nil then
        return redis.error_reply("trypema: unknown comparator op: " .. tostring(comparator_op))
    end

    if not matched then
        return {old_total, old_total, 0}
    end

    if not valid_history_mode(history_mode) then
        return redis.error_reply("trypema: unknown history mode: " .. tostring(history_mode))
    end

    local entity_exists = redis.call("EXISTS", hash_key, active_keys, window_limit_key, total_count_key) > 0
        or redis.call("ZSCORE", active_entities_key, entity) ~= false

    if not entity_exists and pending_count == 0 and count == 0 then
        return {0, 0, 0}
    end

    if count == 0 then
        redis.call("DEL", hash_key, active_keys, window_limit_key, total_count_key)
        redis.call("ZREM", active_entities_key, entity)

        return {0, old_total, 1}
    end

    local existing_limit = tonumber(redis.call("GET", window_limit_key))

    if history_mode ~= "replace" and #expired_keys == 0 and pending_count == 0 and count == old_total and existing_limit == window_limit then
        return {old_total, old_total, 0}
    end

    if history_mode == "replace" then
        redis.call("DEL", hash_key, active_keys)

        if count > 0 then
            local field = tostring(timestamp_ms)

            redis.call("HSET", hash_key, field, count)
            redis.call("ZADD", active_keys, timestamp_ms, field)
        end
    else
        if #expired_keys > 0 then
            redis.call("HDEL", hash_key, unpack(expired_keys))
            redis.call("ZREM", active_keys, unpack(expired_keys))
        end

        if pending_count > 0 then
            local pending_field = tostring(timestamp_ms)

            redis.call("HINCRBY", hash_key, pending_field, pending_count)
            redis.call("ZADD", active_keys, timestamp_ms, pending_field)
        end

        if count > old_total then
            local delta = count - old_total
            local edge

            if history_mode == "preserve_newest" then
                edge = redis.call("ZRANGE", active_keys, 0, 0, "REV")[1]
            else
                edge = redis.call("ZRANGE", active_keys, 0, 0)[1]
            end

            if not edge then
                edge = tostring(timestamp_ms)
                redis.call("ZADD", active_keys, timestamp_ms, edge)
            end

            redis.call("HINCRBY", hash_key, edge, delta)
        elseif count < old_total then
            local remaining = old_total - count
            local ordered

            if history_mode == "preserve_newest" then
                ordered = redis.call("ZRANGE", active_keys, 0, -1)
            else
                ordered = redis.call("ZRANGE", active_keys, 0, -1, "REV")
            end

            for i = 1, #ordered do
                if remaining <= 0 then break end

                local field = ordered[i]
                local bucket_count = tonumber(redis.call("HGET", hash_key, field)) or 0

                if bucket_count <= remaining then
                    remaining = remaining - bucket_count

                    redis.call("HDEL", hash_key, field)
                    redis.call("ZREM", active_keys, field)
                else
                    redis.call("HSET", hash_key, field, bucket_count - remaining)
                    remaining = 0
                end
            end
        end
    end

    redis.call("SET", total_count_key, count)

    redis.call("SET", window_limit_key, window_limit)
    redis.call("EXPIRE", window_limit_key, window_size_seconds)
    redis.call("ZADD", active_entities_key, timestamp_ms, entity)

    return {count, old_total, 1}
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
