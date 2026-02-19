# Redis Provider Documentation

The Redis provider enables distributed rate limiting across multiple processes or servers using Redis as a shared backend.

**Status:**
- ✅ **Absolute strategy:** Implemented and tested
- ✅ **Suppressed strategy:** Implemented and tested

## Requirements

### Redis Version

**Minimum:** Redis >= 6.2.0

**Why:** The implementation uses standard Redis commands:
- String commands: `GET`, `SET`, `INCRBY`, `DECRBY`, `DEL`, `EXPIRE`
- Hash commands: `HGET`, `HDEL`, `HINCRBY`, `HMGET`
- Sorted Set commands: `ZADD`, `ZREM`, `ZRANGE` (with `REV` option from 6.2.0), `ZCARD`
- Server commands: `TIME`

**Tested version:** `redis:6.2-alpine` (used in CI and test harness)

### Runtime

Requires an async runtime:
- **Tokio** (feature: `redis-tokio`, enabled by default)
- **Smol** (feature: `redis-smol`)

## Key Validation

User-provided keys use the `RedisKey` newtype with strict validation:

```rust,no_run
use trypema::redis::RedisKey;

// Construction validates:
// - not empty
// - <= 255 bytes
// - does not contain ':'
let key = RedisKey::try_from("user_123".to_string()).unwrap();
let _ = key;
```

**Why the `:` restriction?** The limiter uses `:` as an internal separator when constructing Redis keys. For example:

```
<prefix>:<user_key>:<strategy>:<data_type>
```

**Examples:**

```rust,no_run
use trypema::redis::RedisKey;

// Valid
let _ = RedisKey::try_from("user_123".to_string()).unwrap();
let _ = RedisKey::try_from("api-endpoint-v2".to_string()).unwrap();

// Invalid
let _ = RedisKey::try_from("user:123".to_string());
let _ = RedisKey::try_from("".to_string());
let long_key = "x".repeat(256);
let _ = RedisKey::try_from(long_key);
```

## Absolute Strategy Implementation

**Source:** `src/redis/absolute_redis_rate_limiter.rs`

The absolute Redis strategy is a distributed sliding-window limiter with bucket coalescing.

### API Methods

#### `inc(key, rate_limit, count) -> Result<RateLimitDecision, TrypemaError>`

Checks admission and atomically records the increment if allowed.

```rust,no_run
use trypema::{
    HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision, RateLimiter, RateLimiterOptions,
    WindowSizeSeconds,
};
use trypema::local::LocalRateLimiterOptions;
use trypema::redis::{RedisKey, RedisRateLimiterOptions};

async fn example() -> Result<(), trypema::TrypemaError> {
    let rl = RateLimiter::new(RateLimiterOptions {
        local: LocalRateLimiterOptions {
            window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
            rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
            hard_limit_factor: HardLimitFactor::default(),
        },
        redis: RedisRateLimiterOptions {
            connection_manager: todo!("create redis::aio::ConnectionManager"),
            prefix: None,
            window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
            rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
            hard_limit_factor: HardLimitFactor::default(),
        },
    });

    let limiter = rl.redis().absolute();
    let key = RedisKey::try_from("user_123".to_string())?;
    let rate = RateLimit::try_from(10.0)?;

    match limiter.inc(&key, &rate, 1).await? {
        RateLimitDecision::Allowed => {}
        RateLimitDecision::Rejected { retry_after_ms, .. } => {
            let _ = retry_after_ms;
        }
        _ => unreachable!(),
    }

    Ok(())
}
```

**Behavior:**
- If allowed: increment recorded, returns `Allowed`
- If over limit: increment **not** recorded, returns `Rejected` with backoff hints

#### `is_allowed(key) -> Result<RateLimitDecision, TrypemaError>`

Checks current window usage without recording an increment (read-only).

```rust,no_run
use trypema::RateLimitDecision;

async fn check(limiter: &trypema::AbsoluteRedisRateLimiter, key: &trypema::RedisKey) -> Result<(), trypema::TrypemaError> {
    match limiter.is_allowed(key).await? {
        RateLimitDecision::Allowed => {}
        RateLimitDecision::Rejected { .. } => {}
        _ => unreachable!(),
    }
    Ok(())
}
```

**Use case:** Preview admission decision without affecting state (e.g., for middleware that needs to check before doing expensive work).

### Implementation

Both methods are implemented as **atomic Lua scripts** executed on Redis via `EVALSHA`.

### Window Capacity

Computed per key as:

```
window_capacity = window_size_seconds × rate_limit
```

**Example:** 60-second window with 10 req/s limit = 600 total requests allowed in window

**Implementation notes:**
- `RateLimit` is `f64` (supports non-integer rates like `5.5` req/s)
- Internal counters are `u64` (integer operations in Lua)
- Lua scripts compare integer sum against `window_capacity`

### Bucket Coalescing

To reduce write amplification and memory usage, nearby increments are merged into the same bucket.

**Algorithm** (in `inc` Lua script):

1. Each bucket is identified by a millisecond timestamp
2. Before creating a new bucket:
   - Check the most recent active bucket
   - If bucket age < `rate_group_size_ms`, add to existing bucket
   - Otherwise, create a new bucket

**Benefits:**
- ✅ Fewer Redis writes
- ✅ Lower memory usage
- ✅ Better performance under high throughput

**Trade-offs:**
- ❌ Coarser timing granularity
- ❌ Less precise rejection metadata (`retry_after_ms`)

**Example:**

With `rate_group_size_ms = 10`:
- Request at t=100ms → creates bucket at 100
- Request at t=105ms → adds to bucket at 100 (within 10ms window)
- Request at t=115ms → creates new bucket at 115 (> 10ms since bucket creation)

## Redis Data Model

For each user key, the limiter stores data in **three Redis keys** under a configurable prefix.

### Key Schema

Given:
- `prefix` = `"trypema"` (default) or custom `RedisKey`
- `key` = user's `RedisKey`

The following Redis keys are created:

#### 1. Bucket Hash: `<prefix>:<key>:absolute:h`

**Type:** Hash

**Purpose:** Stores count for each time bucket

**Structure:**
```
Field: "1234567890123" (bucket timestamp in milliseconds)
Value: "42" (count as string)
```

**Operations:**
- `HINCRBY` - Increment bucket count
- `HMGET` - Get multiple bucket values
- `HDEL` - Delete expired buckets

#### 2. Active Bucket Index: `<prefix>:<key>:absolute:a`

**Type:** Sorted Set

**Purpose:** Track which buckets are active (for efficient range queries)

**Structure:**
```
Member: "1234567890123" (bucket timestamp)
Score: 1234567890123.0 (same timestamp as float)
```

**Operations:**
- `ZADD` - Add new bucket timestamp
- `ZREMRANGEBYSCORE` - Remove expired buckets by time range
- `ZRANGE ... 0 0` - Find oldest bucket (for `retry_after_ms`)

#### 3. Window Limit Cache: `<prefix>:<key>:absolute:w`

**Type:** String

**Purpose:** Cache the computed window limit so `is_allowed()` doesn't need the rate limit parameter

**Structure:**
```
Value: "300.0" (window_size_seconds × rate_limit)
```

**Operations:**
- `SET` - Store on first increment
- `EXPIRE` - Refresh TTL on each increment
- `GET` - Read cached limit

### Example

For user key `"user_123"` with default prefix:

```
trypema:user_123:absolute:h     → Hash of bucket counts
trypema:user_123:absolute:a     → Sorted set of active bucket timestamps
trypema:user_123:absolute:w     → Cached window limit
```

## Expiration & Cleanup

### Automatic Expiration

#### Bucket Fields (Hash)

Expired buckets are **lazily removed** during `inc()` and `is_allowed()` operations:

```lua
-- Find expired bucket timestamps
local to_remove_keys = redis.call("ZRANGE", active_keys, "-inf", timestamp_ms - window_size_seconds * 1000, "BYSCORE")

-- Remove from hash
if #to_remove_keys > 0 then
    redis.call("HDEL", hash_key, unpack(to_remove_keys))
    redis.call("ZREM", active_keys, unpack(to_remove_keys))
end
```

**Effect:** Old buckets are removed from the hash during the next operation after they expire.

#### Window Limit Cache

The `<prefix>:<key>:absolute:w` key is refreshed on each `inc()`:

```lua
redis.call('EXPIRE', window_limit_key, window_size_seconds)
```

**Effect:** Inactive keys expire after `window_size_seconds` of no activity.

### Lazy Cleanup

Both `inc()` and `is_allowed()` perform **lazy cleanup** of expired buckets:

```lua
-- Find expired buckets
local to_remove_keys = redis.call("ZRANGE", active_keys, "-inf", timestamp_ms - window_size_seconds * 1000, "BYSCORE")

-- Remove from hash and sorted set
if #to_remove_keys > 0 then
    redis.call("HDEL", hash_key, unpack(to_remove_keys))
    redis.call("ZREM", active_keys, unpack(to_remove_keys))
end
```

**What it does:** Removes bucket timestamps and hash fields older than the window from both the hash and sorted set.

### Cleanup Loop

For proactive cleanup of completely inactive keys, use:

```rust,no_run
use std::sync::Arc;

use trypema::{HardLimitFactor, RateGroupSizeMs, RateLimiter, RateLimiterOptions, WindowSizeSeconds};
use trypema::local::LocalRateLimiterOptions;
use trypema::redis::RedisRateLimiterOptions;

let rl = Arc::new(RateLimiter::new(RateLimiterOptions {
    local: LocalRateLimiterOptions {
        window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
        rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
        hard_limit_factor: HardLimitFactor::default(),
    },
    redis: RedisRateLimiterOptions {
        connection_manager: todo!("create redis::aio::ConnectionManager"),
        prefix: None,
        window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
        rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
        hard_limit_factor: HardLimitFactor::default(),
    },
}));

rl.run_cleanup_loop();
rl.run_cleanup_loop_with_config(600_000, 30_000);
```

**What it cleans:**
- **Local provider:** Removes keys from in-memory maps
- **Redis provider:** Currently no-op (see Known Limitations)

### Known Limitations

⚠️ **Memory leak potential:** The Redis scripts do **not** delete the hash or sorted set keys when they become empty.

**Symptom:** If you have high key cardinality (many unique keys), Redis memory usage grows over time even as keys become inactive.

**Workaround:** Implement periodic cleanup using Redis `SCAN` + `TTL` checks:

```bash
# Find keys with no recent activity
redis-cli --scan --pattern "trypema:*:absolute:w" | while read key; do
  ttl=$(redis-cli TTL "$key")
  if [ "$ttl" -eq -1 ] || [ "$ttl" -eq -2 ]; then
    # Key expired or doesn't exist, clean up related keys
    user_key=$(echo "$key" | cut -d: -f2)
    redis-cli DEL "trypema:$user_key:absolute:h"
    redis-cli DEL "trypema:$user_key:absolute:a"
    redis-cli DEL "trypema:$user_key:absolute:w"
  fi
done
```

**Future work:** Global key index for automatic cleanup (tracked internally).

## Rejection Metadata

When a request is rejected, the Lua script returns:

```rust
use trypema::RateLimitDecision;

let decision = RateLimitDecision::Rejected {
    window_size_seconds: 60,
    retry_after_ms: 2500,
    remaining_after_waiting: 45,
};
```

### Field Meanings

#### `retry_after_ms`

**Computation:** Time until the oldest active bucket expires

```lua
-- Calculate remaining time for oldest bucket
local oldest_hash_field_group_timestamp = tonumber(oldest_hash_fields[2])
local oldest_hash_field_ttl = (window_size_seconds * 1000) - timestamp_ms + oldest_hash_field_group_timestamp
```

**Interpretation:** **Best-effort** hint for "how long to wait before capacity becomes available"

**Caveats:**
- Assumes the oldest bucket will expire without new increments
- Coalescing can make this less accurate
- Concurrent requests can change the actual wait time

#### `remaining_after_waiting`

**Computation:** Count stored in the oldest bucket

```lua
local oldest_count = redis.call('HGET', hash_key, oldest_bucket_timestamp)
```

**Interpretation:** **Estimate** of window usage after waiting `retry_after_ms`

**Caveats:**
- If heavily coalesced, may be `0` even when far over limit
- Concurrent activity changes the actual remaining count
- Use for rough backoff, not precise capacity planning

### Best Practices

✅ **Do:**
- Use as a backoff hint for retry logic
- Expose to clients for rate limit feedback
- Log for monitoring rate limit hits

❌ **Don't:**
- Rely on exact timing for strict guarantees
- Use for precise capacity calculations
- Expect deterministic behavior under high concurrency

## Concurrency & Consistency

### Atomicity

✅ **Each Lua script execution is atomic** within a single Redis instance.

**Guarantees:**
- No partial updates
- Consistent view of buckets during script execution
- No race conditions within a single script

### Best-Effort Limiting

❌ **Overall rate limiting is best-effort**, not strictly linearizable.

**Why:**

1. **Multiple clients** can check the limit concurrently
2. **All see "allowed"** if under limit at check time
3. **All proceed** and increment, causing temporary overshoot
4. **Bucket coalescing** can merge concurrent requests into same bucket

**Example:**

```
Window limit: 100 requests
Current usage: 95 requests

Time T:
  Client A: checks → 95 < 100 → allowed
  Client B: checks → 95 < 100 → allowed
  Client C: checks → 95 < 100 → allowed

Time T+1:
  All three increment → total = 98
  
Temporary overshoot possible if timing is tight.
```

**Implication:** This is **expected behavior**. The limiter provides approximate distributed rate limiting, not strict enforcement.

### Redis Cluster

⚠️ **Not tested with Redis Cluster**

**Potential issues:**
- Lua scripts with multiple keys require all keys to be on the same shard
- Current implementation may not use hash tags correctly for cluster mode
- Cross-slot operations will fail

**If you need cluster support:** File an issue with your use case.

## Usage Examples

### Basic Rate Limiting

```rust,no_run
use trypema::{RateLimit, RateLimitDecision, RedisKey};

let key = RedisKey::try_from("user_123".to_string())?;
let rate = RateLimit::try_from(10.0)?; // 10 requests per second

let decision = limiter.inc(&key, &rate, 1).await?;

match decision {
    RateLimitDecision::Allowed => {
        // Process request
    }
    RateLimitDecision::Rejected { retry_after_ms, remaining_after_waiting, .. } => {
        // Send 429 Too Many Requests with Retry-After header
        log::warn!("Rate limit exceeded for {key}, retry in {retry_after_ms}ms");
    }
    _ => unreachable!("absolute strategy never returns Suppressed"),
}
```

### Read-Only Check

```rust,no_run
// Check if request would be allowed without recording it
let decision = limiter.is_allowed(&key).await?;

if matches!(decision, RateLimitDecision::Allowed) {
    // Do expensive work, then record
    let result = expensive_operation().await;
    limiter.inc(&key, &rate, 1).await?;
    return result;
}
```

### HTTP Middleware Example

```rust,no_run
use std::sync::Arc;

use trypema::{AbsoluteRedisRateLimiter, RateLimit, RateLimitDecision};
use trypema::redis::RedisKey;

struct Request;
struct Response;

#[derive(Debug)]
struct Error;

fn extract_user_id(_req: &Request) -> Result<String, Error> {
    Ok("user_123".to_string())
}

async fn handle_request(_req: Request) -> Result<Response, Error> {
    Ok(Response)
}

async fn rate_limit_middleware(
    req: Request,
    limiter: Arc<AbsoluteRedisRateLimiter>,
) -> Result<Response, Error> {
    let user_id = extract_user_id(&req)?;
    let key = RedisKey::try_from(format!("api_{}", user_id)).map_err(|_| Error)?;
    let rate = RateLimit::try_from(100.0).map_err(|_| Error)?;

    match limiter.inc(&key, &rate, 1).await.map_err(|_| Error)? {
        RateLimitDecision::Allowed => handle_request(req).await,
        RateLimitDecision::Rejected {
            retry_after_ms,
            remaining_after_waiting,
            ..
        } => {
            let _ = (retry_after_ms, remaining_after_waiting);
            Err(Error)
        }
        _ => unreachable!(),
    }
}
```

### Multi-Tier Rate Limiting

```rust,no_run
use trypema::{AbsoluteRedisRateLimiter, RateLimit, RateLimitDecision, TrypemaError};
use trypema::redis::RedisKey;

enum UserTier {
    Free,
    Pro,
    Enterprise,
}

async fn check_user_rate_limit(
    user_id: &str,
    user_tier: UserTier,
    limiter: &AbsoluteRedisRateLimiter,
) -> Result<RateLimitDecision, TrypemaError> {
    let key = RedisKey::try_from(format!("user_{}", user_id))?;

    let rate = match user_tier {
        UserTier::Free => RateLimit::try_from(10.0)?,
        UserTier::Pro => RateLimit::try_from(100.0)?,
        UserTier::Enterprise => RateLimit::try_from(1000.0)?,
    };

    limiter.inc(&key, &rate, 1).await
}
```

## Monitoring & Observability

### Logging Redis Operations

Enable Redis command logging with `tracing`:

```rust
use tracing_subscriber;

tracing_subscriber::fmt()
    .with_max_level(tracing::Level::DEBUG)
    .init();
```

Look for:
```
WARN Redis cleanup failed, will retry: <error details>
```

### Metrics to Track

Recommended metrics for production:

1. **Rate limit hit rate:**
   ```rust
   use trypema::RateLimitDecision;

   match decision {
       RateLimitDecision::Rejected { .. } => {
           metrics.increment("rate_limit.rejected", tags);
       }
       RateLimitDecision::Allowed => {
           metrics.increment("rate_limit.allowed", tags);
       }
       _ => {}
   }
   ```

2. **Redis operation latency:**
   ```rust,no_run
   use std::time::Instant;

   let start = Instant::now();
   let decision = limiter.inc(&key, &rate, 1).await?;
   metrics.histogram("rate_limit.redis.latency_ms", start.elapsed().as_millis());
   let _ = decision;
   ```

3. **Redis connection errors:**
   ```rust,no_run
   use trypema::TrypemaError;

   match limiter.inc(&key, &rate, 1).await {
       Err(TrypemaError::RedisError(_)) => {
           metrics.increment("rate_limit.redis.errors");
       }
       _ => {}
   }
   ```

4. **Key cardinality:** Periodically scan Redis to track unique keys
   ```bash
   redis-cli --scan --pattern "trypema:*:absolute:w" | wc -l
   ```

## Troubleshooting

### Problem: Rate limits not working (always allowed)

**Check:**
1. Verify Redis version: `redis-cli INFO server | grep redis_version`
2. Check connection: `redis-cli PING`
3. Verify key prefix in logs
4. Check that keys are being created: `redis-cli KEYS "trypema:*"`

### Problem: Memory usage growing unbounded

**Cause:** Keys not expiring (see Known Limitations)

**Solution:** Implement periodic cleanup (see Expiration & Cleanup section)

### Problem: High latency on rate limit checks

**Check:**
1. Redis network latency: `redis-cli --latency`
2. Redis CPU usage: `redis-cli INFO stats`
3. Script caching: Ensure `EVALSHA` is used (not `EVAL`)
4. Consider connection pooling if using many connections

### Problem: Inconsistent rate limiting behavior

**Likely causes:**
1. Multiple processes with different `window_size_seconds` or `rate_group_size_ms` configs
2. Clock skew between application servers
3. Redis replication lag (if using replica reads)

**Solution:** Ensure all application instances use identical configuration.
