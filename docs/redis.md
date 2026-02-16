# Redis Provider (Experimental)

This crate includes a Redis-backed provider exposed via `RateLimiter::redis()`.

Status notes:

- `absolute`: implemented
- `suppressed`: placeholder (not yet implemented)

## Redis Version Requirement

The Redis scripts use hash field TTL commands (`HPEXPIRE`, `HPTTL ... FIELDS`), which require Redis >= 7.4.

The test harness and CI workflow pin Redis to `redis:7.4.2-alpine`.

## Key Validation

Redis keys passed into the limiter use the `RedisKey` newtype:

- must not be empty
- must be <= 255 bytes
- must not contain `:`

The limiter itself composes Redis keys using `:` as a separator, so user keys cannot contain `:`.

## AbsoluteRedisRateLimiter

Path: `src/redis/absolute_redis_rate_limiter.rs`

This implementation is a sliding-window limiter with bucket coalescing.

### Public API

- `inc(key, rate_limit, count) -> RateLimitDecision`
  - If allowed, the increment is recorded and `Allowed` is returned.
  - If over limit, the increment is not applied and `Rejected { retry_after_ms, remaining_after_waiting, .. }` is returned.
- `is_allowed(key) -> RateLimitDecision`
  - Reads the current window total and returns `Allowed` / `Rejected`.
  - Does not record an increment.

Both methods are implemented as Lua scripts invoked via `redis::Script`.

### Window Limit

The limiter computes a per-window capacity from the configured options:

- `window_limit = window_size_seconds * rate_limit`

Notes:

- `RateLimit` is an `f64` to allow non-integer per-second limits.
- Internally, counters are integer (`u64`). The Lua scripts compare the integer sum of counts to `window_limit`.

### Bucket Coalescing (`rate_group_size_ms`)

To reduce write amplification, increments that happen close together can be merged into one bucket.

Mechanism (implemented in the `inc` script):

- Each bucket is keyed by a millisecond timestamp (`timestamp_ms`).
- Before writing a new bucket, the script checks the most recent active bucket.
- If that bucket is still alive and the "age since its creation" is less than `rate_group_size_ms`, the new increment is added to the most recent bucket instead of creating a new one.

This improves performance but makes per-bucket timing coarser, which also affects rejection metadata.

### Redis Data Model

For each logical user key `key`, the limiter uses three Redis keys under a configurable prefix.

Given:

- `prefix`: `RedisKey` (defaults to `trypema`)
- `key`: `RedisKey`

Key schema:

- Hash of buckets:
  - `<prefix>:<key>:absolute:h`
  - fields: bucket timestamp in ms (string)
  - values: integer count (string/int)
- Sorted set of active buckets:
  - `<prefix>:<key>:absolute:a`
  - members: bucket timestamp in ms (string)
  - score: bucket timestamp in ms (number)
- Window-limit cache:
  - `<prefix>:<key>:absolute:w`
  - value: computed window limit (`window_size_seconds * rate_limit`)

How these are used:

- The sorted set is used to find and evict expired bucket timestamps (`ZREMRANGEBYSCORE`), and to locate the oldest bucket when generating rejection metadata (`ZRANGE ... 0 0`).
- The hash stores counts per bucket timestamp.
- The window-limit key allows `is_allowed` to work without the caller supplying `rate_limit` (the limit is cached on first bucket creation).

### Expiration / Cleanup

Bucket expiration:

- Each bucket field in the hash gets a per-field TTL of `window_size_seconds * 1000` using `HPEXPIRE ... FIELDS`.

Window-limit expiration:

- `<prefix>:<key>:absolute:w` is set when a bucket is created and is refreshed via `EXPIRE` on each `inc` call.

Active bucket eviction:

- Both `inc` and `is_allowed` remove old timestamps from the sorted set with:
  - `ZREMRANGEBYSCORE active_keys -inf (now_ms - window_ms)`

Important caveat:

- The code includes a TODO to clean up "active keys" sets globally. Per-key sorted sets are cleaned by score, but there is no global index to delete per-key data when it becomes completely inactive.
- The scripts do not delete the hash key or active set key when they become empty.

### Rejection Metadata

When rejecting, the scripts return:

- `retry_after_ms`: the remaining TTL (ms) of the oldest active hash field
- `remaining_after_waiting`: the count stored in the oldest bucket

These are surfaced as:

- `RateLimitDecision::Rejected { retry_after_ms, remaining_after_waiting, window_size_seconds }`

Interpretation:

- `retry_after_ms` is a best-effort backoff hint for "when to try again".
- `remaining_after_waiting` is the number of tokens that will be freed when that oldest bucket expires.

Because of coalescing and concurrency, these hints can be imprecise.

### Concurrency / Consistency

Within a single Redis instance, each script invocation is atomic.

However, the limiter is still best-effort in the sense that:

- multiple clients can be allowed concurrently up to the window limit; the scripts enforce the limit at evaluation time, but timing and coalescing can cause behavior that looks "bursty" around the boundary
- rejection metadata is advisory

## Usage Example

```rust,no_run
use trypema::{RateLimit, RateLimitDecision, RedisKey};

let key = RedisKey::try_from("user_123".to_string()).unwrap();
let rate = RateLimit::try_from(5.0).unwrap();

// limiter: &AbsoluteRedisRateLimiter
let decision = limiter.inc(&key, &rate, 1).await.unwrap();

match decision {
    RateLimitDecision::Allowed => {}
    RateLimitDecision::Rejected { retry_after_ms, .. } => {
        let _ = retry_after_ms;
    }
    RateLimitDecision::Suppressed { .. } => {}
}
```
