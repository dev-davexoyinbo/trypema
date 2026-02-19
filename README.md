# Trypema Rate Limiter

## Name and Biblical Inspiration

The name is inspired by the Koine Greek word "τρυπήματος" (trypematos, "hole/opening") from the phrase "διὰ τρυπήματος ῥαφίδος" ("through the eye of a needle") in the Bible: Matthew 19:24, Mark 10:25, Luke 18:25

## Overview

Trypema is a Rust rate limiting library supporting both in-process and Redis-backed distributed enforcement. It emphasizes predictable behavior, low overhead, and flexible rate limiting strategies.

### Features

**Providers:**

- **Local provider** (`local`): In-process rate limiting with per-key state
- **Redis provider** (`redis`): Distributed rate limiting backed by Redis 6.2+

**Strategies:**

- **Absolute** (`absolute`): Deterministic sliding-window limiter with strict enforcement
- **Suppressed** (`suppressed`): Probabilistic strategy that can gracefully degrade under load

**Key capabilities:**

- Non-integer rate limits (e.g., `5.5` requests per second)
- Sliding time windows for smooth burst handling
- Bucket coalescing to reduce overhead
- Automatic cleanup of stale keys
- Best-effort rejection metadata for backoff hints

### Non-goals

This crate is **not** designed for:

- Strictly linearizable admission control under high concurrency
- Strong consistency guarantees in distributed scenarios

Rate limiting is best-effort: concurrent requests may temporarily overshoot limits.

## Implementation Status

| Provider | Strategy   | Status         |
| -------- | ---------- | -------------- |
| Local    | Absolute   | ✅ Implemented |
| Local    | Suppressed | ✅ Implemented |
| Redis    | Absolute   | ✅ Implemented |
| Redis    | Suppressed | ✅ Implemented |

## Quick Start

### Local Provider (In-Process)

Use the local provider for single-process rate limiting with no external dependencies:

```toml
[dependencies]
trypema = "*"
```

```rust,ignore
use std::sync::Arc;
use trypema::{
    HardLimitFactor, LocalRateLimiterOptions, RateGroupSizeMs, RateLimit, RateLimitDecision,
    RateLimiter, RateLimiterOptions, WindowSizeSeconds,
};

// Create rate limiter with a 60-second sliding window
let rl = Arc::new(RateLimiter::new(RateLimiterOptions {
    local: LocalRateLimiterOptions {
        window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
        rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
        hard_limit_factor: HardLimitFactor::default(),
    },
}));

// Optional: Start background cleanup to remove stale keys
rl.run_cleanup_loop();

// Rate limit a key to 5 requests per second
let key = "user:123";
let rate_limit = RateLimit::try_from(5.0).unwrap();

match rl.local().absolute().inc(key, &rate_limit, 1) {
    RateLimitDecision::Allowed => {
        // Request allowed, proceed
    }
    RateLimitDecision::Rejected { retry_after_ms, .. } => {
        // Request rejected, backoff for retry_after_ms
    }
    RateLimitDecision::Suppressed { .. } => {
        // Only returned by suppressed strategy
    }
}
```

### Redis Provider (Distributed)

Use the Redis provider for distributed rate limiting across multiple processes/servers:

**Requirements:**

- Redis >= 6.2
- Tokio or Smol async runtime

```toml
[dependencies]
trypema = { version = "*", features = ["redis-tokio"] }
redis = { version = "0.27", features = ["aio", "tokio-comp"] }
tokio = { version = "1", features = ["full"] }
```

```rust,ignore
use std::sync::Arc;
use trypema::{
    HardLimitFactor, LocalRateLimiterOptions, RateGroupSizeMs, RateLimit,
    RateLimiter, RateLimiterOptions, RedisKey, RedisRateLimiterOptions, WindowSizeSeconds,
};

#[tokio::main]
async fn main() {
    // Connect to Redis
    let client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
    let connection_manager = client.get_connection_manager().await.unwrap();

    // Create rate limiter with both local and Redis providers
    let rl = Arc::new(RateLimiter::new(RateLimiterOptions {
        local: LocalRateLimiterOptions {
            window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
            rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
            hard_limit_factor: HardLimitFactor::default(),
        },
        redis: RedisRateLimiterOptions {
            connection_manager,
            prefix: None, // Optional: prefix all Redis keys with "myapp"
            window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
            rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
            hard_limit_factor: HardLimitFactor::default(),
        },
    }));

    // Optional: Start background cleanup
    rl.run_cleanup_loop();

    let rate_limit = RateLimit::try_from(5.0).unwrap();

    // Redis keys must not contain ':' (used internally as separator)
    let key = RedisKey::try_from("user_123".to_string()).unwrap();

    match rl.redis().absolute().inc(&key, &rate_limit, 1).await {
        Ok(decision) => {
            // Handle decision
        }
        Err(e) => {
            // Handle Redis errors
        }
    }
}
```

## Core Concepts

### Keyed Limiting

Each key maintains independent rate limiting state. Keys are arbitrary strings (e.g., `"user:123"`, `"api_endpoint_v2"`).

### Rate Limits

Rate limits are expressed as **requests per second** using the `RateLimit` type, which wraps a positive `f64`. This allows non-integer limits like `5.5` requests/second.

The actual window capacity is computed as: `window_size_seconds × rate_limit`

**Example:** With a 60-second window and a rate limit of 5.0:

- Window capacity = 60 × 5.0 = 300 requests

### Sliding Windows

Admission decisions are based on activity within the last `window_size_seconds`. As time progresses, old buckets expire and new capacity becomes available.

Unlike fixed windows, sliding windows provide smoother rate limiting without boundary resets.

### Bucket Coalescing

To reduce memory and computational overhead, increments that occur within `rate_group_size_ms` of each other are merged into the same time bucket.

**Trade-off:** Larger coalescing intervals improve performance but make rejection metadata (like `retry_after_ms`) less precise.

## Configuration

### LocalRateLimiterOptions

| Field                 | Type                | Description                                                           | Typical Values      |
| --------------------- | ------------------- | --------------------------------------------------------------------- | ------------------- |
| `window_size_seconds` | `WindowSizeSeconds` | Length of the sliding window for admission decisions                  | 10-300 seconds      |
| `rate_group_size_ms`  | `RateGroupSizeMs`   | Coalescing interval for grouping nearby increments                    | 10-100 milliseconds |
| `hard_limit_factor`   | `HardLimitFactor`   | Multiplier for hard cutoff in suppressed strategy (1.0 = no headroom) | 1.0-2.0             |

### RedisRateLimiterOptions

Additional fields for Redis provider:

| Field                | Type                | Description                                               |
| -------------------- | ------------------- | --------------------------------------------------------- |
| `connection_manager` | `ConnectionManager` | Redis connection manager from `redis` crate               |
| `prefix`             | `Option<RedisKey>`  | Optional prefix for all Redis keys (default: `"trypema"`) |

Plus the same `window_size_seconds`, `rate_group_size_ms`, and `hard_limit_factor` fields.

## Rate Limit Decisions

All strategies return a `RateLimitDecision` enum:

### `Allowed`

The request is allowed and the increment has been recorded.

```rust,ignore
RateLimitDecision::Allowed
```

### `Rejected`

The request exceeds the rate limit and should not proceed. The increment was **not** recorded.

```rust,ignore
RateLimitDecision::Rejected {
    window_size_seconds: 60,
    retry_after_ms: 2500,        // Backoff hint: retry in ~2.5 seconds
    remaining_after_waiting: 45,  // Estimated remaining count after waiting
}
```

**Fields:**

- `window_size_seconds`: The configured sliding window size
- `retry_after_ms`: **Best-effort** estimate of milliseconds until capacity becomes available (based on oldest bucket's TTL)
- `remaining_after_waiting`: **Best-effort** estimate of window usage after waiting (may be `0` if heavily coalesced)

**Important:** These hints are approximate due to bucket coalescing and concurrent access. Use them for backoff guidance, not strict guarantees.

### `Suppressed`

Only returned by the suppressed strategy. Indicates probabilistic suppression is active.

```rust,ignore
RateLimitDecision::Suppressed {
    suppression_factor: 0.3,  // 30% suppression rate
    is_allowed: true,         // This specific call was allowed
}
```

**Fields:**

- `suppression_factor`: Calculated suppression rate (0.0 = no suppression, 1.0 = full suppression)
- `is_allowed`: Whether this specific call was admitted (**use this as the admission signal**)

When `is_allowed: false`, the increment was **not** recorded in the accepted series.

## Rate Limiting Strategies

### Absolute Strategy

**Access:** `rl.local().absolute()` or `rl.redis().absolute()`

A deterministic sliding-window limiter that strictly enforces rate limits.

**Behavior:**

- Window capacity = `window_size_seconds × rate_limit`
- Per-key limits are **sticky**: the first call for a key stores the rate limit; subsequent calls don't update it
- Requests exceeding the window capacity are immediately rejected

**Use cases:**

- Simple per-key rate caps
- Predictable, strict enforcement
- Single-process (local) or multi-process (Redis) deployments

**Concurrency note:** Best-effort under concurrent load. Multiple threads/processes may temporarily overshoot limits as admission checks and increments are not atomic across calls.

### Suppressed Strategy

**Access:** `rl.local().suppressed()` or `rl.redis().suppressed()`

A probabilistic strategy that gracefully degrades under load by suppressing a portion of requests.

**Dual tracking:**

- **Observed limiter:** Tracks all calls (including suppressed ones)
- **Accepted limiter:** Tracks only admitted calls

**Behavior:**

1. **Below capacity** (`accepted_usage < window_capacity`):
   - Suppression is bypassed, calls return `Allowed`
2. **At or above capacity:**
   - Suppression activates probabilistically based on current rate
   - Returns `Suppressed { is_allowed: true/false }` to indicate suppression state
3. **Above hard limit** (`accepted_usage >= rate_limit × hard_limit_factor`):
   - Returns `Rejected` (hard rejection, cannot be suppressed)

**Suppression calculation:**

```text
suppression_factor = 1.0 - (perceived_rate / rate_limit)
```

**Use cases:**

- Graceful degradation under load spikes
- Observability: distinguish between "hitting limit" and "over limit"
- Load shedding with visibility into suppression rates

**Inspiration:** Based on [Ably's distributed rate limiting approach](https://ably.com/blog/distributed-rate-limiting-scale-your-platform), which favors probabilistic suppression over hard cutoffs for better system behavior.

## Important Semantics & Limitations

### Eviction Granularity

**Local provider:** Uses `Instant::elapsed().as_millis()` for bucket expiration (millisecond granularity).

**Effect:** Buckets expire close to `window_size_seconds` (subject to ~1ms truncation and lazy eviction timing).

**Redis provider:** Bucket eviction uses Redis server time in milliseconds inside Lua scripts; additionally uses standard Redis TTL commands (`EXPIRE`, `SET` with `PX` option) for auxiliary keys.

### Memory Growth

Keys are **not automatically removed** from the internal map (local provider) or Redis (Redis provider) when they become inactive.

**Risk:** Unbounded or attacker-controlled key cardinality can lead to memory growth.

**Mitigation:** Use `run_cleanup_loop()` to periodically remove stale keys:

```rust,ignore
let rl = Arc::new(RateLimiter::new(options));
rl.run_cleanup_loop(); // Default: removes keys inactive for 10 minutes, checks every 30 seconds
```

**Memory safety:** The cleanup loop holds only a `Weak<RateLimiter>` reference, so dropping all `Arc` references automatically stops cleanup.

## Tuning Guide

### `window_size_seconds`

**What it controls:** Length of the sliding window for rate limiting decisions.

**Trade-offs:**

- **Larger windows** (60-300s):
  - ✅ Smooth out burst traffic
  - ✅ More forgiving for intermittent usage patterns
  - ❌ Slower recovery after hitting limits (old activity stays in window longer)
  - ❌ Higher memory usage per key

- **Smaller windows** (5-30s):
  - ✅ Faster recovery after hitting limits
  - ✅ Lower memory usage
  - ❌ Less burst tolerance
  - ❌ More sensitive to temporary spikes

**Recommendation:** Start with 60 seconds for most use cases.

### `rate_group_size_ms`

**What it controls:** How aggressively increments are coalesced into buckets.

**Trade-offs:**

- **Larger coalescing** (50-100ms):
  - ✅ Lower memory usage (fewer buckets)
  - ✅ Better performance (fewer atomic operations)
  - ❌ Coarser rejection metadata (`retry_after_ms` less accurate)

- **Smaller coalescing** (1-20ms):
  - ✅ More accurate rejection metadata
  - ✅ Finer-grained tracking
  - ❌ Higher memory usage
  - ❌ More overhead

**Recommendation:** Start with 10ms. Increase to 50-100ms if memory or performance becomes an issue.

### `hard_limit_factor`

**What it controls:** Hard cutoff multiplier for the suppressed strategy.

**Calculation:** `hard_limit = rate_limit × hard_limit_factor`

**Values:**

- `1.0`: No headroom; hard limit equals base limit (suppression less useful)
- `1.5-2.0`: **Recommended**; allows 50-100% burst above target rate before hard rejection
- `> 2.0`: Very permissive; large gap between target and hard limit

**Only relevant for:** Suppressed strategy. Ignored by absolute strategy.

## Project Structure

```text
src/
├── rate_limiter.rs              # Top-level RateLimiter facade
├── common.rs                    # Shared types (RateLimitDecision, RateLimit, etc.)
├── error.rs                     # Error types
├── local/
│   ├── mod.rs
│   ├── local_rate_limiter_provider.rs
│   ├── absolute_local_rate_limiter.rs   # Local absolute strategy
│   └── suppressed_local_rate_limiter.rs # Local suppressed strategy
└── redis/
    ├── mod.rs
    ├── redis_rate_limiter_provider.rs
    ├── absolute_redis_rate_limiter.rs   # Redis absolute strategy (Lua scripts)
    ├── suppressed_redis_rate_limiter.rs # Redis suppressed strategy (Lua scripts)
    └── common.rs                        # Redis-specific utilities

docs/
├── redis.md                     # Redis provider details
└── testing.md                   # Testing guide
```

## Redis Provider Details

### Requirements

- **Redis version:** >= 6.2.0
- **Async runtime:** Tokio or Smol

### Key Constraints

Redis keys use the `RedisKey` newtype with validation:

- **Must not be empty**
- **Must be ≤ 255 bytes**
- **Must not contain** `:` (used internally as a separator)

```rust,ignore
// ✅ Valid
RedisKey::try_from("user_123".to_string()).unwrap();
RedisKey::try_from("api_v2_endpoint".to_string()).unwrap();

// ❌ Invalid
RedisKey::try_from("user:123".to_string()); // Error: contains ':'
RedisKey::try_from("".to_string());         // Error: empty
```

### Feature Flags

Control Redis support at compile time:

```toml
# Default: Redis enabled with Tokio
trypema = { version = "*" }

# Disable Redis entirely
trypema = { version = "*", default-features = false }

# Use Smol runtime instead
trypema = { version = "*", default-features = false, features = ["redis-smol"] }
```

### Data Model & Cleanup

See [docs/redis.md](docs/redis.md) for detailed information on:

- Redis key schema
- Lua script implementation
- Expiration and cleanup behavior
- Concurrency semantics

## Testing

### Local Tests Only

```bash
cargo test --no-default-features
```

### With Redis Integration Tests

Requires Docker and Docker Compose:

```bash
# Start Redis (6.2+) and run all tests
make test-redis

# Use custom Redis port
REDIS_PORT=6379 make test-redis

# Point to existing Redis instance
REDIS_URL=redis://127.0.0.1:6379 cargo test
```

See [docs/testing.md](docs/testing.md) for detailed testing documentation.

## Roadmap

**Planned:**

- [ ] Comprehensive benchmarking suite
- [ ] Metrics and observability hooks

**Future considerations:**

- [ ] Additional backends (e.g., in-memory distributed cache)
- [ ] Token bucket strategy
- [ ] Async local provider (currently sync only)

**Non-goals:**

- Strict linearizability (by design)
- Built-in retry logic (use case specific)

## Contributing

Feedback, issues, and PRs welcome. Please include tests for new features.

## License

MIT License. See the LICENSE file in the repository for details.
