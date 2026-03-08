# Trypema Rate Limiter

## Name and Biblical Inspiration

The name **Trypema** is derived from the Koine Greek word **"τρυπήματος"** (_trypematos_),
meaning "hole" or "opening." It appears in the phrase **"διὰ τρυπήματος ῥαφίδος"**
("through the eye of a needle"), spoken by Jesus in three of the four Gospels:

- **Matthew 19:24** — _"Again I tell you, it is easier for a camel to go through the eye of a
  needle than for someone who is rich to enter the kingdom of God."_
- **Mark 10:25** — _"It is easier for a camel to go through the eye of a needle than for someone
  who is rich to enter the kingdom of God."_
- **Luke 18:25** — _"Indeed, it is easier for a camel to go through the eye of a needle than for
  someone who is rich to enter the kingdom of God."_

Just as the eye of a needle is a narrow passage that restricts what can pass through,
a rate limiter is a narrow gate that controls the flow of requests into a system.

## Overview

Trypema is a Rust rate limiting library that supports both in-process and Redis-backed
distributed enforcement. It provides two complementary strategies — strict enforcement and
probabilistic suppression — across three provider backends.

**Documentation:** <https://trypema.davidoyinbo.com>

**Benchmark comparisons:**

- Local provider: <https://trypema.davidoyinbo.com/benchmarks/benchmark-results/local-benchmark-comparison>
- Redis provider: <https://trypema.davidoyinbo.com/benchmarks/benchmark-results/redis-benchmark-comparison>

### Providers

Trypema organises rate limiting into three **providers**, each suited to different deployment
scenarios:

| Provider   | Backend                               | Latency                     | Consistency               | Use Case                         |
| ---------- | ------------------------------------- | --------------------------- | ------------------------- | -------------------------------- |
| **Local**  | In-process `DashMap` + atomics        | Sub-microsecond             | Single-process only       | CLI tools, single-server APIs    |
| **Redis**  | Redis 7.2+ via Lua scripts            | Network round-trip per call | Distributed (best-effort) | Multi-process / multi-server     |
| **Hybrid** | Local fast-path + periodic Redis sync | Sub-microsecond (fast-path) | Distributed with sync lag | High-throughput distributed APIs |

### Strategies

Each provider exposes two **strategies** that determine how rate limit decisions are made:

- **Absolute** — A deterministic sliding-window limiter. Requests under the window capacity
  are allowed; requests over it are immediately rejected. Simple and predictable.

- **Suppressed** — A probabilistic strategy inspired by
  [Ably's distributed rate limiting approach](https://ably.com/blog/distributed-rate-limiting-scale-your-platform).
  Instead of a hard cutoff, it computes a _suppression factor_ and probabilistically denies a
  fraction of requests proportional to how far over the limit the key is. This produces smooth
  degradation rather than a cliff-edge rejection.

### Key Capabilities

- Non-integer rate limits (e.g., `0.5` or `5.5` requests per second)
- Sliding time windows for smooth burst handling (no fixed-window boundary resets)
- Configurable bucket coalescing to trade timing precision for lower overhead
- Automatic background cleanup of stale keys (with `Weak` reference — no leak risk)
- Best-effort rejection metadata (`retry_after_ms`, `remaining_after_waiting`) for backoff hints
- Feature-flag driven compilation — zero Redis dependencies when you only need local limiting

### Non-Goals

This crate is **not** designed for:

- **Strictly linearizable admission control.** Concurrent requests may temporarily overshoot
  limits. This is by design: the library prioritises throughput over strict serialisation.
- **Strong consistency in distributed scenarios.** Redis-backed limiting is best-effort;
  multiple clients can exceed limits simultaneously during network partitions or high concurrency.
- **Built-in retry logic.** Retry/backoff policies are application-specific and left to the caller.

## Quick Start

### Local Provider (In-Process)

Use the local provider when you need single-process rate limiting with no external dependencies.

```toml
[dependencies]
trypema = "1.0"
```

```rust,no_run
use std::sync::Arc;

use trypema::{
    HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision, RateLimiter, RateLimiterOptions,
    SuppressionFactorCacheMs, WindowSizeSeconds,
};
use trypema::local::LocalRateLimiterOptions;

let window_size_seconds = WindowSizeSeconds::try_from(60).unwrap();
let rate_group_size_ms = RateGroupSizeMs::try_from(10).unwrap();
let hard_limit_factor = HardLimitFactor::default();
let suppression_factor_cache_ms = SuppressionFactorCacheMs::default();

let options = RateLimiterOptions {
    local: LocalRateLimiterOptions {
        window_size_seconds,
        rate_group_size_ms,
        hard_limit_factor,
        suppression_factor_cache_ms,
    },
};

let rl = Arc::new(RateLimiter::new(options));

// Start background cleanup to remove stale keys.
// Idempotent: calling this multiple times is a no-op while running.
rl.run_cleanup_loop();

// Define a rate limit of 5 requests per second.
// With a 60-second window, the window capacity is 60 × 5 = 300 requests.
let key = "user_123";
let rate_limit = RateLimit::try_from(5.0).unwrap();

// --- Absolute strategy: deterministic sliding-window enforcement ---
match rl.local().absolute().inc(key, &rate_limit, 1) {
    RateLimitDecision::Allowed => {
        // Under capacity — proceed with the request.
    }
    RateLimitDecision::Rejected { retry_after_ms, .. } => {
        // Over capacity — reject and suggest the client retry later.
        // `retry_after_ms` is a best-effort hint based on the oldest bucket's TTL.
        let _ = retry_after_ms;
    }
    RateLimitDecision::Suppressed { .. } => {
        unreachable!("absolute strategy never returns Suppressed");
    }
}

// --- Suppressed strategy: probabilistic suppression near/over the limit ---
//
// You can query the suppression factor at any time for metrics/debugging.
let sf = rl.local().suppressed().get_suppression_factor(key);
let _ = sf;

match rl.local().suppressed().inc(key, &rate_limit, 1) {
    RateLimitDecision::Allowed => {
        // Below capacity — no suppression active, proceed normally.
    }
    RateLimitDecision::Suppressed {
        is_allowed: true,
        suppression_factor,
    } => {
        // At/above capacity — suppression is active, but this particular request
        // was probabilistically allowed. Proceed, but consider logging the factor.
        let _ = suppression_factor;
    }
    RateLimitDecision::Suppressed {
        is_allowed: false,
        suppression_factor,
    } => {
        // At/above capacity — this request was probabilistically denied.
        // Do NOT proceed. When over the hard limit, suppression_factor will be 1.0.
        let _ = suppression_factor;
    }
    RateLimitDecision::Rejected { .. } => {
        unreachable!("local suppressed strategy never returns Rejected");
    }
}
```

### Redis Provider (Distributed)

Use the Redis provider for distributed rate limiting across multiple processes or servers.
Every `inc()` and `is_allowed()` call executes an atomic Lua script against Redis.

**Requirements:**

- Redis >= 7.2
- Tokio or Smol async runtime

```toml
[dependencies]
trypema = { version = "1.0", features = ["redis-tokio"] }
redis = { version = "1", default-features = false, features = ["aio", "tokio-comp", "connection-manager"] }
tokio = { version = "1", features = ["full"] }
```

```rust,no_run
use std::sync::Arc;

use trypema::{
    HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision, RateLimiter, RateLimiterOptions,
    SuppressionFactorCacheMs, WindowSizeSeconds,
};
use trypema::hybrid::SyncIntervalMs;
use trypema::local::LocalRateLimiterOptions;
use trypema::redis::{RedisKey, RedisRateLimiterOptions};

#[tokio::main]
async fn main() -> Result<(), trypema::TrypemaError> {
    // Create a Redis connection manager (handles pooling and reconnection).
    let client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
    let connection_manager = client.get_connection_manager().await.unwrap();

    let window_size_seconds = WindowSizeSeconds::try_from(60).unwrap();
    let rate_group_size_ms = RateGroupSizeMs::try_from(10).unwrap();
    let hard_limit_factor = HardLimitFactor::default();
    let suppression_factor_cache_ms = SuppressionFactorCacheMs::default();
    let sync_interval_ms = SyncIntervalMs::default();

    let rl = Arc::new(RateLimiter::new(RateLimiterOptions {
        local: LocalRateLimiterOptions {
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            suppression_factor_cache_ms,
        },
        redis: RedisRateLimiterOptions {
            connection_manager,
            prefix: None, // Defaults to "trypema"
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            suppression_factor_cache_ms,
            sync_interval_ms,
        },
    }));

    rl.run_cleanup_loop();

    let rate_limit = RateLimit::try_from(5.0).unwrap();
    let key = RedisKey::try_from("user_123".to_string()).unwrap();

    // --- Absolute strategy ---
    match rl.redis().absolute().inc(&key, &rate_limit, 1).await? {
        RateLimitDecision::Allowed => {
            // Request allowed, proceed.
        }
        RateLimitDecision::Rejected { retry_after_ms, .. } => {
            // Rejected — send HTTP 429 with Retry-After header.
            let _ = retry_after_ms;
        }
        RateLimitDecision::Suppressed { .. } => {
            unreachable!("absolute strategy never returns Suppressed");
        }
    }

    // --- Suppressed strategy ---
    let sf = rl.redis().suppressed().get_suppression_factor(&key).await?;
    let _ = sf;

    match rl.redis().suppressed().inc(&key, &rate_limit, 1).await? {
        RateLimitDecision::Allowed => {
            // Below capacity, proceed.
        }
        RateLimitDecision::Suppressed {
            is_allowed: true,
            suppression_factor,
        } => {
            // Suppression active but this request admitted.
            let _ = suppression_factor;
        }
        RateLimitDecision::Suppressed {
            is_allowed: false,
            suppression_factor,
        } => {
            // Suppressed — do not proceed. When over the hard limit, suppression_factor is 1.0.
            let _ = suppression_factor;
        }
        RateLimitDecision::Rejected { .. } => {
            unreachable!("suppressed strategy never returns Rejected");
        }
    }

    Ok(())
}
```

### Hybrid Provider (Local Fast-Path + Redis Sync)

The hybrid provider combines the low latency of in-process state with the distributed
consistency of Redis. It maintains a local counter per key and periodically flushes
accumulated increments to Redis in batches. Between flushes, admission decisions are
served from local state without any Redis I/O.

This is the recommended provider for high-throughput distributed APIs where per-request
Redis round-trips are too expensive.

**Trade-off:** Admission decisions reflect Redis state with up to `sync_interval_ms` of lag.

```rust,no_run
use std::sync::Arc;

use trypema::{
    HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimiter, RateLimiterOptions,
    SuppressionFactorCacheMs, WindowSizeSeconds,
};
use trypema::hybrid::SyncIntervalMs;
use trypema::local::LocalRateLimiterOptions;
use trypema::redis::{RedisKey, RedisRateLimiterOptions};

#[tokio::main]
async fn main() -> Result<(), trypema::TrypemaError> {
    let client = redis::Client::open("redis://127.0.0.1:6379/").unwrap();
    let connection_manager = client.get_connection_manager().await.unwrap();

    let window_size_seconds = WindowSizeSeconds::try_from(60).unwrap();
    let rate_group_size_ms = RateGroupSizeMs::try_from(10).unwrap();
    let hard_limit_factor = HardLimitFactor::default();
    let suppression_factor_cache_ms = SuppressionFactorCacheMs::default();
    let sync_interval_ms = SyncIntervalMs::default();

    let rl = Arc::new(RateLimiter::new(RateLimiterOptions {
        local: LocalRateLimiterOptions {
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            suppression_factor_cache_ms,
        },
        redis: RedisRateLimiterOptions {
            connection_manager,
            prefix: None,
            window_size_seconds,
            rate_group_size_ms,
            hard_limit_factor,
            suppression_factor_cache_ms,
            sync_interval_ms,
        },
    }));

    let key = RedisKey::try_from("user_123".to_string())?;
    let rate = RateLimit::try_from(10.0)?;

    // Absolute: same API as the Redis provider, served from local state.
    let _decision = rl.hybrid().absolute().inc(&key, &rate, 1).await?;

    // Suppressed: probabilistic admission from local state.
    let _decision = rl.hybrid().suppressed().inc(&key, &rate, 1).await?;

    // Query suppression factor for observability.
    let _sf = rl.hybrid().suppressed().get_suppression_factor(&key).await?;

    Ok(())
}
```

## Core Concepts

### Keyed Limiting

Every rate limiting operation targets a specific **key** — a string that identifies the
resource being limited (e.g., a user ID, API endpoint, or IP address). Each key maintains
completely independent rate limiting state.

- **Local provider:** Keys are arbitrary `&str` values. Any string is valid.
- **Redis / Hybrid providers:** Keys use the `RedisKey` newtype, which enforces validation
  rules (non-empty, ≤ 255 bytes, no `:` character).

### Rate Limits

Rate limits are expressed as **requests per second** using the `RateLimit` type, which wraps
a positive `f64`. Non-integer rates like `0.5` (one request every 2 seconds) or `5.5` are
fully supported.

The actual **window capacity** — the maximum number of requests allowed within the sliding
window — is computed as:

```text
window_capacity = window_size_seconds × rate_limit
```

**Example:** With a 60-second window and a rate limit of `5.0`:

- Window capacity = 60 × 5.0 = **300 requests**

### Sliding Windows

Trypema uses a **sliding time window** for admission decisions. At any point in time,
the limiter considers all activity within the last `window_size_seconds`. As time advances,
old buckets expire and new capacity becomes available continuously.

This avoids the boundary-reset problem of fixed windows, where a burst at the end of one
window followed by a burst at the start of the next could allow 2× the intended rate.

### Bucket Coalescing

To reduce memory and computational overhead, increments that occur within
`rate_group_size_ms` of each other are merged into the same time bucket rather than
tracked individually.

For example, with `rate_group_size_ms = 10`:

- 50 requests arriving within 10ms produce **1 bucket** with count = 50
- 50 requests spread over 100ms produce roughly **10 buckets** with count ≈ 5 each

Coarser coalescing (larger values) means fewer buckets, less memory, and faster iteration
but also less precise `retry_after_ms` estimates in rejection metadata.

### Sticky Rate Limits

The first `inc()` call for a given key stores the rate limit for that key's lifetime in the
limiter. Subsequent `inc()` calls for the same key use the stored limit and ignore the
`rate_limit` argument.

This prevents races where concurrent callers might specify different limits for the same key.
If you need to change a key's rate limit, you must let the old entry expire (or be cleaned up)
and start fresh.

## Configuration

### `LocalRateLimiterOptions`

| Field                         | Type                       | Default      | Valid Range | Description                                                                   |
| ----------------------------- | -------------------------- | ------------ | ----------- | ----------------------------------------------------------------------------- |
| `window_size_seconds`         | `WindowSizeSeconds`        | _(required)_ | ≥ 1         | Length of the sliding window. Larger = smoother but slower recovery.          |
| `rate_group_size_ms`          | `RateGroupSizeMs`          | 100 ms       | ≥ 1         | Bucket coalescing interval. Larger = less memory, coarser timing.             |
| `hard_limit_factor`           | `HardLimitFactor`          | 1.0          | ≥ 1.0       | Multiplier for hard cutoff in suppressed strategy. Ignored by absolute.       |
| `suppression_factor_cache_ms` | `SuppressionFactorCacheMs` | 100 ms       | ≥ 1         | How long to cache the computed suppression factor per key before recomputing. |

### `RedisRateLimiterOptions`

Includes the same four fields as `LocalRateLimiterOptions`, plus:

| Field                | Type                | Default      | Description                                                                                                      |
| -------------------- | ------------------- | ------------ | ---------------------------------------------------------------------------------------------------------------- |
| `connection_manager` | `ConnectionManager` | _(required)_ | Redis connection (handles pooling and reconnection).                                                             |
| `prefix`             | `Option<RedisKey>`  | `"trypema"`  | Namespace prefix for all Redis keys. Pattern: `{prefix}:{user_key}:{rate_type}:{suffix}`.                        |
| `sync_interval_ms`   | `SyncIntervalMs`    | 10 ms        | How often the **hybrid** provider flushes local increments to Redis. The pure Redis provider ignores this value. |

## Rate Limit Decisions

All strategies return a `RateLimitDecision` enum with three variants:

### `Allowed`

The request is within the rate limit. The increment has been recorded in the limiter's state.

### `Rejected`

The request exceeds the rate limit and should not proceed. The increment was **not** recorded.

**Fields:**

- `window_size_seconds` — The configured sliding window size (in seconds).
- `retry_after_ms` — **Best-effort** estimate of milliseconds until capacity becomes available.
  Computed from the oldest active bucket's remaining TTL. Use this to set an HTTP `Retry-After`
  header or as a backoff hint.
- `remaining_after_waiting` — **Best-effort** estimate of how many requests will still be
  counted in the window after `retry_after_ms` elapses.

**Important:** These hints are approximate. Bucket coalescing and concurrent access both
reduce accuracy. Use them for guidance, not as strict guarantees.

### `Suppressed`

Only returned by the suppressed strategy. Indicates that probabilistic suppression is active.

**Fields:**

- `suppression_factor` — The current suppression rate (0.0 = no suppression, 1.0 = full suppression).
- `is_allowed` — Whether this specific call was admitted. **Always use this field as the
  admission decision**, not the variant name alone.

**Tracking observed vs. declined:**

The suppressed strategy tracks two counters per key:

- **observed** (`total`): all calls seen (always incremented, regardless of admission)
- **declined**: calls denied by suppression (`is_allowed: false`)

From these you can derive accepted usage: `accepted = observed - declined`.

## Rate Limiting Strategies

### Absolute Strategy

**Access:** `rl.local().absolute()`, `rl.redis().absolute()`, or `rl.hybrid().absolute()`

A deterministic sliding-window limiter that strictly enforces rate limits.

**How it works:**

1. Compute window capacity: `window_size_seconds × rate_limit`
2. Sum all bucket counts within the current sliding window
3. If `total < capacity` → allow and record the increment
4. If `total >= capacity` → reject (increment is **not** recorded)

**Characteristics:**

- Predictable, binary decisions (allowed or rejected)
- Rate limits are **sticky** — the first `inc()` call for a key stores the limit
- Rejected requests include best-effort backoff hints
- Best-effort under concurrent load: multiple threads may observe "allowed" simultaneously

**Use cases:**

- Simple per-key rate caps (e.g., API rate limiting)
- Scenarios where predictable enforcement matters more than graceful degradation
- Both single-process (local) and multi-process (Redis/hybrid) deployments

### Suppressed Strategy

**Access:** `rl.local().suppressed()`, `rl.redis().suppressed()`, or `rl.hybrid().suppressed()`

A probabilistic strategy that gracefully degrades under load by suppressing an increasing
fraction of requests as the key approaches and exceeds its limit.

**Three operating regimes:**

1. **Below capacity** — Suppression factor is `0.0`. All requests return `Allowed`.

2. **At or above capacity (below hard limit)** — A suppression factor is computed and
   each request is probabilistically allowed with probability `1.0 - suppression_factor`.
   Returns `Suppressed { is_allowed: true/false, suppression_factor }`.

3. **Over hard limit** (`observed_usage >= window_capacity × hard_limit_factor`) —
   Suppression factor is `1.0`. All requests denied.

**Suppression factor calculation:**

```text
suppression_factor = 1.0 - (rate_limit / perceived_rate)

where: perceived_rate = max(average_rate_in_window, rate_in_last_1000ms)
```

The `rate_in_last_1000ms` term is computed at millisecond granularity, allowing suppression
to respond quickly to short traffic spikes.

**Inspiration:** Based on
[Ably's distributed rate limiting approach](https://ably.com/blog/distributed-rate-limiting-scale-your-platform).

## Providers In-Depth

### Local Provider

**Access:** `rl.local()`

Stores all state in-process using `DashMap` with atomic counters. Sub-microsecond latency,
no external dependencies.

**Best-effort concurrency:** The admission check and increment are not a single atomic
operation. Under high concurrency, multiple threads can observe "allowed" simultaneously
and all proceed, causing temporary overshoot. This is by design for performance.

### Redis Provider

**Access:** `rl.redis()`

Executes all operations as atomic Lua scripts against Redis 7.2+. Each call results in one
Redis round-trip.

**Server-side timestamps:** Lua scripts use `redis.call("TIME")` for all timestamp
calculations, avoiding client clock skew issues.

**Data model:** For a key `K` with prefix `P` and rate type `T`:

| Redis Key           | Type       | Purpose                                                 |
| ------------------- | ---------- | ------------------------------------------------------- |
| `P:K:T:h`           | Hash       | Sliding window buckets (`timestamp_ms → count`)         |
| `P:K:T:a`           | Sorted Set | Active bucket timestamps (for efficient eviction)       |
| `P:K:T:w`           | String     | Window limit (set on first call, refreshed with EXPIRE) |
| `P:K:T:t`           | String     | Total count across all buckets                          |
| `P:K:T:d`           | String     | Total declined count (suppressed strategy only)         |
| `P:K:T:hd`          | Hash       | Declined counts per bucket (suppressed only)            |
| `P:K:T:sf`          | String     | Cached suppression factor with PX TTL (suppressed only) |
| `P:active_entities` | Sorted Set | All active keys (used by cleanup)                       |

### Hybrid Provider

**Access:** `rl.hybrid()`

Maintains local in-memory state per key with a background actor that periodically batches
increments and flushes them to Redis. Between flushes, admission is served from local state
without Redis I/O.

**When to use:** When per-request Redis latency is unacceptable. Trade-off: admission decisions
may lag behind Redis state by up to `sync_interval_ms`.

## Important Semantics & Limitations

### Eviction Granularity

**Local provider:** Uses `Instant::elapsed().as_millis()` (millisecond granularity, lazy eviction).

**Redis provider:** Uses Redis server time in milliseconds inside Lua scripts; auxiliary keys
use standard TTL commands.

### Memory Growth

Keys are **not automatically removed** when inactive. Use `run_cleanup_loop()` to periodically
remove stale keys:

```rust,no_run
use std::sync::Arc;

use trypema::{HardLimitFactor, RateGroupSizeMs, RateLimiter, RateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds};
use trypema::local::LocalRateLimiterOptions;

let window_size_seconds = WindowSizeSeconds::try_from(60).unwrap();
let rate_group_size_ms = RateGroupSizeMs::try_from(10).unwrap();
let hard_limit_factor = HardLimitFactor::default();
let suppression_factor_cache_ms = SuppressionFactorCacheMs::default();

let options = RateLimiterOptions {
    local: LocalRateLimiterOptions {
        window_size_seconds,
        rate_group_size_ms,
        hard_limit_factor,
        suppression_factor_cache_ms,
    },
};

let rl = Arc::new(RateLimiter::new(options));

// Default: stale_after = 10 minutes, cleanup_interval = 30 seconds.
rl.run_cleanup_loop();

// Or with custom timing:
// rl.run_cleanup_loop_with_config(5 * 60 * 1000, 60 * 1000);

// Stop cleanup (best-effort, exits on next tick).
rl.stop_cleanup_loop();
```

**Memory safety:** The cleanup loop holds only a `Weak<RateLimiter>` reference. Dropping all
`Arc` references automatically stops cleanup.

## Tuning Guide

### `window_size_seconds`

**What it controls:** How far back in time the limiter looks when making admission decisions.

- **Larger windows** (60–300s): smooth out bursts, more forgiving. But slower recovery and
  higher memory per key.
- **Smaller windows** (5–30s): faster recovery, lower memory. But less burst tolerance.

**Recommendation:** Start with **60 seconds**.

### `rate_group_size_ms`

**What it controls:** Coalescing interval for grouping increments into time buckets.

- **Larger** (50–100ms): fewer buckets, less memory, better performance. Coarser `retry_after_ms`.
- **Smaller** (1–20ms): more accurate metadata, finer tracking. Higher memory and overhead.

**Recommendation:** Start with **10ms**.

### `hard_limit_factor`

**What it controls:** Hard cutoff multiplier for the suppressed strategy.
`hard_limit = rate_limit × hard_limit_factor`.

- `1.0` (default): no headroom, suppressed strategy behaves more like absolute.
- `1.5–2.0` (**recommended**): smooth degradation with 50–100% burst headroom.
- `> 2.0`: very permissive gap between suppression start and hard limit.

**Only relevant for:** Suppressed strategy. Absolute ignores this.

### `suppression_factor_cache_ms`

**What it controls:** How long the computed suppression factor is cached per key.

- Shorter (10–50ms): faster reaction to traffic changes, more CPU.
- Longer (100–1000ms): less overhead, slower reaction.

**Recommendation:** Start with **100ms** (the default).

### `sync_interval_ms` (Hybrid provider only)

**What it controls:** How often the hybrid provider flushes local increments to Redis.

- Shorter (5–10ms): less lag, more Redis writes.
- Longer (50–100ms): fewer writes, more stale decisions.

**Recommendation:** Start with **10ms** (the default). Keep `sync_interval_ms` ≤ `rate_group_size_ms`.

## Redis Provider Details

This section applies to both the **Redis** and **Hybrid** providers.

### Requirements

- **Redis version:** >= 7.2.0
- **Async runtime:** Tokio or Smol

### Key Constraints

Redis keys use the `RedisKey` newtype with validation:

- **Must not be empty**
- **Must be ≤ 255 bytes**
- **Must not contain** `:` (used internally as a separator)

```rust,no_run
use trypema::redis::RedisKey;

// Valid keys
let _ = RedisKey::try_from("user_123".to_string()).unwrap();
let _ = RedisKey::try_from("api_v2_endpoint".to_string()).unwrap();

// Invalid: contains ':'
let _ = RedisKey::try_from("user:123".to_string()); // Err

// Invalid: empty
let _ = RedisKey::try_from("".to_string()); // Err
```

### Feature Flags

Control Redis support at compile time:

```toml
# Default: local-only (no Redis dependency)
trypema = { version = "1.0" }

# Enable Redis + hybrid providers with Tokio runtime
trypema = { version = "1.0", features = ["redis-tokio"] }

# Enable Redis + hybrid providers with Smol runtime
trypema = { version = "1.0", default-features = false, features = ["redis-smol"] }
```

The `redis-tokio` and `redis-smol` features are **mutually exclusive**. Enabling both
produces a compile error.

## Project Structure

```text
src/
├── lib.rs                               # Crate root: re-exports, feature gates, module declarations
├── rate_limiter.rs                      # RateLimiter facade + RateLimiterOptions
├── common.rs                            # Shared types (RateLimitDecision, RateLimit, WindowSizeSeconds, etc.)
├── error.rs                             # TrypemaError enum
├── runtime.rs                           # Async runtime abstraction (tokio / smol)
├── local/
│   ├── mod.rs                           # Local provider module
│   ├── local_rate_limiter_provider.rs   # LocalRateLimiterProvider + LocalRateLimiterOptions
│   ├── absolute_local_rate_limiter.rs   # AbsoluteLocalRateLimiter
│   └── suppressed_local_rate_limiter.rs # SuppressedLocalRateLimiter
├── redis/
│   ├── mod.rs                           # Redis provider module
│   ├── common.rs                        # RedisKey, RedisKeyGenerator
│   ├── redis_rate_limiter_provider.rs   # RedisRateLimiterProvider + RedisRateLimiterOptions
│   ├── absolute_redis_rate_limiter.rs   # AbsoluteRedisRateLimiter (Lua scripts)
│   └── suppressed_redis_rate_limiter.rs # SuppressedRedisRateLimiter (Lua scripts)
└── hybrid/
    ├── mod.rs                           # Hybrid provider module
    ├── common.rs                        # SyncIntervalMs, committer utilities
    ├── hybrid_rate_limiter_provider.rs  # HybridRateLimiterProvider
    ├── absolute_hybrid_rate_limiter.rs  # AbsoluteHybridRateLimiter (state machine)
    ├── absolute_hybrid_redis_proxy.rs   # Redis I/O proxy for absolute hybrid
    ├── suppressed_hybrid_rate_limiter.rs # SuppressedHybridRateLimiter (state machine)
    ├── suppressed_hybrid_redis_proxy.rs # Redis I/O proxy for suppressed hybrid
    └── redis_commiter.rs               # RedisCommitter: background batch-flush actor
```

## Running Redis-Backed Tests

The Redis and hybrid provider integration tests require a running Redis instance.
Set `REDIS_URL` to enable them:

```bash
REDIS_URL=redis://127.0.0.1:6379/ cargo test --features redis-tokio
```

## Roadmap

**Planned:**

- [ ] Metrics and observability hooks

**Non-goals:**

- Strict linearizability (by design)
- Built-in retry logic (application-specific)

## Contributing

Feedback, issues, and PRs are welcome. Please include tests for new features.

## License

MIT License. See the [LICENSE](LICENSE) file for details.
