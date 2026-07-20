# Trypema Rate Limiter

[![Crates.io](https://img.shields.io/crates/v/trypema.svg)](https://crates.io/crates/trypema)
[![Documentation](https://docs.rs/trypema/badge.svg)](https://docs.rs/trypema)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

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

Trypema is a sliding-window rate limiting crate with:

- **Local** in-memory limiting for single-process workloads
- **Redis** best-effort distributed limiting with atomic Lua scripts
- **Hybrid** best-effort distributed limiting with a local fast path and periodic Redis sync

Each provider offers:

- **Absolute** deterministic allow/reject decisions
- **Suppressed** probabilistic degradation near or above the target rate

## Picking a Provider

| Provider   | Best for                                   | Trade-off                                        |
| ---------- | ------------------------------------------ | ------------------------------------------------ |
| **Local**  | single-process services, jobs, CLIs        | not shared across processes                      |
| **Redis**  | shared limits across processes or machines | every check performs Redis I/O                   |
| **Hybrid** | high-throughput distributed request paths  | state can lag behind Redis by `sync_interval_ms` |

Redis and hybrid providers require Redis 7.2+ and exactly one runtime feature: `redis-tokio` or
`redis-smol`.

## Installation

Local-only:

```toml
[dependencies]
trypema = "1"
```

Redis with Tokio:

```toml
[dependencies]
trypema = { version = "1", features = ["redis-tokio"] }
```

Redis with Smol:

```toml
[dependencies]
trypema = { version = "1", features = ["redis-smol"] }
```

## Quick Start

### Common Types

`RateLimit` is the per-second limit value used by all providers. `RedisKey` is the validated key
type required by the Redis and hybrid providers. `RateLimitDecision` is the result returned by
`inc()` and `is_allowed()`, and `RateLimiter` is the top-level entry point used throughout the
examples. `SuppressedRateLimitSnapshot` is returned by `get()` on each suppressed provider.

```rust,no_run
use trypema::RateLimit;
use trypema::redis::RedisKey;

let _rate_a = RateLimit::new(5.0).unwrap();
let _rate_b = RateLimit::try_from(5.0).unwrap();
let _rate_c = RateLimit::new_or_panic(5.0);

let _key_a = RedisKey::new("user_123".to_string()).unwrap();
let _key_b = RedisKey::try_from("user_123".to_string()).unwrap();
let _key_c = RedisKey::new_or_panic("user_123".to_string());
```

### Create a `RateLimiter`

These examples show the local-only and Redis-enabled builder paths.

```rust
use trypema::{RateLimit, RateLimitDecision, RateLimiterBuilder};

let rl = RateLimiterBuilder::default()
    // Optional: override the sliding window size.
    .window_size_seconds(60)
    // Optional: override bucket coalescing.
    .rate_group_size_ms(10)
    // Optional: tune suppressed-mode headroom.
    .hard_limit_factor(1.5)
    // Optional: tune cleanup cadence.
    .cleanup_interval_ms(15_000)
    .build()
    .unwrap();

let rate = RateLimit::try_from(5.0).unwrap();

assert!(matches!(
    rl.local().absolute().inc("user_123", &rate, 1),
    RateLimitDecision::Allowed
));
```

```rust
use trypema::{RateLimit, RateLimitDecision, RateLimiter};

let rl = RateLimiter::builder().build().unwrap();
let rate = RateLimit::try_from(5.0).unwrap();

assert!(matches!(
    rl.local().absolute().inc("user_123", &rate, 1),
    RateLimitDecision::Allowed
));
```

```rust,no_run
use trypema::{RateLimit, RateLimitDecision, RateLimiter};
use trypema::redis::RedisKey;

async fn example() -> Result<(), trypema::TrypemaError> {
    let url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    let connection_manager = redis::Client::open(url)?
        .get_connection_manager()
        .await?;

    let rl = RateLimiter::builder(connection_manager)
        // Optional: override the sliding window size.
        .window_size_seconds(60)
        // Optional: override bucket coalescing.
        .rate_group_size_ms(10)
        // Optional: tune suppressed-mode headroom.
        .hard_limit_factor(1.5)
        // Optional: only available with `redis-tokio` or `redis-smol`.
        .redis_prefix(RedisKey::new_or_panic("docs".to_string()))
        // Optional: only available with `redis-tokio` or `redis-smol`.
        .sync_interval_ms(10)
        .build()?;

    let key = RedisKey::try_from("user_123".to_string())?;
    let rate = RateLimit::try_from(5.0).unwrap();

    assert!(matches!(
        rl.redis().absolute().inc(&key, &rate, 1).await?,
        RateLimitDecision::Allowed
    ));

    Ok(())
}
```

```rust
use std::sync::Arc;

use trypema::{
    HardLimitFactor, RateGroupSizeMs, RateLimit, RateLimitDecision, RateLimiter,
    RateLimiterOptions, SuppressionFactorCacheMs, WindowSizeSeconds,
};
use trypema::local::LocalRateLimiterOptions;

let options = RateLimiterOptions {
    local: LocalRateLimiterOptions {
        window_size_seconds: WindowSizeSeconds::new_or_panic(60),
        rate_group_size_ms: RateGroupSizeMs::new_or_panic(10),
        hard_limit_factor: HardLimitFactor::new_or_panic(1.5),
        suppression_factor_cache_ms: SuppressionFactorCacheMs::new_or_panic(100),
    },
};

let rl = Arc::new(RateLimiter::new(options));
rl.run_cleanup_loop();

let rate = RateLimit::try_from(5.0).unwrap();

assert!(matches!(
    rl.local().absolute().inc("user_123", &rate, 1),
    RateLimitDecision::Allowed
));
```

`build()` starts the cleanup loop automatically. `RateLimiter::new(...)` does not, so call
`run_cleanup_loop()` yourself when you want background cleanup of stale keys.

### Local Read-Only Check

```rust
use trypema::{RateLimit, RateLimitDecision, RateLimiter};

let rl = RateLimiter::builder().build().unwrap();
let limiter = rl.local().absolute();
let rate = RateLimit::try_from(5.0).unwrap();

assert!(matches!(limiter.is_allowed("user_123"), RateLimitDecision::Allowed));
assert!(matches!(
    limiter.inc("user_123", &rate, 1),
    RateLimitDecision::Allowed
));
```

### Read and Conditionally Adjust Current Totals

Every strategy exposes `get()`. Absolute strategies return the live sliding-window total directly.
Suppressed strategies return a `SuppressedRateLimitSnapshot` containing the observed total,
declined total, and current suppression factor. Unknown keys return zero-valued results without
creating limiter state. Reads may lazily evict expired buckets; hybrid snapshots also include
pending increments and declines from the current instance.

Hybrid absolute also exposes `get_inferred()`. It uses initialized local limiting state when that
state can answer the read. Undefined state and expired rejection caches refresh from Redis first,
then the method infers the total from the updated state. The local fast path can lag later remote
commits and Redis-side expiration. Use `get()` when every read must consult Redis and overlay this
instance's pending increments.

`set_if()` replaces the total only when its `RateLimitComparator` matches the current live total.
A comparator miss changes nothing. A match replaces history, recomputes the stored window limit
(the hard window limit for suppressed strategies) from the supplied rate limit, and returns
`(new_total, old_total)`. A matched target of zero deletes the key completely.

`set_if_preserve_history()` keeps history instead of replacing it. `PreserveNewest` reductions
consume oldest buckets and increases extend the newest bucket. `PreserveOldest` does the reverse.
Redis and hybrid variants use the same contract asynchronously.

```rust
use trypema::{
    HistoryPreservation, RateLimit, RateLimitComparator, RateLimitDecision, RateLimiter,
};

let rl = RateLimiter::builder().build().unwrap();
let limiter = rl.local().absolute();
let rate = RateLimit::try_from(10.0).unwrap();
assert!(matches!(limiter.inc("user_123", &rate, 2), RateLimitDecision::Allowed));
assert_eq!(limiter.get("user_123"), 2);

assert_eq!(
    limiter.set_if("user_123", &rate, RateLimitComparator::Eq(2), 5),
    (5, 2)
);
assert_eq!(
    limiter.set_if_preserve_history(
        "user_123",
        &rate,
        RateLimitComparator::Nil,
        3,
        HistoryPreservation::PreserveNewest,
    ),
    (3, 5)
);
assert_eq!(
    limiter.set_if("user_123", &rate, RateLimitComparator::Nil, 0),
    (0, 3)
);
```

### Read Current Suppression State

```rust
use trypema::RateLimiter;

let rl = RateLimiter::builder().build().unwrap();

let snapshot = rl.local().suppressed().get("user_123");
assert_eq!(snapshot.total, 0);
assert_eq!(snapshot.total_declined, 0);
assert_eq!(snapshot.suppression_factor, 0.0);
```

### Redis Absolute

```rust,no_run
use trypema::{RateLimit, RateLimitDecision, RateLimiter};
use trypema::redis::RedisKey;

async fn example() -> Result<(), trypema::TrypemaError> {
    let url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    let connection_manager = redis::Client::open(url)?
        .get_connection_manager()
        .await?;

    let rl = RateLimiter::builder(connection_manager).build()?;
    let key = RedisKey::try_from("user_123".to_string())?;
    let rate = RateLimit::try_from(5.0).unwrap();

    assert!(matches!(
        rl.redis().absolute().inc(&key, &rate, 1).await?,
        RateLimitDecision::Allowed
    ));

    Ok(())
}
```

### Redis Suppressed State

```rust,no_run
use trypema::RateLimiter;
use trypema::redis::RedisKey;

async fn example() -> Result<(), trypema::TrypemaError> {
    let url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    let connection_manager = redis::Client::open(url)?
        .get_connection_manager()
        .await?;

    let rl = RateLimiter::builder(connection_manager).build()?;
    let key = RedisKey::try_from("user_123".to_string())?;

    let snapshot = rl.redis().suppressed().get(&key).await?;
    assert_eq!(snapshot.total, 0);
    assert_eq!(snapshot.total_declined, 0);
    assert_eq!(snapshot.suppression_factor, 0.0);

    Ok(())
}
```

### Hybrid Absolute

```rust,no_run
use trypema::{RateLimit, RateLimitDecision, RateLimiter};
use trypema::redis::RedisKey;

async fn example() -> Result<(), trypema::TrypemaError> {
    let url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
    let connection_manager = redis::Client::open(url)?
        .get_connection_manager()
        .await?;

    let rl = RateLimiter::builder(connection_manager).build()?;
    let key = RedisKey::try_from("user_123".to_string())?;
    let rate = RateLimit::try_from(10.0).unwrap();

    assert!(matches!(
        rl.hybrid().absolute().inc(&key, &rate, 1).await?,
        RateLimitDecision::Allowed
    ));
    assert_eq!(rl.hybrid().absolute().get_inferred(&key).await?, 1);
    assert_eq!(rl.hybrid().absolute().get(&key).await?, 1);

    Ok(())
}
```

## Rate Limit Decisions

Every strategy returns `RateLimitDecision`:

- `Allowed` means the request should proceed.
- `Rejected` means the absolute strategy denied the request. Its best-effort hints report the
  remaining wait until the oldest live bucket expires and the capacity released at that point.
- `Suppressed` means the suppressed strategy is active; check `is_allowed` for the admission
  result.

## Notes

- Rate limits are sticky per key: the first `inc()` computes and stores the key's window limit
  (the hard window limit for suppressed strategies).
- Bucket coalescing trades timing precision for lower overhead.
- Redis and hybrid modes provide best-effort distributed limiting, not strict linearizability.

## Testing Redis-Backed Docs

The canonical runnable examples live in the crate docs and API docs. Redis and hybrid doctests
need a live Redis instance and `REDIS_URL`, for example:

```bash
REDIS_URL=redis://127.0.0.1:6379/ cargo test -p trypema --doc --features redis-tokio
```

Use `redis-smol` instead when validating the Smol-backed feature set.
