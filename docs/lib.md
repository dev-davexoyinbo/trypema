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

Trypema provides sliding-window rate limiting with two strategies across three providers:

- **Local** for single-process, in-memory limiting
- **Redis** for best-effort distributed limiting with one Redis round-trip per call
- **Hybrid** for best-effort distributed limiting with a local fast path and periodic Redis sync

Each provider offers:

- **Absolute** for deterministic allow/reject decisions
- **Suppressed** for probabilistic degradation near or above the target rate

## Choosing a Provider

| Provider   | Best for                                             | Trade-off                                            |
| ---------- | ---------------------------------------------------- | ---------------------------------------------------- |
| **Local**  | single-process services, jobs, CLIs                  | state is not shared across processes                 |
| **Redis**  | strictest distributed coordination this crate offers | every check performs Redis I/O                       |
| **Hybrid** | high-throughput distributed paths                    | decisions may lag behind Redis by `sync_interval_ms` |

## Installation

Local-only usage:

```toml
[dependencies]
trypema = "1"
```

Redis-backed usage with Tokio:

```toml
[dependencies]
trypema = { version = "1", features = ["redis-tokio"] }
```

Redis-backed usage with Smol:

```toml
[dependencies]
trypema = { version = "1", features = ["redis-smol"] }
```

Redis and hybrid providers require:

- Redis 7.2+
- exactly one runtime feature: `redis-tokio` or `redis-smol`

## Quick Start

### Common Types

[`RateLimit`](crate::RateLimit) is the per-second limit value used by all providers.
`RedisKey` is the validated key type required by the Redis and hybrid providers when those
features are enabled.
[`RateLimitDecision`](crate::RateLimitDecision) is the result returned by methods such as
[`AbsoluteLocalRateLimiter::inc`](crate::AbsoluteLocalRateLimiter::inc) and
[`AbsoluteLocalRateLimiter::is_allowed`](crate::AbsoluteLocalRateLimiter::is_allowed), and
[`RateLimiter`](crate::RateLimiter) is the top-level entry point used throughout the examples.

```rust
use trypema::RateLimit;
# #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
use trypema::redis::RedisKey;

let _rate_a = RateLimit::new(5.0).unwrap();
let _rate_b = RateLimit::try_from(5.0).unwrap();
let _rate_c = RateLimit::new_or_panic(5.0);

# #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
# {
let _key_a = RedisKey::new("user_123".to_string()).unwrap();
let _key_b = RedisKey::try_from("user_123".to_string()).unwrap();
let _key_c = RedisKey::new_or_panic("user_123".to_string());
# }
```

### Create a [`RateLimiter`](crate::RateLimiter)

[`RateLimiterBuilder`](crate::RateLimiterBuilder) is the fluent setup type behind
[`RateLimiter::builder`](crate::RateLimiter::builder). Without Redis features you can start from
`RateLimiterBuilder::default()` or [`RateLimiter::builder`](crate::RateLimiter::builder). With
`redis-tokio` or `redis-smol`, use [`RateLimiter::builder`](crate::RateLimiter::builder) with a
`connection_manager`, because the builder needs Redis connectivity up front.

Use `RateLimiterBuilder::default()` when you want to start from local-only defaults and override
just a few optional settings.

```rust
# #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
# {
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
# }
```

Use the local-only [`RateLimiter::builder`](crate::RateLimiter::builder) helper when you want the
simplest setup with defaults and automatic cleanup-loop startup.

```rust
# #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
# {
use trypema::{RateLimit, RateLimitDecision, RateLimiter};

let rl = RateLimiter::builder().build().unwrap();
let rate = RateLimit::try_from(5.0).unwrap();

assert!(matches!(
    rl.local().absolute().inc("user_123", &rate, 1),
    RateLimitDecision::Allowed
));
# }
```

With `redis-tokio` or `redis-smol`, create the builder with a Redis `connection_manager`.

```rust
# #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
# {
# trypema::__doctest_helpers::with_redis_rate_limiter(|_rl| async move {
use trypema::{RateLimit, RateLimitDecision, RateLimiter};
use trypema::redis::RedisKey;

let url = std::env::var("REDIS_URL")
    .unwrap_or_else(|_| "redis://127.0.0.1:6379/".to_string());
let connection_manager = redis::Client::open(url)
    .unwrap()
    .get_connection_manager()
    .await
    .unwrap();

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
    .build()
    .unwrap();

let key = RedisKey::try_from(trypema::__doctest_helpers::unique_key()).unwrap();
let rate = RateLimit::try_from(5.0).unwrap();

assert!(matches!(
    rl.redis().absolute().inc(&key, &rate, 1).await.unwrap(),
    RateLimitDecision::Allowed
));
# let _ = _rl;
# });
# }
```

Use [`RateLimiterOptions`](crate::RateLimiterOptions) and
[`LocalRateLimiterOptions`](crate::local::LocalRateLimiterOptions) when you want explicit control
over configuration.

```rust
# #[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
# {
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
# }
```

[`RateLimiterBuilder::build`](crate::RateLimiterBuilder::build) starts the cleanup loop
automatically. [`RateLimiter::new`](crate::RateLimiter::new) does not, so call
[`RateLimiter::run_cleanup_loop`](crate::RateLimiter::run_cleanup_loop) yourself when you want
background cleanup of stale keys.

### Local Read-Only Check

Use [`AbsoluteLocalRateLimiter::is_allowed`](crate::AbsoluteLocalRateLimiter::is_allowed) when you
want to inspect whether a request would be allowed without recording an increment.

```rust
# let rl = trypema::__doctest_helpers::rate_limiter();
use trypema::{RateLimit, RateLimitDecision};

let limiter = rl.local().absolute();
let rate = RateLimit::try_from(5.0).unwrap();

assert!(matches!(limiter.is_allowed("user_123"), RateLimitDecision::Allowed));
assert!(matches!(
    limiter.inc("user_123", &rate, 1),
    RateLimitDecision::Allowed
));
```

### Read Current Suppression State

Use [`SuppressedLocalRateLimiter::get_suppression_factor`](crate::SuppressedLocalRateLimiter::get_suppression_factor)
to inspect suppression state without recording a request.

```rust
# let rl = trypema::__doctest_helpers::rate_limiter();

let sf = rl.local().suppressed().get_suppression_factor("user_123");
assert_eq!(sf, 0.0);
```

### Redis Absolute

Use the Redis provider when multiple processes or machines must share rate-limit state.

```rust
# #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
# {
# trypema::__doctest_helpers::with_redis_rate_limiter(|rl| async move {
use trypema::{RateLimit, RateLimitDecision};
use trypema::redis::RedisKey;

let key = RedisKey::try_from(trypema::__doctest_helpers::unique_key()).unwrap();
let rate = RateLimit::try_from(5.0).unwrap();

assert!(matches!(
    rl.redis().absolute().inc(&key, &rate, 1).await.unwrap(),
    RateLimitDecision::Allowed
));
# });
# }
```

### Redis Suppressed State

Use `SuppressedRedisRateLimiter::get_suppression_factor` to inspect suppression state for a
Redis-backed key.

```rust
# #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
# {
# trypema::__doctest_helpers::with_redis_rate_limiter(|rl| async move {
use trypema::redis::RedisKey;

let key = RedisKey::try_from(trypema::__doctest_helpers::unique_key()).unwrap();

assert_eq!(rl.redis().suppressed().get_suppression_factor(&key).await.unwrap(), 0.0);
# });
# }
```

### Hybrid

The hybrid provider keeps a local fast path and periodically flushes to Redis, which is useful
when per-request Redis I/O would be too expensive.

```rust
# #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
# {
# trypema::__doctest_helpers::with_redis_rate_limiter(|rl| async move {
use trypema::{RateLimit, RateLimitDecision};
use trypema::redis::RedisKey;

let key = RedisKey::try_from(trypema::__doctest_helpers::unique_key()).unwrap();
let rate = RateLimit::try_from(10.0).unwrap();

assert!(matches!(
    rl.hybrid().absolute().inc(&key, &rate, 1).await.unwrap(),
    RateLimitDecision::Allowed
));
# });
# }
```

## Understanding Decisions

All strategies return [`RateLimitDecision`]:

- [`Allowed`](crate::RateLimitDecision::Allowed): the request should proceed
- [`Rejected`](crate::RateLimitDecision::Rejected): the absolute strategy denied the request and
  includes best-effort backoff hints
- [`Suppressed`](crate::RateLimitDecision::Suppressed): the suppressed strategy is active; check
  `is_allowed` for the admission decision

The rejection metadata and suppression factor are operational hints, not strict guarantees.
Coalescing, concurrent callers, and distributed coordination can all affect precision.

## Core Concepts

### Sliding Windows

Trypema uses a sliding window rather than a fixed window. Capacity becomes available continuously
as old buckets expire, which avoids large boundary effects.

### Bucket Coalescing

Increments close together in time are merged into one bucket using `rate_group_size_ms`. Larger
values reduce overhead but make timing less precise.

### Sticky Per-Key Limits

The first [`AbsoluteLocalRateLimiter::inc`](crate::AbsoluteLocalRateLimiter::inc) call for a key
stores that key's rate limit. Later calls for the same key reuse the stored limit so concurrent
callers cannot race different limits into the same state.

### Best-Effort Distributed Limiting

Redis and hybrid providers are designed for high throughput, not strict linearizability.
Concurrent callers can temporarily overshoot the configured rate.

## Running the Examples

Local examples run with standard doctests.

Redis and hybrid examples require a live Redis server and `REDIS_URL`, for example:

```bash
REDIS_URL=redis://127.0.0.1:6379/ cargo test -p trypema --doc --features redis-tokio
```

Use `redis-smol` instead when testing the Smol-backed feature set.
