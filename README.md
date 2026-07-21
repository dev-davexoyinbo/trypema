# Trypema

High-performance sliding-window rate limiting primitives for Rust, designed for concurrency
safety, low overhead, and predictable latency.

Trypema offers two strategies:

- Absolute: rejects requests after the configured window capacity is reached.
- Suppressed: progressively suppresses traffic between a soft target and a hard cutoff.

And three independently constructed providers:

- Local: in-process, synchronous, always available.
- Redis: distributed, async, one atomic Redis script per operation.
- Hybrid: local fast path with mandatory periodic Redis synchronization.

## Install

```toml
[dependencies]
trypema = "2"
```

For Redis or hybrid providers, enable exactly one runtime feature:

```toml
trypema = { version = "2", features = ["redis-tokio"] }
# or: features = ["redis-smol"]
```

Redis-backed providers require Redis 7.2 or newer.

## Local example

```rust
use std::time::Duration;
use trypema::{
    BucketSize, RateLimit, RateLimitDecision, RateLimiterBuilder, WindowSize,
    local::LocalRateLimiterProvider,
};

let provider = LocalRateLimiterProvider::builder()
    .window_size(WindowSize::minutes_or_panic(1))
    .bucket_size(BucketSize::milliseconds_or_panic(10))
    .cleanup_interval(Duration::from_secs(30))
    .build()
    .unwrap();

let rate = RateLimit::per_second_or_panic(10.0);
let decision = provider.absolute().inc("user-123", &rate, 1);
assert!(matches!(decision, RateLimitDecision::Allowed));
```

All concrete builders implement the shared `RateLimiterBuilder` trait. `build()` returns an
`Arc` and starts stale-state cleanup by default. Use `.disable_cleanup()` to opt out, or control
conditional builder configuration with `.enable_cleanup()`. After construction, use the provider's
idempotent `start_cleanup_loop()` and `stop_cleanup_loop()` methods.

## Redis and hybrid construction

```rust,ignore
use trypema::{BucketSize, RateLimiterBuilder, WindowSize};
use trypema::redis::{RedisKey, RedisRateLimiterProvider};

let connection = redis::Client::open("redis://127.0.0.1/")?
    .get_connection_manager()
    .await?;
let provider = RedisRateLimiterProvider::builder(connection)
    .prefix(RedisKey::try_from("my-service")?)
    .window_size(WindowSize::minutes_or_panic(1))
    .bucket_size(BucketSize::milliseconds_or_panic(10))
    .build()?;
```

Hybrid adds a provider-specific synchronization interval:

```rust,ignore
use trypema::{RateLimiterBuilder, hybrid::{HybridRateLimiterProvider, SyncInterval}};

let provider = HybridRateLimiterProvider::builder(connection)
    .sync_interval(SyncInterval::milliseconds_or_panic(10))
    .build()?;
```

Redis and hybrid operations accept `&RedisKey`. Local operations accept `&str`. Invalid Redis
keys are rejected; keys are never silently sanitized.

## Semantic time values

The public API uses validated, unit-aware values:

| Type | Constructors |
| --- | --- |
| `RateLimit` | `per_second`, `per_minute`, `per_hour`, `per_day`, `per_week`, `per_month` |
| `WindowSize` | `seconds`, `minutes`, `hours`, `days`, `weeks`, `months` |
| `BucketSize` | `milliseconds`, `seconds`, `minutes`, `hours`, `days`, `weeks`, `months` |
| `SuppressionFactorCachePeriod` | `milliseconds`, `seconds`, `minutes`, `hours`, `days` |
| `hybrid::SyncInterval` | `milliseconds`, `seconds`, `minutes`, `hours` |

Provider builders require `bucket_size` to be less than or equal to `window_size`; equality is
valid. The relationship is checked by `build()`, regardless of setter order.

One month is defined as 30 days. Every constructor has an `_or_panic` counterpart. `RateLimit`
provides matching `as_per_second()`, `as_per_minute()`, `as_per_hour()`, `as_per_day()`,
`as_per_week()`, and `as_per_month()` getters. `WindowSize` provides `as_seconds()`,
`as_milliseconds()`, `as_minutes()`, `as_hours()`, `as_days()`, `as_weeks()`, and `as_months()`;
the minute-and-larger getters return `f64`. `SyncInterval` provides `as_milliseconds()`,
`as_seconds()`, `as_minutes()`, and `as_hours()`; the second-and-larger getters return `f64`.
These getters expose values without leaking or permitting mutation of their representation.

## Conditional updates

`set_if` and `set_if_preserve_history` return `ConditionalSetOutcome`:

- `matched` distinguishes a comparator miss from a successful no-op.
- `previous_total` is the live total used by the comparison.
- `current_total` is the total after the operation.

Use `RateLimitComparator::Always` for an unconditional update through the conditional path.
Matched zero targets delete state. Matched updates also replace the sticky window capacity.
`set_if_preserve_history` additionally retains one side of live history:

- `PreserveNewest` consumes oldest buckets first; increases extend newest history.
- `PreserveOldest` consumes newest buckets first; increases extend oldest history.

## Decisions and behavior

`RateLimitDecision::Rejected` exposes `window_size: WindowSize`, `retry_after: Duration`, and
`remaining_after_waiting`. Metadata is best-effort under bucket coalescing and concurrency.
Suppressed decisions expose `is_allowed` and `suppression_factor`.

Absolute `get` returns the live total as `u64`. Suppressed `get` returns
`SuppressedRateLimitSnapshot` with observed usage, declined usage, and suppression factor.
Unknown keys return zero-valued results without creating state. Hybrid reads include this
instance's pending local counts; `get_estimate` may answer from local state without Redis I/O.

`RateLimitDecision` is exhaustive, while the fields of its `Rejected` and `Suppressed` variants
are non-exhaustive; match those variants with `{ .. }`. Other public result structs and the error
model are non-exhaustive. Absolute admission is best-effort under concurrency and may temporarily
overshoot. Reads report live state and may lazily evict expired buckets.

## License

MIT

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
