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

Hybrid adds a synchronization interval and an allowed lag. `sync_interval` (default 10 ms) sets
how often local increments reach Redis. `allowed_lag` (default 1 second) sets the maximum age of the
local state that answers a read-only call: `is_allowed` and `get_estimate` on the absolute limiter,
`get_suppression_factor` and `get_estimate` on the suppressed limiter. Older accepting or
absent-key state is read from Redis again first; concurrent calls for one key share that read.

```rust,ignore
use trypema::{
    RateLimiterBuilder,
    hybrid::{AllowedLag, HybridRateLimiterProvider, SyncInterval},
};

let provider = HybridRateLimiterProvider::builder(connection)
    // Optional: only available with 'redis-tokio' or 'redis-smol'
    .sync_interval(SyncInterval::milliseconds_or_panic(10))
    // Optional: only available with 'redis-tokio' or 'redis-smol'
    .allowed_lag(AllowedLag::milliseconds_or_panic(500))
    .build()?;
```

Hybrid limiters also expose a synchronous `try_inc(key, count)`, taking a `RedisKeyRef`: a
borrowed key validated by the same rules as `RedisKey`, built from a `&str` without allocating
(or from an owned key with `as_key_ref()`). It decides from this instance's local state without
awaiting and returns `Ok(None)`, recording nothing, when the decision needs Redis; call `inc` then.
Hot paths can use it to skip async machinery on most requests.

Redis and hybrid operations accept `&RedisKey` (`try_inc` its borrowed form, `RedisKeyRef`).
Local operations accept `&str`. Invalid Redis keys are rejected; keys are never silently
sanitized.

## Building one strategy directly

A provider builds both strategies. When an application uses only one strategy, build that
limiter directly. Each of the six limiters has a `builder()` that returns its own builder, for
example `AbsoluteLocalRateLimiter::builder()` or
`AbsoluteHybridRateLimiter::builder(connection_manager)`. The limiter builder implements
the `RateLimiterBuilder` trait and has the same backend settings as the provider builder.
`allowed_lag_applies_to_rejections` is only on the absolute hybrid builder. `build()` returns an
`Arc` of the limiter. It does not create the state, Lua scripts, or background workers of the
other strategy.

A limiter built with prefix `P` uses the same Redis keys as the provider with prefix `P`, so the
two share state. `build()` starts the stale-state cleanup loop of the limiter unless cleanup is
disabled. The limiter has idempotent `start_cleanup_loop()` and `stop_cleanup_loop()` methods.
Its cleanup loop and the hybrid synchronization worker end after the last `Arc` drops. The
absolute builders accept `hard_limit_factor` and `suppression_factor_cache_period` because the
trait is shared, but these settings have no effect on the absolute strategy.

```rust
use std::time::Duration;
use trypema::{
    BucketSize, RateLimit, RateLimitDecision, RateLimiterBuilder, WindowSize,
    local::AbsoluteLocalRateLimiter,
};

let limiter = AbsoluteLocalRateLimiter::builder()
    .window_size(WindowSize::minutes_or_panic(1))
    .bucket_size(BucketSize::milliseconds_or_panic(10))
    .stale_after(Duration::from_secs(300))
    .build()
    .unwrap();

let rate = RateLimit::per_second_or_panic(10.0);
assert!(matches!(limiter.inc("user-123", &rate, 1), RateLimitDecision::Allowed));
```

The Redis and hybrid limiter builders take the connection manager, as the provider builders do:

```rust,ignore
use trypema::{RateLimiterBuilder, WindowSize};
use trypema::redis::{AbsoluteRedisRateLimiter, RedisKey};

let limiter = AbsoluteRedisRateLimiter::builder(connection)
    .prefix(RedisKey::try_from("my-service")?)
    .window_size(WindowSize::minutes_or_panic(1))
    .build()?;
```

```rust,ignore
use std::time::Duration;
use trypema::{
    BucketSize, RateLimiterBuilder, WindowSize,
    hybrid::{AbsoluteHybridRateLimiter, SyncInterval},
    redis::RedisKey,
};

let limiter = AbsoluteHybridRateLimiter::builder(connection)
    .prefix(RedisKey::try_from("ip-ban")?)
    .window_size(WindowSize::minutes_or_panic(1))
    .bucket_size(BucketSize::milliseconds_or_panic(100))
    .stale_after(Duration::from_secs(600))
    // Optional: only available with 'redis-tokio' or 'redis-smol'
    .sync_interval(SyncInterval::milliseconds_or_panic(10))
    // Optional: only available with 'redis-tokio' or 'redis-smol'
    .allowed_lag_applies_to_rejections(true)
    .build()?;
```

## Semantic time values

The public API uses validated, unit-aware values:

| Type | Constructors |
| --- | --- |
| `RateLimit` | `per_second`, `per_minute`, `per_hour`, `per_day`, `per_week`, `per_month` |
| `WindowSize` | `seconds`, `minutes`, `hours`, `days`, `weeks`, `months` |
| `BucketSize` | `milliseconds`, `seconds`, `minutes`, `hours`, `days`, `weeks`, `months` |
| `SuppressionFactorCachePeriod` | `milliseconds`, `seconds`, `minutes`, `hours`, `days` |
| `hybrid::SyncInterval` | `milliseconds`, `seconds`, `minutes`, `hours` |
| `hybrid::AllowedLag` | `milliseconds`, `seconds`, `minutes`, `hours` |

Provider and limiter builders require `bucket_size` to be less than or equal to
`window_size`; equality is valid. The relationship is checked by `build()`, regardless of
setter order.

One month is defined as 30 days. Every constructor has an `_or_panic` counterpart. `RateLimit`
provides matching `as_per_second()`, `as_per_minute()`, `as_per_hour()`, `as_per_day()`,
`as_per_week()`, and `as_per_month()` getters. `WindowSize` provides `as_seconds()`,
`as_milliseconds()`, `as_minutes()`, `as_hours()`, `as_days()`, `as_weeks()`, and `as_months()`;
the minute-and-larger getters return `f64`. `SyncInterval` and `AllowedLag` provide
`as_milliseconds()`, `as_seconds()`, `as_minutes()`, and `as_hours()`; the second-and-larger
getters return `f64`.
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

## Managing existing keys

Every limiter exposes `set_rate_limit`, `delete`, and `clear`. `set_rate_limit` changes only an
existing key's sticky capacity and returns its previous effective `RateLimit`; it preserves live
history, totals, timestamps, and declined usage. Missing keys return `None`, and an equivalent
effective rate is a no-op. Changed suppressed rates invalidate their cached suppression factor.

`delete` removes one key and returns its live pre-delete usage in `Some`: the live total for
absolute limiters and accepted usage (`total - total_declined`) for suppressed limiters. Existing
zero-usage or cache-only state returns `Some(0)`; missing state returns `None`. `clear` removes all
keys for only the called strategy and configured prefix. Local methods are synchronous; Redis and
hybrid methods are asynchronous and return `Result`. Hybrid revisions prevent stale cached totals
and limits from overriding these mutations, but concurrent or remote pending increments can still
recreate a key afterward.

## Decisions and behavior

`RateLimitDecision::Rejected` exposes `window_size: WindowSize`, `retry_after: Duration`, and
`remaining_after_waiting`. Metadata is best-effort under bucket coalescing and concurrency.
Suppressed decisions expose `is_allowed` and `suppression_factor`.

Absolute `get` returns the live total as `u64`. Suppressed `get` returns
`SuppressedRateLimitSnapshot` with observed usage, declined usage, and suppression factor.
Unknown keys return zero-valued results without creating state. Hybrid reads include this
instance's pending local counts; `get_estimate` may answer from local state without Redis I/O.

Increments from a hybrid instance reach Redis within `sync_interval`. Hybrid read-only calls
answer from accepting state, or from the local entry of a key with no stored limit, only while
that state is younger than `allowed_lag`. A cached rejection answers until its retry TTL ends (up
to one window), and a cached suppression state until its cache period ends. The hybrid builder
setting `allowed_lag_applies_to_rejections(true)` (default `false`) bounds read-only answers from
an absolute rejection cache by `allowed_lag` as well, so a limit that another instance deletes or
raises becomes visible within that lag. `inc` and `try_inc` keep the retry TTL rule, and the
suppressed strategy does not use this setting.

Hybrid read-only calls record no activity. A key in accepting state that is only checked becomes
stale, and cleanup removes it. A key with a cached rejection or suppression state that is checked
after the cache ends gets a new cache from Redis each time, so cleanup keeps it while it is
checked.

A hybrid check of a key with no stored limit is allowed and creates no Redis key. The instance
keeps a small local entry that answers later checks until `allowed_lag` passes. Only the cleanup
loop removes that entry. With cleanup disabled, start the loop with `start_cleanup_loop()`, or the
entries of unknown keys stay in memory.

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
