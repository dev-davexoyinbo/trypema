# Trypema

Trypema provides concurrent sliding-window rate limiters with absolute and probabilistic
suppression strategies. Providers are constructed independently, so applications only create
the local, Redis, or hybrid workers they actually use.

## Local provider

```
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

Every concrete builder implements [`RateLimiterBuilder`]. Shared methods configure windowing,
suppression, and stale-state cleanup. `build()` returns an `Arc` and starts cleanup by default;
use `disable_cleanup()` to opt out and `enable_cleanup()` to opt back in while configuring the
builder. A provider's idempotent `start_cleanup_loop()` and `stop_cleanup_loop()` methods control
cleanup after construction.

## Time values

Configuration types expose unit-named constructors and explicit getters:

- [`RateLimit`]: `per_second`, `per_minute`, `per_hour`, `per_day`, `per_week`, and
  `per_month`, with matching `as_per_*` getters; one month is 30 days.
- [`WindowSize`]: `seconds`, `minutes`, `hours`, `days`, `weeks`, and `months`, with matching
  `as_*` getters; minute-and-larger getters return `f64`.
- [`BucketSize`]: `milliseconds`, `seconds`, `minutes`, `hours`, `days`, `weeks`, and `months`.
- [`SuppressionFactorCachePeriod`]: `milliseconds`, `seconds`, `minutes`, `hours`, and `days`.
- `hybrid::SyncInterval`: `milliseconds`, `seconds`, `minutes`, and `hours`, with matching
  `as_*` getters; second-and-larger getters return `f64`.

Provider builders require `bucket_size` to be less than or equal to `window_size`; equality is
valid. This relationship is checked by `build()`, regardless of setter order.

Each constructor has an `_or_panic` counterpart. Fallible constructors reject zero, invalid
floating-point values, conversion underflow, and overflow. The explicit getters report the
stable semantic unit while leaving internal representation private.

## Redis and hybrid providers

Enable exactly one runtime feature: `redis-tokio` or `redis-smol`. Redis 7.2 or newer is
required. Construct the provider directly from the public `redis::ConnectionManager`:

```no_run
# #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
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
# let _ = provider;
# Ok(())
# }
```

The hybrid builder additionally exposes `sync_interval`. Its synchronization worker is always
started and is independent of optional stale-state cleanup.

```no_run
# #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
# async fn example(connection: trypema::redis::ConnectionManager) -> Result<(), trypema::TrypemaError> {
use trypema::{RateLimiterBuilder, hybrid::{HybridRateLimiterProvider, SyncInterval}};

let provider = HybridRateLimiterProvider::builder(connection)
    .sync_interval(SyncInterval::milliseconds_or_panic(10))
    .build()?;
# let _ = provider;
# Ok(())
# }
```

Redis and hybrid operations accept `&RedisKey`; local operations accept `&str`. Redis keys are
validated and never silently sanitized.

## Decisions and conditional updates

Absolute admission returns [`RateLimitDecision::Allowed`] or
[`RateLimitDecision::Rejected`]. Rejection metadata uses `Duration` and is best-effort under
bucket coalescing and concurrency. Suppressed admission may return
[`RateLimitDecision::Suppressed`], whose `is_allowed` field is the admission result.

Conditional update methods return [`ConditionalSetOutcome`]. `matched` distinguishes a
comparator miss from a successful no-op; `previous_total` and `current_total` expose totals
before and after the operation. [`RateLimitComparator::Always`] requests an unconditional
update through this path.

[`HistoryPreservation::PreserveNewest`] consumes oldest buckets first and extends newest
history. [`HistoryPreservation::PreserveOldest`] does the reverse. Matched zero targets remove
the key; every matched update replaces its sticky window capacity.

[`RateLimitDecision`] is exhaustive, so callers can match all three variants without a wildcard.
The fields of its `Rejected` and `Suppressed` variants remain non-exhaustive, so match those
variants with `{ .. }`:

```
use trypema::RateLimitDecision;

fn admitted(decision: RateLimitDecision) -> bool {
    match decision {
        RateLimitDecision::Allowed => true,
        RateLimitDecision::Rejected { .. } => false,
        RateLimitDecision::Suppressed { is_allowed, .. } => is_allowed,
    }
}
```

Other public result structs and [`TrypemaError`] remain non-exhaustive.

`inc` limits are sticky per key: the first increment stores the computed window capacity.
Matched conditional updates replace that stored capacity. A matched target of zero removes the
key. Absolute admission is best-effort under concurrency and may temporarily overshoot.

## Reading live state

Absolute `get` methods return the live total as `u64`. Suppressed `get` methods return
[`SuppressedRateLimitSnapshot`], containing observed usage, declined usage, and the current
suppression factor. Unknown keys return zero-valued results without creating state. Reads may
perform lazy expiration maintenance. Hybrid reads include this instance's pending local counts;
`get_estimate` may use initialized local state instead of consulting Redis.

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
