# Trypema

Trypema provides concurrent sliding-window rate limiters with absolute and probabilistic
suppression strategies. Providers are constructed independently, so applications only create
the local, Redis, or hybrid workers they actually use.

## Local provider

```
use std::{sync::Arc, time::Duration};
use trypema::{
    BucketSize, RateLimit, RateLimitDecision, RateLimiterBuilder, WindowSize,
    local::LocalRateLimiterProvider,
};

let provider: Arc<LocalRateLimiterProvider> = RateLimiterBuilder::local()
    .window_size(WindowSize::minutes_or_panic(1))
    .bucket_size(BucketSize::milliseconds_or_panic(10))
    .cleanup_interval(Duration::from_secs(30))
    .build()?;

let rate = RateLimit::per_second_or_panic(10.0);
let decision = provider.absolute().inc("user-123", &rate, 1);
assert!(matches!(decision, RateLimitDecision::Allowed));
# Ok::<(), trypema::TrypemaError>(())
```

[`RateLimiterBuilder`] builds every provider and limiter. Start from the backend:
`RateLimiterBuilder::local()`, `RateLimiterBuilder::redis(connection_manager)`, or
`RateLimiterBuilder::hybrid(connection_manager)`. The Redis and hybrid constructors are only
available with `redis-tokio` or `redis-smol`. The type that the caller expects from `build()`
selects the target: the provider of the backend or one of its two limiters. Without an
annotation, name the target with a turbofish, such as `build::<LocalRateLimiterProvider>()`. A
target of another backend, such as a hybrid limiter from `RateLimiterBuilder::local()`, does not
compile.

Each provider and limiter also has a `builder()` shortcut, such as
`LocalRateLimiterProvider::builder()` or
`AbsoluteHybridRateLimiter::builder(connection_manager)`. The Redis and hybrid shortcuts are
only available with `redis-tokio` or `redis-smol`. A shortcut returns the same builder with the
target fixed to its type, so its `build()` needs no annotation.

Shared methods configure windowing, suppression, and stale-state cleanup. `build()` returns an
`Arc` and starts cleanup by default; use `disable_cleanup()` to opt out and `enable_cleanup()` to
opt back in while configuring the builder. The idempotent `start_cleanup_loop()` and
`stop_cleanup_loop()` methods of a provider or limiter control cleanup after construction. Each
limiter has its own cleanup loop. A provider starts and stops the loops of its two limiters, and
`provider.absolute().stop_cleanup_loop()` stops the cleanup of that strategy only.

## Time values

Configuration types expose unit-named constructors and explicit getters:

- [`RateLimit`]: `per_second`, `per_minute`, `per_hour`, `per_day`, `per_week`, and
  `per_month`, with matching `as_per_*` getters; one month is 30 days.
- [`WindowSize`]: `seconds`, `minutes`, `hours`, `days`, `weeks`, and `months`, with matching
  `as_*` getters; minute-and-larger getters return `f64`.
- [`BucketSize`]: `milliseconds`, `seconds`, `minutes`, `hours`, `days`, `weeks`, and `months`.
- [`SuppressionFactorCachePeriod`]: `milliseconds`, `seconds`, `minutes`, `hours`, and `days`.
- `hybrid::SyncInterval` and `hybrid::AllowedLag`: `milliseconds`, `seconds`, `minutes`, and
  `hours`, with matching `as_*` getters; second-and-larger getters return `f64`.

`RateLimiterBuilder` requires `bucket_size` to be less than or equal to
`window_size`; equality is valid. This relationship is checked by `build()`, regardless of
setter order.

Each constructor has an `_or_panic` counterpart. Fallible constructors reject zero, invalid
floating-point values, conversion underflow, and overflow. The explicit getters report the
stable semantic unit while leaving internal representation private.

## Redis and hybrid providers

Enable exactly one runtime feature: `redis-tokio` or `redis-smol`. Redis 7.2 or newer is
required. Construct the provider directly from the public `redis::ConnectionManager`:

```no_run
# #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use std::sync::Arc;
use trypema::{BucketSize, RateLimiterBuilder, WindowSize};
use trypema::redis::{RedisKey, RedisRateLimiterProvider};

let connection = redis::Client::open("redis://127.0.0.1/")?
    .get_connection_manager()
    .await?;
let provider: Arc<RedisRateLimiterProvider> = RateLimiterBuilder::redis(connection)
    // Optional: only available with 'redis-tokio' or 'redis-smol'
    .prefix(RedisKey::try_from("my-service")?)
    .window_size(WindowSize::minutes_or_panic(1))
    .bucket_size(BucketSize::milliseconds_or_panic(10))
    .build()?;
# let _ = provider;
# Ok(())
# }
```

The hybrid builder additionally exposes `sync_interval` and `allowed_lag`. Its synchronization
worker is always started and is independent of optional stale-state cleanup. `sync_interval`
(default 10 ms) sets how often local increments reach Redis. `allowed_lag` (default 1 second) sets
the maximum age of the local state that answers a read-only call: `is_allowed` and `get_estimate`
on the absolute limiter, `get_suppression_factor` and `get_estimate` on the suppressed limiter.
Older accepting or absent-key state is read from Redis again first; concurrent calls for one key
share that read.

```no_run
# #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
# async fn example(connection: trypema::redis::ConnectionManager) -> Result<(), Box<dyn std::error::Error>> {
use std::sync::Arc;
use trypema::{
    RateLimiterBuilder,
    hybrid::{AllowedLag, HybridRateLimiterProvider, SyncInterval},
};

let provider: Arc<HybridRateLimiterProvider> = RateLimiterBuilder::hybrid(connection)
    // Optional: only available with 'redis-tokio' or 'redis-smol'
    .sync_interval(SyncInterval::milliseconds_or_panic(10))
    // Optional: only available with 'redis-tokio' or 'redis-smol'
    .allowed_lag(AllowedLag::milliseconds_or_panic(500))
    .build()?;
# let _ = provider;
# Ok(())
# }
```

Hybrid limiters also expose a synchronous `try_inc(key, count)`, taking a `RedisKeyRef`: a
borrowed key validated by the same rules as `RedisKey`, built from a `&str` without allocating
(or from an owned key with `as_key_ref()`). It decides from this instance's local state without
awaiting and returns `Ok(None)`, recording nothing, when the decision needs Redis; call `inc` then.
Hot paths can use it to skip async machinery on most requests.

Redis and hybrid operations accept `&RedisKey` (`try_inc` its borrowed form, `RedisKeyRef`);
local operations accept `&str`. Redis keys are validated and never silently sanitized.

## Building one strategy directly

A provider builds both strategies. When an application uses only one strategy, build that
limiter directly: annotate the result of `build()` with the limiter type. `build()` returns an
`Arc` of the limiter. It does not create the state, Lua scripts, or background workers of the
other strategy.

A limiter built with prefix `P` uses the same Redis keys as the provider with prefix `P`, so the
two share state. `build()` starts the stale-state cleanup loop of the limiter unless cleanup is
disabled. The limiter has idempotent `start_cleanup_loop()` and `stop_cleanup_loop()` methods.
The cleanup loop ends at once after `stop_cleanup_loop()` or after the last `Arc` drops. The
hybrid synchronization worker also ends at once after the last `Arc` drops. The builder of a
backend accepts every setting of that backend for every target. The absolute limiters do not use
`hard_limit_factor` and `suppression_factor_cache_period`, and the suppressed hybrid limiter does
not use `allowed_lag_applies_to_rejections`.
Each provider and limiter returns the window it was built with from `window_size()`.

```
use std::{sync::Arc, time::Duration};
use trypema::{
    BucketSize, RateLimit, RateLimitDecision, RateLimiterBuilder, WindowSize,
    local::AbsoluteLocalRateLimiter,
};

let limiter: Arc<AbsoluteLocalRateLimiter> = RateLimiterBuilder::local()
    .window_size(WindowSize::minutes_or_panic(1))
    .bucket_size(BucketSize::milliseconds_or_panic(10))
    .stale_after(Duration::from_secs(300))
    .build()?;

let rate = RateLimit::per_second_or_panic(10.0);
assert!(matches!(limiter.inc("user-123", &rate, 1), RateLimitDecision::Allowed));
# Ok::<(), trypema::TrypemaError>(())
```

The Redis and hybrid limiters take the connection manager, as their providers do:

```no_run
# #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
# async fn example(connection: trypema::redis::ConnectionManager) -> Result<(), Box<dyn std::error::Error>> {
use std::sync::Arc;
use trypema::{RateLimiterBuilder, WindowSize};
use trypema::redis::{AbsoluteRedisRateLimiter, RedisKey};

let limiter: Arc<AbsoluteRedisRateLimiter> = RateLimiterBuilder::redis(connection)
    // Optional: only available with 'redis-tokio' or 'redis-smol'
    .prefix(RedisKey::try_from("my-service")?)
    .window_size(WindowSize::minutes_or_panic(1))
    .build()?;
# let _ = limiter;
# Ok(())
# }
```

```no_run
# #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
# async fn example(connection: trypema::redis::ConnectionManager) -> Result<(), Box<dyn std::error::Error>> {
use std::{sync::Arc, time::Duration};
use trypema::{
    BucketSize, RateLimiterBuilder, WindowSize,
    hybrid::{AbsoluteHybridRateLimiter, SyncInterval},
    redis::RedisKey,
};

let limiter: Arc<AbsoluteHybridRateLimiter> = RateLimiterBuilder::hybrid(connection)
    // Optional: only available with 'redis-tokio' or 'redis-smol'
    .prefix(RedisKey::try_from("ip-ban")?)
    .window_size(WindowSize::minutes_or_panic(1))
    .bucket_size(BucketSize::milliseconds_or_panic(100))
    .stale_after(Duration::from_secs(600))
    // Optional: only available with 'redis-tokio' or 'redis-smol'
    .sync_interval(SyncInterval::milliseconds_or_panic(10))
    // Optional: only available with 'redis-tokio' or 'redis-smol'
    .allowed_lag_applies_to_rejections(true)
    .build()?;
# let _ = limiter;
# Ok(())
# }
```

## Migrating from 2.x

In 3.0, `RateLimiterBuilder` is a struct, not a trait. `RateLimiterBuilder<B, T>` takes the
backend `B` and the target `T`. The backends are `local::LocalBackend`, `redis::RedisBackend`,
and `hybrid::HybridBackend`; the last two are only available with `redis-tokio` or `redis-smol`.

- Removed: `LocalRateLimiterBuilder`, `RedisRateLimiterBuilder`, and
  `HybridRateLimiterBuilder`. Name the builder of a shortcut as
  `RateLimiterBuilder<LocalBackend, LocalRateLimiterProvider>`, and the builder of a backend
  constructor as `RateLimiterBuilder<LocalBackend>`.
- Unchanged: shortcut chains such as
  `HybridRateLimiterProvider::builder(connection).window_size(window).build()?` compile and
  return the same `Arc`. `use trypema::RateLimiterBuilder;` compiles; when the code calls only
  shortcuts, the import is unused.
- A bound on the old trait, such as `fn build<B: RateLimiterBuilder>(builder: B) ->
  Arc<B::Provider>`, becomes a bound on the sealed `RateLimiterTarget` trait:

```
use std::sync::Arc;
use trypema::{RateLimiterBuilder, RateLimiterTarget, TrypemaError, WindowSize};

fn build_with_window<T: RateLimiterTarget>(
    builder: RateLimiterBuilder<T::Backend, T>,
) -> Result<Arc<T>, TrypemaError> {
    builder.window_size(WindowSize::seconds_or_panic(10)).build()
}

let provider = build_with_window(trypema::local::LocalRateLimiterProvider::builder())?;
# let _ = provider;
# Ok::<(), trypema::TrypemaError>(())
```

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

## Managing existing keys

All six limiter variants expose `set_rate_limit`, `delete`, and `clear`. `set_rate_limit` changes
only an existing key's sticky capacity and returns its previous effective [`RateLimit`]. It
preserves live buckets, timestamps, totals, and declined usage. A missing key returns `None`; an
equivalent effective rate performs no TTL, cache, activity-metadata, or hybrid-revision writes.
A changed suppressed rate invalidates its cached suppression factor.

`delete` removes one key and returns its live pre-delete usage in `Some`: the live total for
absolute limiters and accepted usage (`total - total_declined`) for suppressed limiters. Existing
zero-usage or cache-only state returns `Some(0)`; missing state returns `None`. `clear` removes all
keys belonging to only the called strategy and configured prefix. Local methods are synchronous;
Redis and hybrid methods are asynchronous and return [`Result`](std::result::Result). Hybrid state
revisions prevent stale cached totals and limits from undoing these mutations on later Redis
interaction. Concurrent or remote pending increments may still recreate keys afterward.

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
