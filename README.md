# Trypema Rate Limiter

Status: in development (pre-release).

## Name and Biblical Inspiration

The name is inspired by the Koine Greek word "τρυπήματος" (trypematos, "hole/opening") from the phrase "διὰ τρυπήματος ῥαφίδος" ("through the eye of a needle") in the Bible: Matthew 19:24, Mark 10:25, Luke 18:25

## Overview

Trypema provides rate limiting primitives designed for multi-threaded, in-process use with low overhead and predictable latency characteristics.

What you get today:

- A `RateLimiter` facade that exposes a `local` provider.
- A deterministic sliding-window strategy (`absolute`) and a suppression-capable strategy (`suppressed`).

What this crate is not (currently):

- A distributed/shared rate limiter (Redis, memcached, etc.).
- A strict/linearizable admission controller under high concurrency.

## Status

- `local` provider: implemented
- additional providers: planned/experimental

## Quick Start

```rust
use trypema::{
    HardLimitFactor, LocalRateLimiterOptions, RateGroupSizeMs, RateLimit, RateLimitDecision,
    RateLimiter, RateLimiterOptions, WindowSizeSeconds,
};

let rl = RateLimiter::new(RateLimiterOptions {
    local: LocalRateLimiterOptions {
        window_size_seconds: WindowSizeSeconds::try_from(60).unwrap(),
        rate_group_size_ms: RateGroupSizeMs::try_from(10).unwrap(),
        hard_limit_factor: HardLimitFactor::default(),
    },
});

let key = "user:123";
let rate_limit = RateLimit::try_from(5.0).unwrap();

// check + record work (count is usually 1)
match rl.local().absolute().inc(key, &rate_limit, 1) {
    RateLimitDecision::Allowed => {
        // proceed
    }
    RateLimitDecision::Rejected {
        window_size_seconds,
        retry_after_ms,
        remaining_after_waiting,
    } => {
        let _ = (window_size_seconds, retry_after_ms, remaining_after_waiting);
        // reject / retry later
    }
    RateLimitDecision::Suppressed { is_allowed, .. } => {
        // only returned by strategies that support suppression
        if is_allowed {
            // proceed
        } else {
            // suppressed / retry later
        }
    }
}
```

## Core Concepts

- Keyed limiting: each `key` has independent state.
- `RateLimit`: per-second limit for a key (positive `f64`, so non-integer limits are allowed).
- Sliding window: admission is based on the last `window_size_seconds` of history.
- Bucket coalescing: increments close together can be merged into time buckets to reduce overhead.

## Configuration

`LocalRateLimiterOptions`:

- `window_size_seconds`: sliding window length used for admission.
- `rate_group_size_ms`: coalescing interval for increments close in time.
- `hard_limit_factor`: used by the suppressed strategy as a hard cutoff multiplier.

## Decisions

All strategies return `RateLimitDecision`:

- `Allowed`: proceed; the increment was applied.
- `Rejected { window_size_seconds, retry_after_ms, remaining_after_waiting }`:
  do not proceed; includes best-effort backoff hints.
- `Suppressed { suppression_factor, is_allowed }`:
  returned by suppression-based strategies; treat `is_allowed` as the admission decision.

Notes on metadata:

- `retry_after_ms` is computed from the oldest in-window bucket, so it is best-effort (especially with coalescing and concurrency).
- `remaining_after_waiting` is also best-effort; if usage is heavily coalesced into one bucket it can be `0`.

## Local Strategies

### Absolute (`rl.local().absolute()`)

Deterministic sliding-window limiter with per-key state stored in-process.

Behavior:

- Window capacity is approximately `W * R` (window seconds `W` times per-second limit `R`).
- Per-key limit is sticky: the first call for a key stores the `RateLimit`; later calls for that key do not update it.

Good for:

- simple per-key rate caps
- low overhead checks in a single process

### Suppressed (`rl.local().suppressed()`)

Strategy that can probabilistically deny work while tracking both:

- observed usage (all calls)
- accepted usage (only admitted calls)

This strategy can return `RateLimitDecision::Suppressed` to expose suppression metadata. It also enforces a hard cutoff:

- hard cutoff: `rate_limit * hard_limit_factor`
- hitting the hard cutoff returns `Rejected` (a hard rejection, not suppressible)

Suppression activation:

- Suppression is only considered once accepted usage meets/exceeds the base window capacity (`window_size_seconds * rate_limit`).
- Below that capacity, suppression is bypassed (calls return `Allowed`, subject to the hard cutoff).

Inspiration:

- The suppressed strategy is inspired by Ably's approach to distributed rate limiting, where they describe preferring suppression over a strict hard limit once the target rate is exceeded: <https://ably.com/blog/distributed-rate-limiting-scale-your-platform>

## Semantics (Important)

- Best-effort under concurrency: `inc` does an admission check and then applies the increment. Under high contention, several threads can observe `Allowed` and increment concurrently, so temporary overshoot is possible.
- Eviction granularity: eviction uses `Instant::elapsed().as_secs()` (whole-second truncation). This is conservative; e.g. a `1s` window can effectively require ~`2s` before a bucket is considered expired.
- Key cardinality: keys are not automatically removed from the internal map; unbounded/attacker-controlled keys can grow memory usage.

## Practical Tuning

- `window_size_seconds`:
  larger windows smooth bursts but increase the amount of history affecting admission/unblocking.
- `rate_group_size_ms`:
  larger values reduce overhead by coalescing increments into fewer buckets, but make rejection metadata coarser.

## Crate Layout

- `src/rate_limiter.rs`: `RateLimiter` facade and options
- `src/local/absolute_local_rate_limiter.rs`: absolute local implementation
- `src/local/suppressed_local_rate_limiter.rs`: suppression-capable local implementation
- `src/common.rs`: shared types (`RateLimitDecision`, newtypes, internal series)

## Roadmap

Planned directions (subject to change):

- additional providers (shared/distributed state)
- additional strategies and tighter semantics where needed
