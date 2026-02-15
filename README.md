# Trypema Rate Limiter

Status: in development (pre-release).

The name is inspired by the Koine Greek word "τρυπήματος" (trypematos, "hole/opening") from the phrase "διὰ τρυπήματος ῥαφίδος" ("through the eye of a needle") in the Bible:

- Matthew 19:24
- Mark 10:25
- Luke 18:25

Koine Greek (one common form):

"εὐκοπώτερόν ἐστιν κάμηλον διὰ τρυπήματος ῥαφίδος εἰσελθεῖν ἢ πλούσιον εἰς τὴν βασιλείαν τοῦ θεοῦ"

Word breakdown:

- κάμηλον (kamēlon) — camel
- διὰ (dia) — through
- τρυπήματος (trypēmatos) — hole/opening
- ῥαφίδος (rhaphidos) — of a needle
- εἰσελθεῖν (eiselthein) — to enter
- πλούσιον (plousion) — a rich man
- βασιλείαν τοῦ θεοῦ — kingdom of God

High-performance rate limiting primitives in Rust, designed for concurrency safety, low overhead, and predictable latency.

This repository accompanies a post that demonstrates one possible implementation approach, walking through the core data structures, atomic operations, and key design trade-offs for real-time systems.

## Status

- `local` provider: implemented (see `AbsoluteLocalRateLimiter`)
- additional providers: planned

## Crate layout

- `src/rate_limiter.rs`: top-level `RateLimiter` and options
- `src/local/absolute_local_rate_limiter.rs`: local implementation
- `src/common.rs`: shared types (`RateLimitDecision`, counters, series)

## Usage

```rust
use trypema::{LocalRateLimiterOptions, RateLimitDecision, RateLimiter, RateLimiterOptions};

let rl = RateLimiter::new(RateLimiterOptions {
    local: LocalRateLimiterOptions {
        window_size_seconds: 60,
        rate_group_size_ms: 10,
    },
});

let key = "user:123";

// check + record work
match rl.local().absolute().inc(key, /*rate_limit=*/ 5, /*count=*/ 1) {
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
}
```

## Configuration

- `window_size_seconds`: sliding window length for admission decisions
- `rate_group_size_ms`: coalescing interval for increments close in time
