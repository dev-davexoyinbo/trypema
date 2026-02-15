# rate_limiter

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
use rate_limiter::{LocalRateLimiterOptions, RateLimiter, RateLimiterOptions};

let rl = RateLimiter::new(RateLimiterOptions {
    local: LocalRateLimiterOptions {
        window_size_seconds: 60,
        rate_group_size_ms: 10,
    },
});

let key = "user:123";

// record work
rl.local().absolute().inc(key, /*rate_limit=*/ 5, /*count=*/ 1);

// check admission
match rl.local().absolute().is_allowed(key) {
    rate_limiter::RateLimitDecision::Allowed => {
        // proceed
    }
    rate_limiter::RateLimitDecision::Rejected {
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
