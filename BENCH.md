# Benchmarking / Load Testing

This repo includes:

- Criterion microbenchmarks (`benches/`) for per-operation performance.
- A stress/load harness binary (workspace crate: `stress/`, package `trypema-stress`) for system-like throughput + latency.

Notes:

- Use `--release` for anything you want to compare.
- Redis benchmarks require Redis 7.2+.
- Redis- and hybrid-backed unit tests require `REDIS_URL` to be set.
- The stress harness is a separate workspace crate and does not require a crate feature.

## Quick Start

Local microbenchmarks:

```bash
make bench-local
```

Redis microbenchmarks (starts Redis via docker compose):

```bash
make bench-redis
```

Local stress test (max throughput):

```bash
make stress-local-hot
make stress-local-uniform
make stress-local-uniform-matrix
```

Redis stress test (contention + high-cardinality skew):

```bash
make stress-redis-hot
make stress-redis-skew
```

## Microbenchmarks (Criterion)

Bench binaries:

- `benches/local_absolute.rs`
- `benches/local_suppressed.rs`
- `benches/redis_absolute.rs` (requires `--features redis-tokio`)
- `benches/redis_suppressed.rs` (requires `--features redis-tokio`)

Run one bench directly:

```bash
cargo bench --bench local_absolute
```

## Stress Harness

The stress harness lives in a separate workspace crate: `stress/` (package: `trypema-stress`).

Examples:

```bash
# Local: hot key, absolute
cargo run --release -p trypema-stress -- \
  --provider local --strategy absolute --threads 16 \
  --key-dist hot --duration-s 30

# Local: 100k keys uniform, absolute
cargo run --release -p trypema-stress -- \
  --provider local --strategy absolute --threads 16 \
  --key-dist uniform --key-space 100000 --duration-s 60

# Local: uniform sweep across (key_space, rate_limit_per_s).
make stress-local-uniform-matrix

# Local: skewed distribution, burst traffic, suppressed
cargo run --release -p trypema-stress -- \
  --provider local --strategy suppressed --threads 16 \
  --key-dist skewed --key-space 100000 --hot-fraction 0.8 \
  --mode target-qps --target-qps 20000 --burst-qps 200000 \
  --burst-period-ms 30000 --burst-duration-ms 5000 \
  --duration-s 120
```

Redis examples:

```bash
# Start Redis
docker compose up -d redis

# (Optional) run Redis/hybrid unit tests
export REDIS_URL=redis://127.0.0.1:16379/
cargo test --features redis-tokio

# Redis: worst-case contention
cargo run --release -p trypema-stress --features redis-tokio -- \
  --provider redis --strategy absolute --threads 256 \
  --key-dist hot --duration-s 60

# Redis: high cardinality skew
cargo run --release -p trypema-stress --features redis-tokio -- \
  --provider redis --strategy suppressed --threads 256 \
  --key-dist skewed --key-space 100000 --hot-fraction 0.8 \
  --duration-s 120
```

## Redis Comparison (trypema vs redis-cell vs GCRA)

This suite benchmarks three Redis-backed limiters using the same harness:

- `trypema` Redis provider (Lua scripts)
- `trypema` hybrid provider (local counters + Redis sync)
- `redis-cell` module (`CL.THROTTLE`)
- GCRA Lua script (equivalent to go-redis/redis_rate `allowN`)

Run:

```bash
make stress-redis-compare
```

Notes:

- `redis-cell` is loaded into the Redis container as a module via `compose.yaml`.
- `--redis-limiter cell|gcra` uses different semantics than trypema's sliding window; treat results as backend cost comparisons, not strict behavioral equivalence.

## Redis Uniform Matrix (trypema vs redis-cell vs GCRA)

Uniform key distribution sweep across `(key_space, rate_limit_per_s)`.
16 threads, 30s duration, window=10s, group=10ms, mode=max.
Cell and GCRA use burst=1,000,000 (effectively unlimited) for throughput comparison.

Run:

```bash
make stress-redis-uniform-matrix
```

### Tokio runtime

#### key_space = 10

| Limiter | Rate/s | Ops/s | p50 (µs) | p95 (µs) | p99 (µs) | p999 (µs) | Max (µs) |
|---------|--------|-------|----------|----------|----------|-----------|----------|
| redis-cell | 1 | 55,822 | 279 | 377 | 450 | 740 | 1,285 |
| GCRA | 1 | 50,614 | 314 | 399 | 447 | 605 | 1,119 |
| Trypema Redis (Absolute) | 1 | 46,008 | 346 | 449 | 505 | 710 | 923 |
| Trypema Redis (Suppressed) | 1 | 38,184 | 407 | 575 | 668 | 931 | 4,619 |
| Trypema Hybrid (Absolute) | 1 | 10,658,472 | 1 | 1 | 1 | 1 | 9,961,471 |
| Trypema Hybrid (Suppressed) | 1 | 7,860,245 | 1 | 1 | 1 | 2 | 99,327 |
| redis-cell | 10 | 61,310 | 255 | 337 | 383 | 539 | 854 |
| GCRA | 10 | 49,610 | 320 | 410 | 459 | 680 | 1,710 |
| Trypema Redis (Absolute) | 10 | 45,067 | 352 | 462 | 523 | 748 | 1,031 |
| Trypema Redis (Suppressed) | 10 | 38,977 | 402 | 557 | 640 | 1,005 | 1,257 |
| Trypema Hybrid (Absolute) | 10 | 8,285,258 | 1 | 1 | 1 | 1 | 9,953,279 |
| Trypema Hybrid (Suppressed) | 10 | 7,994,442 | 1 | 1 | 1 | 2 | 99,135 |
| redis-cell | 100 | 61,369 | 255 | 336 | 383 | 503 | 898 |
| GCRA | 100 | 49,433 | 321 | 409 | 458 | 631 | 2,571 |
| Trypema Redis (Absolute) | 100 | 44,462 | 356 | 470 | 528 | 713 | 1,125 |
| Trypema Redis (Suppressed) | 100 | 37,995 | 408 | 589 | 687 | 1,314 | 5,543 |
| Trypema Hybrid (Absolute) | 100 | 9,134,903 | 1 | 1 | 1 | 1 | 10,807 |
| Trypema Hybrid (Suppressed) | 100 | 8,697,216 | 1 | 1 | 1 | 1 | 99,135 |
| redis-cell | 10,000 | 61,762 | 253 | 336 | 382 | 477 | 675 |
| GCRA | 10,000 | 49,548 | 320 | 408 | 461 | 686 | 1,428 |
| Trypema Redis (Absolute) | 10,000 | 42,977 | 368 | 496 | 568 | 821 | 1,768 |
| Trypema Redis (Suppressed) | 10,000 | 38,816 | 403 | 566 | 656 | 978 | 1,861 |
| Trypema Hybrid (Absolute) | 10,000 | 8,684,796 | 1 | 1 | 1 | 5 | 13,567 |
| Trypema Hybrid (Suppressed) | 10,000 | 7,115,638 | 1 | 1 | 1 | 3 | 99,711 |
| redis-cell | 100,000 | 61,210 | 256 | 337 | 378 | 473 | 1,023 |
| GCRA | 100,000 | 49,664 | 319 | 410 | 461 | 731 | 1,428 |
| Trypema Redis (Absolute) | 100,000 | 42,274 | 374 | 501 | 576 | 715 | 1,198 |
| Trypema Redis (Suppressed) | 100,000 | 38,553 | 407 | 562 | 649 | 883 | 2,069 |
| Trypema Hybrid (Absolute) | 100,000 | 8,053,277 | 1 | 1 | 1 | 5 | 19,471 |
| Trypema Hybrid (Suppressed) | 100,000 | 9,142,906 | 1 | 1 | 1 | 1 | 161,919 |

#### key_space = 1,000

| Limiter | Rate/s | Ops/s | p50 (µs) | p95 (µs) | p99 (µs) | p999 (µs) | Max (µs) |
|---------|--------|-------|----------|----------|----------|-----------|----------|
| redis-cell | 1 | 60,618 | 259 | 340 | 388 | 512 | 1,144 |
| GCRA | 1 | 48,719 | 325 | 417 | 468 | 695 | 1,464 |
| Trypema Redis (Absolute) | 1 | 41,874 | 378 | 509 | 583 | 920 | 2,119 |
| Trypema Redis (Suppressed) | 1 | 27,744 | 558 | 815 | 945 | 1,346 | 3,999 |
| Trypema Hybrid (Absolute) | 1 | 7,350,822 | 1 | 1 | 1 | 4 | 9,854,975 |
| Trypema Hybrid (Suppressed) | 1 | 2,343,675 | 1 | 1 | 1 | 668 | 509,951 |
| redis-cell | 10 | 53,992 | 288 | 389 | 464 | 636 | 1,559 |
| GCRA | 10 | 44,076 | 359 | 466 | 545 | 813 | 1,583 |
| Trypema Redis (Absolute) | 10 | 37,450 | 421 | 581 | 677 | 952 | 1,746 |
| Trypema Redis (Suppressed) | 10 | 28,353 | 546 | 797 | 941 | 1,246 | 2,061 |
| Trypema Hybrid (Absolute) | 10 | 8,243,810 | 1 | 1 | 1 | 11 | 34,303 |
| Trypema Hybrid (Suppressed) | 10 | 2,590,528 | 1 | 1 | 1 | 662 | 454,399 |
| redis-cell | 100 | 53,991 | 287 | 387 | 471 | 679 | 1,290 |
| GCRA | 100 | 44,357 | 357 | 463 | 544 | 743 | 3,253 |
| Trypema Redis (Absolute) | 100 | 33,770 | 459 | 652 | 823 | 1,303 | 3,767 |
| Trypema Redis (Suppressed) | 100 | 29,844 | 518 | 761 | 899 | 1,527 | 2,965 |
| Trypema Hybrid (Absolute) | 100 | 8,191,257 | 1 | 1 | 1 | 1 | 10,279 |
| Trypema Hybrid (Suppressed) | 100 | 3,145,538 | 1 | 1 | 1 | 639 | 338,943 |
| redis-cell | 10,000 | 54,046 | 288 | 388 | 471 | 754 | 3,579 |
| GCRA | 10,000 | 44,142 | 357 | 464 | 545 | 803 | 2,305 |
| Trypema Redis (Absolute) | 10,000 | 34,226 | 457 | 637 | 768 | 1,319 | 3,121 |
| Trypema Redis (Suppressed) | 10,000 | 29,605 | 525 | 762 | 902 | 1,312 | 2,027 |
| Trypema Hybrid (Absolute) | 10,000 | 9,008,798 | 1 | 1 | 1 | 9 | 4,411 |
| Trypema Hybrid (Suppressed) | 10,000 | 8,487,169 | 1 | 1 | 1 | 2 | 1,991 |
| redis-cell | 100,000 | 53,807 | 287 | 392 | 475 | 747 | 3,865 |
| GCRA | 100,000 | 44,339 | 357 | 465 | 539 | 755 | 1,204 |
| Trypema Redis (Absolute) | 100,000 | 34,103 | 458 | 646 | 784 | 1,454 | 2,635 |
| Trypema Redis (Suppressed) | 100,000 | 29,428 | 528 | 772 | 920 | 1,491 | 2,523 |
| Trypema Hybrid (Absolute) | 100,000 | 7,358,353 | 1 | 1 | 1 | 6 | 3,647 |
| Trypema Hybrid (Suppressed) | 100,000 | 7,216,898 | 1 | 1 | 1 | 8 | 1,297 |

#### key_space = 10,000

| Limiter | Rate/s | Ops/s | p50 (µs) | p95 (µs) | p99 (µs) | p999 (µs) | Max (µs) |
|---------|--------|-------|----------|----------|----------|-----------|----------|
| redis-cell | 1 | 53,367 | 291 | 394 | 472 | 697 | 2,705 |
| GCRA | 1 | 43,677 | 363 | 472 | 560 | 807 | 3,661 |
| Trypema Redis (Absolute) | 1 | 36,326 | 433 | 594 | 718 | 1,286 | 2,631 |
| Trypema Redis (Suppressed) | 1 | 29,249 | 529 | 785 | 947 | 1,961 | 12,455 |
| Trypema Hybrid (Absolute) | 1 | 6,234,742 | 1 | 1 | 1 | 366 | 526,335 |
| Trypema Hybrid (Suppressed) | 1 | 51,110 | 1 | 774 | 2,103 | 3,063 | 1,065,983 |
| redis-cell | 10 | 53,748 | 287 | 390 | 480 | 1,251 | 3,209 |
| GCRA | 10 | 43,898 | 361 | 468 | 552 | 853 | 2,153 |
| Trypema Redis (Absolute) | 10 | 33,690 | 466 | 657 | 779 | 1,363 | 2,841 |
| Trypema Redis (Suppressed) | 10 | 30,091 | 514 | 756 | 914 | 1,791 | 9,247 |
| Trypema Hybrid (Absolute) | 10 | 8,275,750 | 1 | 1 | 1 | 46 | 251,647 |
| Trypema Hybrid (Suppressed) | 10 | 213,605 | 1 | 1 | 1,057 | 2,081 | 539,647 |
| redis-cell | 100 | 59,491 | 258 | 337 | 390 | 2,209 | 4,959 |
| GCRA | 100 | 48,508 | 327 | 417 | 476 | 620 | 1,150 |
| Trypema Redis (Absolute) | 100 | 35,655 | 427 | 598 | 747 | 3,203 | 38,815 |
| Trypema Redis (Suppressed) | 100 | 33,470 | 460 | 677 | 778 | 1,371 | 3,115 |
| Trypema Hybrid (Absolute) | 100 | 5,683,238 | 1 | 1 | 1 | 85 | 52,063 |
| Trypema Hybrid (Suppressed) | 100 | 1,096,356 | 1 | 1 | 1 | 1,610 | 492,543 |
| redis-cell | 10,000 | 59,707 | 258 | 341 | 394 | 1,984 | 4,495 |
| GCRA | 10,000 | 48,515 | 327 | 418 | 469 | 651 | 1,484 |
| Trypema Redis (Absolute) | 10,000 | 37,454 | 418 | 581 | 663 | 899 | 1,402 |
| Trypema Redis (Suppressed) | 10,000 | 31,533 | 490 | 716 | 836 | 1,794 | 2,393 |
| Trypema Hybrid (Absolute) | 10,000 | 10,069,860 | 1 | 1 | 1 | 2 | 1,291 |
| Trypema Hybrid (Suppressed) | 10,000 | 6,293,406 | 1 | 1 | 1 | 11 | 13,119 |
| redis-cell | 100,000 | 59,048 | 260 | 341 | 402 | 2,383 | 3,965 |
| GCRA | 100,000 | 48,577 | 328 | 418 | 472 | 637 | 2,419 |
| Trypema Redis (Absolute) | 100,000 | 38,260 | 408 | 572 | 654 | 968 | 2,069 |
| Trypema Redis (Suppressed) | 100,000 | 33,729 | 457 | 678 | 805 | 1,354 | 2,381 |
| Trypema Hybrid (Absolute) | 100,000 | 9,690,774 | 1 | 1 | 1 | 9 | 11,175 |
| Trypema Hybrid (Suppressed) | 100,000 | 9,469,741 | 1 | 1 | 1 | 1 | 2,695 |

## Local Comparison (trypema vs burster vs governor)

This suite compares three in-process limiters using the same stress harness:

- `trypema` local provider (bucketed/coalesced rolling window)
- `burster` `SlidingWindowLog` (strict rolling window log)
- `governor` (GCRA; different semantics)

Notes:

- For `burster`, the window is a const-generic (`W` ms). The harness currently supports `--window-s 10|60|300`.
- For `governor`, we configure `Quota::per_second(rate).allow_burst(rate * window_s)` to roughly match "capacity per window". This is still not a strict sliding window.

Run:

```bash
make stress-local-compare
```

### Hot Key (Single Key, 16 threads, 30s, 1000 ops/s limit, 10s window)

| Limiter | Ops/s | Allowed | Rejected | p50 (µs) | p95 (µs) | p99 (µs) | p999 (µs) | Max (µs) |
|---------|-------|---------|----------|----------|----------|----------|-----------|----------|
| Burster | 413k | 30k | 12.4M | 6 | 207 | 463 | 974 | 3457 |
| Governor | 4.9M | 40k | 147.7M | 1 | 3 | 10 | 54 | 28095 |
| Trypema (Absolute) | 3.5M | 30k | 106.2M | 1 | 5 | 31 | 129 | 26463 |
| Trypema (Suppressed) | 3.6M | 30.5k | 0 | 1 | 3 | 9 | 65 | 92031 |

### Uniform Keys (100k keys, 16 threads, 30s, 1B ops/s limit, 10s window)

| Limiter | Ops/s | Allowed | Rejected | p50 (µs) | p95 (µs) | p99 (µs) | p999 (µs) | Max (µs) |
|---------|-------|---------|----------|----------|----------|----------|-----------|----------|
| Burster | 55k | 1.7M | 0 | 105 | 969 | 1565 | 7415 | 105407 |
| Governor | 6.3M | 188.9M | 0 | 1 | 1 | 1 | 1 | 12727 |
| Trypema (Absolute) | 6.1M | 182.1M | 0 | 1 | 1 | 3 | 359 | 9167 |
| Trypema (Suppressed) | 7.5M | 224.1M | 0 | 1 | 2 | 43 | 135 | 5767 |

## Recommended Baseline Suite (1/2/3)

Single-host throughput (local):

- `local/absolute`: hot key, threads sweep; and 100k uniform keys
- `local/suppressed`: below-capacity and over-hard-limit

Tail latency under burst + high cardinality:

- 100k skewed keys, `--mode target-qps` with bursts
- compare `absolute` vs `suppressed`
- sweep `--group-ms` (1, 10, 50) and `--window-s` (10, 60, 300)

Redis distributed contention:

- hot key (key_space=1) at high concurrency
- 100k skewed keys at high concurrency
