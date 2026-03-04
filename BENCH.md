# Benchmarking / Load Testing

This repo includes:

- Criterion microbenchmarks (`benches/`) for per-operation performance.
- A stress/load harness binary (workspace crate: `stress/`, package `trypema-stress`) for system-like throughput + latency.

Notes:

- Use `--release` for anything you want to compare.
- Redis benchmarks require Redis 6.2+.
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

## Local Comparison (trypema vs burster vs governor)

This suite compares three in-process limiters using the same stress harness:

- `trypema` local provider (bucketed/coalesced rolling window)
- `burster` `SlidingWindowLog` (strict rolling window log)
- `governor` (GCRA; different semantics)

Notes:

- For `burster`, the window is a const-generic (`W` ms). The harness currently supports `--window-s 10|60|300`.
- For `governor`, we configure `Quota::per_second(rate).allow_burst(rate * window_s)` to roughly match “capacity per window”. This is still not a strict sliding window.

Run:

```bash
make stress-local-compare
```

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
