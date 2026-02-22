# Benchmarking / Load Testing

This repo includes:

- Criterion microbenchmarks (`benches/`) for per-operation performance.
- A stress/load harness binary (workspace crate: `stress/`, package `trypema-stress`) for system-like throughput + latency.

Notes:

- Use `--release` for anything you want to compare.
- Redis benchmarks require Redis 6.2+.
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
