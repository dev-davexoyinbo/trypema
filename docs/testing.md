# Testing Guide

This guide covers running tests for Trypema, including local-only tests and Redis integration tests.

## Quick Start

### Local Tests Only (No Redis)

Run tests without Redis dependencies:

```bash
cargo test --no-default-features
```

**What gets tested:**
- Local provider (absolute and suppressed strategies)
- Configuration validation
- Type conversions
- Error handling

### All Tests (Including Redis)

Requires a running Redis instance:

```bash
# Using Docker (recommended)
make test-redis

# Or with existing Redis
REDIS_URL=redis://127.0.0.1:6379 cargo test
```

**What gets tested:**
- Everything in local tests
- Redis provider (absolute strategy)
- Redis connection handling
- Lua script execution
- Distributed rate limiting behavior

## Feature Flags

Redis support is controlled by feature flags:

| Configuration | Features | Runtime | Command |
|---------------|----------|---------|---------|
| Default | `redis-tokio` | Tokio | `cargo test` |
| No Redis | (none) | Sync only | `cargo test --no-default-features` |
| Smol runtime | `redis-smol` | Smol | `cargo test --no-default-features --features redis-smol` |

## Redis Integration Tests

### Using Docker (Recommended)

The repository includes a test harness (`compose.yaml` + `Makefile`) that:
1. Starts Redis 7.4.2-alpine in a container
2. Waits for Redis to be ready
3. Runs all tests
4. Cleans up the container

**Requirements:**
- Docker
- Docker Compose v2 (`docker compose` command, not `docker-compose`)

**Run tests:**

```bash
# Default port 16379
make test-redis

# Custom port
REDIS_PORT=6379 make test-redis

# Keep container running for debugging
make start-redis
cargo test
make stop-redis
```

### Using Existing Redis

If you already have Redis 6.2+ running:

```bash
# Standard Redis
REDIS_URL=redis://127.0.0.1:6379 cargo test

# Redis with auth
REDIS_URL=redis://:password@127.0.0.1:6379 cargo test

# Redis TLS
REDIS_URL=rediss://127.0.0.1:6380 cargo test
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://127.0.0.1:16379` | Full Redis connection URL |
| `REDIS_PORT` | `16379` | Port to bind Redis container (Makefile only) |

## Test Structure

### Test Organization

```
src/
├── tests/
│   ├── mod.rs
│   ├── test_absolute_local_rate_limiter.rs      # Local absolute strategy
│   ├── test_suppressed_local_rate_limiter.rs    # Local suppressed strategy
│   ├── test_absolute_redis_rate_limiter.rs      # Redis absolute strategy
│   ├── test_suppressed_redis_rate_limiter.rs    # Redis suppressed (placeholder)
│   ├── test_cleanup_loop.rs                     # Cleanup functionality
│   └── test_common_validation.rs                # Type validation
```

### Running Specific Tests

```bash
# Run only local tests
cargo test --test test_absolute_local_rate_limiter

# Run only Redis tests
REDIS_URL=redis://127.0.0.1:6379 cargo test --test test_absolute_redis_rate_limiter

# Run specific test function
cargo test --test test_cleanup_loop test_cleanup_removes_stale_keys

# Run with output
cargo test -- --nocapture
```

### Debugging Tests

Enable tracing logs:

```bash
RUST_LOG=debug cargo test -- --nocapture
```

Enable Redis command logging (with `redis-tokio` feature):

```rust,ignore
tracing_subscriber::fmt()
    .with_env_filter("trypema=debug,redis=debug")
    .init();
```

## Continuous Integration

### GitHub Actions

**Workflow:** `.github/workflows/workflow.yml`

**What it does:**

1. Starts `redis:7.4.2-alpine` as a service container
2. Waits for Redis readiness (`redis-cli ping`)
3. Runs `cargo test` with `REDIS_URL=redis://127.0.0.1:6379`
4. Runs tests on multiple Rust versions (stable, beta, nightly)

**Triggering:**
- Automatic on push to `main` or `develop`
- Automatic on pull requests
- Manual via GitHub Actions UI

### Local CI Simulation

Run the same checks as CI locally:

```bash
# Check formatting
cargo fmt --check

# Run clippy
cargo clippy --all-targets --all-features -- -D warnings

# Run tests
make test-redis

# Build docs
cargo doc --no-deps --all-features
```

## Troubleshooting

### Problem: `REDIS_URL` not set

**Error:**
```
thread 'main' panicked at 'REDIS_URL environment variable not set'
```

**Solution:**
```bash
# Set before running tests
export REDIS_URL=redis://127.0.0.1:6379
cargo test

# Or inline
REDIS_URL=redis://127.0.0.1:6379 cargo test

# Or use Makefile
make test-redis
```

### Problem: Redis connection refused

**Error:**
```
Error: RedisError(Io(Os { code: 111, kind: ConnectionRefused, message: "Connection refused" }))
```

**Checks:**
1. Is Redis running? `redis-cli ping`
2. Correct port? `netstat -tuln | grep 6379`
3. Firewall blocking? Check `iptables` or firewall rules
4. Docker network? Use `host.docker.internal` on macOS/Windows

**Solution:**
```bash
# Start Redis with Docker
docker run -d --name redis -p 6379:6379 redis:7.4.2-alpine

# Or use make
make start-redis
```

### Problem: Tests timeout

**Possible causes:**
1. Redis is slow or unresponsive
2. Network issues
3. Resource exhaustion

**Debug:**
```bash
# Check Redis performance
redis-cli --latency

# Check Redis info
redis-cli INFO stats

# Run single test with timeout
cargo test --test test_absolute_redis_rate_limiter -- --test-threads=1
```

### Problem: Docker Compose not found

**Error:**
```
make: docker: command not found
```

**Solution:**
- Install Docker Desktop (macOS/Windows)
- Install Docker Engine + Docker Compose plugin (Linux)
- Ensure `docker compose` (not `docker-compose`) works

### Problem: Port already in use

**Error:**
```
Error starting userland proxy: listen tcp4 0.0.0.0:16379: bind: address already in use
```

**Solution:**
```bash
# Use different port
REDIS_PORT=16380 make test-redis

# Or kill existing process
lsof -ti:16379 | xargs kill
```

### Problem: Redis version too old

**Cause:** Redis version < 6.2.0

**Solution:**
```bash
# Check version
redis-cli INFO server | grep redis_version

# Upgrade Redis (using latest for best compatibility)
docker run -d --name redis -p 6379:6379 redis:7.4.2-alpine
```

## Best Practices

### Writing Tests

1. **Use descriptive names:**
   ```rust,ignore
   #[tokio::test]
   async fn test_absolute_rejects_when_over_limit() { ... }
   ```

2. **Clean up after tests:**
   ```rust,ignore
   #[tokio::test]
   async fn test_redis_rate_limit() {
       let key = RedisKey::try_from(format!("test_{}", uuid::Uuid::new_v4()))?;
       // ... test logic ...
       
       // Cleanup
       redis::cmd("DEL").arg(&format!("trypema:{}:*", key)).query_async(&mut conn).await?;
   }
   ```

3. **Use unique keys:**
   ```rust,ignore
   use uuid::Uuid;
   let key = format!("test_{}", Uuid::new_v4());
   ```

4. **Test concurrency:**
   ```rust,ignore
   let handles: Vec<_> = (0..100)
       .map(|_| tokio::spawn(limiter.inc(&key, &rate, 1)))
       .collect();
   
   for handle in handles {
       handle.await??;
   }
   ```

### Performance Testing

For load testing, use a separate benchmark suite:

```bash
# Example using criterion (not included)
cargo bench --bench rate_limiter_bench
```

Consider testing:
- Sustained throughput under load
- Behavior with many concurrent clients
- Memory usage with high key cardinality
- Redis network latency impact
