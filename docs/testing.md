# Testing

## Local Tests

Run the full suite:

```bash
cargo test
```

Note: the Redis integration tests require `REDIS_URL` and will panic if it is not set.

## Feature Flags

Redis support is feature-gated:

- Default: Redis enabled via `redis-tokio`
- Disable Redis entirely:

```bash
cargo test --no-default-features
```

- Use smol integration:

```bash
cargo test --no-default-features --features redis-smol
```

## Redis Integration Tests (Docker)

The repo includes a small harness (`compose.yaml` + `Makefile`) to run tests against a pinned Redis version.

Requirements:

- Docker
- Docker Compose v2 (`docker compose ...`)

Run tests:

```bash
make test-redis
```

Configuration:

- `REDIS_PORT` (default `16379`): host port to bind Redis to
- `REDIS_URL` (default `redis://127.0.0.1:$(REDIS_PORT)`): URL used by the tests

Examples:

```bash
REDIS_PORT=6379 make test-redis
```

```bash
REDIS_URL=redis://127.0.0.1:6379 cargo test
```

## GitHub Actions

Workflow: `.github/workflows/workflow.yml`

What it does:

- starts `redis:7.4.2-alpine` as a service container
- waits for `redis-cli ping`
- runs `cargo test` with `REDIS_URL=redis://127.0.0.1:6379`
