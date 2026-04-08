# Documentation Conventions

This file is the canonical documentation style guide for this repository.

## Top-Level Docs Split

- `docs/lib.md` is the canonical docs.rs source and is included by `src/lib.rs`.
- `README.md` is the GitHub- and crates-facing companion document.
- Keep the two documents structurally aligned, but let `docs/lib.md` carry the richer Rustdoc
  linking and feature-aware behavior.

## Link Policy

- In `docs/lib.md`, prefer Rustdoc intra-doc links for public types, functions, and enums when the
  target is available in that feature context.
- In `README.md`, use inline code in prose instead of Rustdoc links.
- Do not add links that break `cargo rustdoc --no-default-features -D warnings`.
- For feature-gated APIs, either:
  - gate the linked prose so it only appears when the item exists, or
  - use plain inline code in shared prose.

## Example Policy

- Prefer runnable examples and doctests whenever practical.
- `docs/lib.md` may use hidden `#` lines to keep examples clean on docs.rs.
- `README.md` should not show docs.rs-only hidden scaffolding.
- Keep examples focused and short. Prefer one concept per example.
- When an example depends on Redis-backed features, make that explicit in prose or comments.

## Builder and Setup Policy

- Explain the local-only vs Redis-enabled builder split clearly:
  - local-only: `RateLimiterBuilder::default()` or `RateLimiter::builder()`
  - Redis-enabled: `RateLimiter::builder(connection_manager)`
- Document the cleanup difference clearly:
  - `RateLimiterBuilder::build()` starts cleanup automatically
  - `RateLimiter::new(...)` does not
- In builder examples, add short comments before chained setters:
  - `Optional: ...`
  - For Redis-only setters: `Optional: only available with 'redis-tokio' or 'redis-smol'`

## Public API Doc Style

- Start with a short purpose sentence.
- Link related public APIs where it helps orientation.
- Show flexibility in examples:
  - multiple constructors (`new`, `try_from`, `new_or_panic`) where applicable
  - defaults plus one override for options/builder types
- Prefer behavior-level explanations over internal implementation detail unless the detail is
  important for users.

## Feature-Gated Items

- Redis and hybrid APIs require Redis 7.2+.
- Redis-only builder setters and Redis-related types should be explicitly marked as
  feature-gated.
- Keep docs warning-clean in both:
  - `cargo rustdoc -p trypema --no-default-features -- -D warnings`
  - `cargo rustdoc -p trypema --features redis-tokio -- -D warnings`
