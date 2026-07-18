# Trypema Code and Documentation Conventions

This document is the canonical style and design-convention guide for Trypema. It explains how
code, APIs, tests, benchmarks, Lua scripts, and documentation should be shaped so that changes
remain consistent across the repository.

`AGENTS.md` defines behavioral contracts, workflow requirements, and the verification matrix.
This file defines how implementations should be organized and expressed. Read both before making
substantial changes. If they appear to conflict, preserve observable behavior and correctness
first, then follow the more specific rule. Do not change a public contract merely to satisfy a
style preference.

The words **must**, **must not**, **should**, and **should not** are normative. Examples marked
“prefer” describe the default; depart from them only when the alternative is materially clearer
or required for correctness.

## 1. Guiding Principles

Apply these principles in order:

1. Preserve correctness and documented behavior.
2. Preserve concurrency properties and hot-path performance.
3. Make ownership, mutation, state transitions, and units obvious.
4. Prefer established Rust conventions over repository-specific novelty.
5. Let tools decide mechanical formatting.
6. Keep related provider implementations recognizable without forcing false symmetry.
7. Keep documentation current with the code and avoid duplicate sources of truth.
8. Optimize for the next maintainer reading the diff, not only for the current author writing it.

Consistency is valuable because it reduces the amount of code a reader must reinterpret. It is
not a reason to hide meaningful differences between local, Redis, and hybrid behavior.

## 2. Sources of Authority

Use the following precedence when deciding how code should look or behave:

1. Public API contracts, tests, and the behavioral rules in `AGENTS.md`.
2. This repository's established domain vocabulary and module structure.
3. `cargo fmt` and the default Rust style.
4. The Rust API Guidelines for public API design.
5. The Rustdoc Book for public documentation.
6. Existing nearby code when none of the above decides the question.

Do not copy general Rust formatting rules into this file when `rustfmt` already enforces them.
Run the tool and accept its output. If this guide and `rustfmt` disagree on mechanical layout,
`rustfmt` wins unless the repository later adds an explicit `rustfmt.toml`.

Before introducing a new convention:

- Confirm the problem occurs more than once.
- Prefer an automated check over a prose-only rule where practical.
- State the reason, the scope, and any legitimate exceptions.
- Update this file in the same change.

## 3. Repository and Module Organization

Keep responsibilities in their existing layers:

| Area | Responsibility |
| --- | --- |
| `src/common.rs` | Public shared value types, decisions, comparators, and history modes |
| `src/local/` | Always-available in-memory implementations |
| `src/redis/` | Redis providers, Redis key types, Rust orchestration, and Lua scripts |
| `src/redis/scripts.rs` | Canonical home of all rate-limiter Lua bodies and Lua helpers |
| `src/hybrid/` | Local pending state, Redis proxies, flushing, and coordination |
| `src/runtime.rs` | Runtime abstraction shared by Tokio and Smol builds |
| `src/tests/` | Unit, behavioral, concurrency, and raw Redis-state regression tests |
| `benches/` | Criterion benchmarks and benchmark-only helpers |
| `stress/` | Long-running or comparative load generation |
| `docs/lib.md` | Canonical docs.rs crate documentation |
| `README.md` | GitHub and crates.io companion documentation |
| `BENCH.md` | Benchmark inventory and operating guidance |

Follow these organization rules:

- Put behavior beside the type that owns it.
- Extract a helper only when it names a real concept, removes meaningful duplication, or
  centralizes an invariant.
- Keep a helper private until multiple modules genuinely need it.
- Prefer `pub(crate)` to `pub` for cross-module implementation details.
- Re-export public API intentionally from the narrowest sensible module.
- Do not create a generic “utils” module. Name modules after the responsibility they own.
- Do not move feature gates into always-available local code.
- Keep runtime-specific code behind the runtime abstraction instead of duplicating Tokio and Smol
  implementations.

File names and module names use `snake_case`. Types should live in a file whose name makes their
role discoverable. A large file may contain tightly coupled private support types; split it when
the responsibilities can be named independently, not solely because of line count.

## 4. Rust Formatting and Source Layout

Use stable `rustfmt` through:

```bash
cargo fmt --all
```

The checked-in result must pass:

```bash
cargo fmt --all -- --check
```

Source layout conventions:

- Use four-space indentation and spaces rather than tabs in Rust and Lua.
- Keep ordinary Rust lines within the formatter's default width.
- Use trailing commas in multiline Rust lists, calls, literals, and match arms.
- Put one attribute per line unless `rustfmt` combines or preserves a conventional form.
- Separate conceptual phases of a function with a single blank line.
- Do not use repeated blank lines as visual separators.
- Use closing comments in implementation files so long methods and impl blocks have an explicit
  boundary. Use `// end constructor` for `new`, `// end method <name>` for public methods,
  `// end fn <name>` for private helpers, and `// end impl` for an impl block. Keep the wording
  exact rather than introducing variants such as `// end of impl`.
- Avoid manual alignment that the formatter will destroy.
- Prefer early returns when they make guard conditions and no-op paths explicit.
- Prefer a `match` when branches produce the same kind of value; prefer `if let` or `let else`
  when only one shape matters.
- Use `let else` for required destructuring that returns early on absence.
- Keep a local close to its first use. Do not front-load a function with values needed only by
  later branches.

Do not reformat unrelated code. Mechanical cleanup belongs in a focused change unless it is
necessary to edit the requested lines safely.

## 5. Naming and Domain Vocabulary

Follow standard Rust casing:

- Modules, functions, methods, locals, and fields: `snake_case`.
- Structs, enums, traits, and enum variants: `UpperCamelCase`.
- Constants and statics: `SCREAMING_SNAKE_CASE`.
- Lifetimes: short lowercase names such as `'a`.
- Generic type parameters: short `UpperCamelCase` names such as `T`, or descriptive names when a
  single letter would obscure the role.

Use words consistently across Rust, Lua, tests, benches, and documentation.

### 5.1 Rate-limit vocabulary

- `rate_limit` means the caller-supplied per-second `RateLimit`.
- `window_limit` means the computed absolute capacity for one complete window.
- `hard_window_limit` means the suppressed strategy's computed hard capacity, including
  `hard_limit_factor`.
- `soft_window_limit` means the derived suppressed admission threshold.
- Internal count fields and locals use a descriptive prefix followed by `_count`, such as
  `total_count`, `total_declined_count`, `pending_count`, or `accepted_count`.
- Preserve established public fields such as `SuppressedRateLimitSnapshot::total` and
  `SuppressedRateLimitSnapshot::total_declined`; this naming rule does not authorize a breaking
  public API change.
- `count` remains the increment argument and the count stored in one `Bucket`; do not rename those
  to `increment_count` or `bucket_count` merely to satisfy the suffix rule.
- `declined_count` means the declined portion of that bucket or operation.
- `Bucket` is the internal timestamped aggregation type. A history collection is named `buckets`.
  A value obtained from `buckets.back()` is `latest_bucket`, and a value obtained from
  `buckets.front()` is `oldest_bucket`. Use `bucket` when the position is selected dynamically,
  such as a preservation-mode match whose branches choose between `front()` and `back()`.
- `series` means the per-key history plus its stored window limit and aggregate metadata. Always
  name a local DashMap guard or per-key state reference `series`, including mutable and downgraded
  guards. Shadow `series` across guard transitions instead of introducing names such as
  `rate_limit_series`, `series_guard`, or `series_entry`.
- `pending` means hybrid state not yet committed to Redis.
- `live` means not expired at the operation's comparison point.
- `stale` is reserved for entity cleanup based on inactivity, not bucket expiration.
- Keep `RateGroupSizeMs` and `rate_group_size_ms` for the public configuration interval. Internal
  state that describes a concrete bucket uses `bucket`, for example `oldest_bucket_ttl` and
  `oldest_bucket_count`, rather than `last_rate_group_ttl` or `last_rate_group_count`.

Do not shorten `window_limit`, `hard_window_limit`, or `soft_window_limit` to `limit` in code that
could refer to more than one of them. Shared physical Redis-key functions may retain the generic
`window_limit_key` name because both strategies use that storage slot.

### 5.2 Provider and strategy names

Use the ordering `<Strategy><Provider><Role>` for types:

- `AbsoluteLocalRateLimiter`
- `SuppressedRedisRateLimiter`
- `AbsoluteHybridRedisProxy`

Use `absolute` and `suppressed` consistently. Do not introduce synonyms such as `fixed`,
`probabilistic`, or `soft` in identifiers unless they name a genuinely different concept.

### 5.3 Method names

- Constructors use `new`, `default`, `try_from`, or `new_or_panic` according to their behavior.
- Fallible conversion should use standard traits such as `TryFrom` where appropriate.
- Borrowed accessors normally omit `get_`; retain established public names such as limiter `get`
  and Redis key builders such as `get_window_limit_key` unless intentionally making a breaking
  API change.
- Use `as_` for cheap borrowed views, `to_` for conversions that may allocate or perform work,
  and `into_` for consuming conversions.
- Boolean methods should read as predicates: `is_allowed`, `is_expired`, `has_pending_state`.
- A value of `RateLimitDecision` is named `decision`. Reserve `is_*` and `should_*` locals for
  booleans; do not name an enum result `is_allowed`.
- Methods that mutate should use a verb that names the mutation: `inc`, `cleanup`, `commit`,
  `invalidate`, `set_if`.
- Keep corresponding method names aligned across local, Redis, and hybrid variants.

Avoid names that expose incidental implementation details. Prefer `cleanup_expired_suppressed`
over a name tied to the exact Redis commands used internally.

## 6. Public API Design

Public APIs should be predictable, type-safe, documented, and difficult to misuse.

- Preserve source compatibility unless the task explicitly authorizes a breaking change.
- Use domain newtypes for validated quantities such as `RateLimit`, `WindowSizeSeconds`, and
  `RateGroupSizeMs`.
- Prefer an enum over a boolean parameter when both values express distinct policies. For
  example, use `HistoryPreservation`, not `preserve_newest: bool`.
- Keep public struct fields private unless direct construction and future compatibility have been
  considered deliberately.
- Implement common traits when their semantics are unsurprising: `Debug`, `Clone`, `Copy`,
  `Default`, `Eq`, `Hash`, or conversion traits as appropriate.
- Do not implement `Deref` merely to save method calls.
- Accept borrowed inputs when ownership is unnecessary; take ownership when the operation must
  retain or consume the value.
- Return meaningful intermediate results when callers would otherwise repeat work. Conditional
  sets therefore retain `(new_total, old_total)`.
- Do not add out-parameters.
- Avoid public aliases that create two names for the same concept.
- New public items require a deliberate re-export path and warning-clean Rustdoc.

The local API is synchronous. Redis and hybrid APIs are asynchronous and return the repository's
existing `Result` shape. Do not make local callers depend on a Redis runtime or feature.

## 7. Values, Units, and Calculations

Units must be visible in names:

- Seconds: `_seconds` or `_s` in benchmark/stress command-line contexts.
- Milliseconds: `_ms`.
- Counts: a descriptive `_count` suffix internally, with the explicit `count` and public-API
  exceptions in section 5.1.
- Ratios and factors: `_factor`.

Calculation conventions:

- Compute window capacity using the repository's established truncating behavior.
- Make integer truncation explicit at the boundary where it becomes part of the contract.
- Do not scatter repeated casts through a multi-step calculation; calculate once near first use
  when the value is reused.
- Do not calculate a requested window limit before a guard or missing-key branch proves it is
  needed.
- Put missing-key calculations inside `or_insert_with` so a racing insert does not perform wasted
  work.
- Prefer direct field access for one-use reads.
- Introduce a local when a value is used repeatedly, derived further, read through a mutex, must
  survive dropping a guard, or clarifies a multi-stage calculation.
- Use checked or saturating arithmetic when overflow is possible under valid inputs and ordinary
  arithmetic would violate an invariant.
- Keep time-boundary operators consistent across all providers. A grouping interval uses the
  inclusive comparison `age_ms <= rate_group_size_ms`; expiration must match the documented
  live-window boundary.

Never encode a unit only in a comment. Put it in the type or identifier.

## 8. Ownership, Mutation, and Concurrency

Mutation should be visible and acquired as late as possible.

### 8.1 DashMap access

- Every internal `DashMap` uses `common::RandomState`, including series maps, hybrid state and
  lock maps, and caches. The alias centralizes the selected hashing algorithm and documents
  alternatives; do not import a concrete hash builder directly at individual map sites.
- Start with `DashMap::get` for existing-key inspection.
- Complete read-only and atomic-only fast paths under the shared guard.
- Drop the shared guard before calling `get_mut`, `entry`, `remove`, or another exclusive
  operation.
- Re-read and revalidate state after acquiring exclusive access.
- Use `entry(...).or_insert_with(...).downgrade()` when a missing entry must be created and the
  following work only needs immutable or atomic access.
- Do not insert, discard the entry guard, and perform a second `get`.
- Every `get_mut`, `entry`, `remove`, `remove_if`, and `retain` must protect a concrete mutation.
- Do not hold a DashMap guard across `.await`.

`downgrade()` is the canonical missing-key transition from the mutable entry guard returned by
`or_insert_with` to the immutable guard used by the rest of the operation. Do not call it
`upgrade`, and do not replace it with an insert followed by a second `get`.

Atomic mutation of bucket counters is compatible with a shared map guard when the bucket
structure does not change. Appending, removing, draining, or reordering buckets is structural and
requires exclusive access.

### 8.2 Atomics and mutexes

- Use atomics for independent numeric state whose invariants can be maintained without a wider
  critical section.
- Atomic loads use `Ordering::Acquire`, stores use `Ordering::Release`, and read-modify-write
  operations such as `fetch_add`, `fetch_sub`, and `swap` use `Ordering::AcqRel`.
- Do not use `Relaxed` or `SeqCst` as a local exception. If the standard ordering policy is ever
  insufficient, document the invariant and update this convention deliberately.
- Use a mutex when multiple ordinary fields must change as one logical unit.
- Keep critical sections short and never perform Redis I/O while holding a local mutable guard.
- Do not hide lock acquisition inside an innocently named helper.
- Keep lock ordering explicit when an operation needs more than one lock.

### 8.3 Async code

- Gather owned snapshots before `.await`.
- Do not carry borrowed map guards, mutex guards, or temporary references across `.await`.
- Keep runtime-neutral behavior outside Tokio- or Smol-specific branches.
- Use the existing runtime abstraction in production code and tests.
- Coordinate per-key hybrid flush and conditional-set paths through the established per-key lock.

## 9. State Transitions and Control Flow

Organize state-changing functions into visible phases:

1. Resolve the key and inspect existing state.
2. Remove or logically exclude expired history when required.
3. Evaluate admission or comparator guards.
4. Return immediately on a no-op or rejection.
5. Acquire mutation capability only if the transition requires it.
6. Revalidate after any guard or lock transition.
7. Apply history and aggregate changes together.
8. Update caches, limits, TTLs, and activity metadata only when required.
9. Return the public result.

Keep no-op paths genuinely cheap. A comparator miss should be visually obvious and should not
fall through a block containing writes.

Use comments to explain why a phase or ordering constraint exists, especially around races,
virtual hybrid buckets, cache invalidation, and Redis atomicity. Do not narrate syntax that is
already evident from the code.

## 10. Errors, Panics, and Logging

- Return `TrypemaError` for recoverable library failures.
- Preserve useful context when mapping an external error.
- New error messages should be concise, lowercase fragments unless they are complete sentences.
- Do not use `unwrap`, `expect`, `unreachable!`, or indexing when valid concurrent behavior can
  make the assumed value absent.
- A panic is appropriate only for a documented programmer error or an invariant that is locally
  proven and cannot be invalidated by another thread.
- Public panic conditions must have a `# Panics` Rustdoc section.
- Fallible public methods must document meaningful failure conditions in `# Errors`.
- Do not log and return the same error at the library boundary unless the log adds information the
  caller cannot provide.
- Never log Redis credentials, full connection URLs, or user-provided keys when they may be
  sensitive.

Tests may use `unwrap` when failure should abort the test and the preceding setup makes the
expectation clear. Prefer an assertion with context when diagnosing failure would otherwise be
difficult.

## 11. Redis and Lua Conventions

All rate-limiter Lua code belongs in `src/redis/scripts.rs`.

### 11.1 Script composition

- Cross-strategy helpers belong in `COMMON_LUA_HELPERS`.
- Absolute-only helpers belong in `ABSOLUTE_LUA_HELPERS`.
- Suppressed-only helpers belong in `SUPPRESSED_LUA_HELPERS`.
- Construct scripts only through `lua_script`, `absolute_lua_script`, or
  `suppressed_lua_script`.
- Provider and proxy files contain Rust orchestration, key/argument binding, result decoding, and
  no embedded Lua bodies.
- Use `now_ms()` for Redis server time.
- Reuse common expiry, summing, comparator, history-mode, grouping, metadata, and stale-cleanup
  helpers.

### 11.2 Lua naming and layout

- Lua locals and functions use `snake_case`.
- Rust script constants use `<STRATEGY>_<OPERATION>_LUA`.
- Name Redis keys with a `_key` suffix and sorted sets containing bucket fields `active_keys`.
- Name `ARGV` values after their logical meaning, not their argument position.
- Convert numeric `ARGV` values with `tonumber` immediately after binding.
- Keep `KEYS` bindings together, then `ARGV` bindings, then reads/calculation, then writes.
- Use `local` for every script-local value and helper.
- Avoid single-letter loop variables except conventional short indices inside a tiny loop.
- Do not shadow an outer loop index in a nested loop.
- Return stable tuple shapes that Rust decodes explicitly.

### 11.3 Redis atomicity and writes

- Use Redis server time for scores and TTL-sensitive behavior.
- Compute the logical live total before a conditional guard.
- Perform no writes before a comparator-miss return.
- Keep hashes, sorted sets, exact totals, decline totals, stored limits, caches, and active-entity
  membership consistent.
- Use `redis.error_reply` for invalid internal protocol values such as an unknown comparator or
  history mode.
- Treat Redis key order and `ARGV` order as an interface between Rust and Lua. Update both sides
  together and cover them with raw-state tests.
- A successful zero-target conditional set must delete every per-entity key relevant to the
  strategy and remove active membership.

Do not extract a Lua helper that obscures which writes occur before a guard. Atomicity is more
important than reducing a few repeated lines.

## 12. Tests

Tests are executable contracts, not demonstrations that the current implementation happens to
pass.

### 12.1 Test names and structure

- Test names use `snake_case` and describe behavior: `unknown_get_returns_zero_without_inserting`.
- Include the provider or strategy in the module name rather than repeating it in every test.
- Use Arrange–Act–Assert conceptually, but do not add those comments when the phases are already
  obvious.
- Keep one principal reason to fail per test.
- A regression test should fail under the bug it is intended to catch.
- Prefer exact assertions for deterministic values.
- Assert setup operations when later assertions depend on them.
- Minimize production-code `#[cfg(test)]` helpers. One or two test-only methods may remain in the
  production type's impl with individual `#[cfg(test)]` attributes. When a type needs three or
  more test-only methods, place one inherent impl block in the appropriate `src/tests/` file,
  named `test_<production_file_name>.rs`.
- Fields needed by an impl in `src/tests/` may be `pub(crate)`; do not make them public. Test-only
  helpers must not bypass the public API to construct limiter state.

### 12.2 State construction

- Create limiter state through public APIs: `inc`, `set_if`, and
  `set_if_preserve_history`.
- Do not fabricate private series, buckets, timestamps, totals, or decline counts.
- Test-only accessors may inspect postconditions or hold a real shared guard for lock testing.
- Raw Redis commands are appropriate for verifying persisted state after public operations, not
  as a shortcut for arranging ordinary behavioral tests.

### 12.3 Time and concurrency

- Keep real sleeps short and choose configurations with a clear scheduling margin.
- Use direct comparisons for time ranges.
- Do not require an exact millisecond value from wall-clock tests.
- Synchronize concurrency tests with barriers or channels rather than hopeful sleeps.
- Release held guards after timeout before joining worker threads.
- Do not weaken a test because a race exposes an implementation bug.

### 12.4 Cross-provider coverage

When behavior is shared, cover the applicable matrix:

- Absolute and suppressed.
- Local, Redis, and hybrid.
- Tokio and Smol for Redis-backed code.
- Behavioral API results and raw Redis state.
- Missing, fresh, expired, matched, guard-miss, zero-target, and racing states as relevant.

Use unique Redis prefixes per test. Unknown-read no-write tests must inspect all relevant
per-entity keys and active membership.

## 13. Benchmarks and Stress Tests

Benchmarks measure a named mechanism; they should not mix setup work into the measurement.

- Use Criterion for repeatable microbenchmarks.
- Put reusable constructors and measurement helpers in `benches/common.rs`.
- Put async runtime adaptation in `benches/runtime.rs`.
- Keep local benchmark operations feature-independent.
- Pre-seed keys and use long windows when measuring fresh read contention.
- Exclude thread creation and readiness synchronization from timed work.
- Distribute the requested aggregate operation count exactly across workers.
- Use distinct keys and Redis prefixes for independent cases.
- Keep guard-miss, replacement, `PreserveNewest`, and `PreserveOldest` cases separate.
- Do not assert performance thresholds in unit tests.
- Do not commit Criterion reports or other generated benchmark artifacts.

Stress tests may trade determinism for load realism, but their command-line parameters, key
distribution, provider, strategy, duration, and runtime must be visible in output so results are
reproducible.

## 14. Documentation and Markdown

Documentation changes are part of the code change. Update prose, examples, and benchmark
inventories whenever their described behavior changes.

### 14.1 Document ownership

- `docs/lib.md` is the canonical docs.rs source included by `src/lib.rs`.
- `README.md` is the GitHub and crates.io companion.
- Keep their major structure and public behavior aligned.
- Let `docs/lib.md` contain richer Rustdoc links, doctest scaffolding, and feature-aware detail.
- Keep `README.md` readable outside Rustdoc.
- `BENCH.md` owns the benchmark catalog and benchmark-running guidance.
- `CONVENTIONS.md` owns repository-wide code and documentation conventions.
- `AGENTS.md` owns behavioral contracts, agent workflow, and verification obligations.
- `docs/conventions.md` is a compatibility pointer only; do not add new rules there.

### 14.2 Markdown layout

- Use exactly one H1 title.
- Start with a short introduction before the first H2.
- Use ATX headings (`## Heading`), with one blank line before and after.
- Use descriptive, unique headings so generated anchors are meaningful.
- Keep heading depth shallow; do not skip levels.
- Wrap ordinary prose near 100 columns to align with the Rust source width.
- Long links, tables, headings, and code blocks may exceed the prose width.
- Use fenced code blocks and always specify a language such as `rust`, `bash`, `lua`, or `text`.
- Use backticks for identifiers, commands, file names, feature names, and literal values.
- Prefer Markdown to embedded HTML.
- Use informative link text rather than “here” or a bare URL.
- Keep tables compact; use a list when cells contain long prose.
- Do not use trailing whitespace for manual line breaks.

### 14.3 Rustdoc structure

Public item documentation should normally follow this order:

1. One concise summary sentence.
2. Additional behavior, invariants, and caller-relevant concurrency semantics.
3. `# Examples` or an immediately visible fenced example.
4. `# Errors` when a method returns meaningful failures.
5. `# Panics` when caller input can trigger a panic.
6. `# Safety` for unsafe APIs. Trypema currently forbids unsafe code.

Document behavior, units, side effects, no-op cases, and consistency guarantees. Do not restate
types already visible in the signature. Explain implementation details only when they affect
correctness, latency, atomicity, or the caller's mental model.

Examples should:

- Be runnable doctests where practical.
- Show the simplest useful case first.
- Use hidden scaffolding in `docs/lib.md` when it improves readability.
- Show multiple constructors such as `new`, `try_from`, and `new_or_panic` when the choice is
  caller-relevant.
- Show defaults plus one useful override for option and builder types.
- Prefer `?` in fallible examples; use `unwrap` only when the example is explicitly demonstrating
  infallible validated input and surrounding repository conventions already do so.
- Use unique Redis keys and state Redis feature requirements.
- Avoid shortcuts users should not copy into production.

Use Rustdoc intra-doc links for public items available in the active feature context. Do not add a
feature-gated link that breaks no-feature Rustdoc.

### 14.4 Builder and feature documentation

Explain the local-only and Redis-enabled builder forms explicitly:

- Local-only: `RateLimiterBuilder::default()` or `RateLimiter::builder()`.
- Redis-enabled: `RateLimiter::builder(connection_manager)`.

Document the cleanup lifecycle:

- `RateLimiterBuilder::build()` starts cleanup automatically.
- `RateLimiter::new(...)` does not.

In builder examples, put a short comment before optional chained setters. Use `Optional: ...` for
general setters and `Optional: only available with 'redis-tokio' or 'redis-smol'` for Redis-only
setters.

Redis and hybrid APIs require Redis 7.2+ and one runtime feature. Mark feature-gated public items
and examples clearly. Shared prose must not link to a feature-gated Rustdoc item unless the prose
is gated too; use plain inline code when a link cannot be valid in every documented feature
configuration.

Keep public documentation warning-clean under both commands:

```bash
cargo rustdoc -p trypema --no-default-features -- -D warnings
cargo rustdoc -p trypema --features redis-tokio -- -D warnings
```

### 14.5 Comments

- Comments explain intent, invariants, races, units, or non-obvious ordering.
- Do not translate the next line of code into English.
- Keep comments current when code changes.
- Prefer a meaningful identifier or extracted concept to a comment that compensates for a vague
  name.
- TODO comments must state a concrete action and, when available, an issue reference.
- Do not leave commented-out code in source control.

## 15. Cargo Features and Dependencies

- `redis-tokio` and `redis-smol` are mutually exclusive.
- Local APIs compile with neither Redis feature enabled.
- Feature names describe the capability directly and avoid filler prefixes such as `use-` or
  `with-`.
- Keep feature-specific imports and modules behind the narrowest practical `cfg`.
- Do not duplicate an implementation solely to accommodate the two runtimes.
- Add a dependency only when the standard library and existing dependencies do not solve the
  problem cleanly.
- Keep optional dependencies optional and connected to the feature that requires them.
- Review default features before enabling them.
- New public dependencies require extra scrutiny because their types may become part of the
  compatibility surface.
- Do not add `#[inline]`, `#[inline(always)]`, or another inline attribute. Existing attributes
  manually added by the maintainer remain in place unless the maintainer explicitly changes them.

When editing `Cargo.toml`, preserve its established grouping and let `cargo` validate the feature
graph. Never test with both Redis runtime features enabled.

## 16. Change and Review Discipline

- Keep diffs scoped to one coherent purpose.
- Preserve unrelated dirty-worktree changes.
- Do not rename, reorder, or reformat nearby code without a reason tied to the task.
- When a convention is applied across providers, audit all corresponding variants.
- Pair behavior changes with tests and caller-facing changes with documentation.
- Pair Lua changes with raw Redis-state tests.
- Pair benchmark additions or renames with `Cargo.toml`, Makefile, and `BENCH.md` updates.
- State intentional exceptions in the code or review description when a future maintainer might
  otherwise “fix” them.

Before handoff, use the applicable checklist in `AGENTS.md`. At minimum for documentation-only
convention changes, run:

```bash
cargo fmt --all -- --check
git diff --check
```

## 17. Quick Review Checklist

- [ ] Names use established Rust casing and Trypema vocabulary.
- [ ] Absolute values use `window_limit`; suppressed values use `hard_window_limit`.
- [ ] Units are visible in identifiers or types.
- [ ] Values are calculated only when needed and near first use.
- [ ] Public APIs use meaningful types rather than policy booleans.
- [ ] Mutation and exclusive locks are acquired only for concrete writes.
- [ ] Guards and borrowed state do not cross `.await`.
- [ ] Redis scripts use shared preludes and centralized constructors.
- [ ] Lua guard misses perform no writes.
- [ ] Tests arrange state through public APIs and assert their setup.
- [ ] Cross-provider and runtime variants were audited where applicable.
- [ ] Benchmarks isolate the mechanism they claim to measure.
- [ ] Public documentation is current, linked, runnable, and warning-clean.
- [ ] Markdown has one H1, descriptive headings, fenced language-tagged code, and no trailing
      whitespace.
- [ ] Formatting and the relevant verification matrix pass.

## 18. External Basis

This guide adapts, rather than reproduces, established upstream guidance:

- [The Rust Style Guide](https://doc.rust-lang.org/style-guide/) — default formatting and the
  rationale for delegating mechanical style to `rustfmt`.
- [Rust API Guidelines checklist](https://rust-lang.github.io/api-guidelines/checklist.html) —
  naming, interoperability, predictability, type safety, documentation, and future-proofing.
- [Rust API Guidelines: Naming](https://rust-lang.github.io/api-guidelines/naming.html) —
  casing, conversions, accessors, feature names, and consistent word order.
- [The Rustdoc Book: How to write documentation](https://doc.rust-lang.org/rustdoc/how-to-write-documentation.html)
  — concise summaries, complete public documentation, examples, and failure sections.
- [Google Markdown style guide](https://google.github.io/styleguide/docguide/style.html) —
  maintainable Markdown structure, headings, fences, links, and source readability.
- [Google documentation best practices](https://google.github.io/styleguide/docguide/best_practices.html)
  — keep documentation close to code, current, useful, and free of duplicated sources of truth.

These sources are advisory. Trypema's explicit contracts in `AGENTS.md` and this file govern this
repository.
