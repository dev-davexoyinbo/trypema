# Trypema Contributor and Agent Guide

This file applies to the entire repository. It records the behavioral, concurrency, testing,
benchmarking, and workflow rules that must be preserved when changing Trypema.

Read `CONVENTIONS.md` alongside this file. `AGENTS.md` owns behavioral contracts, workflow, and
verification requirements; `CONVENTIONS.md` is the canonical guide for code organization,
naming, API shape, source style, tests, benchmarks, Lua, and documentation.

The words **must**, **must not**, **should**, and **should not** are intentional. If current code
appears to disagree with this guide, first inspect the most recent commits and tests. Do not
silently weaken a test or change a contract merely to make the current implementation pass.

## 1. Start by Establishing Ownership and Scope

This repository is often developed with a dirty worktree while the maintainer makes nearby
manual commits. Before editing anything:

```bash
git status --short
git log -8 --oneline --stat
git diff --check
```

Follow these rules:

- Treat recent maintainer commits as authoritative context. Do not amend, reorder, squash,
  revert, or rewrite them unless explicitly asked.
- Treat pre-existing uncommitted changes as work that must be preserved. Do not reset, restore,
  or overwrite unrelated changes.
- Keep your own edits within the requested scope. If a file already has uncommitted edits,
  inspect the complete diff before patching overlapping code.
- Do not commit benchmark output, Criterion reports, Redis data, generated artifacts, or
  `target/` contents.
- Do not create commits, branches, or pull requests unless explicitly requested.
- When a behavior changes across providers or strategies, audit every corresponding variant;
  do not assume the local implementation is the only consumer of the contract.

## 2. Repository Shape and Feature Model

Trypema has two rate-limiting strategies across three providers:

| Provider | Absolute | Suppressed | Availability |
| --- | --- | --- | --- |
| Local | `src/local/absolute_local_rate_limiter.rs` | `src/local/suppressed_local_rate_limiter.rs` | Always compiled |
| Redis | `src/redis/absolute_redis_rate_limiter.rs` | `src/redis/suppressed_redis_rate_limiter.rs` | Requires one Redis runtime feature |
| Hybrid | `src/hybrid/absolute_hybrid_rate_limiter.rs` | `src/hybrid/suppressed_hybrid_rate_limiter.rs` | Requires one Redis runtime feature |

Supporting areas:

- Shared public types and comparator/history enums: `src/common.rs`
- Redis Lua scripts: `src/redis/scripts.rs`
- Shared Redis types and Rust helpers: `src/redis/common.rs`
- Hybrid Redis proxies and commit coordination: `src/hybrid/`
- Unit/integration tests: `src/tests/`
- Criterion benchmarks: `benches/`
- Stress harness: `stress/`
- Canonical docs.rs content: `docs/lib.md`
- Code and documentation conventions: `CONVENTIONS.md`
- Benchmark documentation: `BENCH.md`

Feature rules:

- The local rate-limiter API is always available, with or without Redis features.
- `redis-tokio` and `redis-smol` are mutually exclusive. Never enable both in one command.
- Redis and hybrid code must work under both supported runtimes.
- Redis-backed functionality requires Redis 7.2+.
- Do not scatter Redis feature `cfg` blocks through local implementation or local benchmark
  logic. Isolate feature-dependent construction in the existing shared helpers.

The crate uses Rust 2024, denies missing public documentation, and forbids unsafe code. New
public items therefore require useful Rustdoc and must remain warning-clean in every applicable
feature configuration.

## 3. Core Sliding-Window Semantics

### 3.1 Capacity and sticky limits

- Window capacity is calculated as `*window_size * rate_limit`, converted to `u64` using
  the existing truncating behavior.
- Per-key limiter state stores the computed per-window limit, not the raw per-second
  [`RateLimit`]. Absolute code names this value `window_limit`; suppressed code names it
  `hard_window_limit` because it includes the configured `hard_limit_factor`.
- `inc` stores the computed window limit when a key is first created. Later `inc` calls for the
  same key do not redefine that stored limit.
- A matched conditional set recomputes and redefines the stored window limit from the supplied
  rate limit.
- Increments within `bucket_size` of the newest bucket are coalesced into that bucket.
- A bucket remains live while its age is within the configured window. Expiration and boundary
  comparisons must stay consistent across all operations.
- Absolute admission is best-effort under concurrency. Concurrent callers may all observe an
  allowed state and temporarily overshoot. Do not accidentally promise strict cross-thread
  atomicity for `inc`.

Naming and initialization are part of this contract:

- Use `window_limit` for absolute fields, parameters, locals, Lua values, test helpers, and
  benchmark values.
- Use `hard_window_limit` for the corresponding suppressed values. Use `soft_window_limit` for
  the derived suppression threshold. Do not shorten either one to an ambiguous `limit`.
- Shared physical Redis key APIs such as `get_window_limit_key`, `window_limit_key`, and the `w`
  suffix may keep the generic name because both strategies use the same storage slot. The value
  read from that key must still use the strategy-specific logical name.
- Calculate a requested window limit at the latest point where it is actually needed. In
  particular, do not calculate it before a comparator guard can return, before a missing-key path
  has decided to insert, or at the start of a method when only one later branch uses it.
- Prefer direct field access for one-use reads, such as
  `total_count < series.window_limit as u64` or
  `total_count >= series.hard_window_limit as u64`. Do not introduce a separate local merely to rename
  a directly accessible field.
- A local copy is appropriate when the value is used more than once, is derived further, is read
  through a mutex, must survive dropping a DashMap guard, or materially improves a multi-step
  calculation. Keep that local immediately before its first use.
- Put missing-key calculations inside `or_insert_with` so they are not performed when a racing
  thread has already inserted the key.
- The internal timestamped history type is `Bucket`, and the deque in `RateLimitSeries` is
  `buckets`. Bind `buckets.back()` as `latest_bucket` and `buckets.front()` as `oldest_bucket`.
  Use `bucket` when a branch dynamically chooses between the front and back. Do not reintroduce
  `InstantRate`, `instant_rate`, `last_bucket`, or ambiguous element names.
- Internal count fields and locals use a descriptive `_count` suffix. The increment argument and
  `Bucket::count` remain exactly `count`. Existing public fields, including suppressed snapshot
  `total` and `total_declined`, remain source-compatible.
- Always name a per-key series reference or DashMap guard `series`. Shadow it when transitioning
  between shared, mutable, entry, and downgraded guards rather than introducing
  `rate_limit_series`, `series_guard`, or `series_entry`.
- Name `RateLimitDecision` values `decision`; reserve `is_*` and `should_*` locals for booleans.
- Keep the public `WindowSize`, `BucketSize`, `SuppressionFactorCachePeriod`, and `SyncInterval`
  types with their unit-neutral configuration field names. Raw internal time values retain unit
  suffixes. Internal state for a concrete bucket uses `_bucket_`, such as `oldest_bucket_ttl` and
  `oldest_bucket_count`.

### 3.2 Public read behavior

For `get`:

- Absolute providers return the live total as `u64`; unknown keys return `0`.
- Suppressed providers return `SuppressedRateLimitSnapshot` with `total`, `total_declined`, and
  `suppression_factor`; unknown keys return a zero-valued snapshot.
- Unknown reads must not insert limiter state.
- The returned value includes only live buckets.
- Suppressed `total` includes accepted and declined usage, while `total_declined` includes only
  declined usage and must never exceed `total`.
- Redis suppressed reads return all three snapshot fields from one atomic script. Hybrid
  suppressed reads overlay this instance's pending local counts and declines on Redis state.
- Expired buckets may be lazily evicted. The caller-visible operation is a read, but internal
  expiration maintenance is allowed and expected when necessary.
- A fresh-key read must use compatible shared DashMap access. It must not unconditionally take
  a mutable entry guard.
- If the oldest bucket is expired, the implementation may drop the shared guard, acquire
  exclusive access, re-check the entry, and evict stale buckets.
- Do not write tests that require expired-key `get` calls to complete while another shared guard
  is deliberately held. Expiration is precisely the case where exclusive access can be needed.

For `is_allowed`:

- Unknown keys are allowed without insertion.
- Fresh under-limit checks stay on the shared fast path.
- Expiration maintenance is lazy and may require exclusive access.
- Rejected decisions expose best-effort metadata:
  - `retry_after` is the remaining `Duration` until the oldest live bucket expires, not the bucket's
    elapsed age.
  - `remaining_after_waiting` is the capacity released when that oldest bucket expires: the
    amount the caller can send after `retry_after`. For a full window with buckets containing
    `3` then `7`, this value is `3`, not `7` and not the `7` count units still in the window.
  - If grouping merged all usage into the oldest bucket, `remaining_after_waiting` is the full
    merged bucket count.

For `get_suppression_factor`:

- Unknown keys return `0.0` without creating per-entity data, a suppression-cache key, or active
  entity membership.
- Expiration that removes history affecting a cached factor must invalidate that cache before the
  factor is reused or recomputed.
- When the soft and hard limits are identical, reaching that boundary means full suppression;
  local, Redis, and freshly initialized hybrid state must all report `1.0` consistently.

### 3.3 Increment behavior

- Always establish admission before recording the requested count.
- Rejected increments do not consume capacity.
- If an existing newest bucket is inside the grouping interval, its atomic count and the atomic
  total may be updated while holding a shared map guard. This does not structurally mutate the
  DashMap entry.
- Appending a new bucket is a structural mutation and requires exclusive entry access.
- When a `get` misses and the operation must create the key before continuing with immutable or
  atomic access, use `entry(...).or_insert_with(...).downgrade()` and return that immutable guard
  directly. Do not insert and then perform a second `get`.
- Code must tolerate cleanup or removal racing between lookup and insertion. Do not use
  `unreachable!` or an unconditional `expect` for a map entry that another thread can legitimately
  remove.

## 4. DashMap Locking Is a Performance Contract

DashMap locking discipline is a first-class project requirement, not a micro-optimization.
`get_mut`, `entry`, `remove`, `retain`, and similar operations can acquire exclusive shard or
entry access and increase contention on hot keys.

### 4.1 Shared-first rule

For every operation:

1. Inspect existing state through `DashMap::get` whenever possible.
2. Complete shared-only fast paths under that guard.
3. Return immediately when no structural or ordinary-field mutation is needed.
4. If mutation is required, explicitly drop the shared guard.
5. Acquire mutable/entry access only then.
6. Recompute and revalidate any condition that may have changed before mutating.

The typical shape is:

```rust,ignore
let Some(series) = map.get(key) else {
    // Insert only if the operation actually needs a new key.
    return ...;
};

if shared_fast_path_applies(&series) {
    return ...;
}

drop(series);

let Some(mut series) = map.get_mut(key) else {
    // The key may have been removed concurrently; handle this normally.
    return ...;
};

// Re-check before applying structural mutation.
```

All internal DashMaps use `common::RandomState`, including rate-limit series, caches, hybrid
state, and per-key lock maps. Do not select a concrete hasher independently at each map site.

### 4.2 Missing-key insertion must downgrade directly

There is a recurring pattern where an operation wants an immutable guard regardless of whether
the key already existed. This includes the common `series.get(...)` pattern: try the shared lookup
first and, only when it misses, use `entry(...).or_insert_with(...)`. Do not discard the guard
returned by `or_insert_with` and then call `get` again. Downgrade that guard and use it as the
immutable reference. The canonical implementation is:

```rust,ignore
let series = match map.get(key) {
    Some(series) => series,
    None => map
        .entry(key.to_string())
        .or_insert_with(|| {
            RateLimitSeries::new(**rate_limit * *window_size as f64)
        })
        .downgrade(),
};
```

This rule is important for both locking and correctness:

- `get` keeps the existing-key path shared.
- `entry` is reached only after the shared lookup proves the key is missing.
- `or_insert_with` safely handles another thread inserting between `get` and `entry`.
- `downgrade` converts the returned mutable entry guard into the immutable guard the rest of the
  path needs.
- The value returned by the `match` is therefore an immutable guard on both branches; callers
  should continue directly with that guard instead of performing another lookup.
- Returning the downgraded guard avoids a second hash lookup and a second shard-lock acquisition.
- It also avoids a race where cleanup removes the newly inserted entry before a follow-up `get`,
  which previously encouraged invalid `expect` or `unreachable!` assumptions.

Do not write this pattern:

```rust,ignore
if map.get(key).is_none() {
    map.entry(key.to_string())
        .or_insert_with(|| {
            RateLimitSeries::new(**rate_limit * *window_size as f64)
        });
}

let series = map
    .get(key)
    .expect("key should still be present");
```

The second form performs redundant lookups and is incorrect under concurrent cleanup/removal.
This prohibition applies even when the follow-up `get` currently uses `expect`, `unwrap`, a
conditional fallback, or an early return: the intervening removal race still exists.
If the code truly needs to continue structurally mutating the entry immediately, retaining the
mutable entry guard can be appropriate. Otherwise, downgrade as soon as insertion is complete.

### 4.3 What does and does not need exclusive access

Usually compatible with a shared DashMap guard:

- Reading the stored window limit, history, timestamps, or atomic totals.
- Loading atomic bucket counts or declined counts.
- Atomic increments/decrements when the bucket structure itself is unchanged.
- Comparator evaluation.
- Fresh-key `get` and most fresh `is_allowed` paths.

Requires exclusive access:

- Inserting a missing key.
- Appending, popping, draining, clearing, or otherwise restructuring a bucket deque.
- Removing expired buckets.
- Changing a non-atomic stored window limit.
- Removing a key after a matched zero-target conditional set.
- Invalidating or replacing state that is not independently synchronized.

Important details:

- `DashMap::entry` is not a harmless read. Use it only for actual insertion or mutation.
- Do not call `get_mut` merely because mutation might become necessary later.
- After dropping a shared guard and acquiring exclusive access, re-read ordinary fields such as a
  stored window limit; another operation may have replaced them between guards.
- Do not hold a DashMap guard across `.await`.
- Do not hold a mutable DashMap guard while performing Redis I/O.
- Do not acquire another operation's per-key lock while retaining a mutable DashMap guard unless
  the lock order is explicitly established and tested.
- Local expiration cleanup is permitted after shared inspection discovers stale history. This
  does not justify unconditional mutable access for fresh entries.
- Atomic loads use `Ordering::Acquire`, atomic stores use `Ordering::Release`, and atomic
  read-modify-write operations use `Ordering::AcqRel`. Do not introduce `Relaxed` or `SeqCst`.
- Do not add an `#[inline(...)]` attribute. Existing maintainer-added inline attributes remain
  untouched unless the maintainer explicitly changes them.

### 4.4 Required lock audit

After changes to local or hybrid state handling, review every write-locking operation:

```bash
rg -n '\.(get_mut|entry|remove|remove_if|retain)\(' src benches
```

For each result, be able to identify the actual mutation it protects. If there is no concrete
mutation, replace it with shared access or move it behind the condition that requires mutation.

## 5. Conditional-Set Contract

All six limiter variants expose replacement conditional sets, and all six must expose matching
history-preserving conditional sets. Keep the existing sync/async shapes and return contract:

```rust,ignore
set_if(key, rate_limit, comparator, count) -> (new_total, old_total)

set_if_preserve_history(
    key,
    rate_limit,
    comparator,
    count,
    preservation: HistoryPreservation,
) -> (new_total, old_total)
```

Redis and hybrid variants return these values inside their existing `Result`/async APIs.

### 5.1 Comparator evaluation

- Evaluate comparators against the logical live total; expired buckets do not count.
- A comparator miss does not apply the requested count or rate limit.
- Missing keys are compared as total `0`.
- Missing positive targets create state only when the comparator matches `0`.
- Missing zero targets remain absent.
- Local live-total evaluation may perform necessary lazy eviction before the comparator result is
  known. Do not confuse this housekeeping with applying a comparator miss.
- Redis Lua can calculate the logical live total without pruning first; a Redis comparator miss
  must not change history, totals, TTLs, active-entity metadata, limits, or suppression caches.
- When a local implementation releases shared access and later takes exclusive access, re-check
  the comparator and live state under the exclusive guard before applying the requested update.

### 5.2 Matched replacement

When `set_if` matches:

- Replace live history with one current-timestamp bucket holding `count`.
- Recompute and redefine the stored window limit from the supplied `rate_limit`.
- Return `(count, old_live_total)`.
- A matched target of `0` removes the key completely. Do not leave an empty series behind.
- For Redis, delete the entity's history, ordering sets, totals, limit/TTL state, suppression
  metadata where applicable, and active-entity membership as required by the data model.
- For hybrid, clear/invalidate both committed and pending representations without losing
  increments that race after the protected snapshot.

### 5.3 History-preserving updates

The public enum is exactly:

```rust
pub enum HistoryPreservation {
    PreserveNewest,
    PreserveOldest,
}
```

Do not reintroduce names such as `RateLimitHistoryPreservation`, `PreserveLast`, or
`PreserveEarly`.

`PreserveNewest`:

- Retains the newest history.
- Reductions consume buckets from the oldest/front edge.
- Partial reductions subtract from the oldest boundary bucket.
- Increases add the delta to the newest/back edge.

`PreserveOldest`:

- Retains the oldest history.
- Reductions consume buckets from the newest/back edge.
- Partial reductions subtract from the newest boundary bucket.
- Increases add the delta to the oldest/front edge.

For either direction:

- Prune expired history before applying a matched update.
- Remove fully consumed buckets. Never retain zero-count boundary buckets.
- If no retained bucket exists and the target is positive, create a current-timestamp bucket.
- A matched target of `0` removes the key/state completely.
- A matched call redefines the stored window limit even when the requested total equals the old
  total.
- If total, live history, stored window limit, and all related metadata are already correct, avoid
  needless mutable access and cache invalidation.

### 5.4 Suppressed-history accounting

Suppressed variants track both observed count and declined count. Preserve these invariants:

- `declined <= count` per bucket and in aggregate.
- When a partial reduction retains part of a suppressed bucket, calculate the retained declined
  value with integer truncation:

  ```text
  retained_declined = floor(old_declined * retained_count / old_count)
  ```

- Additions represent accepted usage and add no declines.
- Exact totals and aggregate declined counters must match the retained bucket history.
- Invalidate suppression-factor caches only when state affecting the cached result changed.
- A comparator miss must not invalidate a valid suppression cache.
- A matched reset to zero removes count, decline, history, and suppression metadata.

## 6. Redis Lua Rules

Conditional updates must remain atomic within Redis. All rate-limiter Lua scripts belong in
`src/redis/scripts.rs`; provider and proxy files should contain only Rust orchestration. Reuse a
single script constant when Redis and hybrid providers use the same script and data model.

Lua requirements:

- Put cross-strategy helpers in the common Lua prelude, and strategy-specific helpers in the
  absolute or suppressed prelude in `src/redis/scripts.rs`. Construct scripts through
  `lua_script`, `absolute_lua_script`, or `suppressed_lua_script`; do not manually concatenate
  helper strings in providers or proxies.
- Call `now_ms()` for Redis server time. Do not duplicate `redis.call("TIME")` conversion inside
  individual script bodies.
- Reuse the shared expiry, grouping, comparator, history-mode, and cleanup helpers instead of
  copying those state-transition fragments into individual bodies.
- Use Redis server time, not client time, for bucket scores and TTL-sensitive behavior.
- Compute expired contributions and the logical live total before deciding whether the comparator
  matches.
- Perform no writes before a possible comparator-miss return.
- Accept an explicit history mode: replace, preserve newest, or preserve oldest.
- Support optional hybrid pending-count overlays; suppressed scripts also overlay pending
  declined counts.
- Materialize pending hybrid counts as newest logical history only on a successful match.
- Keep hashes and sorted-set ordering consistent.
- Keep exact total and declined-total keys consistent with history.
- Refresh limit TTL and active-entity metadata only when required by a successful state change.
- Remove all entity state for a successful zero target.
- Use distinct Redis prefixes in tests and benchmarks to prevent cross-case contamination.

Whenever a Lua script changes, add or update raw Redis-state tests. Public return values alone are
not enough. Inspect, as applicable:

- History hashes.
- Declined-history hashes.
- Sorted-set members, scores, and ordering.
- Exact total keys.
- Exact declined-total keys.
- Stored window limits.
- TTLs.
- Suppression-factor cache keys.
- Active-entity membership.

When a test claims an unknown Redis read wrote no state, checking only the total and history hash
is insufficient. Check every per-entity key relevant to that strategy plus active-entity
membership.

## 7. Hybrid Coordination Rules

Hybrid conditional sets combine committed Redis history with local pending state. Preserve these
rules:

- Snapshot pending local count through immutable state access.
- Treat the snapshot as a newest virtual bucket during the atomic Redis comparison.
- Leave pending local state untouched when the comparator misses.
- On a match, mutate/remove local state only after the Redis operation succeeds.
- Preserve increments that arrive after the snapshot. Commit them on top of the requested target
  rather than dropping them.
- Coordinate flushing, resetting, and conditional setting with the existing per-key lock.
- Gather candidates before awaiting; never hold DashMap guards across Redis awaits.
- Keep Redis and local invalidation ordering explicit and test failure paths.
- Test both a pending snapshot and an increment that races after that snapshot.

## 8. Test Construction Rules

Tests are regression specifications. They should fail when the implementation violates the
contract; do not write assertions broad enough to accommodate a known bug.

### 8.1 Use the public API to create state

This is non-negotiable for limiter behavior tests:

- Seed usage with `inc`, `set_if`, or `set_if_preserve_history`.
- Observe behavior with `get`, `is_allowed`, admission decisions, and conditional-set returns.
- Use real grouping waits to create multiple buckets.
- Use real window passage to create expired history.
- Never add an `insert_series` helper.
- Never insert fabricated `RateLimitSeries`, `Bucket`, timestamps, totals, or declined counts
  directly into the DashMap merely to arrange a scenario.

Test-only accessors may be used narrowly to:

- Hold a real shared DashMap guard for a lock-contention regression.
- Inspect post-operation bucket order, counts, totals, limits, or key absence.

They must not be used to bypass the public API when constructing initial state.

Minimize test-only helpers in production impl blocks:

- One or two test-only methods may stay beside the production type with individual `#[cfg(test)]`
  attributes.
- For three or more test-only methods, put an inherent impl block in the appropriate
  `src/tests/test_<production_file_name>.rs` file.
- Fields required by that test-area impl may be `pub(crate)`, but must not become public API.

### 8.2 Make setup prove itself

- Assert that each setup `inc` is allowed when the test depends on the increment being recorded.
- Do not ignore a setup decision and later assume its count exists.
- Use a rate limit large enough for every setup increment unless rejection is the setup's purpose.
- Verify exact accepted/rejected operation counts and volumes when deterministic.
- Verify rejected increments do not change `get`.
- Verify unknown-key `get` returns zero without inserting.
- Verify a matched zero target removes an existing key, for both replacement and preservation.
- Verify a missing zero target leaves the key absent.
- For directional preservation, inspect bucket order and partial boundary counts after creating the
  buckets through public calls.

### 8.3 Time and retry assertions

- Prefer deterministic behavioral assertions over exact wall-clock values.
- Give scheduler jitter a reasonable margin without making the assertion meaningless.
- Express numeric timing bounds with direct comparisons:

  ```rust
  assert!(retry_after >= Duration::from_millis(min_ms)
      && retry_after <= Duration::from_millis(max_ms));
  ```

- Do not use range containment for these assertions.
- Keep waits as short as the public configuration allows. A one-second window is usually enough
  for expiry tests.
- For separate buckets, sleep beyond `bucket_size` while staying comfortably inside the
  window.
- For mixed expired/fresh history, create both buckets first, then wait until only the oldest has
  crossed the window.
- Assert `remaining_after_waiting` independently from `retry_after` so one failure does not
  obscure the other semantic.

### 8.4 Lock regression tests

For fresh local reads:

1. Create a fresh key through `inc`.
2. Hold a DashMap shared guard in the test thread.
3. Synchronize a worker with a barrier.
4. Call `get` in the worker.
5. Require completion within two seconds while the original shared guard is still held.
6. Drop the guard before joining after a timeout so a regression cannot strand the test process.

An unconditional `get_mut` will block and fail this test. Do not apply this pattern to an expired
key, because legitimate eviction may require exclusive access.

Concurrency tests must use public operations when racing cleanup, reads, increments, or resets.
They must not hide races by directly replacing map entries.

### 8.5 Redis and hybrid tests

- Generate a unique Redis prefix per test with the shared test helpers.
- Run behavioral tests and raw-state tests.
- Cover both Tokio and Smol.
- Do not use fixed Redis keys that can collide across parallel tests unless the prefix guarantees
  isolation.
- Test guard misses, matched changes, zero removal, TTL behavior, history direction, suppression
  accounting, pending hybrid increments, and post-snapshot races.
- Use the runtime abstraction in `src/tests/runtime.rs`; do not duplicate Tokio-only test logic.

## 9. Criterion Benchmark Rules

Criterion is the performance regression signal. Do not add brittle performance assertions to unit
tests.

### 9.1 Benchmark organization

- Local benchmark binaries are `benches/local_absolute.rs` and
  `benches/local_suppressed.rs`.
- Redis benchmark binaries are `benches/redis_absolute.rs` and
  `benches/redis_suppressed.rs`.
- Hybrid benchmark binaries are `benches/hybrid_absolute.rs` and
  `benches/hybrid_suppressed.rs`.
- Shared construction belongs in `benches/common.rs`.
- Async runtime adaptation belongs in `benches/runtime.rs`.
- Keep feature-dependent Redis construction in `benches/common.rs`; local benchmark operation code
  should not be littered with `cfg` branches.
- The local API must benchmark successfully with no Redis feature, `redis-tokio`, or `redis-smol`.

### 9.2 Required contention cases

Both local strategies should retain `get` hot-key groups for:

- Single-thread fresh-key reads.
- Same-key reads with 2 threads.
- Same-key reads with 8 threads.
- Same-key reads with 16 threads.

Use a long window and a pre-seeded fresh entry so the timed loop measures shared-read contention,
not expiration, insertion, or eviction.

Use `measure_parallel` from `benches/common.rs`:

- Distribute exactly the requested aggregate iteration count across workers.
- Assign the remainder deterministically.
- Synchronize readiness and timed release with barriers.
- Exclude thread creation and worker setup from measured time.

Do not replace it with a helper that accidentally performs `iterations * thread_count`
operations or times thread creation.

### 9.3 Conditional-set cases

All applicable suites should compare:

- Comparator guard miss.
- Matched replacement.
- Matched `PreserveNewest` adjustment.
- Matched `PreserveOldest` adjustment.

Alternate increase and decrease targets so matched cases exercise real history changes. Keep
distinct keys for guard-miss, replacement, and each preservation direction. Redis and hybrid
suites must use distinct prefixes.

When adding or renaming benchmark cases:

- Update `Cargo.toml` bench entries if a binary is new.
- Update Makefile targets for both Tokio and Smol where applicable.
- Update `BENCH.md`.
- Do not commit `target/criterion` results.

## 10. Documentation and Public API Rules

Follow `CONVENTIONS.md`.

- `docs/lib.md` is the canonical docs.rs source.
- `README.md` is the GitHub/crates companion.
- Keep them structurally aligned when public behavior changes.
- Prefer Rustdoc intra-doc links in `docs/lib.md` when valid under the active feature set.
- Avoid feature-gated links that break no-feature Rustdoc.
- Public docs should describe caller-visible behavior accurately, including sticky `inc` limits,
  matched conditional-set limit replacement, zero-target deletion, and best-effort concurrency.
- Do not describe `get` as never mutating internal storage. Say it returns live state and may
  perform lazy eviction. Keep the absolute `u64` and suppressed snapshot return contracts clear.
- Re-export new public common types from `src/lib.rs`.
- Keep sync local examples and async Redis/hybrid examples consistent with their APIs.

Required Rustdoc checks when documentation or public APIs change:

```bash
cargo rustdoc -p trypema --no-default-features -- -D warnings
cargo rustdoc -p trypema --features redis-tokio -- -D warnings
```

## 11. Verification Matrix

Run checks in proportion to the change, but semantic changes spanning providers require the full
matrix.

### 11.1 Formatting and local checks

```bash
cargo fmt --all -- --check
cargo test -p trypema --no-default-features
cargo test -p trypema --doc --no-default-features
git diff --check
```

For timing- or lock-sensitive absolute-local work, also run the focused suite serially:

```bash
cargo test -p trypema --no-default-features \
  tests::test_absolute_local_rate_limiter -- --test-threads=1
```

### 11.2 Redis Tokio and Smol

The Makefile can manage a local Redis instance:

```bash
make redis-up
REDIS_URL=redis://127.0.0.1:16379/ \
  cargo test -p trypema --features redis-tokio
REDIS_URL=redis://127.0.0.1:16379/ \
  cargo test -p trypema --doc --features redis-tokio
REDIS_URL=redis://127.0.0.1:16379/ \
  cargo test -p trypema --features redis-smol
REDIS_URL=redis://127.0.0.1:16379/ \
  cargo test -p trypema --doc --features redis-smol
make redis-down
```

Equivalent Makefile entry points include:

```bash
make test-redis-tokio
make test-redis-smol
make test-redis
```

Never combine `redis-tokio` and `redis-smol` in one feature list.

### 11.3 Benchmarks

At minimum, compile or run every touched benchmark binary. Examples:

```bash
cargo bench -p trypema --no-default-features --bench local_absolute
cargo bench -p trypema --no-default-features --bench local_suppressed

REDIS_URL=redis://127.0.0.1:16379/ \
  cargo bench -p trypema --features redis-tokio --bench redis_absolute
REDIS_URL=redis://127.0.0.1:16379/ \
  cargo bench -p trypema --features redis-tokio --bench redis_suppressed
REDIS_URL=redis://127.0.0.1:16379/ \
  cargo bench -p trypema --features redis-tokio --bench hybrid_absolute
REDIS_URL=redis://127.0.0.1:16379/ \
  cargo bench -p trypema --features redis-tokio --bench hybrid_suppressed
```

Use the corresponding Smol feature when validating Smol. The complete repository entry point is:

```bash
make sanity
```

Criterion runs can be filtered during development, but do not treat a filtered run as full
verification of a cross-provider change.

## 12. Final Review Checklist

Before handing work back, confirm all applicable items:

- [ ] Recent maintainer commits and unrelated uncommitted changes are untouched.
- [ ] Local API code remains feature-independent.
- [ ] Tokio and Smol remain mutually exclusive and individually supported.
- [ ] Fresh DashMap paths use shared access first.
- [ ] Missing-key `entry(...).or_insert_with(...)` paths call `downgrade()` when subsequent access
      is immutable or atomic, with no second `get`.
- [ ] Every `get_mut`, `entry`, `remove`, and `retain` protects a concrete mutation.
- [ ] Shared guards are dropped before exclusive access or `.await`.
- [ ] Expired buckets are excluded from logical totals and evicted when appropriate.
- [ ] Absolute `get` returns a `u64`; suppressed `get` returns a snapshot whose total, declined
      total, and suppression factor all reflect live state and provider-specific pending overlays.
- [ ] `retry_after` is remaining time, not elapsed age.
- [ ] `remaining_after_waiting` is the oldest live bucket count: the capacity available after
      `retry_after`, not the count that remains in the window.
- [ ] Conditional-set comparator semantics use the live total.
- [ ] Absolute values use `window_limit`; suppressed values use `hard_window_limit`; neither uses
      an ambiguous `limit` alias.
- [ ] Window-limit calculations and locals are initialized only where needed; one-use field reads
      access the field directly.
- [ ] Matched conditional sets recompute and redefine the stored window limit.
- [ ] Zero targets remove present state and do not create missing state.
- [ ] `PreserveNewest` and `PreserveOldest` operate from the correct edges.
- [ ] Suppressed decline ratios and totals remain consistent.
- [ ] Redis guard misses perform no writes.
- [ ] Hybrid post-snapshot increments cannot be lost.
- [ ] Tests seed state only through public APIs.
- [ ] Setup operations and exact postconditions are asserted.
- [ ] Timing bounds use explicit comparisons, not range containment.
- [ ] Local contention benchmarks retain 1/2/8/16-reader coverage.
- [ ] Benchmark construction and feature handling remain centralized.
- [ ] Documentation and benchmark docs match public behavior.
- [ ] Formatting, relevant test matrices, doctests, and diff checks were run.
- [ ] No benchmark artifacts or unrelated files were added.

## 13. Common Failure Modes to Avoid

- Replacing every `get` with `get_mut` for convenience.
- Calling `entry` before knowing insertion or mutation is required.
- Inserting through `entry` and then calling `get` again instead of using the returned guard's
  `downgrade()` method.
- Assuming a newly inserted key must still exist after releasing its guard while cleanup can run.
- Holding a shared guard and then trying to acquire a mutable guard without dropping it.
- Assuming a read-only public method can never perform expiration maintenance.
- Requiring expired-key reads to remain compatible with an artificially held shared guard.
- Returning elapsed bucket age as `retry_after`.
- Treating the newest bucket count as `remaining_after_waiting`.
- Computing `remaining_after_waiting` as `total_count - oldest_bucket_count`.
- Ignoring setup `inc` decisions and accidentally testing a rejected increment.
- Fabricating buckets or timestamps through private test helpers.
- Leaving an empty map entry after a successful zero-target conditional set.
- Reversing the preservation directions.
- Leaving zero-count buckets after a full boundary reduction.
- Resetting suppressed counts without resetting declined metadata.
- Invalidating Redis TTLs or caches on comparator misses.
- Claiming an unknown Redis read is non-creating while checking only a subset of its possible keys.
- Reusing a cached suppression factor after the history that produced it was evicted.
- Losing hybrid increments that arrive after a conditional-set snapshot.
- Holding DashMap guards across Redis awaits.
- Adding Redis feature gates throughout local benchmark code.
- Measuring thread creation in hot-key contention benchmarks.
- Giving each worker the full Criterion iteration count.
- Using the same Redis prefix across benchmark suites.
- Enabling both Redis runtime features at once.
- Weakening a correct regression test to match a buggy implementation.
