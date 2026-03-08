//! Async runtime abstraction for criterion benchmarks.
//!
//! Include in a bench file with:
//! ```ignore
//! #[path = "runtime.rs"]
//! mod runtime;
//! ```
//!
//! Mirrors the structure of `src/tests/runtime.rs` and `stress/src/runtime.rs`,
//! adapted for the bench context where:
//! - The runtime handle is built once and reused across all iterations.
//! - `block_on` takes the handle by reference (not owned).
//! - `spawn` / `join` / `async_sleep` are omitted — benchmarks do not need them.

/// The runtime handle used to drive async bench futures.
///
/// - **tokio**: `tokio::runtime::Runtime` (multi-thread, 2 workers)
/// - **smol**: `()` — `smol::block_on` requires no persistent handle
#[cfg(feature = "redis-tokio")]
pub type Runtime = tokio::runtime::Runtime;

#[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
pub type Runtime = ();

/// Build the runtime handle once per bench binary.
#[cfg(feature = "redis-tokio")]
pub fn build() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .unwrap()
}

#[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
pub fn build() -> Runtime {}

/// Drive a future to completion on the given runtime handle.
#[cfg(feature = "redis-tokio")]
pub fn block_on<F, T>(rt: &Runtime, f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    rt.block_on(f)
}

#[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
pub fn block_on<F, T>(_rt: &Runtime, f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    smol::block_on(f)
}
