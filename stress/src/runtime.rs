#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
use std::time::Duration;

// ---------------------------------------------------------------------------
// async_sleep
// ---------------------------------------------------------------------------

#[cfg(feature = "redis-tokio")]
pub(crate) async fn async_sleep(d: Duration) {
    tokio::time::sleep(d).await;
}

#[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
pub(crate) async fn async_sleep(d: Duration) {
    smol::Timer::after(d).await;
}

// ---------------------------------------------------------------------------
// spawn_task / join_task
// ---------------------------------------------------------------------------

#[cfg(feature = "redis-tokio")]
pub(crate) fn spawn_task<F>(f: F) -> tokio::task::JoinHandle<hdrhistogram::Histogram<u64>>
where
    F: std::future::Future<Output = hdrhistogram::Histogram<u64>> + Send + 'static,
{
    tokio::spawn(f)
}

#[cfg(feature = "redis-tokio")]
pub(crate) async fn join_task(
    j: tokio::task::JoinHandle<hdrhistogram::Histogram<u64>>,
) -> hdrhistogram::Histogram<u64> {
    j.await.unwrap()
}

#[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
pub(crate) fn spawn_task<F>(f: F) -> smol::Task<hdrhistogram::Histogram<u64>>
where
    F: std::future::Future<Output = hdrhistogram::Histogram<u64>> + Send + 'static,
{
    smol::spawn(f)
}

#[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
pub(crate) async fn join_task(
    j: smol::Task<hdrhistogram::Histogram<u64>>,
) -> hdrhistogram::Histogram<u64> {
    j.await
}

// ---------------------------------------------------------------------------
// block_on  (used by local.rs when building connection manager without a runtime)
// ---------------------------------------------------------------------------

/// Run a future to completion on a single-threaded executor.
/// Used outside of an existing async context (e.g. building a connection manager
/// from a sync thread).
#[cfg(feature = "redis-tokio")]
pub(crate) fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}

#[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
pub(crate) fn block_on<F: std::future::Future>(f: F) -> F::Output {
    smol::block_on(f)
}
