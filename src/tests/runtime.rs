use std::{future::Future, time::Duration};

#[cfg(feature = "redis-tokio")]
pub(super) fn block_on<F, T>(f: F) -> T
where
    F: Future<Output = T>,
{
    tokio::runtime::Runtime::new().unwrap().block_on(f)
}

#[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
pub(super) fn block_on<F, T>(f: F) -> T
where
    F: Future<Output = T>,
{
    smol::block_on(f)
}

#[cfg(feature = "redis-tokio")]
pub(super) async fn async_sleep(d: Duration) {
    tokio::time::sleep(d).await;
}

#[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
pub(super) async fn async_sleep(d: Duration) {
    smol::Timer::after(d).await;
}
