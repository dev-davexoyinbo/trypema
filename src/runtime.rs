use std::{future::Future, time::Duration};

#[cfg(all(feature = "redis-tokio", not(feature = "redis-smol")))]
pub(crate) type TaskHandle = tokio::task::JoinHandle<()>;

#[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
pub(crate) type TaskHandle = smol::Task<()>;

#[cfg(all(feature = "redis-tokio", not(feature = "redis-smol")))]
pub(crate) type Interval = tokio::time::Interval;

#[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
pub(crate) type Interval = smol::Timer;

#[cfg(all(feature = "redis-tokio", not(feature = "redis-smol")))]
pub(crate) fn new_interval(sync_interval: Duration) -> Interval {
    tokio::time::interval(sync_interval)
}

#[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
pub(crate) fn new_interval(sync_interval: Duration) -> Interval {
    smol::Timer::interval(sync_interval)
}

#[cfg(all(feature = "redis-tokio", not(feature = "redis-smol")))]
pub(crate) fn spawn_task<F>(fut: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(fut);
}

#[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
pub(crate) fn spawn_task<F>(fut: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    smol::spawn(fut).detach();
}

pub(crate) fn spawn_task_handle<F>(fut: F) -> TaskHandle
where
    F: Future<Output = ()> + Send + 'static,
{
    #[cfg(all(feature = "redis-tokio", not(feature = "redis-smol")))]
    {
        tokio::spawn(fut)
    }

    #[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
    {
        smol::spawn(fut)
    }
}

#[cfg(all(feature = "redis-tokio", not(feature = "redis-smol")))]
pub(crate) async fn tick(interval: &mut Interval) {
    interval.tick().await;
}

#[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
pub(crate) async fn tick(interval: &mut Interval) {
    use futures::StreamExt;
    interval.next().await;
}

#[cfg(all(feature = "redis-tokio", not(feature = "redis-smol")))]
pub(crate) async fn sleep(d: Duration) {
    tokio::time::sleep(d).await;
}

#[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
pub(crate) async fn sleep(d: Duration) {
    smol::Timer::after(d).await;
}

#[cfg(all(feature = "redis-tokio", not(feature = "redis-smol")))]
pub(crate) async fn _yield_now() {
    tokio::task::yield_now().await;
}

#[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
pub(crate) async fn _yield_now() {
    smol::future::yield_now().await;
}
