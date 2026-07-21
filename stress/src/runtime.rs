#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
use std::time::Duration;

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
type WorkerHistogram = hdrhistogram::Histogram<u64>;

#[cfg(feature = "redis-tokio")]
pub(crate) fn name() -> &'static str {
    "tokio"
} // end fn name

#[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
pub(crate) fn name() -> &'static str {
    "smol"
} // end fn name

#[cfg(not(any(feature = "redis-tokio", feature = "redis-smol")))]
pub(crate) fn name() -> &'static str {
    "none"
} // end fn name

#[cfg(feature = "redis-tokio")]
pub(crate) async fn async_sleep(duration: Duration) {
    tokio::time::sleep(duration).await;
} // end fn async_sleep

#[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
pub(crate) async fn async_sleep(duration: Duration) {
    smol::Timer::after(duration).await;
} // end fn async_sleep

#[cfg(feature = "redis-tokio")]
pub(crate) async fn yield_now() {
    tokio::task::yield_now().await;
} // end fn yield_now

#[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
pub(crate) async fn yield_now() {
    smol::future::yield_now().await;
} // end fn yield_now

#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
pub(crate) async fn create_redis_connection_manager(
    redis_url: &str,
) -> Result<redis::aio::ConnectionManager, redis::RedisError> {
    let client = redis::Client::open(redis_url)?;
    client.get_connection_manager().await
} // end fn create_redis_connection_manager

#[cfg(feature = "redis-tokio")]
pub(crate) fn spawn_task<Future>(future: Future) -> tokio::task::JoinHandle<WorkerHistogram>
where
    Future: std::future::Future<Output = WorkerHistogram> + Send + 'static,
{
    tokio::spawn(future)
} // end fn spawn_task

#[cfg(feature = "redis-tokio")]
pub(crate) async fn join_task(
    join_handle: tokio::task::JoinHandle<WorkerHistogram>,
) -> WorkerHistogram {
    join_handle.await.unwrap()
} // end fn join_task

#[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
pub(crate) fn spawn_task<Future>(future: Future) -> smol::Task<WorkerHistogram>
where
    Future: std::future::Future<Output = WorkerHistogram> + Send + 'static,
{
    smol::spawn(future)
} // end fn spawn_task

#[cfg(all(feature = "redis-smol", not(feature = "redis-tokio")))]
pub(crate) async fn join_task(join_handle: smol::Task<WorkerHistogram>) -> WorkerHistogram {
    join_handle.await
} // end fn join_task
