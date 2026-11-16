//! Bounded waits for conditions that background threads and tasks make true.
//!
//! A fixed sleep fails when the scheduler delays the work past it. These waits poll the condition
//! instead and fail after `WAIT_BOUND`, so a condition that never holds, such as a leaked strong
//! reference, still fails the test.

use std::time::{Duration, Instant};

pub(super) const WAIT_BOUND: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(5);

/// Wait on this thread until `condition` holds, or fail with `message` after `WAIT_BOUND`.
pub(super) fn wait_until(mut condition: impl FnMut() -> bool, message: &str) {
    let deadline = Instant::now() + WAIT_BOUND;
    while !condition() {
        assert!(Instant::now() < deadline, "{message} within {WAIT_BOUND:?}");
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// Wait on the runtime until `condition` holds, or fail with `message` after `WAIT_BOUND`.
#[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
pub(super) async fn wait_until_async(mut condition: impl AsyncFnMut() -> bool, message: &str) {
    let deadline = Instant::now() + WAIT_BOUND;
    while !condition().await {
        assert!(Instant::now() < deadline, "{message} within {WAIT_BOUND:?}");
        super::runtime::async_sleep(POLL_INTERVAL).await;
    }
}
