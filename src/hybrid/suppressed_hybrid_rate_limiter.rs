use std::{
    ops::Deref,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use dashmap::{DashMap, mapref::entry::Entry, mapref::one::Ref};
use tokio::sync::{mpsc, watch};

use crate::{
    HardLimitFactor, HistoryPreservation, RateLimit, RateLimitComparator, RateLimitDecision,
    RedisKey, RedisRateLimiterOptions, SuppressedRateLimitSnapshot, SuppressionFactorCachePeriod,
    TrypemaError, WindowSize,
    common::{HistoryUpdateMode, RandomState},
    hybrid::{
        AbsoluteHybridCommitterSignal, RedisCommitter, RedisCommitterOptions, RedisProxyCommitter,
        SuppressedHybridCommit, SuppressedHybridPendingState, SuppressedHybridRedisProxy,
        SuppressedHybridRedisProxyOptions, SuppressedHybridRedisProxyReadStateResult,
        common::{EPOCH_CHANGE_INTERVAL, RedisRateLimiterSignal},
    },
    redis::{mutex_lock, spawn_task},
    runtime,
};

mod helpers;

#[derive(Debug)]
enum SuppressedRedisLimitingState {
    Accepting {
        hard_window_limit: Mutex<f64>,
        starting_count: Mutex<u64>,
        count: AtomicU64,
        declined_count: Mutex<u64>,
        last_modified: Mutex<Instant>,
    },
    Undefined,
    Suppressing {
        time_instant: Mutex<Instant>,
        hard_window_limit: Mutex<f64>,
        suppression_factor: Mutex<f64>,
        suppression_factor_ttl_ms: Mutex<u64>,
        starting_count: Mutex<u64>,
        starting_declined_count: Mutex<u64>,
        count: AtomicU64,
        declined_count: AtomicU64,
    },
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct SuppressedHybridSetIfTestHook {
    pub snapshot_taken: tokio::sync::Notify,
    pub resume: tokio::sync::Notify,
}

/// Probabilistic suppression rate limiter with a local fast-path and periodic Redis sync.
///
/// This is the hybrid counterpart to [`SuppressedRedisRateLimiter`](crate::redis::SuppressedRedisRateLimiter).
/// Like the absolute hybrid limiter, it maintains local in-memory state per key and
/// periodically flushes accumulated increments (both observed and declined) to Redis.
///
/// # State Machine
///
/// Each key transitions through three states:
///
/// - **`Undefined`** — No local state exists. The first call reads Redis to initialise.
///
/// - **`Accepting`** — Below capacity. All requests are allowed from local state with
///   no Redis I/O. When the local counter exceeds the soft capacity, it commits to Redis
///   and transitions based on the result.
///
/// - **`Suppressing`** — At or above capacity. Probabilistic admission based on the
///   suppression factor from the last Redis read. Continues until the local state is
///   flushed and refreshed from Redis.
///
/// # Thundering Herd Prevention
///
/// Same per-key `tokio::sync::Mutex` mechanism as the absolute hybrid limiter.
#[derive(Debug)]
pub struct SuppressedHybridRateLimiter {
    window_size: WindowSize,
    commiter_sender: mpsc::Sender<AbsoluteHybridCommitterSignal<SuppressedHybridCommit>>,
    redis_proxy: SuppressedHybridRedisProxy,
    limiting_state: DashMap<RedisKey, SuppressedRedisLimitingState, RandomState>,
    hard_limit_factor: HardLimitFactor,
    suppression_factor_cache_period: SuppressionFactorCachePeriod,
    epoch: AtomicU64,
    last_commited_epoch: AtomicU64,
    is_active_watch: watch::Sender<u64>,
    reset_locks: DashMap<RedisKey, Arc<tokio::sync::Mutex<()>>, RandomState>,
    #[cfg(test)]
    set_if_test_hook: Mutex<Option<Arc<SuppressedHybridSetIfTestHook>>>,
}

impl SuppressedHybridRateLimiter {
    pub(crate) fn new(options: RedisRateLimiterOptions) -> Arc<Self> {
        let prefix = options.prefix.unwrap_or_else(RedisKey::default_prefix);

        let (tx, rx) = tokio::sync::mpsc::channel::<RedisRateLimiterSignal>(1);
        let hard_limit_factor = options.hard_limit_factor;
        let suppression_factor_cache_period = options.suppression_factor_cache_period;
        let window_size = options.window_size;
        let bucket_size = options.bucket_size;

        let redis_proxy = SuppressedHybridRedisProxy::new(SuppressedHybridRedisProxyOptions {
            prefix: prefix.clone(),
            connection_manager: options.connection_manager,
            hard_limit_factor,
            suppression_factor_cache_period,
            bucket_size,
            window_size,
        });

        let is_active_watch = watch::Sender::new(0u64);

        let sync_interval = Duration::from_millis(*options.sync_interval);

        let commiter_sender = RedisCommitter::run(RedisCommitterOptions {
            sync_interval,
            channel_capacity: 8192,
            max_batch_size: 4,
            limiter_sender: tx,
            redis_proxy: Box::new(redis_proxy.clone()),
            is_active_watch: is_active_watch.subscribe(),
        });

        let limiter = Self {
            window_size,
            commiter_sender,
            redis_proxy,
            limiting_state: DashMap::default(),
            hard_limit_factor,
            suppression_factor_cache_period,
            is_active_watch,
            epoch: AtomicU64::new(0),
            last_commited_epoch: AtomicU64::new(0),
            reset_locks: DashMap::default(),
            #[cfg(test)]
            set_if_test_hook: Mutex::new(None),
        };

        let limiter = Arc::new(limiter);

        limiter.listen_for_committer_signals(rx);
        limiter.epoch_change_task();

        limiter
    } // end constructor

    fn listen_for_committer_signals(
        self: &Arc<Self>,
        mut rx: mpsc::Receiver<RedisRateLimiterSignal>,
    ) {
        let limitter = Arc::downgrade(self);

        spawn_task(async move {
            while let Some(signal) = rx.recv().await {
                let Some(limiter) = limitter.upgrade() else {
                    break;
                };

                match signal {
                    RedisRateLimiterSignal::Flush => {
                        if let Err(err) = limiter.flush().await {
                            tracing::error!(error = ?err, "Failed to flush redis rate limiter");
                        }
                    }
                }
            }
        });
    } // end fn listen_for_committer_signals

    fn epoch_change_task(self: &Arc<Self>) {
        self.epoch.fetch_add(1, Ordering::AcqRel);
        let limiter = Arc::downgrade(self);

        spawn_task(async move {
            loop {
                runtime::sleep(EPOCH_CHANGE_INTERVAL).await;
                let Some(limiter) = limiter.upgrade() else {
                    break;
                };
                limiter.epoch.fetch_add(1, Ordering::AcqRel);
            }
        });
    } // end fn epoch_change_task

    #[inline(always)]
    fn send_epoch_change_if_needed(&self) {
        let epoch = self.epoch.load(Ordering::Acquire);

        if self.last_commited_epoch.load(Ordering::Acquire) < epoch {
            let _ = self.is_active_watch.send(epoch);
            self.last_commited_epoch.store(epoch, Ordering::Release);
        }
    } // end fn send_epoch_change_if_needed

    /// Acquire (or create) the per-key reset lock and return an `Arc` to it.
    fn get_or_create_reset_lock(&self, key: &RedisKey) -> Arc<tokio::sync::Mutex<()>> {
        if let Some(lock) = self.reset_locks.get(key) {
            return Arc::clone(&lock);
        }

        self.reset_locks
            .entry(key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .downgrade()
            .clone()
    }

    fn get_or_create_limiting_state(
        &self,
        key: &RedisKey,
    ) -> Ref<'_, RedisKey, SuppressedRedisLimitingState> {
        match self.limiting_state.get(key) {
            Some(state) => state,
            None => self
                .limiting_state
                .entry(key.clone())
                .or_insert_with(|| SuppressedRedisLimitingState::Undefined)
                .downgrade(),
        }
    }

    /// Get the current suppression factor for `key`.
    ///
    /// Returns a value in the range `[0.0, 1.0]`:
    /// - `0.0` — no suppression (below capacity or key not found)
    /// - `0.0 < sf < 1.0` — partial suppression (at capacity)
    /// - `1.0` — full suppression (cached at the hard boundary or on a forecast above it)
    ///
    /// On the fast-path (key in `Suppressing` state with a fresh cached factor), this is
    /// served entirely from local state with no Redis I/O. Otherwise, it performs a Redis
    /// read to refresh the state. Unknown keys remain absent, and Redis history eviction
    /// invalidates a factor cached from the expired history.
    ///
    /// This method is read-only with respect to request counts. It is useful for metrics
    /// and observability dashboards.
    ///
    /// # Examples
    ///
    /// ```
    /// # trypema::__doctest_helpers::with_redis_rate_limiter(|rl| async move {
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from(trypema::__doctest_helpers::unique_key()).unwrap();
    /// // No state yet → 0.0 (no suppression)
    /// assert_eq!(rl.hybrid().suppressed().get_suppression_factor(&key).await.unwrap(), 0.0);
    /// # });
    /// ```
    pub async fn get_suppression_factor(&self, key: &RedisKey) -> Result<f64, TrypemaError> {
        self.send_epoch_change_if_needed();

        if let Some(state) = self.limiting_state.get(key)
            && let Some(suppression_factor) = self.local_suppression_factor(state.deref())?
        {
            return Ok(suppression_factor);
        }

        let lock = self.get_or_create_reset_lock(key);
        let _guard = lock.lock().await;

        if let Some(state) = self.limiting_state.get(key)
            && let Some(suppression_factor) = self.local_suppression_factor(state.deref())?
        {
            return Ok(suppression_factor);
        }

        let mut rng = |p: f64| rand::random_bool(p);
        let read_state_result = self.redis_proxy.read_state(key).await?;
        let decision = self
            .reset_single_state_from_read_result(read_state_result, 0, 0, None, &mut rng)
            .await?;

        if let Some(state) = self.limiting_state.get(key)
            && let Some(suppression_factor) = self.local_suppression_factor(state.deref())?
        {
            return Ok(suppression_factor);
        }

        match decision {
            RateLimitDecision::Allowed => Ok(0f64),
            RateLimitDecision::Rejected { .. } => Err(TrypemaError::CustomError(
                "suppressed hybrid returned an absolute rejection decision".to_string(),
            )),
            RateLimitDecision::Suppressed {
                suppression_factor, ..
            } => Ok(suppression_factor),
        }
    } // end method get_suppression_factor

    async fn set_if_with_history_mode(
        &self,
        key: &RedisKey,
        rate_limit: &RateLimit,
        comparator: RateLimitComparator,
        count: u64,
        mode: HistoryUpdateMode,
    ) -> Result<(u64, u64), TrypemaError> {
        self.send_epoch_change_if_needed();

        let lock = self.get_or_create_reset_lock(key);
        let _guard = lock.lock().await;

        let pending_state = self.snapshot_pending_state(key);

        #[cfg(test)]
        {
            let hook = mutex_lock(&self.set_if_test_hook, "set_if_test_hook")?.take();
            if let Some(hook) = hook {
                hook.snapshot_taken.notify_one();
                hook.resume.notified().await;
            }
        }

        let hard_window_limit = *self.window_size as f64 * **rate_limit * *self.hard_limit_factor;

        let (new_total, old_total, changed) = self
            .redis_proxy
            .set_if(
                key,
                hard_window_limit,
                comparator,
                count,
                mode,
                pending_state,
            )
            .await?;

        if !changed {
            return Ok((new_total, old_total));
        }

        self.reconcile_post_set_increments(key, hard_window_limit, pending_state)
            .await?;

        Ok((new_total, old_total))
    } // end fn set_if_with_history_mode

    /// Infer current window state from this instance's local limiting state.
    ///
    /// An initialized state stays on local fast path. Undefined or expired suppression state is
    /// refreshed from Redis first. Result can lag commits from other instances and Redis-side
    /// expiration; use [`Self::get`] when every read must consult Redis.
    ///
    /// # Errors
    ///
    /// Returns an error when required Redis or locally cached state cannot be read.
    ///
    /// # Examples
    ///
    /// ```
    /// # trypema::__doctest_helpers::with_redis_rate_limiter(|rl| async move {
    /// use trypema::RateLimit;
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from(trypema::__doctest_helpers::unique_key()).unwrap();
    /// assert_eq!(
    ///     rl.hybrid().suppressed().get_inferred(&key).await.unwrap().total,
    ///     0
    /// );
    ///
    /// let rate = RateLimit::per_second(10.0).unwrap();
    /// rl.hybrid().suppressed().inc(&key, &rate, 3).await.unwrap();
    /// assert_eq!(
    ///     rl.hybrid().suppressed().get_inferred(&key).await.unwrap().total,
    ///     3
    /// );
    /// # });
    /// ```
    pub async fn get_inferred(
        &self,
        key: &RedisKey,
    ) -> Result<SuppressedRateLimitSnapshot, TrypemaError> {
        // this triggers the state to be evaluated and cached
        self.get_suppression_factor(key).await?;

        let Some(state) = self.limiting_state.get(key) else {
            return Ok(SuppressedRateLimitSnapshot::default());
        };

        Ok(self.local_snapshot(state.deref())?.unwrap_or_default())
    } // end method get_inferred

    /// Current live window state for `key` (best-effort).
    ///
    /// Returns Redis state after lazily evicting expired buckets, overlaid with this instance's
    /// pending local increments and declines. The observed total includes accepted and declined
    /// calls, matching the counter that suppression decisions are based on. Pending increments
    /// held by other instances are not visible until their next flush, so the result may lag the
    /// true global state by up to `sync_interval`.
    ///
    /// # Examples
    ///
    /// ```
    /// # trypema::__doctest_helpers::with_redis_rate_limiter(|rl| async move {
    /// use trypema::RateLimit;
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from(trypema::__doctest_helpers::unique_key()).unwrap();
    /// let snapshot = rl.hybrid().suppressed().get(&key).await.unwrap();
    /// assert_eq!(snapshot.total, 0);
    /// assert_eq!(snapshot.total_declined, 0);
    /// assert_eq!(snapshot.suppression_factor, 0.0);
    ///
    /// let rate = RateLimit::per_second(10.0).unwrap();
    /// rl.hybrid().suppressed().inc(&key, &rate, 3).await.unwrap();
    /// assert_eq!(rl.hybrid().suppressed().get(&key).await.unwrap().total, 3);
    /// # });
    /// ```
    pub async fn get(&self, key: &RedisKey) -> Result<SuppressedRateLimitSnapshot, TrypemaError> {
        self.send_epoch_change_if_needed();

        let lock = self.get_or_create_reset_lock(key);
        let _guard = lock.lock().await;

        let read_state_result = self.redis_proxy.read_state(key).await?;

        let mut total = read_state_result.current_total_count;
        let mut total_declined = read_state_result.current_total_declined;
        let mut suppression_factor = read_state_result.suppression_factor;

        if let Some(state_entry) = self.limiting_state.get(key) {
            if let Some(local_suppression_factor) =
                self.local_suppression_factor(state_entry.deref())?
            {
                suppression_factor = suppression_factor.max(local_suppression_factor);
            }

            match state_entry.deref() {
                SuppressedRedisLimitingState::Accepting { count, .. } => {
                    total = total.saturating_add(count.load(Ordering::Acquire));
                }
                SuppressedRedisLimitingState::Suppressing {
                    count,
                    declined_count,
                    ..
                } => {
                    total = total.saturating_add(count.load(Ordering::Acquire));
                    total_declined = total_declined
                        .saturating_add(declined_count.load(Ordering::Acquire))
                        .min(total);
                }
                SuppressedRedisLimitingState::Undefined => {}
            }
        }

        Ok(SuppressedRateLimitSnapshot {
            total,
            total_declined,
            suppression_factor,
        })
    } // end method get

    /// Conditionally replace the window total for `key` (atomic on Redis).
    ///
    /// When `comparator` matches the key's current post-eviction window total, the
    /// window contents are replaced by a single current-timestamp bucket holding
    /// `count`, with **no declines** recorded against it; otherwise nothing is
    /// written. On a match the key's hard window limit is (re)defined as
    /// `window_size × rate_limit × hard_limit_factor` and the cached
    /// suppression factor is deleted so it is recomputed from the new state.
    /// A matched target of zero removes committed count, decline, limit, cache, and
    /// history state; increments racing after the pending snapshot are retained.
    ///
    /// This instance's pending local increments and declines are included in the
    /// atomic comparison as a newest virtual bucket. Local state remains untouched
    /// on a miss. After a match it is invalidated, and increments that arrived after
    /// the snapshot are committed on top of the requested target. Pending increments
    /// on **other** instances flush
    /// later (within `sync_interval`) and land on top of a matched write — with a
    /// `Lt(count)` guard the total can drift above `count` but is never lowered below
    /// it by those flushes.
    ///
    /// # Arguments
    ///
    /// - `key`: Validated [`RedisKey`] identifying the rate-limited resource
    /// - `rate_limit`: Per-second rate limit used to (re)define the hard window limit
    /// - `comparator`: Guard evaluated against the current window total
    /// - `count`: The total to write when the guard matches
    ///
    /// # Returns
    ///
    /// `(new_total, old_total)` — `old_total` is the post-eviction total the
    /// comparator was evaluated against; `new_total` is `count` when the guard
    /// matched, `old_total` otherwise.
    ///
    /// # Priming Idiom
    ///
    /// `set_if(key, rate, RateLimitComparator::Lt(count), count)` raises the window
    /// total to at least `count` and never lowers it — idempotent and safe to retry,
    /// e.g. for seeding a quota window from an external usage store.
    ///
    /// # Examples
    ///
    /// ```
    /// # trypema::__doctest_helpers::with_redis_rate_limiter(|rl| async move {
    /// use trypema::{RateLimit, RateLimitComparator};
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from(trypema::__doctest_helpers::unique_key()).unwrap();
    /// let rate = RateLimit::per_second(10.0).unwrap();
    ///
    /// // Prime the window to 40.
    /// let (new_total, old_total) = rl
    ///     .hybrid()
    ///     .suppressed()
    ///     .set_if(&key, &rate, RateLimitComparator::Lt(40), 40)
    ///     .await
    ///     .unwrap();
    /// assert_eq!((new_total, old_total), (40, 0));
    ///
    /// // Re-priming is a no-op: the guard no longer matches.
    /// let (new_total, old_total) = rl
    ///     .hybrid()
    ///     .suppressed()
    ///     .set_if(&key, &rate, RateLimitComparator::Lt(40), 40)
    ///     .await
    ///     .unwrap();
    /// assert_eq!((new_total, old_total), (40, 40));
    /// # });
    /// ```
    pub async fn set_if(
        &self,
        key: &RedisKey,
        rate_limit: &RateLimit,
        comparator: RateLimitComparator,
        count: u64,
    ) -> Result<(u64, u64), TrypemaError> {
        self.set_if_with_history_mode(
            key,
            rate_limit,
            comparator,
            count,
            HistoryUpdateMode::Replace,
        )
        .await
    } // end method set_if

    /// Conditionally set the observed total while retaining the selected side of
    /// the shared Redis sliding-window history.
    pub async fn set_if_preserve_history(
        &self,
        key: &RedisKey,
        rate_limit: &RateLimit,
        comparator: RateLimitComparator,
        count: u64,
        preservation: HistoryPreservation,
    ) -> Result<(u64, u64), TrypemaError> {
        self.set_if_with_history_mode(
            key,
            rate_limit,
            comparator,
            count,
            HistoryUpdateMode::Preserve(preservation),
        )
        .await
    } // end method set_if_preserve_history

    /// Check admission and record the increment for `key` using probabilistic suppression.
    ///
    /// On the fast-path (key in `Accepting` state with capacity remaining), this is a
    /// single atomic increment with no Redis I/O. When local capacity is exhausted or the
    /// key enters the `Suppressing` state, probabilistic admission is applied based on the
    /// suppression factor from the last Redis read.
    ///
    /// # Arguments
    ///
    /// - `key`: Validated [`RedisKey`] identifying the rate-limited resource
    /// - `rate_limit`: Per-second rate limit (sticky — stored on first call per key)
    /// - `count`: Amount to increment (typically `1`)
    ///
    /// # Returns
    ///
    /// - `Ok(Allowed)` — the increment remains within soft capacity or reaches the hard limit
    ///   exactly
    /// - `Ok(Suppressed { is_allowed, suppression_factor })` — the forecasted accepted total is
    ///   above soft capacity without landing exactly on the hard limit; check `is_allowed`
    /// - `Err(TrypemaError)` — Redis error during state refresh
    ///
    /// The total observed counter is always incremented. If the increment reaches the hard limit
    /// exactly, it is admitted and local full suppression is cached for subsequent calls. If
    /// `is_allowed` is `false`, the declined counter is also incremented.
    ///
    /// # Examples
    ///
    /// ```
    /// # trypema::__doctest_helpers::with_redis_rate_limiter(|rl| async move {
    /// use trypema::{RateLimit, RateLimitDecision};
    /// use trypema::redis::RedisKey;
    ///
    /// let key = RedisKey::try_from(trypema::__doctest_helpers::unique_key()).unwrap();
    /// let rate = RateLimit::per_second(10.0).unwrap();
    /// // Under limit → Allowed
    /// assert!(matches!(
    ///     rl.hybrid().suppressed().inc(&key, &rate, 1).await.unwrap(),
    ///     RateLimitDecision::Allowed
    /// ));
    /// # });
    /// ```
    pub async fn inc(
        &self,
        key: &RedisKey,
        rate_limit: &RateLimit,
        count: u64,
    ) -> Result<RateLimitDecision, TrypemaError> {
        let mut rng = |p: f64| rand::random_bool(p);

        let decision = self
            .inc_with_rng(key, count, Some(rate_limit), &mut rng)
            .await?;

        Ok(decision)
    } // end method inc

    /// Remove stale Redis state and local state stale since its suppression cache expired.
    pub(crate) async fn cleanup(&self, stale_after_ms: u64) -> Result<(), TrypemaError> {
        self.redis_proxy.cleanup(stale_after_ms).await?;

        let candidates = self
            .limiting_state
            .iter()
            .filter(|state| Self::should_cleanup_local_state(state.value(), stale_after_ms))
            .map(|state| state.key().clone())
            .collect::<Vec<_>>();

        for key in candidates {
            let lock = self.get_or_create_reset_lock(&key);
            let _guard = lock.lock().await;

            let should_remove = self.limiting_state.get(&key).is_some_and(|state| {
                Self::should_cleanup_local_state(state.value(), stale_after_ms)
            });

            if !should_remove {
                continue;
            }

            match self.limiting_state.entry(key) {
                Entry::Occupied(entry)
                    if Self::should_cleanup_local_state(entry.get(), stale_after_ms) =>
                {
                    entry.remove();
                }
                Entry::Occupied(_) | Entry::Vacant(_) => {}
            }
        }

        Ok(())
    } // end fn cleanup

    async fn flush(&self) -> Result<(), TrypemaError> {
        let (mut resets, stale) = self.collect_flush_candidates();

        let mut guards = Vec::with_capacity(resets.len() + stale.len());
        for key in resets.iter().chain(&stale) {
            guards.push(self.get_or_create_reset_lock(key).lock_owned().await);
        }

        for key in stale {
            self.reset_stale_local_state(&key)?;
        }

        resets.retain(|key| {
            self.limiting_state
                .get(key)
                .is_some_and(|state| Self::has_pending_counts(state.value()))
        });

        if resets.is_empty() {
            return Ok(());
        }

        let read_state_results = self.redis_proxy.batch_read_state(&resets).await?;

        let mut rng = |p: f64| rand::random_bool(p);

        for result in read_state_results {
            if let Err(err) = self
                .reset_single_state_from_read_result(result, 0, 0, None, &mut rng)
                .await
            {
                tracing::error!(error = ?err, "Failed to reset state from redis read result");
                continue;
            }
        }

        Ok(())
    } // end fn flush
} // end impl
