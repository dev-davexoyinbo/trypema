use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use hdrhistogram::Histogram;
use trypema::RateLimitDecision;

use crate::args::{Args, KeyDist, Mode};

// ---------------------------------------------------------------------------
// Counts
// ---------------------------------------------------------------------------

#[derive(Default)]
pub(crate) struct Counts {
    pub(crate) allowed: AtomicU64,
    pub(crate) rejected: AtomicU64,
    pub(crate) suppressed_allowed: AtomicU64,
    pub(crate) suppressed_denied: AtomicU64,
    pub(crate) errors: AtomicU64,
}

// ---------------------------------------------------------------------------
// ErrorStats
// ---------------------------------------------------------------------------

pub(crate) struct ErrorStats {
    by_message: DashMap<String, u64>,
    samples: Mutex<Vec<String>>,
}

impl Default for ErrorStats {
    fn default() -> Self {
        Self {
            by_message: DashMap::new(),
            samples: Mutex::new(Vec::new()),
        }
    }
}

impl ErrorStats {
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    const MAX_SAMPLES: usize = 10;

    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    pub(crate) fn record(&self, message: String, sample: Option<String>) {
        self.by_message
            .entry(message)
            .and_modify(|c| *c = c.saturating_add(1))
            .or_insert(1);

        if let Some(s) = sample {
            let mut guard = self.samples.lock().expect("error samples mutex poisoned");
            if guard.len() < Self::MAX_SAMPLES {
                guard.push(s);
            }
        }
    }

    fn top_by_message(&self, n: usize) -> Vec<(String, u64)> {
        let mut v: Vec<(String, u64)> = self
            .by_message
            .iter()
            .map(|kv| (kv.key().clone(), *kv.value()))
            .collect();
        v.sort_by(|a, b| b.1.cmp(&a.1));
        v.truncate(n);
        v
    }

    fn samples(&self) -> Vec<String> {
        self.samples
            .lock()
            .expect("error samples mutex poisoned")
            .clone()
    }
}

// ---------------------------------------------------------------------------
// Printing
// ---------------------------------------------------------------------------

pub(crate) fn print_results(
    args: &Args,
    elapsed: Duration,
    ops: u64,
    ops_s: f64,
    hist: &Histogram<u64>,
    counts: &Counts,
) {
    println!(
        "provider={:?} strategy={:?} mode={:?}",
        args.provider, args.strategy, args.mode
    );
    println!(
        "threads={} duration_s={} window_s={} group_ms={} key_dist={:?} key_space={}",
        args.threads, args.duration_s, args.window_s, args.group_ms, args.key_dist, args.key_space
    );
    println!(
        "elapsed_s={:.3} ops={} ops_per_s={:.0}",
        elapsed.as_secs_f64(),
        ops,
        ops_s
    );
    println!(
        "allowed={} rejected={} suppressed_allowed={} suppressed_denied={} errors={}",
        counts.allowed.load(Ordering::Relaxed),
        counts.rejected.load(Ordering::Relaxed),
        counts.suppressed_allowed.load(Ordering::Relaxed),
        counts.suppressed_denied.load(Ordering::Relaxed),
        counts.errors.load(Ordering::Relaxed)
    );
    if !hist.is_empty() {
        println!(
            "lat_us p50={} p95={} p99={} p999={} max={}",
            hist.value_at_quantile(0.50),
            hist.value_at_quantile(0.95),
            hist.value_at_quantile(0.99),
            hist.value_at_quantile(0.999),
            hist.max()
        );
        println!("sample_every={} samples={}", args.sample_every, hist.len());
    } else {
        println!("no latency samples collected");
    }
}

pub(crate) fn print_error_stats(errors: u64, stats: &ErrorStats) {
    if errors == 0 {
        return;
    }

    println!("errors_total={errors}");

    let top = stats.top_by_message(10);
    if !top.is_empty() {
        println!("errors_by_message_top10:");
        for (msg, c) in top {
            println!("error_count={c} error=\"{msg}\"");
        }
    }

    let samples = stats.samples();
    if !samples.is_empty() {
        println!("errors_samples_first{}:", samples.len());
        for s in samples {
            println!("error_sample=\"{s}\"");
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub(crate) fn should_sample(iter: u64, sample_every: u64) -> bool {
    if sample_every <= 1 {
        return true;
    }
    iter.is_multiple_of(sample_every)
}

pub(crate) fn qps_for_now(args: &Args, started: Instant) -> Option<u64> {
    if args.mode == Mode::Max {
        return None;
    }
    let base = args.target_qps?;
    if let Some(burst_qps) = args.burst_qps {
        let elapsed_ms = started.elapsed().as_millis() as u64;
        if elapsed_ms % args.burst_period_ms < args.burst_duration_ms {
            return Some(burst_qps);
        }
    }
    Some(base)
}

// ---------------------------------------------------------------------------
// WorkerLoop
// ---------------------------------------------------------------------------

/// Shared per-iteration bookkeeping for every worker thread/task.
///
/// Handles: QPS pacing (sync), key selection, histogram recording, ops counting,
/// decision counting, and error counting.  The caller provides only the
/// limiter-specific call at each iteration.
pub(crate) struct WorkerLoop {
    // config (immutable references owned by the spawning side)
    args: Args,
    keys: Vec<String>,
    started: Instant,
    deadline: Instant,
    // shared state
    pub(crate) counts: Arc<Counts>,
    pub(crate) total_ops: Arc<AtomicU64>,
    #[cfg_attr(
        not(any(feature = "redis-tokio", feature = "redis-smol")),
        allow(dead_code)
    )]
    pub(crate) error_stats: Arc<ErrorStats>,
    // per-worker mutable state
    pub(crate) hist: Histogram<u64>,
    i: u64,
    seed: u64,
    pub(crate) next_deadline: Instant,
}

impl WorkerLoop {
    pub(crate) fn new(
        args: Args,
        keys: Vec<String>,
        counts: Arc<Counts>,
        total_ops: Arc<AtomicU64>,
        error_stats: Arc<ErrorStats>,
        thread_idx: usize,
        started: Instant,
        deadline: Instant,
    ) -> Self {
        Self {
            args,
            keys,
            started,
            deadline,
            counts,
            total_ops,
            error_stats,
            hist: Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap(),
            i: 0,
            seed: (thread_idx as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15),
            next_deadline: Instant::now(),
        }
    }

    /// xorshift64* PRNG — fast, good distribution.
    pub(crate) fn rng(&mut self) -> u64 {
        self.seed ^= self.seed >> 12;
        self.seed ^= self.seed << 25;
        self.seed ^= self.seed >> 27;
        self.seed = self.seed.wrapping_mul(0x2545_F491_4F6C_DD1D);
        self.seed
    }

    /// Returns `true` while the loop should continue running.
    pub(crate) fn should_continue(&self) -> bool {
        Instant::now() < self.deadline
    }

    /// Pick a key index according to the configured distribution.
    pub(crate) fn pick_idx(&mut self) -> usize {
        let len = self.keys.len().max(1);
        match self.args.key_dist {
            KeyDist::Hot => 0,
            KeyDist::Uniform => (self.rng() as usize) % len,
            KeyDist::Skewed => {
                let r = (self.rng() % 10_000) as f64 / 10_000.0;
                if r < self.args.hot_fraction {
                    0
                } else {
                    let tail = len.saturating_sub(1).max(1);
                    1 + ((self.rng() as usize) % tail)
                }
            }
        }
    }

    /// Return a reference to the key at index `idx`.
    pub(crate) fn key(&self, idx: usize) -> &str {
        &self.keys[idx % self.keys.len().max(1)]
    }

    /// Advance the iteration counter and optionally start a latency sample.
    pub(crate) fn begin_iter(&mut self) -> Option<Instant> {
        self.i = self.i.wrapping_add(1);
        if should_sample(self.i, self.args.sample_every) {
            Some(Instant::now())
        } else {
            None
        }
    }

    /// Finish an iteration: record latency sample and increment total_ops.
    pub(crate) fn end_iter(&mut self, t0: Option<Instant>) {
        if let Some(t0) = t0 {
            let us = t0.elapsed().as_micros() as u64;
            let _ = self.hist.record(us.max(1));
        }
        self.total_ops.fetch_add(1, Ordering::Relaxed);
    }

    /// Synchronous QPS pacing — for sync (threaded) runners.
    pub(crate) fn pace_sync(&mut self) {
        if self.args.mode == Mode::Max {
            return;
        }
        if let Some(qps) = qps_for_now(&self.args, self.started) {
            let per_op_ns = 1_000_000_000u64 / qps.max(1);
            let now = Instant::now();
            if now < self.next_deadline {
                std::thread::sleep(self.next_deadline - now);
            }
            self.next_deadline += Duration::from_nanos(per_op_ns);
        }
    }

    /// Record a `RateLimitDecision` into the shared counts.
    pub(crate) fn record_decision(&self, decision: RateLimitDecision) {
        match decision {
            RateLimitDecision::Allowed => {
                self.counts.allowed.fetch_add(1, Ordering::Relaxed);
            }
            RateLimitDecision::Rejected { .. } => {
                self.counts.rejected.fetch_add(1, Ordering::Relaxed);
            }
            RateLimitDecision::Suppressed { is_allowed, .. } => {
                if is_allowed {
                    self.counts
                        .suppressed_allowed
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    self.counts
                        .suppressed_denied
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// Record a simple allowed/rejected bool (for Cell, Gcra, etc.).
    pub(crate) fn record_allowed(&self, allowed: bool) {
        if allowed {
            self.counts.allowed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counts.rejected.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record an error into both the error count and the error stats map.
    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    pub(crate) fn record_error(&self, message: String, sample: Option<String>) {
        self.counts.errors.fetch_add(1, Ordering::Relaxed);
        self.error_stats.record(message, sample);
    }

    /// Consume the loop and return the latency histogram.
    pub(crate) fn into_hist(self) -> Histogram<u64> {
        self.hist
    }
}
