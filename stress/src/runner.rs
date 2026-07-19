use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use hdrhistogram::Histogram;
use trypema::RateLimitDecision;

use crate::args::{Args, KeyDist, Mode, Provider, Strategy};

const MAX_ERROR_SAMPLES: usize = 10;
const MAX_RECORDED_LATENCY_US: u64 = 60_000_000;

#[derive(Default)]
pub(crate) struct Counts {
    allowed_count: AtomicU64,
    rejected_count: AtomicU64,
    suppressed_allowed_count: AtomicU64,
    suppressed_denied_count: AtomicU64,
    error_count: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct CountSnapshot {
    allowed_count: u64,
    rejected_count: u64,
    suppressed_allowed_count: u64,
    suppressed_denied_count: u64,
    error_count: u64,
}

impl CountSnapshot {
    fn accounted_count(self) -> u64 {
        self.allowed_count
            .saturating_add(self.rejected_count)
            .saturating_add(self.suppressed_allowed_count)
            .saturating_add(self.suppressed_denied_count)
            .saturating_add(self.error_count)
    } // end fn accounted_count

    fn accepted_count(self) -> u64 {
        self.allowed_count
            .saturating_add(self.suppressed_allowed_count)
    } // end fn accepted_count

    fn denied_count(self) -> u64 {
        self.rejected_count
            .saturating_add(self.suppressed_denied_count)
    } // end fn denied_count
} // end impl

impl Counts {
    fn snapshot(&self) -> CountSnapshot {
        CountSnapshot {
            allowed_count: self.allowed_count.load(Ordering::Acquire),
            rejected_count: self.rejected_count.load(Ordering::Acquire),
            suppressed_allowed_count: self.suppressed_allowed_count.load(Ordering::Acquire),
            suppressed_denied_count: self.suppressed_denied_count.load(Ordering::Acquire),
            error_count: self.error_count.load(Ordering::Acquire),
        }
    } // end fn snapshot
} // end impl

fn validate_count_snapshot(total_count: u64, counts: CountSnapshot) -> Result<(), String> {
    let accounted_count = counts.accounted_count();
    if accounted_count != total_count {
        return Err(format!(
            "completed operation count {total_count} does not match classified outcome count {accounted_count}"
        ));
    }
    if counts.error_count > 0 {
        return Err(format!(
            "{} limiter operation(s) returned errors",
            counts.error_count
        ));
    }
    Ok(())
} // end fn validate_count_snapshot

#[derive(Default)]
pub(crate) struct ErrorStats {
    by_message: Mutex<HashMap<String, u64>>,
    samples: Mutex<Vec<String>>,
}

impl ErrorStats {
    fn record(&self, message: String, sample: Option<String>) {
        let mut by_message = self
            .by_message
            .lock()
            .expect("error message stats mutex poisoned");
        let message_count = by_message.entry(message).or_default();
        *message_count = message_count.saturating_add(1);
        drop(by_message);

        if let Some(sample) = sample {
            let mut samples = self.samples.lock().expect("error samples mutex poisoned");
            if samples.len() < MAX_ERROR_SAMPLES {
                samples.push(sample);
            }
        }
    } // end fn record

    fn top_by_message(&self, limit: usize) -> Vec<(String, u64)> {
        let mut messages: Vec<_> = self
            .by_message
            .lock()
            .expect("error message stats mutex poisoned")
            .iter()
            .map(|(message, count)| (message.clone(), *count))
            .collect();
        messages.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        messages.truncate(limit);
        messages
    } // end fn top_by_message

    fn samples(&self) -> Vec<String> {
        self.samples
            .lock()
            .expect("error samples mutex poisoned")
            .clone()
    } // end fn samples
} // end impl

#[derive(Default)]
struct TargetPacer {
    next_slot: Mutex<Option<Instant>>,
}

impl TargetPacer {
    fn reserve_at(&self, args: &Args, started: Instant, now: Instant) -> Option<Instant> {
        let qps = qps_for_elapsed(args, now.saturating_duration_since(started))?;
        let interval = Duration::from_secs_f64(1.0 / qps as f64);
        let mut next_slot = self.next_slot.lock().expect("target pacer mutex poisoned");
        let slot = next_slot.unwrap_or(now).max(now);
        *next_slot = Some(slot + interval);
        Some(slot)
    } // end fn reserve_at

    fn delay(&self, args: &Args, started: Instant) -> Option<Duration> {
        let now = Instant::now();
        self.reserve_at(args, started, now)
            .and_then(|slot| slot.checked_duration_since(now))
    } // end fn delay
} // end impl

#[derive(Clone, Default)]
pub(crate) struct RunState {
    counts: Arc<Counts>,
    total_count: Arc<AtomicU64>,
    error_stats: Arc<ErrorStats>,
    pacer: Arc<TargetPacer>,
}

impl RunState {
    #[cfg(test)]
    pub(crate) fn error_count(&self) -> u64 {
        self.counts.error_count.load(Ordering::Acquire)
    } // end method error_count
} // end impl

pub(crate) struct StartGate {
    worker_count: usize,
    ready_count: AtomicUsize,
    started: OnceLock<Instant>,
}

impl StartGate {
    pub(crate) fn new(worker_count: usize) -> Self {
        assert!(worker_count > 0, "start gate requires at least one worker");
        Self {
            worker_count,
            ready_count: AtomicUsize::new(0),
            started: OnceLock::new(),
        }
    } // end constructor

    fn open(&self) -> Instant {
        let started = Instant::now();
        self.started
            .set(started)
            .expect("stress start gate must be opened exactly once");
        started
    } // end fn open

    pub(crate) fn start_sync(&self) -> Instant {
        while self.ready_count.load(Ordering::Acquire) < self.worker_count {
            std::thread::yield_now();
        }
        self.open()
    } // end method start_sync

    pub(crate) fn wait_sync(&self) -> Instant {
        self.ready_count.fetch_add(1, Ordering::AcqRel);
        loop {
            if let Some(started) = self.started.get() {
                return *started;
            }
            std::thread::yield_now();
        }
    } // end method wait_sync

    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    pub(crate) async fn start_async(&self) -> Instant {
        while self.ready_count.load(Ordering::Acquire) < self.worker_count {
            crate::runtime::yield_now().await;
        }
        self.open()
    } // end method start_async

    #[cfg(any(feature = "redis-tokio", feature = "redis-smol"))]
    pub(crate) async fn wait_async(&self) -> Instant {
        self.ready_count.fetch_add(1, Ordering::AcqRel);
        loop {
            if let Some(started) = self.started.get() {
                return *started;
            }
            crate::runtime::yield_now().await;
        }
    } // end method wait_async
} // end impl

pub(crate) fn should_sample(iteration: u64, sample_every: u64) -> bool {
    iteration.is_multiple_of(sample_every)
} // end fn should_sample

pub(crate) fn qps_for_elapsed(args: &Args, elapsed: Duration) -> Option<u64> {
    if args.mode == Mode::Max {
        return None;
    }

    if let Some(burst_qps) = args.burst_qps {
        let elapsed_ms = elapsed.as_millis() as u64;
        if elapsed_ms % args.burst_period_ms < args.burst_duration_ms {
            return Some(burst_qps);
        }
    }

    args.target_qps
} // end fn qps_for_elapsed

/// Shared per-iteration bookkeeping for every worker thread or task.
pub(crate) struct WorkerLoop {
    args: Arc<Args>,
    key_count: usize,
    started: Instant,
    deadline: Instant,
    state: RunState,
    histogram: Histogram<u64>,
    iteration: u64,
    seed: u64,
}

impl WorkerLoop {
    pub(crate) fn new(
        args: Arc<Args>,
        key_count: usize,
        state: RunState,
        worker_index: usize,
        started: Instant,
    ) -> Self {
        Self {
            deadline: started + Duration::from_secs(args.duration_s),
            args,
            key_count,
            started,
            state,
            histogram: Histogram::new_with_bounds(1, MAX_RECORDED_LATENCY_US, 3).unwrap(),
            iteration: 0,
            seed: (worker_index as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15),
        }
    } // end constructor

    fn random(&mut self) -> u64 {
        let mut state = self.seed;
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        self.seed = state;
        state.wrapping_mul(0x2545_F491_4F6C_DD1D)
    } // end fn random

    pub(crate) fn should_continue(&self) -> bool {
        Instant::now() < self.deadline
    } // end method should_continue

    pub(crate) fn pacing_delay(&self) -> Option<Duration> {
        self.state.pacer.delay(&self.args, self.started)
    } // end method pacing_delay

    pub(crate) fn pick_index(&mut self) -> usize {
        match self.args.key_dist {
            KeyDist::Hot => 0,
            KeyDist::Uniform => (self.random() as usize) % self.key_count,
            KeyDist::Skewed => {
                let random_fraction = (self.random() % 10_000) as f64 / 10_000.0;
                if random_fraction < self.args.hot_fraction || self.key_count == 1 {
                    0
                } else {
                    1 + ((self.random() as usize) % (self.key_count - 1))
                }
            }
        }
    } // end method pick_index

    pub(crate) fn begin_iteration(&mut self) -> Option<Instant> {
        self.iteration = self.iteration.wrapping_add(1);
        should_sample(self.iteration, self.args.sample_every).then(Instant::now)
    } // end method begin_iteration

    pub(crate) fn end_iteration(&mut self, sample_started: Option<Instant>) {
        if let Some(sample_started) = sample_started {
            let latency_us = sample_started.elapsed().as_micros() as u64;
            let _ = self.histogram.record(latency_us.max(1));
        }
        self.state.total_count.fetch_add(1, Ordering::AcqRel);
    } // end method end_iteration

    pub(crate) fn record_decision(
        &self,
        strategy: Strategy,
        decision: RateLimitDecision,
        sample: impl FnOnce() -> String,
    ) {
        match (strategy, decision) {
            (_, RateLimitDecision::Allowed) => {
                self.state
                    .counts
                    .allowed_count
                    .fetch_add(1, Ordering::AcqRel);
            }
            (Strategy::Absolute, RateLimitDecision::Rejected { .. }) => {
                self.state
                    .counts
                    .rejected_count
                    .fetch_add(1, Ordering::AcqRel);
            }
            (
                Strategy::Suppressed,
                RateLimitDecision::Suppressed {
                    is_allowed: true, ..
                },
            ) => {
                self.state
                    .counts
                    .suppressed_allowed_count
                    .fetch_add(1, Ordering::AcqRel);
            }
            (
                Strategy::Suppressed,
                RateLimitDecision::Suppressed {
                    is_allowed: false, ..
                },
            ) => {
                self.state
                    .counts
                    .suppressed_denied_count
                    .fetch_add(1, Ordering::AcqRel);
            }
            (Strategy::Absolute, RateLimitDecision::Suppressed { .. }) => self.record_error(
                "absolute strategy returned a suppressed decision".to_string(),
                Some(sample()),
            ),
            (Strategy::Suppressed, RateLimitDecision::Rejected { .. }) => self.record_error(
                "suppressed strategy returned a rejected decision".to_string(),
                Some(sample()),
            ),
        }
    } // end method record_decision

    pub(crate) fn record_allowed(&self, is_allowed: bool) {
        if is_allowed {
            self.state
                .counts
                .allowed_count
                .fetch_add(1, Ordering::AcqRel);
        } else {
            self.state
                .counts
                .rejected_count
                .fetch_add(1, Ordering::AcqRel);
        }
    } // end method record_allowed

    pub(crate) fn record_error(&self, message: String, sample: Option<String>) {
        self.state.counts.error_count.fetch_add(1, Ordering::AcqRel);
        self.state.error_stats.record(message, sample);
    } // end method record_error

    pub(crate) fn into_histogram(self) -> Histogram<u64> {
        self.histogram
    } // end method into_histogram
} // end impl

pub(crate) fn finish_run(
    args: &Args,
    started: Instant,
    histograms: impl IntoIterator<Item = Histogram<u64>>,
    state: &RunState,
) {
    let mut histogram = Histogram::new_with_bounds(1, MAX_RECORDED_LATENCY_US, 3).unwrap();
    for worker_histogram in histograms {
        histogram.add(worker_histogram).unwrap();
    }

    let elapsed = started.elapsed();
    let total_count = state.total_count.load(Ordering::Acquire);
    let counts = state.counts.snapshot();
    let accounted_count = counts.accounted_count();

    println!(
        "provider={:?} strategy={:?} mode={:?} runtime={}",
        args.provider,
        args.strategy,
        args.mode,
        crate::runtime::name(),
    );
    println!(
        "threads={} duration_s={} window_s={} group_ms={} key_dist={:?} key_space={} hot_fraction={}",
        args.threads,
        args.duration_s,
        args.window_s,
        args.group_ms,
        args.key_dist,
        if args.key_dist == KeyDist::Hot {
            1
        } else {
            args.key_space
        },
        args.hot_fraction,
    );
    println!(
        "rate_limit_per_s={} hard_limit_factor={} suppression_cache_ms={} sample_every={}",
        args.rate_limit_per_s, args.hard_limit_factor, args.suppression_cache_ms, args.sample_every,
    );
    println!(
        "target_qps={:?} burst_qps={:?} burst_period_ms={} burst_duration_ms={}",
        args.target_qps, args.burst_qps, args.burst_period_ms, args.burst_duration_ms,
    );
    if matches!(args.provider, Provider::Redis | Provider::Hybrid) {
        println!("redis_prefix={}", args.redis_prefix);
    }
    println!(
        "elapsed_s={:.3} ops={} ops_per_s={:.0}",
        elapsed.as_secs_f64(),
        total_count,
        total_count as f64 / elapsed.as_secs_f64()
    );
    println!(
        "allowed={} rejected={} suppressed_allowed={} suppressed_denied={} errors={} accepted={} denied={} accounted={}",
        counts.allowed_count,
        counts.rejected_count,
        counts.suppressed_allowed_count,
        counts.suppressed_denied_count,
        counts.error_count,
        counts.accepted_count(),
        counts.denied_count(),
        accounted_count,
    );

    if histogram.is_empty() {
        println!("no latency samples collected");
    } else {
        println!(
            "lat_us p50={} p95={} p99={} p999={} max={}",
            histogram.value_at_quantile(0.50),
            histogram.value_at_quantile(0.95),
            histogram.value_at_quantile(0.99),
            histogram.value_at_quantile(0.999),
            histogram.max()
        );
        println!("samples={}", histogram.len());
    }

    print_error_stats(counts.error_count, &state.error_stats);
    if let Err(error) = validate_count_snapshot(total_count, counts) {
        eprintln!("invalid stress result: {error}");
        std::process::exit(1);
    }
} // end fn finish_run

fn print_error_stats(error_count: u64, stats: &ErrorStats) {
    if error_count == 0 {
        return;
    }

    println!("errors_total={error_count}");
    for (message, count) in stats.top_by_message(10) {
        println!("error_count={count} error=\"{message}\"");
    }
    for sample in stats.samples() {
        println!("error_sample=\"{sample}\"");
    }
} // end fn print_error_stats

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    fn args(extra: &[&str]) -> Arc<Args> {
        let mut raw = vec!["trypema-stress"];
        raw.extend_from_slice(extra);
        Arc::new(Args::parse_from(raw))
    }

    #[test]
    fn target_pacer_reserves_one_global_sequence() {
        let args = args(&["--mode", "target-qps", "--target-qps", "1000"]);
        let pacer = TargetPacer::default();
        let now = Instant::now();
        let first = pacer.reserve_at(&args, now, now).unwrap();
        let second = pacer.reserve_at(&args, now, now).unwrap();
        let third = pacer.reserve_at(&args, now, now).unwrap();

        assert_eq!(second.duration_since(first), Duration::from_millis(1));
        assert_eq!(third.duration_since(second), Duration::from_millis(1));
    }

    #[test]
    fn start_gate_releases_all_ready_workers_at_the_same_instant() {
        let gate = Arc::new(StartGate::new(2));
        let (sender, receiver) = std::sync::mpsc::channel();
        let handles = (0..2)
            .map(|_| {
                let gate = Arc::clone(&gate);
                let sender = sender.clone();
                std::thread::spawn(move || sender.send(gate.wait_sync()).unwrap())
            })
            .collect::<Vec<_>>();
        drop(sender);

        let started = gate.start_sync();
        let worker_starts = receiver.into_iter().collect::<Vec<_>>();
        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(worker_starts, vec![started, started]);
    }

    #[test]
    fn burst_qps_uses_elapsed_phase_then_returns_to_base() {
        let args = args(&[
            "--mode",
            "target-qps",
            "--target-qps",
            "100",
            "--burst-qps",
            "1000",
            "--burst-period-ms",
            "100",
            "--burst-duration-ms",
            "20",
        ]);

        assert_eq!(qps_for_elapsed(&args, Duration::from_millis(0)), Some(1000));
        assert_eq!(
            qps_for_elapsed(&args, Duration::from_millis(19)),
            Some(1000)
        );
        assert_eq!(qps_for_elapsed(&args, Duration::from_millis(20)), Some(100));
        assert_eq!(
            qps_for_elapsed(&args, Duration::from_millis(100)),
            Some(1000)
        );
    }

    #[test]
    fn every_completed_iteration_has_exactly_one_outcome() {
        let args = args(&[]);
        let state = RunState::default();
        let started = Instant::now();
        let mut worker = WorkerLoop::new(args, 1, state.clone(), 0, started);

        worker.end_iteration(None);
        worker.record_decision(Strategy::Absolute, RateLimitDecision::Allowed, || {
            "allowed".to_string()
        });
        worker.end_iteration(None);
        worker.record_decision(
            Strategy::Absolute,
            RateLimitDecision::Rejected {
                window_size_seconds: 1,
                retry_after_ms: 1,
                remaining_after_waiting: 1,
            },
            || "rejected".to_string(),
        );
        worker.end_iteration(None);
        worker.record_decision(
            Strategy::Absolute,
            RateLimitDecision::Suppressed {
                suppression_factor: 0.5,
                is_allowed: false,
            },
            || "invalid".to_string(),
        );

        assert_eq!(state.total_count.load(Ordering::Acquire), 3);
        assert_eq!(state.counts.snapshot().accounted_count(), 3);
        assert_eq!(state.error_count(), 1);
    }

    #[test]
    fn run_results_reject_operation_errors_and_accounting_mismatches() {
        let counts = CountSnapshot {
            error_count: 1,
            ..CountSnapshot::default()
        };
        assert!(validate_count_snapshot(1, counts).is_err());
        assert!(validate_count_snapshot(1, CountSnapshot::default()).is_err());
        assert!(validate_count_snapshot(0, CountSnapshot::default()).is_ok());
    }

    #[test]
    fn key_selection_never_leaves_the_effective_key_space() {
        let args = args(&["--key-dist", "skewed", "--key-space", "17"]);
        let mut worker = WorkerLoop::new(args, 17, RunState::default(), 0, Instant::now());
        for _ in 0..10_000 {
            assert!(worker.pick_index() < 17);
        }
    }

    #[test]
    fn skewed_key_selection_respects_the_configured_hot_fraction() {
        let args = args(&[
            "--key-dist",
            "skewed",
            "--key-space",
            "17",
            "--hot-fraction",
            "0.8",
        ]);
        let mut worker = WorkerLoop::new(args, 17, RunState::default(), 0, Instant::now());
        let hot_count = (0..100_000).filter(|_| worker.pick_index() == 0).count();

        assert!(hot_count >= 79_000);
        assert!(hot_count <= 81_000);
    }

    #[test]
    fn sampling_uses_the_configured_iteration_interval() {
        assert!(!should_sample(1, 100));
        assert!(should_sample(100, 100));
        assert!(should_sample(200, 100));
    }
}
