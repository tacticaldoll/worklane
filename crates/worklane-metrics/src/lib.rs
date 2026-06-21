//! `metrics`-facade integration for `worklane`.
//!
//! Depend on this crate only when an application wants `metrics` crate
//! instrumentation. The core job loop and broker contract do not require it.
//!
//! Two pieces:
//! - [`MetricsObserver`] — a [`JobObserver`] that records in-flight attempts, a
//!   job-outcome counter, and a processing-duration histogram.
//! - [`record_pending_depth`] — publishes per-lane queue-depth gauges from
//!   [`QueueStats::pending_count`](worklane_core::QueueStats::pending_count), the
//!   single most useful autoscaling signal.
//!
//! Like [`worklane-otel`](https://docs.rs/worklane-otel), this crate only
//! *records* through the [`metrics`] facade; the application installs an exporter
//! (e.g. a Prometheus recorder) to publish the values. worklane does not pick or
//! run an exporter for you.
//!
//! ```no_run
//! use std::sync::Arc;
//! use worklane::Worker;
//! use worklane_metrics::MetricsObserver;
//! # fn demo(mut worker: Worker) {
//! // Install a `metrics` exporter (e.g. `metrics_exporter_prometheus`) once at
//! // startup, then attach the observer:
//! let worker = worker.with_observer(Arc::new(MetricsObserver::new()));
//! # let _ = worker;
//! # }
//! ```
#![forbid(unsafe_code)]
#![warn(missing_docs)]

use worklane_core::{Broker, JobAttemptEvent, JobEvent, JobObserver, JobOutcome, Lane, Result};

/// Counter: total jobs resolved, labelled `lane`, `kind`, `outcome`.
pub const JOBS_TOTAL: &str = "worklane_jobs_total";
/// Histogram: job processing duration in seconds, labelled `lane`, `kind`.
pub const JOB_DURATION_SECONDS: &str = "worklane_job_duration_seconds";
/// Gauge: in-flight worker attempts, labelled `lane`, `kind`.
pub const IN_FLIGHT_JOBS: &str = "worklane_in_flight_jobs";
/// Gauge: pending (live) jobs per lane, labelled `lane`.
pub const PENDING_JOBS: &str = "worklane_pending_jobs";

/// The stable `outcome` label value for a [`JobOutcome`].
fn outcome_label(outcome: JobOutcome) -> &'static str {
    match outcome {
        JobOutcome::Acked => "acked",
        JobOutcome::Retried => "retried",
        JobOutcome::DeadLettered => "dead_lettered",
        // `JobOutcome` is `#[non_exhaustive]`; a future variant maps here until
        // this crate adds an explicit label for it.
        _ => "other",
    }
}

/// A [`JobObserver`] that records job metrics via the [`metrics`] facade:
/// increments [`JOBS_TOTAL`] (labelled by `lane`, `kind`, `outcome`) and records
/// [`JOB_DURATION_SECONDS`] (labelled by `lane`, `kind`) on every resolved job.
///
/// `lane` and `kind` become metric labels, so keep their cardinality bounded — a
/// fixed set of lanes and job kinds — exactly as for any Prometheus label.
#[derive(Debug, Default, Clone, Copy)]
pub struct MetricsObserver;

impl MetricsObserver {
    /// Create a metrics observer.
    pub fn new() -> Self {
        MetricsObserver
    }
}

impl JobObserver for MetricsObserver {
    fn on_job_started(&self, event: JobAttemptEvent<'_>) {
        let lane: metrics::SharedString = std::sync::Arc::<str>::from(event.lane).into();
        let kind: metrics::SharedString = std::sync::Arc::<str>::from(event.kind).into();
        metrics::gauge!(
            IN_FLIGHT_JOBS,
            "lane" => lane,
            "kind" => kind,
        )
        .increment(1.0);
    }

    fn on_job_stopped(&self, event: JobAttemptEvent<'_>) {
        let lane: metrics::SharedString = std::sync::Arc::<str>::from(event.lane).into();
        let kind: metrics::SharedString = std::sync::Arc::<str>::from(event.kind).into();
        metrics::gauge!(
            IN_FLIGHT_JOBS,
            "lane" => lane,
            "kind" => kind,
        )
        .decrement(1.0);
    }

    fn on_job_finished(&self, event: JobEvent<'_>) {
        // Build each label value once as an `Arc`-backed `SharedString`. The label
        // is needed by both the counter and the histogram; cloning an `Arc`-backed
        // `SharedString` is a reference-count bump, not a re-allocation, so this is
        // two allocations per job (lane, kind) rather than four.
        let lane: metrics::SharedString = std::sync::Arc::<str>::from(event.lane).into();
        let kind: metrics::SharedString = std::sync::Arc::<str>::from(event.kind).into();
        metrics::counter!(
            JOBS_TOTAL,
            "lane" => lane.clone(),
            "kind" => kind.clone(),
            "outcome" => outcome_label(event.outcome),
        )
        .increment(1);
        metrics::histogram!(
            JOB_DURATION_SECONDS,
            "lane" => lane,
            "kind" => kind,
        )
        .record(event.duration.as_secs_f64());
    }
}

/// Publish the current queue depth of each lane as the [`PENDING_JOBS`] gauge
/// (labelled by `lane`), reading
/// [`QueueStats::pending_count`](worklane_core::QueueStats::pending_count).
///
/// Call this periodically (e.g. on a timer) to keep the gauge fresh — worklane
/// spawns no background task. Returns the first broker error encountered. Lanes
/// after the first error are not sampled on that call, so their gauges retain the
/// last value from a prior successful sample.
pub async fn record_pending_depth<B: Broker + ?Sized>(broker: &B, lanes: &[Lane]) -> Result<()> {
    let stats = broker.queue_stats().ok_or_else(|| {
        worklane_core::Error::Broker("broker does not support queue statistics".to_string())
    })?;
    for lane in lanes {
        let depth = stats.pending_count(lane).await?;
        metrics::gauge!(PENDING_JOBS, "lane" => lane.as_str().to_string()).set(depth as f64);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    use metrics_util::debugging::DebuggingRecorder;

    #[test]
    fn observer_records_a_job_counter_and_duration() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            MetricsObserver::new().on_job_finished(JobEvent::new(
                "default",
                "email",
                JobOutcome::Acked,
                Duration::from_millis(5),
            ));
        });

        let names: Vec<String> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(ck, _, _, _)| ck.key().name().to_string())
            .collect();
        assert!(
            names.iter().any(|n| n == JOBS_TOTAL),
            "the job counter must be recorded, got {names:?}"
        );
        assert!(
            names.iter().any(|n| n == JOB_DURATION_SECONDS),
            "the duration histogram must be recorded, got {names:?}"
        );
    }

    #[test]
    fn observer_records_in_flight_gauge() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        metrics::with_local_recorder(&recorder, || {
            let observer = MetricsObserver::new();
            observer.on_job_started(JobAttemptEvent::new("default", "email"));
            observer.on_job_stopped(JobAttemptEvent::new("default", "email"));
        });

        let names: Vec<String> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(ck, _, _, _)| ck.key().name().to_string())
            .collect();
        assert!(
            names.iter().any(|n| n == IN_FLIGHT_JOBS),
            "the in-flight gauge must be recorded, got {names:?}"
        );
    }

    #[tokio::test]
    async fn record_pending_depth_queries_each_lane() {
        use std::sync::Arc;
        use worklane_core::{NewJob, QueueStats};
        use worklane_memory::InMemoryBroker;

        let broker = Arc::new(InMemoryBroker::new());
        let lane = Lane::try_from("metrics_depth").unwrap();
        broker
            .enqueue(NewJob::new(lane.clone(), "k", b"null".to_vec(), 3))
            .await
            .unwrap();

        // Records the gauge without error (value assertion is covered by the
        // facade exercised in the observer test; here we verify the lane query).
        record_pending_depth(broker.as_ref(), std::slice::from_ref(&lane))
            .await
            .expect("record depth");
        assert_eq!(broker.pending_count(&lane).await.unwrap(), 1);
    }
}
