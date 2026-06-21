# worklane-metrics

`metrics`-facade instrumentation for [worklane] workers and queues.

Records job-outcome counters, a processing-duration histogram, an in-flight
gauge, and a per-lane queue-depth gauge through the `metrics` crate. Opt-in: the
core job loop and broker contract do not require it. Like `worklane-otel`, this
crate only *records* — you install an exporter (e.g.
`metrics_exporter_prometheus`) to publish the values; worklane runs none for you.

## When to use it

You want Prometheus/StatsD-style metrics for your workers and an autoscaling
signal (queue depth) without hand-writing an observer.

## How it plugs in

Two pieces:

- `MetricsObserver` — a `JobObserver` you attach to a `Worker`. Records
  `worklane_jobs_total`, `worklane_job_duration_seconds`, `worklane_in_flight_jobs`.
- `record_pending_depth(broker, lanes)` — call on a timer to publish
  `worklane_pending_jobs` per lane from `QueueStats::pending_count` (via
  `Broker::queue_stats()`). It stops at the
  first failing lane, leaving later gauges stale.

```rust,ignore
use std::sync::Arc;
use worklane::Worker;
use worklane_metrics::MetricsObserver;

// install a `metrics` exporter once at startup, then:
let worker = worker.with_observer(Arc::new(MetricsObserver::new()));
```

`lane` and `kind` become labels — keep their cardinality bounded.

## Layer

Implements the `JobObserver` SPI, which lives in `worklane-core`, so this crate
depends only on `worklane-core` (the `worklane` facade is a dev-dependency for
the doctest above).

[worklane]: https://docs.rs/worklane

## License

Licensed under either of MIT or Apache-2.0 at your option.
