# worklane-otel

OpenTelemetry W3C trace-context propagation for [worklane] jobs.

Carries the active trace across the enqueue → store → dispatch boundary so a job
handler continues the span of whoever enqueued it. Opt-in: depend on this crate
only when you want distributed tracing. Pulling in `worklane` alone compiles no
OpenTelemetry code.

## When to use it

You already run OpenTelemetry and want enqueued jobs to appear as child spans of
the request that created them, across process boundaries.

## How it plugs in

Two free functions over the core job types — no broker decorator, no observer,
no wrapper object. You call them at the two ends of the job lifecycle:

- `inject(&mut NewJob)` — at enqueue time, inside an active span, writes
  `traceparent`/`tracestate`/`baggage` into `job.trace_context`.
- `extract(&JobEnvelope) -> opentelemetry::Context` — at dispatch time, rebuilds
  the context to parent your `job.execute` span.

```rust,ignore
use worklane_otel::{inject, extract};

let mut job = NewJob::new(lane, kind, payload, max_attempts);
inject(&mut job);                 // enqueue side, inside a span
client.enqueue(job).await?;

let ctx = extract(&reservation.envelope);   // worker side
let _span = tracer.start_with_context("job.execute", &ctx);
```

The extractor allowlists only `traceparent`/`tracestate`/`baggage`, so a hostile
stored `trace_context` cannot flood the propagator.

## Layer

Sits directly on `worklane-core` (`NewJob`/`JobEnvelope`). Backend- and
facade-agnostic; works with any broker. Tracks the OpenTelemetry 0.27 line.

[worklane]: https://docs.rs/worklane

## License

Licensed under either of MIT or Apache-2.0 at your option.
