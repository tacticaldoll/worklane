# Telemetry Specification

## Purpose

Defines how `worklane` supports distributed tracing by carrying W3C TraceContext
propagation headers through the `JobEnvelope` and how the opt-in
`worklane-otel` crate provides inject/extract helpers.
## Requirements
### Requirement: Optional trace context field on envelope

The `JobEnvelope` SHALL carry an optional `trace_context` field of type
`Option<HashMap<String, String>>`. When absent or `None`, the envelope SHALL
behave identically to envelopes without the field (backward-compatible
deserialization). When present, the map SHALL hold W3C TraceContext propagation
headers (or any string-key propagation format the consumer registers).

#### Scenario: Envelope without trace context deserializes cleanly

- **WHEN** a stored `JobEnvelope` JSON has no `trace_context` key (legacy data)
- **THEN** it SHALL deserialize with `trace_context: None` without error

#### Scenario: Envelope with trace context round-trips through broker

- **WHEN** a `JobEnvelope` with `trace_context: Some(map)` is enqueued and
  reserved
- **THEN** the reserved envelope SHALL contain the same `trace_context` map

### Requirement: Inject current span into a job before enqueue

The `worklane-otel` crate SHALL provide an `inject` function that reads the
active `opentelemetry::Context` (via the global TextMap propagator) and writes
the resulting propagation headers into a `NewJob`'s `trace_context` field.
When there is no active span, the function SHALL leave `trace_context` as
`None`. When `trace_context` already contains caller-provided keys and an active
span is injected, propagation keys produced by the global propagator SHALL be
merged into the existing map, preserving unrelated keys.

#### Scenario: Inject with an active span

- **WHEN** `worklane_otel::inject(&mut job)` is called inside an active OTel span
- **THEN** `job.trace_context` SHALL be `Some(map)` containing at least a
  `traceparent` key

#### Scenario: Inject without an active span

- **WHEN** `worklane_otel::inject(&mut job)` is called with no active OTel span
- **THEN** `job.trace_context` SHALL remain `None`

#### Scenario: Inject preserves unrelated existing keys

- **WHEN** `worklane_otel::inject(&mut job)` is called inside an active OTel span
- **AND** `job.trace_context` already contains a non-propagation key
- **THEN** `job.trace_context` SHALL contain the injected propagation headers
- **AND** the existing non-propagation key SHALL remain present

### Requirement: Extract span context from a reserved envelope

The `worklane-otel` crate SHALL provide an `extract` function that reads the
`trace_context` map from a `JobEnvelope` (via the global TextMap propagator)
and returns the reconstructed `opentelemetry::Context`. If `trace_context` is
`None` or the map contains no recognizable propagation headers, the function
SHALL return the current context unchanged (no-op).

Because `trace_context` is reconstructed from semi-trusted storage bytes,
`extract` SHALL expose only a fixed allowlist of W3C propagation headers
(`traceparent`, `tracestate`, `baggage`) to the propagator, and SHALL NOT
forward arbitrary keys from the stored map. This bounds an oversized or hostile
`trace_context` so it cannot flood the propagator, spans, or baggage with
attacker-influenced keys.

#### Scenario: Non-propagation keys are not forwarded

- **WHEN** `worklane_otel::extract(&envelope)` is called on an envelope whose
  `trace_context` carries keys outside the allowlist (e.g. arbitrary or bulk
  headers)
- **THEN** those keys SHALL NOT be visible to the propagator (only allowlisted
  propagation headers are exposed)

#### Scenario: Extract from envelope with valid traceparent

- **WHEN** `worklane_otel::extract(&envelope)` is called on an envelope whose
  `trace_context` contains a valid `traceparent` header
- **THEN** the returned `Context` SHALL carry the remote span as its parent

#### Scenario: Extract from envelope without trace context

- **WHEN** `worklane_otel::extract(&envelope)` is called on an envelope with
  `trace_context: None`
- **THEN** the returned `Context` SHALL equal the current ambient context
  (effectively a no-op)

### Requirement: worklane-otel is an opt-in crate

The `worklane-otel` crate SHALL be a separate crate in the workspace. It SHALL
NOT be a dependency of `worklane`, `worklane-core`, or any broker crate.
Consumers who do not depend on `worklane-otel` SHALL NOT compile any OTel SDK
code as a result of depending on `worklane`.

#### Scenario: Core crate is free of OTel dependencies

- **WHEN** a consumer depends only on `worklane` and a broker crate
- **THEN** neither `opentelemetry` nor `tracing-opentelemetry` SHALL be
  compiled as a result

### Requirement: W3C trace-flags and tracestate fidelity across round-trip

The inject → store → extract round trip SHALL preserve the W3C sampling
decision and vendor state, not merely the trace and span identifiers, whenever a
`JobEnvelope`'s `trace_context` carries a `traceparent` and/or `tracestate`.
Specifically, the `sampled` bit of the `traceparent` trace-flags byte and the
`tracestate` value present at `inject` SHALL be recoverable from the
`opentelemetry::Context` returned by `extract`. This ensures a downstream worker
honours the upstream sampling decision and propagates vendor state, so a sampled
trace is not silently dropped at the job boundary.

#### Scenario: Sampled flag is preserved end to end

- **WHEN** a job is injected inside a span whose context is sampled
  (trace-flags `01`) and the reserved envelope is later passed to
  `worklane_otel::extract`
- **THEN** the recovered span context SHALL report the trace as sampled

#### Scenario: tracestate is preserved end to end

- **WHEN** a job is injected in a context carrying a non-empty `tracestate`
  (vendor state) and the reserved envelope is later passed to
  `worklane_otel::extract`
- **THEN** the recovered context SHALL carry the same `tracestate` value
