//! OpenTelemetry trace-context propagation helpers for `worklane`.
//!
//! This crate is **opt-in**: add `worklane-otel` as a dependency only when you
//! need distributed tracing. Consumers who do not depend on this crate compile
//! no OpenTelemetry code as a side-effect of depending on `worklane`.
//!
//! # Usage
//!
//! ```rust,ignore
//! use worklane_otel::{inject, extract};
//!
//! // At enqueue time (inside an active span):
//! let mut job = NewJob::new(lane, kind, payload, max_attempts);
//! inject(&mut job);   // writes traceparent into job.trace_context
//! client.enqueue(job).await?;
//!
//! // At dispatch time (inside the worker, before running the handler):
//! let ctx = extract(&envelope);  // reads traceparent from envelope
//! let _span = tracer.start_with_context("job.execute", &ctx);
//! ```
#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::collections::HashMap;

use opentelemetry::propagation::{Extractor, Injector};
use worklane_core::{JobEnvelope, NewJob};

// ── TextMap adaptors ──────────────────────────────────────────────────────────

/// A [`TextMapPropagator`] injector that writes into a `HashMap<String,
/// String>`.
struct MapInjector<'a>(&'a mut HashMap<String, String>);

impl Injector for MapInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_owned(), value);
    }
}

/// The W3C Trace Context / Baggage propagation headers worklane will surface to
/// the propagator. `trace_context` is reconstructed from semi-trusted storage
/// bytes, so the extractor exposes only these well-known keys rather than
/// forwarding an arbitrary, attacker-influenced map of headers (and their values)
/// into the propagator, spans, and baggage.
const PROPAGATION_KEYS: [&str; 3] = ["traceparent", "tracestate", "baggage"];

/// A [`TextMapPropagator`] extractor that reads from an optional `HashMap`,
/// restricted to the [`PROPAGATION_KEYS`] allowlist so an oversized or hostile
/// `trace_context` cannot flood the propagator with unbounded keys.
struct MapExtractor<'a>(Option<&'a HashMap<String, String>>);

impl Extractor for MapExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        if !PROPAGATION_KEYS.contains(&key) {
            return None;
        }
        self.0?.get(key).map(String::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        let Some(map) = self.0 else {
            return Vec::new();
        };
        // Only the allowlisted keys that are actually present, never the raw key
        // set of the (untrusted) map.
        PROPAGATION_KEYS
            .iter()
            .copied()
            .filter(|k| map.contains_key(*k))
            .collect()
    }
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Inject the current OpenTelemetry context (active span) into `job`'s
/// `trace_context` field using the globally-registered TextMap propagator.
///
/// When there is no active span the global propagator will produce an empty
/// map, in which case `job.trace_context` is left as `None` (no-op).
pub fn inject(job: &mut NewJob) {
    let propagator = opentelemetry::global::get_text_map_propagator(|p| {
        let mut map = HashMap::new();
        p.inject(&mut MapInjector(&mut map));
        map
    });

    if !propagator.is_empty() {
        let mut map = job.trace_context.take().unwrap_or_default();
        map.extend(propagator);
        job.trace_context = Some(map);
    }
}

/// Extract the OpenTelemetry context stored in `envelope.trace_context` using
/// the globally-registered TextMap propagator and return it.
///
/// When `trace_context` is `None`, or contains no recognizable propagation
/// headers, the returned [`opentelemetry::Context`] equals the current ambient
/// context (effectively a no-op).
pub fn extract(envelope: &JobEnvelope) -> opentelemetry::Context {
    opentelemetry::global::get_text_map_propagator(|p| {
        p.extract(&MapExtractor(envelope.trace_context.as_ref()))
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::{SpanContext, TraceContextExt, TraceFlags, TraceId, TraceState};
    use opentelemetry::{Context, global};
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use worklane_core::{Broker, JobEnvelope, JobId, Lane, NewJob};
    use worklane_memory::InMemoryBroker;

    fn sample_lane() -> Lane {
        Lane::try_from("test").unwrap()
    }

    fn new_job() -> NewJob {
        NewJob::new(sample_lane(), "test_kind", b"{}".to_vec(), 3)
    }

    fn set_w3c_propagator() {
        global::set_text_map_propagator(TraceContextPropagator::new());
    }

    // ── inject tests ──────────────────────────────────────────────────────────

    #[test]
    fn inject_without_active_span_preserves_existing() {
        set_w3c_propagator();
        let mut job = new_job();
        let mut existing = HashMap::new();
        existing.insert("custom".to_string(), "value".to_string());
        job.trace_context = Some(existing);

        inject(&mut job);

        let tc = job
            .trace_context
            .expect("trace_context should be preserved");
        assert_eq!(
            tc.get("custom").unwrap(),
            "value",
            "existing keys must be preserved"
        );
    }

    #[test]
    fn inject_with_active_span_writes_traceparent() {
        set_w3c_propagator();

        // Build a synthetic remote span context and attach it as current.
        let trace_id = TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap();
        let span_ctx = SpanContext::new(
            trace_id,
            opentelemetry::trace::SpanId::from_hex("00f067aa0ba902b7").unwrap(),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        );
        let ctx = Context::current().with_remote_span_context(span_ctx);
        let _guard = ctx.attach();

        let mut job = new_job();
        inject(&mut job);

        let tc = job.trace_context.expect("trace_context should be Some");
        assert!(
            tc.contains_key("traceparent"),
            "trace_context must contain 'traceparent' key"
        );
        let tp = &tc["traceparent"];
        assert!(
            tp.contains("4bf92f3577b34da6a3ce929d0e0e4736"),
            "traceparent must embed the trace-id"
        );
    }

    #[test]
    fn inject_with_active_span_preserves_existing_custom_keys() {
        set_w3c_propagator();

        let trace_id = TraceId::from_hex("1234567890abcdef1234567890abcdef").unwrap();
        let span_ctx = SpanContext::new(
            trace_id,
            opentelemetry::trace::SpanId::from_hex("abcdef1234567890").unwrap(),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        );
        let ctx = Context::current().with_remote_span_context(span_ctx);
        let _guard = ctx.attach();

        let mut job = new_job();
        let mut existing = HashMap::new();
        existing.insert("custom".to_string(), "value".to_string());
        job.trace_context = Some(existing);

        inject(&mut job);

        let tc = job.trace_context.expect("trace_context should be Some");
        assert_eq!(tc.get("custom").unwrap(), "value");
        assert!(tc.contains_key("traceparent"));
    }

    // ── extract tests ─────────────────────────────────────────────────────────

    #[test]
    fn extract_without_trace_context_is_noop() {
        set_w3c_propagator();
        let envelope = make_envelope(None);
        let ctx = extract(&envelope);
        // No trace context → returned context has no remote span.
        let span = ctx.span();
        let sc = span.span_context();
        assert!(
            !sc.is_valid(),
            "context without trace_context must carry an invalid (root) span"
        );
    }

    #[test]
    fn extract_with_valid_traceparent_recovers_trace_id() {
        set_w3c_propagator();
        let mut map = HashMap::new();
        map.insert(
            "traceparent".to_owned(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned(),
        );
        let envelope = make_envelope(Some(map));
        let ctx = extract(&envelope);
        let span = ctx.span();
        let sc = span.span_context();
        assert!(sc.is_valid(), "recovered span context must be valid");
        assert_eq!(
            sc.trace_id(),
            TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap()
        );
    }

    #[test]
    fn extract_ignores_non_allowlisted_keys() {
        set_w3c_propagator();
        let mut map = HashMap::new();
        // A hostile/extra key alongside a valid traceparent.
        map.insert("x-evil".to_owned(), "z".repeat(10_000));
        map.insert(
            "traceparent".to_owned(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned(),
        );
        let envelope = make_envelope(Some(map));

        // The allowlist exposes only propagation headers: the valid traceparent
        // is still honoured, and the non-allowlisted key is invisible.
        let extractor = MapExtractor(envelope.trace_context.as_ref());
        assert!(
            extractor.get("x-evil").is_none(),
            "non-allowlisted key must be hidden"
        );
        assert!(extractor.get("traceparent").is_some());
        assert!(!extractor.keys().contains(&"x-evil"));

        let ctx = extract(&envelope);
        assert!(
            ctx.span().span_context().is_valid(),
            "a valid traceparent is still recovered despite the extra key"
        );
    }

    // ── round-trip test ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn round_trip_inject_enqueue_reserve_extract() {
        set_w3c_propagator();

        // Set a synthetic active span.
        let trace_id = TraceId::from_hex("abcdef1234567890abcdef1234567890").unwrap();
        let span_ctx = SpanContext::new(
            trace_id,
            opentelemetry::trace::SpanId::from_hex("1234567890abcdef").unwrap(),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        );
        let ctx = Context::current().with_remote_span_context(span_ctx);
        let _guard = ctx.attach();

        let mut job = new_job();
        inject(&mut job);
        assert!(job.trace_context.is_some());

        let broker = InMemoryBroker::default();
        let lane = sample_lane();
        broker.enqueue(job).await.unwrap();
        let reservation = broker.reserve(&lane).await.unwrap().unwrap();

        let recovered_ctx = extract(&reservation.envelope);
        let span = recovered_ctx.span();
        let sc = span.span_context();
        assert!(sc.is_valid());
        assert_eq!(sc.trace_id(), trace_id);
    }

    /// The W3C `sampled` trace-flag must survive inject → store → extract, so a
    /// downstream worker honours the upstream sampling decision. Asserting both a
    /// sampled and an unsampled input proves the bit is genuinely carried, not
    /// hardcoded by the propagator.
    #[tokio::test]
    async fn round_trip_preserves_sampled_flag() {
        set_w3c_propagator();
        let trace_id = TraceId::from_hex("11111111111111111111111111111111").unwrap();
        let span_id = opentelemetry::trace::SpanId::from_hex("1111111111111111").unwrap();

        let sampled = round_trip(SpanContext::new(
            trace_id,
            span_id,
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        ))
        .await;
        assert!(
            sampled.is_valid(),
            "a sampled context must round-trip valid"
        );
        assert!(
            sampled.is_sampled(),
            "the sampled flag must survive the round trip"
        );

        let unsampled = round_trip(SpanContext::new(
            trace_id,
            span_id,
            TraceFlags::default(),
            true,
            TraceState::default(),
        ))
        .await;
        assert!(
            !unsampled.is_sampled(),
            "an unsampled trace must not become sampled across the round trip"
        );
    }

    /// W3C `tracestate` (vendor state) must survive inject → store → extract, so
    /// vendor propagation is not dropped at the job boundary.
    #[tokio::test]
    async fn round_trip_preserves_tracestate() {
        set_w3c_propagator();
        let trace_id = TraceId::from_hex("22222222222222222222222222222222").unwrap();
        let span_id = opentelemetry::trace::SpanId::from_hex("2222222222222222").unwrap();
        let state = TraceState::from_key_value(vec![("vendor", "v1")]).unwrap();

        let recovered = round_trip(SpanContext::new(
            trace_id,
            span_id,
            TraceFlags::SAMPLED,
            true,
            state,
        ))
        .await;
        assert!(recovered.is_valid(), "the recovered context must be valid");
        assert_eq!(
            recovered.trace_state().get("vendor"),
            Some("v1"),
            "the tracestate vendor entry must survive the round trip"
        );
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    /// Inject `span_ctx` into a fresh job, enqueue/reserve it through an in-memory
    /// broker, then extract and return the recovered span context — the full
    /// enqueue-side → dispatch-side propagation path.
    async fn round_trip(span_ctx: SpanContext) -> SpanContext {
        let mut job = new_job();
        {
            let ctx = Context::current().with_remote_span_context(span_ctx);
            let _guard = ctx.attach();
            inject(&mut job);
        }
        let broker = InMemoryBroker::default();
        let lane = sample_lane();
        broker.enqueue(job).await.unwrap();
        let reservation = broker.reserve(&lane).await.unwrap().unwrap();
        extract(&reservation.envelope).span().span_context().clone()
    }

    fn make_envelope(trace_context: Option<HashMap<String, String>>) -> JobEnvelope {
        JobEnvelope::new(
            JobId::new(),
            sample_lane(),
            "test_kind",
            b"{}".to_vec(),
            3,
            0,
            trace_context,
        )
    }
}
