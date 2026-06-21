use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::id::JobId;
use crate::lane::Lane;

/// The default `max_attempts` applied to enqueued jobs when a caller does not
/// specify one. Brokers impose no retry policy themselves; this is the default
/// the client- and scheduler-side enqueue paths apply.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 5;

/// A job to be enqueued: the lane it targets, its kind, an already-serialized
/// payload, how many attempts it may take before being dead-lettered, and an
/// optional delay before it becomes visible for reservation.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct NewJob {
    /// The unique job id: a random v4 UUID minted at construction, so every
    /// `NewJob` is unique by construction. Assigned client-side (not by the
    /// broker) to keep enqueue atomic and replay-friendly — the broker persists
    /// this id rather than minting its own. Deduplication is via `unique_key`,
    /// never by reusing an id; the builder and fan-out paths always mint a fresh
    /// id, so no path produces two live jobs that share one id.
    pub id: JobId,
    /// The lane this job is enqueued to.
    pub lane: Lane,
    /// The job kind, matching a [`Job::KIND`](crate::Job::KIND).
    pub kind: String,
    /// The serialized payload bytes.
    ///
    /// There is no early size check here: a payload is bounded only when it
    /// reaches durable storage, by [`MAX_ENVELOPE_BYTES`](crate::spi::MAX_ENVELOPE_BYTES)
    /// (64 MiB) at the encode chokepoint — a sanity ceiling, not a tuning knob.
    /// For genuinely large payloads (documents, media), do not inline them here:
    /// use the Claim Check pattern (`worklane::ClaimCheck` /
    /// [`PayloadStore`](crate::PayloadStore)) to offload the bytes and carry only a
    /// reference, keeping the queue lean.
    pub payload: Vec<u8>,
    /// The maximum number of attempts before the job is dead-lettered.
    pub max_attempts: u32,
    /// How long after enqueue the job becomes visible for reservation. Zero
    /// (the default) makes it immediately visible.
    pub delay: Duration,
    /// An optional uniqueness key. While a live job holds a key, enqueuing
    /// another job with the same key is deduplicated to the existing job.
    pub unique_key: Option<String>,
    /// The priority of the job. Higher value means higher priority. Default is 0.
    pub priority: u8,
    /// Optional W3C TraceContext propagation headers (e.g. `traceparent`,
    /// `tracestate`). Set via `worklane-otel::inject`; `None` when not using
    /// distributed tracing. Brokers treat this as opaque and carry it unchanged,
    /// subject to the `spi::MAX_TRACE_CONTEXT_*` caps enforced at encode time.
    pub trace_context: Option<HashMap<String, String>>,
}

impl NewJob {
    /// Create a job to be enqueued to `lane`, visible immediately.
    pub fn new(lane: Lane, kind: impl Into<String>, payload: Vec<u8>, max_attempts: u32) -> Self {
        NewJob {
            id: JobId::new(),
            lane,
            kind: kind.into(),
            payload,
            max_attempts,
            delay: Duration::ZERO,
            unique_key: None,
            priority: 0,
            trace_context: None,
        }
    }

    /// Set the delay before this job becomes visible for reservation (builder
    /// style). A zero delay (the default) is immediate.
    #[must_use = "this value must be used"]
    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    /// Set the uniqueness key (builder style). While a live job holds this key,
    /// enqueuing another job with it is deduplicated to the existing job.
    #[must_use = "this value must be used"]
    pub fn with_unique_key(mut self, key: impl Into<String>) -> Self {
        self.unique_key = Some(key.into());
        self
    }

    /// Set the priority of this job (builder style). A higher value means higher priority.
    #[must_use = "this value must be used"]
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }

    /// Set the trace-context propagation headers (builder style). These are
    /// carried opaquely on the envelope; `worklane-otel` normally populates them
    /// via its injector, but a caller constructing a job by hand can set them
    /// directly here.
    #[must_use = "this value must be used"]
    pub fn with_trace_context(mut self, trace_context: HashMap<String, String>) -> Self {
        self.trace_context = Some(trace_context);
        self
    }

    /// Build the stored [`JobEnvelope`] for this job under the broker-assigned
    /// `id`, with `attempts = 0`. The broker-side fields (`delay`, `unique_key`)
    /// are not part of the envelope and are dropped. This is the single place the
    /// `NewJob` → `JobEnvelope` field mapping lives, so a new envelope field is a
    /// one-line change here rather than an edit at every backend's enqueue path.
    pub fn into_envelope(self) -> JobEnvelope {
        JobEnvelope::new(
            self.id,
            self.lane,
            self.kind,
            self.payload,
            self.max_attempts,
            self.priority,
            self.trace_context,
        )
    }
}

/// The broker's view of an enqueued job. The payload is opaque: the broker
/// never inspects or deserializes it.
///
/// **Field-default policy (deliberate, not an oversight).** The `#[serde(default)]`
/// attributes are asymmetric on purpose:
/// - `id`, `lane`, `kind`, `payload`, `attempts`, `max_attempts` are **core
///   lifecycle fields present since the first schema version** and carry *no*
///   default — a stored record missing any of them is corrupt, and decoding must
///   fail loudly rather than silently invent an `attempts`/`max_attempts` value
///   (which would change retry/dead-letter behavior).
/// - `priority` and `trace_context` are **later additions** whose defaults exist
///   solely so an envelope written before they existed still decodes, per the
///   schema-version policy.
///
/// The rule: a new field added after v1 gets a `#[serde(default)]` for
/// forward/backward compatibility; an original lifecycle field never does. The
/// `decodes_*` tests below lock this contract so it is not "tidied" into
/// inconsistency.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct JobEnvelope {
    /// The unique job id.
    pub id: JobId,
    /// The lane this job was enqueued to.
    pub lane: Lane,
    /// The job kind, used to dispatch to a handler.
    pub kind: String,
    /// The opaque serialized payload bytes.
    pub payload: Vec<u8>,
    /// The number of attempts made so far.
    pub attempts: u32,
    /// The maximum number of attempts before dead-lettering.
    pub max_attempts: u32,
    /// The priority of the job. Higher value means higher priority.
    #[serde(default)]
    pub priority: u8,
    /// Optional W3C TraceContext propagation headers. Absent in legacy stored
    /// envelopes; deserialized as `None` for backward compatibility.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_context: Option<HashMap<String, String>>,
}

impl JobEnvelope {
    /// Create a freshly enqueued envelope on `lane` with `attempts = 0`.
    pub fn new(
        id: JobId,
        lane: Lane,
        kind: impl Into<String>,
        payload: Vec<u8>,
        max_attempts: u32,
        priority: u8,
        trace_context: Option<HashMap<String, String>>,
    ) -> Self {
        JobEnvelope {
            id,
            lane,
            kind: kind.into(),
            payload,
            attempts: 0,
            max_attempts,
            priority,
            trace_context,
        }
    }
}

/// An opaque token proving authority to resolve a specific reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReservationReceipt(Uuid);

impl ReservationReceipt {
    /// Generate a new opaque reservation receipt.
    pub fn new() -> Self {
        ReservationReceipt(Uuid::new_v4())
    }
}

impl Default for ReservationReceipt {
    fn default() -> Self {
        Self::new()
    }
}

/// A reserved job and the receipt required to resolve it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Reservation {
    /// The reserved job envelope.
    pub envelope: JobEnvelope,
    /// The opaque receipt for this reservation instance.
    pub receipt: ReservationReceipt,
    /// The visibility lease the broker applied to this reservation. A caller can
    /// use it to schedule lease maintenance (for example a heartbeat that calls
    /// [`Broker::extend`](crate::Broker::extend)) without reading the broker's
    /// clock.
    pub lease: Duration,
}

impl Reservation {
    /// Pair a reserved envelope with the receipt that resolves it and the lease
    /// the broker applied.
    pub fn new(envelope: JobEnvelope, receipt: ReservationReceipt, lease: Duration) -> Self {
        Reservation {
            envelope,
            receipt,
            lease,
        }
    }
}

/// A job that exhausted its attempts (or failed unrecoverably), retained for
/// inspection along with the last error message.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DeadLetter {
    /// The envelope as it was when it failed.
    pub envelope: JobEnvelope,
    /// The last error message.
    pub error: String,
}

impl DeadLetter {
    /// Build a dead-letter record retaining the failing envelope and `error`.
    pub fn new(envelope: JobEnvelope, error: impl Into<String>) -> Self {
        DeadLetter {
            envelope,
            error: error.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lane::Lane;

    #[test]
    fn envelope_lane_serializes_transparently() {
        let env = JobEnvelope::new(
            JobId::new(),
            Lane::try_from("critical").unwrap(),
            "send_email",
            b"{}".to_vec(),
            5,
            0,
            None,
        );
        let json = serde_json::to_value(&env).unwrap();
        assert_eq!(json["lane"], "critical");
    }

    #[test]
    fn stored_envelope_with_invalid_lane_round_trips() {
        // A durable envelope persisted with a lane that current validation would
        // reject (surrounding whitespace) must still deserialize: the wire format
        // is trusted on read and lanes are not re-validated (storage contract).
        let invalid = " legacy ";
        assert!(Lane::try_from(invalid).is_err());

        let id = JobId::new();
        let stored = serde_json::json!({
            "id": id,
            "lane": invalid,
            "kind": "ok",
            "payload": [],
            "attempts": 0,
            "max_attempts": 3,
        });
        let env: JobEnvelope = serde_json::from_value(stored).unwrap();
        assert_eq!(env.lane.as_str(), invalid);
        assert_eq!(env.id, id);
    }

    #[test]
    fn decodes_legacy_envelope_missing_optional_fields() {
        // A record written before `priority`/`trace_context` existed (later
        // additions) must still decode, falling back to their documented defaults.
        let stored = serde_json::json!({
            "id": JobId::new(),
            "lane": "ok",
            "kind": "ok",
            "payload": [],
            "attempts": 1,
            "max_attempts": 3,
        });
        let env: JobEnvelope = serde_json::from_value(stored).unwrap();
        assert_eq!(env.priority, 0);
        assert_eq!(env.trace_context, None);
    }

    #[test]
    fn decoding_fails_when_core_lifecycle_field_missing() {
        // `max_attempts` is an original lifecycle field with no serde default:
        // a record missing it is corrupt and must fail loudly, never silently
        // synthesize a retry bound. This locks the field-default policy — if a
        // future edit adds `#[serde(default)]` to `max_attempts`, this test fails.
        let missing_max_attempts = serde_json::json!({
            "id": JobId::new(),
            "lane": "ok",
            "kind": "ok",
            "payload": [],
            "attempts": 0,
        });
        assert!(serde_json::from_value::<JobEnvelope>(missing_max_attempts).is_err());
    }
}
