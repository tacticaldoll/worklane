//! Service-provider interface (SPI) for `Broker` implementors.
//!
//! This module is the extension point for anyone writing a broker — the in-repo
//! backends and any external crate alike. Every durable broker stores the same
//! opaque [`JobEnvelope`] JSON, keys reservations by the same serialized
//! [`ReservationReceipt`], and converts clock durations to integer nanoseconds
//! the same way. Centralizing that plumbing here keeps the wire format
//! single-sourced, so implementations cannot drift apart on it.
//!
//! It is deliberately *not* re-exported from the `worklane` facade: facade users
//! enqueue and run jobs, they do not author brokers. Items here are reachable
//! only as `worklane_core::spi::*`.

use std::time::Duration;

use crate::envelope::{JobEnvelope, ReservationReceipt};
use crate::error::{Error, Result};
use crate::lane::Lane;

/// Upper bound on a single encoded [`JobEnvelope`], enforced on decode.
///
/// `decode_envelope` is the one chokepoint through which every backend turns
/// stored bytes back into an envelope (payload included). A storage record is
/// only semi-trusted: a corrupt, truncated, or hostile value could otherwise
/// drive an allocation proportional to its size. This is a sanity ceiling, not a
/// tuning knob — it is set far above any realistic job payload so legitimate
/// traffic never trips it, while a multi-gigabyte value is rejected before it is
/// materialized into memory. A job whose encoded form exceeds this cannot be
/// enqueued (`encode_envelope` rejects it symmetrically), so the limit can never
/// strand an already-stored job that a prior encode would have refused.
pub const MAX_ENVELOPE_BYTES: usize = 64 * 1024 * 1024;

/// Maximum number of `trace_context` entries carried on an envelope.
///
/// W3C TraceContext is `traceparent` plus a `tracestate` of at most 32
/// list-members, so a well-formed propagation map never exceeds this. The cap
/// stops a buggy or hostile injector from attaching an unbounded header map that
/// would bloat every store, read, and redelivery up to [`MAX_ENVELOPE_BYTES`].
pub const MAX_TRACE_CONTEXT_ENTRIES: usize = 34;

/// Maximum total size, in bytes, of all `trace_context` keys and values combined.
///
/// The W3C `tracestate` value is capped at 512 bytes and `traceparent` is ~55;
/// 8 KiB leaves generous headroom for vendor keys while still bounding the map far
/// below the envelope ceiling.
pub const MAX_TRACE_CONTEXT_BYTES: usize = 8 * 1024;

/// Serialize a job envelope to its opaque storage bytes.
///
/// Rejects an envelope whose encoded form would exceed [`MAX_ENVELOPE_BYTES`] so
/// the storage layer never holds a record that [`decode_envelope`] would later
/// refuse to read back, and whose `trace_context` exceeds the
/// [`MAX_TRACE_CONTEXT_ENTRIES`] / [`MAX_TRACE_CONTEXT_BYTES`] caps so an oversized
/// propagation map cannot ride on every copy of the job.
pub fn encode_envelope(envelope: &JobEnvelope) -> Result<Vec<u8>> {
    if let Some(tc) = &envelope.trace_context {
        if tc.len() > MAX_TRACE_CONTEXT_ENTRIES {
            return Err(Error::Serialization(format!(
                "trace_context has {} entries, over the {MAX_TRACE_CONTEXT_ENTRIES}-entry limit",
                tc.len()
            )));
        }
        let total: usize = tc.iter().map(|(k, v)| k.len() + v.len()).sum();
        if total > MAX_TRACE_CONTEXT_BYTES {
            return Err(Error::Serialization(format!(
                "trace_context is {total} bytes, over the {MAX_TRACE_CONTEXT_BYTES}-byte limit"
            )));
        }
    }
    let bytes = serde_json::to_vec(envelope).map_err(json_err)?;
    if bytes.len() > MAX_ENVELOPE_BYTES {
        return Err(Error::Serialization(format!(
            "encoded job envelope is {} bytes, over the {MAX_ENVELOPE_BYTES}-byte limit",
            bytes.len()
        )));
    }
    Ok(bytes)
}

/// Deserialize a job envelope from its opaque storage bytes.
///
/// Enforces [`MAX_ENVELOPE_BYTES`] *before* deserializing so a hostile or
/// corrupt storage value cannot drive an unbounded allocation.
pub fn decode_envelope(bytes: &[u8]) -> Result<JobEnvelope> {
    if bytes.len() > MAX_ENVELOPE_BYTES {
        return Err(Error::Serialization(format!(
            "stored job envelope is {} bytes, over the {MAX_ENVELOPE_BYTES}-byte limit",
            bytes.len()
        )));
    }
    serde_json::from_slice(bytes).map_err(json_err)
}

/// The opaque storage key for a receipt (its serialized form).
pub fn receipt_key(receipt: &ReservationReceipt) -> Result<String> {
    serde_json::to_string(receipt).map_err(json_err)
}

/// Convert a clock duration to integer nanoseconds for storage, saturating at
/// `i64::MAX` (far beyond any realistic monotonic-since-epoch value).
pub fn nanos(d: Duration) -> i64 {
    i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
}

/// The error a broker returns when a receipt is not the current one for its job
/// (expired or superseded).
pub fn stale(receipt: ReservationReceipt) -> Error {
    Error::StaleReservation(format!("receipt {receipt:?} is not current"))
}

/// Map a JSON (de)serialization failure to a broker error.
pub fn json_err(e: serde_json::Error) -> Error {
    const MAX_LEN: usize = 512;
    let msg = crate::redact::redact_and_truncate(&e.to_string(), MAX_LEN);
    Error::Broker(msg)
}

/// Return `lane`'s string form if it contains no character in `denylist`,
/// otherwise the first offending character.
///
/// A *name-based* broker embeds a lane verbatim in a native key, subject, or
/// queue name and must reject the characters that are structural in *its* scheme
/// (e.g. Redis rejects `:` and the glob metacharacters). This is the shared
/// mechanism for that check: the broker supplies its own `denylist` and maps a
/// rejected character into its own scheme-specific error. A [`Lane`] already
/// guarantees the portable invariant; this is the backend-specific layer on top,
/// kept here in the broker-author SPI so name-based brokers do not each
/// re-implement the scan. Allocates nothing and borrows from `lane`.
pub fn reject_chars<'a>(lane: &'a Lane, denylist: &[char]) -> std::result::Result<&'a str, char> {
    let s = lane.as_str();
    match s.chars().find(|c| denylist.contains(c)) {
        Some(c) => Err(c),
        None => Ok(s),
    }
}

/// Return `lane`'s string form if every character satisfies `allow`, otherwise
/// the first character that does not.
///
/// The allow-list counterpart to [`reject_chars`], for a broker whose key or name
/// scheme is naturally expressed as the *permitted* set (e.g. a subject charset)
/// rather than as a denylist. Allocates nothing and borrows from `lane`.
pub fn allow_only(lane: &Lane, allow: impl Fn(char) -> bool) -> std::result::Result<&str, char> {
    let s = lane.as_str();
    match s.chars().find(|c| !allow(*c)) {
        Some(c) => Err(c),
        None => Ok(s),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_rejects_oversized_input() {
        // An over-cap value is rejected by the length guard before any JSON
        // parse, so a corrupt or hostile record cannot drive a huge allocation.
        let oversized = vec![b' '; MAX_ENVELOPE_BYTES + 1];
        let err = decode_envelope(&oversized).unwrap_err();
        assert!(
            matches!(err, Error::Serialization(_)),
            "over-cap decode must be a serialization error, got {err:?}"
        );
    }

    #[test]
    fn nanos_saturates_at_i64_max() {
        // A duration whose nanoseconds exceed i64::MAX must saturate, never
        // wrap or panic. i64::MAX ns is ~292 years, far beyond any realistic
        // monotonic-since-epoch value, so saturation is the safe ceiling.
        assert_eq!(nanos(Duration::MAX), i64::MAX);
        assert_eq!(nanos(Duration::ZERO), 0);
        assert_eq!(nanos(Duration::from_nanos(1)), 1);
    }

    #[test]
    fn encode_rejects_oversized_trace_context() {
        use crate::envelope::NewJob;
        use std::collections::HashMap;

        // One entry whose value blows past the byte cap.
        let mut tc = HashMap::new();
        tc.insert(
            "tracestate".to_string(),
            "x".repeat(MAX_TRACE_CONTEXT_BYTES),
        );
        let env = NewJob::new(Lane::default(), "k", vec![], 1)
            .with_trace_context(tc)
            .into_envelope();
        let err = encode_envelope(&env).unwrap_err();
        assert!(
            matches!(err, Error::Serialization(_)),
            "oversized trace_context must be rejected, got {err:?}"
        );

        // Too many entries, each tiny.
        let mut tc = HashMap::new();
        for i in 0..=MAX_TRACE_CONTEXT_ENTRIES {
            tc.insert(format!("k{i}"), "v".to_string());
        }
        let env = NewJob::new(Lane::default(), "k", vec![], 1)
            .with_trace_context(tc)
            .into_envelope();
        assert!(matches!(
            encode_envelope(&env).unwrap_err(),
            Error::Serialization(_)
        ));
    }

    #[test]
    fn encode_accepts_a_normal_trace_context() {
        use crate::envelope::NewJob;
        use std::collections::HashMap;

        let mut tc = HashMap::new();
        tc.insert(
            "traceparent".to_string(),
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string(),
        );
        tc.insert("tracestate".to_string(), "vendor=value".to_string());
        let env = NewJob::new(Lane::default(), "k", vec![], 1)
            .with_trace_context(tc)
            .into_envelope();
        assert!(encode_envelope(&env).is_ok());
    }

    #[test]
    fn reject_chars_accepts_a_clean_lane() {
        let lane = Lane::try_from("orders").unwrap();
        assert_eq!(reject_chars(&lane, &[':', '*']), Ok("orders"));
    }

    #[test]
    fn reject_chars_returns_first_denylisted_char() {
        let lane = Lane::try_from("a:b*c").unwrap();
        // `:` precedes `*`, so it is the one reported.
        assert_eq!(reject_chars(&lane, &[':', '*']), Err(':'));
    }

    #[test]
    fn allow_only_accepts_when_all_chars_permitted() {
        let lane = Lane::try_from("orders_v2").unwrap();
        assert_eq!(
            allow_only(&lane, |c| c.is_ascii_alphanumeric() || c == '_'),
            Ok("orders_v2")
        );
    }

    #[test]
    fn allow_only_returns_first_disallowed_char() {
        let lane = Lane::try_from("a.b").unwrap();
        assert_eq!(allow_only(&lane, |c| c.is_ascii_alphanumeric()), Err('.'));
    }
}
