//! The Claim Check pattern: store an oversized job payload externally and carry
//! only a small reference inside the job.
//!
//! A [`JobEnvelope`](crate::JobEnvelope) carries its payload inline, which is the
//! right default — most payloads are small. A few are not (a rendered document, a
//! large batch), and inlining those bloats every store, read, and redelivery. The
//! Claim Check pattern keeps the queue lean: the large bytes go to a
//! [`PayloadStore`] under an opaque key, and the job carries a compact
//! [`claim_check`] reference in place of the payload. The consumer resolves the
//! reference back to the bytes before running, and deletes them once the job
//! succeeds.
//!
//! This module defines only the *contract* ([`PayloadStore`]) and the *reference
//! codec* ([`claim_check`]). The `worklane` facade provides a filesystem store and
//! a `ClaimCheck` helper that wire these into enqueue/dispatch.

use async_trait::async_trait;

use crate::error::Result;

/// An external store for oversized job payloads (the Claim Check pattern).
///
/// Implementations map an opaque key to a blob. Keys are minted by [`put`] and are
/// otherwise opaque to callers. A store is consulted off the hot path (only for
/// payloads large enough to offload), so a simple, durable implementation
/// (filesystem, object storage) is appropriate.
///
/// **Keys passed to [`get`]/[`delete`] are untrusted.** They are decoded from a
/// job's payload reference, which an untrusted producer can forge. An
/// implementation whose key namespace has traversal semantics (a filesystem path,
/// an object-store prefix) **must validate** that a key is one it could have minted
/// before resolving it — otherwise a forged key (`../../etc/passwd`, an absolute
/// path) can escape the store. Validate at this boundary, not in the caller.
///
/// [`put`]: PayloadStore::put
/// [`get`]: PayloadStore::get
/// [`delete`]: PayloadStore::delete
#[async_trait]
pub trait PayloadStore: Send + Sync {
    /// Store `payload` and return its opaque key. The key is later passed to
    /// [`get`](PayloadStore::get) / [`delete`](PayloadStore::delete).
    async fn put(&self, payload: &[u8]) -> Result<String>;

    /// Fetch the payload stored under `key`, or `None` if no such key exists.
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Delete the payload stored under `key`. Deleting an absent key is **not** an
    /// error — `delete` is idempotent, so a redelivered or double-acked job cannot
    /// fail on a second delete.
    async fn delete(&self, key: &str) -> Result<()>;
}

/// The reference codec: encode/decode the compact marker a job carries in place of
/// an offloaded payload.
///
/// A reference is a fixed magic prefix followed by the store key as UTF-8. The
/// prefix begins with a `NUL` byte, which a serde-JSON payload (the only kind
/// `worklane`'s typed jobs produce) can never begin with — so a genuine payload is
/// never mistaken for a reference, and the check is collision-free rather than a
/// heuristic.
pub mod claim_check {
    /// Magic prefix: `NUL` + `"WLCC"` + version byte `0x01`. The leading `NUL`
    /// cannot start a valid JSON value, so this prefix cannot collide with a real
    /// serialized payload.
    const MAGIC: &[u8] = b"\x00WLCC\x01";

    /// Build the reference a job carries in place of an offloaded payload.
    pub fn make_reference(key: &str) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(MAGIC.len() + key.len());
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(key.as_bytes());
        bytes
    }

    /// The store key if `payload` is a claim-check reference, else `None`. Returns
    /// `None` for a payload that bears the magic prefix but whose key is not valid
    /// UTF-8 (a corrupt reference is treated as "not a reference", never a panic).
    pub fn reference_key(payload: &[u8]) -> Option<&str> {
        let rest = payload.strip_prefix(MAGIC)?;
        std::str::from_utf8(rest).ok()
    }

    /// Whether `payload` is a claim-check reference (bears the magic prefix).
    pub fn is_reference(payload: &[u8]) -> bool {
        payload.starts_with(MAGIC)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn round_trips_a_key() {
            let r = make_reference("abc/123");
            assert!(is_reference(&r));
            assert_eq!(reference_key(&r), Some("abc/123"));
        }

        #[test]
        fn a_normal_json_payload_is_not_a_reference() {
            for p in [&b"{}"[..], b"[1,2,3]", b"\"s\"", b"42", b"null", b"true"] {
                assert!(
                    !is_reference(p),
                    "JSON payload {p:?} must not look like a reference"
                );
                assert_eq!(reference_key(p), None);
            }
        }

        #[test]
        fn empty_payload_is_not_a_reference() {
            assert!(!is_reference(b""));
            assert_eq!(reference_key(b""), None);
        }

        #[test]
        fn magic_prefix_with_invalid_utf8_key_is_not_a_reference() {
            let mut bytes = make_reference("");
            bytes.extend_from_slice(&[0xff, 0xfe]); // invalid UTF-8 tail
            assert!(is_reference(&bytes), "the prefix is present");
            assert_eq!(
                reference_key(&bytes),
                None,
                "but a non-UTF-8 key yields no key"
            );
        }
    }
}
