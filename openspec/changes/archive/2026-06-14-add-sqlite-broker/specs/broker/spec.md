## MODIFIED Requirements

### Requirement: Backend-agnostic payloads

The broker SHALL operate only on opaque envelopes and MUST NOT depend on Rust
handler types or inspect payload contents. It SHALL also **preserve** every
envelope field — `id`, `lane`, `kind`, the opaque `payload` bytes, `attempts`,
and `max_attempts` — unchanged across storage and retrieval, returning them
identical from a subsequent `reserve` and in any dead-letter record. An
in-memory broker satisfies this by retaining the value; a durable broker
satisfies it by faithfully reconstructing the envelope from its storage. The
broker MUST NOT alter, re-encode, reorder, or truncate the `payload` bytes.

#### Scenario: Opaque handling

- **WHEN** any broker operation processes a job
- **THEN** it SHALL use only envelope fields (`id`, `lane`, `kind`, `payload`
  bytes, `attempts`, `max_attempts`)
- **AND** it MUST NOT deserialize the payload

#### Scenario: Payload bytes survive a storage round-trip verbatim

- **WHEN** a job whose payload is arbitrary (including non-UTF-8) bytes is
  enqueued and later reserved
- **THEN** the reserved envelope's `payload` SHALL equal the enqueued bytes
  exactly, with no alteration, re-encoding, reordering, or truncation
- **AND** its `kind` and `max_attempts` SHALL also equal the enqueued values
- **AND** its `attempts` SHALL equal the number of prior retries (zero on first
  reservation)
