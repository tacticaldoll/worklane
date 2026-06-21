# Payload Store Specification

## Purpose

Defines optional claim-check payload storage for envelopes whose serialized
payload would exceed the broker envelope size limit.

## Requirements

### Requirement: Payload Store Claim Check

The system SHALL support an optional payload store used as a claim-check backend
for envelopes whose serialized payload would exceed
`worklane_core::spi::MAX_ENVELOPE_BYTES`. When offload is used, broker envelopes
SHALL carry an opaque reference that can be resolved by the worker before
handler dispatch.

#### Scenario: Oversized payload is offloaded

- **WHEN** a client with a configured payload store enqueues a payload larger
  than the envelope cap
- **THEN** the payload bytes SHALL be stored in the payload store
- **AND** the broker envelope SHALL carry a claim-check reference instead of the
  full payload bytes

#### Scenario: Worker resolves claim-check reference

- **WHEN** a worker reserves an envelope containing a claim-check reference
- **THEN** it SHALL load the payload bytes from the payload store before
  deserializing the job input

#### Scenario: Missing payload reference fails predictably

- **WHEN** a worker cannot resolve a claim-check reference
- **THEN** it SHALL route the job through the normal unrecoverable payload
  failure path

### Requirement: Payload Store Cleanup

Payload-store writes performed before broker submission SHALL be cleaned up on a
best-effort basis if the broker submission fails or deduplicates the offloaded
job away. Cleanup failures MUST NOT replace the original broker result.

#### Scenario: Single enqueue failure cleans up

- **WHEN** a single-job enqueue offloads payload bytes
- **AND** broker submission fails
- **THEN** the client SHALL attempt to delete the offloaded bytes
- **AND** the enqueue call SHALL return the original broker error

#### Scenario: Batch enqueue failure cleans up all offloads

- **WHEN** a fan-out or chord enqueue offloads one or more payloads
- **AND** broker batch submission fails
- **THEN** the client SHALL attempt to delete every payload offloaded for that
  failed submission
- **AND** the enqueue call SHALL return the original broker error

#### Scenario: Deduplicated offload cleans up dropped payload

- **WHEN** a unique-key enqueue offloads payload bytes
- **AND** the broker returns an existing live job ID for that unique key
- **THEN** the client SHALL attempt to delete the offloaded bytes for the
  dropped job
