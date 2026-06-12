## 1. Dependencies and core types (worklane-core)

- [x] 1.1 Add dependencies to `worklane-core`: `serde` (derive), `serde_json`, `async-trait`, `uuid` (v4 + serde), `thiserror`, `tracing`
- [x] 1.2 Define `JobId` as a newtype over `uuid::Uuid` with generation and serde support
- [x] 1.3 Define the `Error` enum (`thiserror`) with `Serialization`, `UnknownKind`, `Handler`, and `Broker` variants, plus a `Result<T>` alias
- [x] 1.4 Define `JobEnvelope { id, kind, payload: Vec<u8>, attempts, max_attempts }` and `NewJob { kind, payload, max_attempts }`

## 2. Job model (worklane-core, capability: job-model)

- [x] 2.1 Define the `Job` trait: associated serde `Payload`, `const KIND: &str`, async `run(ctx, payload)`
- [x] 2.2 Define `JobContext` (job id, attempts, max_attempts) passed to handlers
- [x] 2.3 Add payload (de)serialization helpers (serde_json ↔ bytes) that surface serialization errors without panicking

## 3. Broker contract (worklane-core, capability: broker)

- [x] 3.1 Define the async `Broker` trait: `enqueue`, `reserve(lane)`, `ack`, `retry(delay)`, `fail(error)`
- [x] 3.2 Define `RetryPolicy { base, factor, cap }` with `delay_for(attempts) = min(base * factor^attempts, cap)` (max_attempts is per-job on the envelope)
- [x] 3.3 Define the dead-letter record shape (envelope + last error message)

## 4. In-memory broker (worklane-memory, capability: broker)

- [x] 4.1 Add deps (`worklane-core`, `async-trait`, `tokio`) and a clock seam for deterministic time in tests
- [x] 4.2 Implement `enqueue`: assign `JobId`, set `attempts = 0`, store as visible
- [x] 4.3 Implement `reserve` with a visibility lease: requeue expired leases, return one due + visible job and hide it for the lease, return none when empty
- [x] 4.4 Implement `ack` (remove), `retry` (increment attempts, schedule after delay, end lease), and `fail` (move to dead-letter with error)
- [x] 4.5 Expose dead-letter inspection for tests

## 5. Client (worklane, capability: client)

- [x] 5.1 Implement `Client` over a broker handle with a configurable default `max_attempts`
- [x] 5.2 Implement typed `enqueue::<J>(payload)`: serialize payload, submit `NewJob` to the default lane, return `JobId`; on serialization failure return an error without submitting

## 6. Worker (worklane, capability: worker)

- [x] 6.1 Implement the handler registry keyed by `KIND`; reject duplicate kinds
- [x] 6.2 Implement `register::<J>(handler)` storing a type-erased dispatch that deserializes the payload and runs the handler
- [x] 6.3 Implement the sequential run loop: reserve → dispatch → run → resolve (ack/retry/fail) before reserving the next
- [x] 6.4 Apply `RetryPolicy`: retry below max attempts, dead-letter at max; route unknown-kind and deserialization failures to dead-letter and continue the loop
- [x] 6.5 Add `tracing` spans/logs across reserve, dispatch, and resolution

## 7. Facade exports (worklane)

- [x] 7.1 Re-export the public surface from `worklane`: `Job`, `JobContext`, `JobId`, `Error`, `Result`, `Broker`, `RetryPolicy`, `Client`, `Worker`

## 8. Example and tests

- [x] 8.1 Add a runnable in-memory example (`cargo run --example basic`): a `SendEmail` job, enqueue, then run the worker to completion
- [x] 8.2 Test: enqueue → reserve → ack happy path
- [x] 8.3 Test: retry increments `attempts` and respects the delay (via the clock seam)
- [x] 8.4 Test: dead-letter after max attempts, with the error retained
- [x] 8.5 Test: unknown job kind fails predictably to dead-letter and the loop continues
- [x] 8.6 Test: payload round-trip, and corrupt payload yields an error without panicking
- [x] 8.7 Test: lease expiry makes a reserved-but-unresolved job reservable again

## 9. Docs and Definition of Done

- [x] 9.1 Update `README.md` with one working in-memory example
- [x] 9.2 Document the at-least-once / handler-idempotency expectation
- [x] 9.3 Confirm the Definition of Done: `cargo fmt --all --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test` all pass
