## 1. Core types (worklane-core)

- [x] 1.1 Add `lane: String` to `NewJob` and mark `NewJob` `#[non_exhaustive]`
- [x] 1.2 Add `lane: String` to `JobEnvelope` and mark `JobEnvelope` `#[non_exhaustive]`
- [x] 1.3 Mark the other growable public types `#[non_exhaustive]` per AGENTS.md (`JobContext`, `Reservation`, `DeadLetter`, `Error`)
- [x] 1.4 `cargo build` to confirm the core crate compiles

## 2. In-memory broker (worklane-memory)

- [x] 2.1 Carry `NewJob.lane` onto the stored `JobEnvelope` in `enqueue`
- [x] 2.2 Filter `reserve(lane)` candidates to jobs whose envelope lane equals `lane` (keep existing visibility/lease logic)
- [x] 2.3 Confirm `fail` dead-letters retain the lane (free via the envelope; verify by test)

## 3. Client lane routing (worklane)

- [x] 3.1 Add a default lane field to `Client` initialized from `DEFAULT_LANE`
- [x] 3.2 Add `Client::with_lane(impl Into<String>)` consuming builder, symmetric with `with_max_attempts`
- [x] 3.3 Set `NewJob.lane` from the client's lane in `enqueue`
- [x] 3.4 Make `DEFAULT_LANE` the single shared default used by both `Client` and `Worker` (one constant, no duplicated literal)

## 4. Tests

- [x] 4.1 Regression: default-lane enqueue is reserved and run by a default-lane worker (existing core-loop still passes)
- [x] 4.2 A worker on a custom lane receives a job enqueued to that lane
- [x] 4.3 A worker on lane A does NOT reserve a job enqueued to lane B; the lane B job stays reservable on B
- [x] 4.4 Two lanes interleaved: each worker only gets its own lane's job, no cross-contamination
- [x] 4.5 `reserve` on a lane with no jobs (while another lane has jobs) returns `None`
- [x] 4.6 A dead-lettered job retains its lane on the `DeadLetter` envelope

## 5. Definition of Done

- [x] 5.1 `cargo build` passes
- [x] 5.2 `cargo test` passes
- [x] 5.3 `cargo clippy --all-targets -- -D warnings` is clean
- [x] 5.4 `cargo fmt --all --check` passes
- [x] 5.5 README quick-start still compiles/runs under the default lane (no doc change expected)
