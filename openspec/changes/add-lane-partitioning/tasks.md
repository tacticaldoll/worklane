## 1. Core types (worklane-core)

- [ ] 1.1 Add `lane: String` to `NewJob` and mark `NewJob` `#[non_exhaustive]`
- [ ] 1.2 Add `lane: String` to `JobEnvelope` and mark `JobEnvelope` `#[non_exhaustive]`
- [ ] 1.3 Mark the other growable public types `#[non_exhaustive]` per AGENTS.md (`JobContext`, `Reservation`, `DeadLetter`, `Error`)
- [ ] 1.4 `cargo build` to confirm the core crate compiles

## 2. In-memory broker (worklane-memory)

- [ ] 2.1 Carry `NewJob.lane` onto the stored `JobEnvelope` in `enqueue`
- [ ] 2.2 Filter `reserve(lane)` candidates to jobs whose envelope lane equals `lane` (keep existing visibility/lease logic)
- [ ] 2.3 Confirm `fail` dead-letters retain the lane (free via the envelope; verify by test)

## 3. Client lane routing (worklane)

- [ ] 3.1 Add a default lane field to `Client` initialized from `DEFAULT_LANE`
- [ ] 3.2 Add `Client::with_lane(impl Into<String>)` consuming builder, symmetric with `with_max_attempts`
- [ ] 3.3 Set `NewJob.lane` from the client's lane in `enqueue`
- [ ] 3.4 Make `DEFAULT_LANE` the single shared default used by both `Client` and `Worker` (one constant, no duplicated literal)

## 4. Tests

- [ ] 4.1 Regression: default-lane enqueue is reserved and run by a default-lane worker (existing core-loop still passes)
- [ ] 4.2 A worker on a custom lane receives a job enqueued to that lane
- [ ] 4.3 A worker on lane A does NOT reserve a job enqueued to lane B; the lane B job stays reservable on B
- [ ] 4.4 Two lanes interleaved: each worker only gets its own lane's job, no cross-contamination
- [ ] 4.5 `reserve` on a lane with no jobs (while another lane has jobs) returns `None`
- [ ] 4.6 A dead-lettered job retains its lane on the `DeadLetter` envelope

## 5. Definition of Done

- [ ] 5.1 `cargo build` passes
- [ ] 5.2 `cargo test` passes
- [ ] 5.3 `cargo clippy --all-targets -- -D warnings` is clean
- [ ] 5.4 `cargo fmt --all --check` passes
- [ ] 5.5 README quick-start still compiles/runs under the default lane (no doc change expected)
