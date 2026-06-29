## 1. Implementation

- [ ] 1.1 Make the handler future `'static`: refactor the internal middleware
  chain `Next` (`pub(super)`) to **own** its `Arc`s instead of borrowing
  `self.middleware`/`&dyn Dispatch`, and own the payload (`Cow::Owned`). Confirm
  `Next` has no caller needing the borrowed form (internal, no public change).
- [ ] 1.2 In `run_maintained` (`crates/worklane/src/worker/execution.rs`), spawn
  the (now `'static`) handler future on its own task with `tokio::spawn`.
- [ ] 1.3 Replace the inline `select!(handler vs deadline)` with a `select!` that
  races the timeout `deadline` against the handler task's `JoinHandle`; on the
  deadline arm, `abort()` the handle, signal `Cancellation`, and return
  `TimedOut`; on the handler arm, return the joined result, mapping a
  `JoinError::is_panic()` to `Error::Handler` so panic isolation is preserved on
  the spawned path (the no-timeout inline path keeps its `catch_unwind`).
- [ ] 1.4 Keep the no-timeout/no-keepalive path running the handler inline with
  no heartbeat (unchanged); keep the heartbeat task and `AbortOnDrop` teardown.
- [ ] 1.5 Tie the spawned handler task's lifetime to the maintained scope (abort
  on drop) so a hard-cancelled `run` does not orphan the handler beyond the
  worker, consistent with the existing heartbeat teardown.

## 2. Tests

- [ ] 2.1 Multi-thread test (`#[tokio::test(flavor = "multi_thread", worker_threads = 2)]`,
  concurrency ≥ 2): a handler that blocks via `std::thread::sleep` past its
  timeout (deterministic — not a busy CPU loop) is failed/redelivered and its
  slot freed so a second job completes meanwhile. The pre-change inline path
  would wedge instead.
- [ ] 2.2 Verify the existing behaviors still hold: cooperative slow handler kept
  alive by heartbeat and acked once; timed-out cooperative handler retried then
  dead-lettered; default (no timeout/keepalive) neither heartbeats nor times out;
  lost-lease and heartbeat-transport-failure paths unchanged.
- [ ] 2.3 Panic isolation holds on BOTH paths: a panicking handler routes to the
  failure path and the worker survives — once with a timeout configured (spawned
  path → `JoinError`) and once without (inline path → `catch_unwind`).
- [ ] 2.4 Confirm no public API/bound change: the `Worker` / `Job` surface and
  handler signatures compile unchanged (the build itself is the check). The
  thread-saturation residual (R1) is a documented limitation, not a tested
  guarantee.

## 3. Spec sync and docs

- [ ] 3.1 Sync the `worker` delta into `openspec/specs/worker/spec.md` (the
  modified **Bounded long-handler support** requirement and its scenarios).
- [ ] 3.2 Update `docs/known-limitations.md`: the timeout now fires for a
  non-yielding handler (slot freed, job redelivered), but a non-yielding CPU task
  is not preempted (orphan runs to its next yield) and a current-thread runtime
  still blocks — use `spawn_blocking`.
- [ ] 3.3 Add a `CHANGELOG.md` `[Unreleased]` entry (Changed/Fixed): handler
  timeout now bounds non-yielding handlers on a multi-thread runtime.

## 4. Definition of Done

- [ ] 4.1 `cargo build`, `cargo test`, `cargo clippy --all-targets -- -D warnings`,
  `cargo fmt --all --check`, `cargo deny check`, and
  `cargo run -p worklane-governance -- check --manifest-path Cargo.toml` all pass.
- [ ] 4.2 Update `BACKLOG.md` with the ✓ shipped status after archiving (remove
  the "structural handler decoupling" Worker follow-up and its incorrect
  breaking-change rationale).
