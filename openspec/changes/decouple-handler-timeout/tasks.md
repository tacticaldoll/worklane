## 1. Implementation

- [x] 1.1 Make the handler future `'static` **without changing the public `Next`
  type**: build an owned async wrapper that moves in the cloned middleware
  `Arc`s, the dispatch `Arc`, the context, and the owned payload, and runs the
  existing borrowing `Next` chain over those owned locals internally. (`Next` is
  `pub` — part of the `Middleware::handle` signature — so refactoring it would be
  breaking; the wrapper avoids touching it.)
- [x] 1.2 In `run_maintained` (`crates/worklane/src/worker/execution.rs`), spawn
  the (now `'static`) handler future on its own task with `tokio::spawn`.
- [x] 1.3 Replace the inline `select!(handler vs deadline)` with a `select!` that
  races the timeout `deadline` against the handler task's `JoinHandle`; on the
  deadline arm, signal `Cancellation` and return `TimedOut` (dropping the
  `AbortOnDrop` guard aborts the handle); on the handler arm, return the joined
  result, mapping a `JoinError::is_panic()` to `Error::Handler` so panic
  isolation is preserved on the spawned path (the no-timeout inline path keeps
  its `catch_unwind`).
- [x] 1.4 Keep the no-timeout/no-keepalive path running the handler inline with
  no heartbeat (unchanged); keep the heartbeat task and `AbortOnDrop` teardown.
- [x] 1.5 Tie the spawned handler task's lifetime to the maintained scope via a
  generic `AbortOnDrop<T>` guard so a hard-cancelled `run` aborts the handler
  rather than detaching it, consistent with the existing heartbeat teardown.

## 2. Tests

- [x] 2.1 Multi-thread test (`#[tokio::test(flavor = "multi_thread", worker_threads = 4)]`,
  concurrency 2): a handler that blocks via `std::thread::sleep` past its timeout
  (deterministic — not a busy CPU loop) is dead-lettered with a timeout error and
  its slot freed so a second job completes meanwhile. The pre-change inline path
  would wedge instead.
- [x] 2.2 Verify the existing behaviors still hold: cooperative slow handler kept
  alive by heartbeat and acked once; timed-out cooperative handler retried then
  dead-lettered; default (no timeout/keepalive) neither heartbeats nor times out;
  lost-lease and heartbeat-transport-failure paths unchanged.
- [x] 2.3 Panic isolation holds on BOTH paths: a panicking handler routes to the
  failure path and the worker survives — once with a timeout configured (spawned
  path → `JoinError`, new test) and once without (inline path → `catch_unwind`,
  existing `panic_isolation.rs`).
- [x] 2.4 Confirm no public API/bound change: the `Worker` / `Job` / `Next`
  surface and handler signatures compile unchanged (the build is the check). The
  thread-saturation residual (R1) is a documented limitation, not a tested
  guarantee.

## 3. Spec sync and docs

- [ ] 3.1 Sync the `worker` delta into `openspec/specs/worker/spec.md` (the
  modified **Bounded long-handler support** requirement and its scenarios). —
  *sync phase*
- [x] 3.2 Update `docs/known-limitations.md`: the timeout now fires for a
  non-yielding handler (slot freed, job redelivered) given a free worker thread,
  but a non-yielding CPU task is not preempted (orphan runs to its next yield),
  and thread saturation or a current-thread runtime still stalls — use
  `spawn_blocking`.
- [x] 3.3 Add a `CHANGELOG.md` `[Unreleased]` **Fixed** entry: handler timeout
  now bounds non-yielding handlers on a multi-thread runtime.

## 4. Definition of Done

- [x] 4.1 `cargo build`, `cargo test` (affected: worklane / -core / -memory),
  `cargo clippy --all-targets -- -D warnings`, `cargo fmt --all --check`, and
  `cargo run -p worklane-governance -- check --manifest-path Cargo.toml` all pass.
  (`cargo deny check` is CI-only — not installed locally.)
- [ ] 4.2 Update `BACKLOG.md` with the ✓ shipped status after archiving (remove
  the "structural handler decoupling" Worker follow-up and its incorrect
  breaking-change rationale). — *archive phase*
