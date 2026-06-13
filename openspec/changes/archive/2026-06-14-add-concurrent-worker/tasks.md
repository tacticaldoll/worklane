## 1. Concurrency configuration

- [x] 1.1 Add a `concurrency: usize` field to `Worker` (default 1) and a
      `with_concurrency(self, n: usize)` builder; clamp/treat `0` as `1` so a
      worker always makes progress.
- [x] 1.2 Confirm `process_next` and `run_until_idle` are untouched (they stay
      sequential primitives).

## 2. Concurrent run loop (in-task)

- [x] 2.1 Restructure `run` (keeping `&self` and the `run(shutdown: impl Future)`
      signature) to drive up to N `self.process(reservation)` futures in-task via
      `futures_util::stream::FuturesUnordered`.
- [x] 2.2 Top-of-loop non-blocking shutdown probe (biased select over the pinned
      `shutdown` vs `ready`), so a signal fired during/within a handler stops
      reserving between jobs.
- [x] 2.3 Fill spare capacity by reserving while `in_flight.len() < N` and not
      shutting down; idle-wait `poll_interval` (or shutdown) when nothing is in
      flight; when running with spare capacity, also wake on a poll tick to
      re-check the lane.
- [x] 2.4 On shutdown, drain: await `FuturesUnordered` to empty (reserve no
      more) before returning; a non-stale resolution error stops reserving and is
      returned first (fail-fast but drain).
- [x] 2.5 Confirm N=1 is behaviourally identical to the current `run`.

## 3. Tests

- [x] 3.1 N=1 equivalence: existing worker scenarios and facade tests stay green
      unchanged.
- [x] 3.2 Bounded in flight: with concurrency N and >N jobs available, assert at
      most N handlers run simultaneously (e.g. a shared counter + barrier in the
      handler), each resolved with its own receipt.
- [x] 3.3 Shutdown drains all in-flight: fire shutdown while N handlers run;
      assert all complete and resolve before `run` returns.
- [x] 3.4 Lease-too-short redelivery: with a manual clock and a short lease, a
      handler that outlives the lease is redelivered and run again; the original
      resolution is rejected as stale and logged, the worker does not crash.

## 4. DoD

- [x] 4.1 Confirm no change under `crates/worklane-core/` and no `Broker` trait
      change (`git diff`).
- [x] 4.2 `cargo build`, `cargo test`, `cargo clippy --all-targets -- -D warnings`,
      `cargo fmt --all --check` all green across the workspace.

## 5. Record follow-ons

- [x] 5.1 `BACKLOG.md`: record multi-core parallelism (spawn-based executor /
      multiple `run()` futures — this change ships in-task concurrency only), the
      idle thundering-herd consideration (now moot for in-task, but relevant to a
      future spawn-based / network-broker design), and the resilient-daemon error
      option; reaffirm multi-lane / fair scheduling and lease extension as the
      unlocked next steps.
