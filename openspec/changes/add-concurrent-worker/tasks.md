## 1. Concurrency configuration

- [ ] 1.1 Add a `concurrency: usize` field to `Worker` (default 1) and a
      `with_concurrency(self, n: usize)` builder; clamp/treat `0` as `1` so a
      worker always makes progress.
- [ ] 1.2 Confirm `process_next` and `run_until_idle` are untouched (they stay
      sequential primitives).

## 2. Concurrent run loop

- [ ] 2.1 Make the per-worker loop body callable from a spawned task: restructure
      so `run` operates over `Arc<Self>` (each task gets a clone) without changing
      the public `run(shutdown: impl Future)` signature.
- [ ] 2.2 Add a `tokio::sync::watch<bool>` shutdown channel; spawn a small task
      that awaits the caller's `shutdown` future and sets it `true`.
- [ ] 2.3 Spawn N loops (N = concurrency), each: stop when the watch is `true`,
      else `process_next`; on `Ok(true)` continue, on `Ok(false)` wait
      `poll_interval` or the watch, on a non-stale `Err` set the watch and return
      the error.
- [ ] 2.4 Join all N tasks (drain), returning the first error if any; in-flight
      jobs run to completion and resolve before `run` returns.
- [ ] 2.5 Confirm N=1 collapses to a single loop behaviourally identical to the
      current `run`.

## 3. Tests

- [ ] 3.1 N=1 equivalence: existing worker scenarios and facade tests stay green
      unchanged.
- [ ] 3.2 Bounded in flight: with concurrency N and >N jobs available, assert at
      most N handlers run simultaneously (e.g. a shared counter + barrier in the
      handler), each resolved with its own receipt.
- [ ] 3.3 Shutdown drains all in-flight: fire shutdown while N handlers run;
      assert all complete and resolve before `run` returns.
- [ ] 3.4 Lease-too-short redelivery: with a manual clock and a short lease, a
      handler that outlives the lease is redelivered and run again; the original
      resolution is rejected as stale and logged, the worker does not crash.

## 4. DoD

- [ ] 4.1 Confirm no change under `crates/worklane-core/` and no `Broker` trait
      change (`git diff`).
- [ ] 4.2 `cargo build`, `cargo test`, `cargo clippy --all-targets -- -D warnings`,
      `cargo fmt --all --check` all green across the workspace.

## 5. Record follow-ons

- [ ] 5.1 `BACKLOG.md`: record the idle thundering-herd (N empty reserves per
      poll interval; single-reserver / shared idle wake deferred to an
      expensive-reserve network broker) and the resilient-daemon error option;
      reaffirm multi-lane / fair scheduling and lease extension as the unlocked
      next steps.
