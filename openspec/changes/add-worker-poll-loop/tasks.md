## 1. Poll loop on the worker

- [ ] 1.1 Add a `poll_interval: Duration` field to `Worker` with a sensible default (~1s) and a `with_poll_interval` builder
- [ ] 1.2 Add `Worker::run(&self, shutdown: impl Future<Output = ()>) -> Result<()>`: drain currently available jobs via `process_next`, and when idle `tokio::select!` between the (pinned) shutdown future and `tokio::time::sleep(poll_interval)`, breaking on shutdown
- [ ] 1.3 Ensure the shutdown signal is checked only between jobs (in-flight job always completes and resolves first); keep `process_next` and `run_until_idle` unchanged
- [ ] 1.4 Confirm `worklane` builds (tokio `time` feature is already available via the workspace dep)

## 2. Tests (deterministic, no real sleeping)

- [ ] 2.1 `run` drains all currently available jobs, then returns when shutdown fires while idle
- [ ] 2.2 Idle pickup: spawn `run`; with no job it idles; enqueue an immediately-visible job; `tokio::time::advance(poll_interval)`; assert the job is processed (use `#[tokio::test(start_paused = true)]`, drive deterministically, guard with `tokio::time::timeout`)
- [ ] 2.3 Cooperative shutdown mid-job: a handler that signals shutdown while running still completes and is acked before `run` returns
- [ ] 2.4 `with_poll_interval` overrides the default wait

## 3. Definition of Done

- [ ] 3.1 `cargo build` passes
- [ ] 3.2 `cargo test` passes (new poll-loop tests plus existing suites intact)
- [ ] 3.3 `cargo clippy --all-targets -- -D warnings` is clean
- [ ] 3.4 `cargo fmt --all --check` passes
- [ ] 3.5 `worklane-core` and `worklane-memory` are unchanged; `Broker` trait and `Clock` are untouched
