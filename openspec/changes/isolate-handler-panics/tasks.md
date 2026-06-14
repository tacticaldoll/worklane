## 1. Worker: contain handler panics

- [ ] 1.1 In `worker/execution.rs`, wrap the dispatch future in `AssertUnwindSafe(..).catch_unwind()` and `map` the result back to `Result<()>`, mapping a caught panic to `Error::Handler` via a `panic_message` helper.
- [ ] 1.2 Confirm both paths consume the wrapped future unchanged: the no-timeout `handler.await` and the `run_bounded` race (a panic surfaces as `Err` → `handle_failure`).
- [ ] 1.3 Add the `panic_message` helper (downcast `&str` / `String`, generic fallback) with a `format!("handler panicked: {msg}")` shape.

## 2. Tests

- [ ] 2.1 Test: a handler that panics on its final attempt is dead-lettered with a panic error; the worker keeps processing.
- [ ] 2.2 Test: a handler that panics below max attempts is retried, and a later non-panicking attempt acks.
- [ ] 2.3 Test: under concurrency, one panicking handler does not crash the worker or abandon sibling in-flight jobs — siblings still complete and resolve, and `run` returns cleanly on shutdown.

## 3. Definition of Done

- [ ] 3.1 `cargo build`, `cargo test`, `cargo clippy --all-targets -- -D warnings`, and `cargo fmt --all --check` all pass.
- [ ] 3.2 Verify the change with `openspec validate isolate-handler-panics --strict`.
