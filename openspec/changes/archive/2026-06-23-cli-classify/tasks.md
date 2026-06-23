## 1. CLI command

- [x] 1.1 Add a `Classify { job_id, format }` variant to the `Commands` enum in
  `crates/worklane-cli/src/main.rs`, with doc comments matching the existing
  commands and a `--format` option reusing the same format type as the
  dead-letter listing command.
- [x] 1.2 Add `crates/worklane-cli/src/cmd/classify.rs`: parse `<job-id>` into a
  `JobId` and return a non-zero parse error *before* connecting on invalid input;
  call `broker.classify(id)`; render the `JobState` as a human-readable line by
  default and as a JSON object under `--format json`.
- [x] 1.3 Register the handler in `crates/worklane-cli/src/cmd/mod.rs` and the
  dispatch in `main.rs`.

## 2. Tests

- [x] 2.1 Following the existing `worklane-cli` test pattern, cover: a live job →
  `Live`; a dead-lettered job → `DeadLettered`; an acked or never-seen id →
  `CompletedOrUnknown`; an unparseable id → non-zero exit with no broker
  connection; `--format json` output shape.

## 3. Documentation

- [x] 3.1 Update any CLI docs/README that enumerate `wl` commands to include
  `classify`.

## 4. Verification (Definition of Done)

- [x] 4.1 Run `cargo fmt --all --check`.
- [x] 4.2 Run `cargo build`.
- [x] 4.3 Run `cargo test`.
- [x] 4.4 Run `cargo clippy --all-targets -- -D warnings`.
- [x] 4.5 Manually run `wl classify <uuid> --broker sqlite --db <path>` and
  confirm the printed state matches the job's actual lifecycle state.

## 5. Closeout

- [x] 5.1 Update BACKLOG.md with the ✓ shipped status after archiving.
