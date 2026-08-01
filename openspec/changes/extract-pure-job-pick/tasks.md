## 1. Extract

- [x] 1.1 Add `fn pick_best(jobs: &[StoredJob], lane: &Lane, now: Duration) ->
      Option<usize>` containing the existing scan logic, unchanged.
- [x] 1.2 Update `reserve` to call `pick_best` instead of scanning inline.

## 2. Verification

- [x] 2.1 Add a `#[cfg(test)] mod tests` in `crates/worklane-memory/src/lib.rs`
      with unit tests for `pick_best`: lane filtering, lease-visibility
      filtering, `available_at` visibility filtering, priority ordering, and
      the available-at tie-break.
- [x] 2.2 Run the full Definition of Done (existing `Broker` contract tests must
      pass unmodified — the regression backstop for this refactor).

## 3. Sync and archive

- [ ] 3.1 No capability spec changes to sync.
- [ ] 3.2 Archive this change.
