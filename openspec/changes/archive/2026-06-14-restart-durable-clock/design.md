## Context

The `Clock` seam (`worklane-core`) abstracts time so brokers derive visibility,
lease, and retry decisions from an injectable source. The only production impl,
`SystemClock`, uses `Instant::now()` as its epoch: monotonic, immune to
wall-clock adjustments, but **process-local** — `now()` is time since this
process started. `worklane-sqlite` stores `available_at` / `leased_until` as
nanoseconds against that epoch (see `nanos(self.clock.now())`).

That pairing is wrong for a durable backend: across a restart the epoch resets,
so persisted absolute times are nonsense and every job is stranded (the
`reserve` predicate `available_at <= now` never holds). `add-sqlite-broker`
already flagged this in BACKLOG ("restart-durable clock"); it is now the live
durability gap.

## Goals / Non-Goals

**Goals:**
- A `SqliteBroker` that survives a process restart with the same database file:
  persisted visibility and lease schedules stay meaningful.
- Keep the `Clock` trait and `Broker` trait unchanged; this is a clock-impl and
  default-selection change only.

**Non-Goals:**
- Changing the in-memory broker (its jobs do not persist; monotonic local time
  is correct and nicer for tests).
- A hybrid monotonic-with-persisted-epoch clock (more complex; deferred unless a
  wall-clock-jump problem is actually observed).
- Clock-skew correction / NTP smoothing — out of scope.

## Decisions

### Decision 1: add `WallClock` (Unix-epoch) beside `SystemClock` in core

```rust
pub struct WallClock;
impl Clock for WallClock {
    fn now(&self) -> Duration {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO) // pre-1970 system clock: clamp, never panic
    }
}
```

Its epoch is the Unix epoch — identical across processes — so a duration
persisted before a restart is directly comparable to `now()` after one. It lives
in `worklane-core`'s clock module next to `SystemClock`: `Clock` is a core seam
and the other concrete clock already lives there; splitting clock impls across
crates would be worse than the marginal least-commitment cost.

*Alternative considered:* persist a monotonic epoch anchor so `SystemClock` could
stay monotonic and restart-durable. Rejected for now: materially more complex,
and no consumer needs monotonicity guarantees that wall-clock time violates in
practice (see Decision 3).

### Decision 2: `SqliteBroker` defaults to `WallClock`; in-memory keeps `SystemClock`

The durable broker's constructors (`open`, `open_in_memory`) default to
`WallClock`. `with_clock` still overrides it, so the timed conformance tier keeps
injecting a `ManualClock`. `InMemoryBroker` is unchanged: nothing it holds
survives a restart, so a process-local monotonic clock is both correct and
better for determinism.

Note `open_in_memory` is itself non-persistent, but it defaults to `WallClock`
too so its time semantics match the on-disk broker it stands in for.

### Decision 3: accept non-monotonicity, rely on graceful degradation

`WallClock` can step backward (NTP correction). Under at-least-once this is safe:
a backward step at worst holds a lease slightly longer before it expires; a
forward step at worst makes a job visible early and it is redelivered. Neither
loses or duplicates beyond what at-least-once already permits. Monotonicity is
not worth the persisted-anchor complexity here.

## Risks / Trade-offs

- [Wall-clock step backward briefly delays lease expiry / step forward redelivers
  early] → Accepted: bounded, safe under at-least-once; documented on `WallClock`.
- [`SystemTime` before `UNIX_EPOCH` yields an error] → `unwrap_or(Duration::ZERO)`
  clamps; a misconfigured system clock degrades, never panics.
- [Restart-durability cannot be exercised by the shared conformance suite] →
  Correct: the suite is broker-agnostic and the in-memory broker cannot restart.
  Verified instead by a `worklane-sqlite` test that reopens a file-backed DB.

## Migration Plan

Additive: `WallClock` is new; the `SqliteBroker` default changes from
`SystemClock` to `WallClock` (a behaviour fix, not an API change — `with_clock`
still overrides). No `Clock` or `Broker` trait change. Existing sqlite databases
written under the old process-local clock were already meaningless across a
restart, so there is no regression and no migration of stored values.

## Open Questions

None outstanding.
