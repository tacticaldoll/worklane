## Why

`SystemClock` anchors its epoch at `Instant::now()` when it is constructed, so
`now()` is monotonic but **process-local**: it measures time since this process
started. `worklane-sqlite` persists `available_at` and `leased_until` as
durations against that epoch. After a process restart, a new `SqliteBroker` gets
a fresh `SystemClock` whose epoch resets to ~0, while the database still holds
large pre-restart durations — so every persisted job has `available_at > now`
and becomes **permanently unreservable**. A durable broker that strands all its
jobs on the first restart undercuts the reason it exists.

## What Changes

- Add a `WallClock` time source whose epoch is the Unix epoch
  (`SystemTime` since `UNIX_EPOCH`), so its absolute values are stable across
  process restarts.
- `SqliteBroker` SHALL default to `WallClock` so persisted visibility and lease
  times remain meaningful after a restart. `InMemoryBroker` keeps `SystemClock`
  (its jobs do not survive a restart, so a process-local monotonic clock is
  correct and preferable).
- Document the trade-off: `WallClock` is not monotonic (subject to wall-clock
  adjustments). This is acceptable — durable times must survive a restart, and a
  clock step degrades gracefully under at-least-once (a backward step at worst
  holds a lease slightly longer; a forward step at worst redelivers early),
  never losing a job.

## Capabilities

### New Capabilities
<!-- None: this adds a clock impl and a requirement on persistent brokers. -->

### Modified Capabilities
- `broker`: add a **Restart-durable time for persisted jobs** requirement — a
  broker that persists jobs across restarts must derive its time from a
  restart-stable epoch so persisted schedules survive a restart. (In-memory
  brokers are exempt: their jobs do not persist.)

## Impact

- `worklane-core`: add `WallClock` beside `SystemClock` in the clock module and
  export it. No `Clock` trait change, no `Broker` trait change.
- `worklane-sqlite`: default to `WallClock` in the file/in-memory constructors
  (the `with_clock` builder still overrides it, e.g. a `ManualClock` for the
  timed conformance tier). Add a restart-durability test: enqueue, drop and
  reopen the same database file with a fresh clock, assert the job is still
  reservable.
- `worklane-memory`: unchanged (keeps `SystemClock`).
