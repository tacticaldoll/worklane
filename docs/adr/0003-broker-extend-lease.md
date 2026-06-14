# 3. Add Broker::extend for lease renewal

Date: 2026-06-14

## Status

Accepted

## Context

`add-sqlite-broker` validated the `Broker` trait against a durable backend
without changing it, establishing that the contract is portable rather than
in-memory-shaped. `add-concurrent-worker` then made lease-too-short real: a
handler that outlives its reservation lease is redelivered and runs twice, and
its later resolution is rejected as stale.

`bounded-long-handler-support` needs a way to hold a reservation past its lease
while a handler is legitimately still running (a heartbeat). This requires a new
`Broker` method — the first deliberate change to the trait since it was
durable-validated. AGENTS.md anticipates this: the validation exists so the
first required trait change is made "while it is still cheap," recorded in an
ADR.

## Decision

Add one required method to the `Broker` trait:

```rust
async fn extend(&self, receipt: ReservationReceipt) -> Result<()>;
```

`extend` re-applies the broker's own configured lease to the job currently held
under `receipt`, measured from now, and rejects an unknown / superseded /
expired receipt with `Error::StaleReservation` without mutating the job. It
takes no caller-supplied duration: lease policy is broker-owned (as for
`reserve`), whereas `retry(receipt, delay)` takes a delay because backoff is the
worker's policy.

`Reservation` additionally gains a `lease: Duration` field (additive;
`Reservation` is `#[non_exhaustive]`) so a caller can time its heartbeat without
reading the broker's clock.

This is **BREAKING** for any external `Broker` implementor (a new required
method); acceptable now because the only implementors are in-repo
(`worklane-memory`, `worklane-sqlite`) and the trait was validated precisely to
make this change cheap.

## Broker design gate

`extend` is answerable on a SQL/Redis backend, so it does not bind the trait to
an in-memory shape. The SQLite implementation is a single guarded statement,
reusing the same `leased_until > now` validity check as `ack`/`retry`/`fail`:

```sql
UPDATE jobs SET leased_until = :now + :lease
WHERE receipt = :receipt AND leased_until > :now;   -- 0 rows ⇒ StaleReservation
```

## Consequences

- Both in-repo brokers implement `extend` and re-run the full `worklane-test`
  conformance suite, which gains `extend` and observable-lease scenarios.
- External implementors (none today) must add `extend` to compile.
- `Reservation` carrying its lease becomes part of the reserve contract that
  every broker honours.
