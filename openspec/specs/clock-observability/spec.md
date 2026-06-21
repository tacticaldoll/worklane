# clock-observability Specification

## Purpose
Defines opt-in forward-jump observability on `WallClock`: a configurable
threshold that logs a warning and increments a `forward_jumps()` counter when
wall-clock time advances beyond it, so an operator can correlate a clock event
with the widened at-least-once duplicate-execution window. Detection never
changes the value `now` returns or any lease/visibility math.
## Requirements
### Requirement: Opt-in forward clock-jump detection

`WallClock` SHALL support an optional forward-jump threshold. When configured and
a reading of the current time advances beyond the previous reading by more than
the threshold, the clock SHALL emit a `tracing` warning carrying the observed
forward delta and SHALL increment an observable best-effort counter of such
events. Under concurrent reads, the counter is advisory observability and MUST
NOT be used as an exact event log. When no threshold is configured, the clock
SHALL emit nothing and SHALL behave exactly as without this capability.
Thresholds below 1ms SHALL be clamped to 1ms to avoid warning floods from a zero
or near-zero threshold. Detection SHALL NOT alter the value returned by `now`,
nor any lease, visibility, or scheduling computation.

#### Scenario: Jump beyond threshold warns

- **WHEN** a `WallClock` configured with a forward-jump threshold observes a
  time advance greater than the threshold between two readings
- **THEN** it SHALL emit a warning recording the observed delta
- **AND** it SHALL increment its forward-jump counter

#### Scenario: Movement within threshold is silent

- **WHEN** a configured `WallClock` advances by less than the threshold
- **THEN** it SHALL emit no warning
- **AND** the counter SHALL be unchanged

#### Scenario: No threshold means no observation

- **WHEN** a `WallClock` is used without a configured threshold
- **THEN** it SHALL emit no warning regardless of how far time advances
- **AND** `now` SHALL return the same values as without this capability

#### Scenario: Counter is best-effort under concurrency

- **WHEN** multiple threads observe a large forward movement concurrently
- **THEN** the counter MAY record an approximate number of observations
- **AND** lease, visibility, and scheduling computations SHALL remain unchanged

#### Scenario: Zero threshold is clamped

- **WHEN** `WallClock` is configured with a zero forward-jump threshold
- **THEN** ordinary sub-millisecond forward movement SHALL NOT be reported as a
  jump
