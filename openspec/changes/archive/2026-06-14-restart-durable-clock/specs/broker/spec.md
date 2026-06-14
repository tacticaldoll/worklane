## ADDED Requirements

### Requirement: Restart-durable time for persisted jobs

A broker that persists jobs across process restarts SHALL derive its visibility
and lease times from a clock whose epoch is stable across restarts, so that a
persisted job's `available_at` and lease deadline remain meaningful after the
process restarts and reopens the same storage. A broker whose jobs do not
survive a restart (for example an in-memory broker) has no such obligation and
MAY use a process-local monotonic clock.

#### Scenario: Persisted jobs survive a restart

- **WHEN** a job is enqueued to a persistent broker and then the broker is
  restarted (the same storage reopened by a new broker instance with a fresh
  clock of the same kind)
- **THEN** the job SHALL still be reservable after the restart

#### Scenario: A persisted retry delay is honoured across a restart

- **WHEN** a job is retried with a future visibility delay and the broker is then
  restarted before the delay elapses
- **THEN** after the restart the job SHALL remain hidden until the delay elapses
  and become reservable thereafter, consistent with its pre-restart schedule
