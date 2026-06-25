# Broker Conformance Matrix

This matrix records the first-party broker conformance suites exercised by this
workspace. It distinguishes the mandatory lifecycle suite from optional
capability suites. The authoritative behavior remains in `openspec/specs/`.

- In-memory: lifecycle, timed, configured, batch, dead letters, queue stats,
  scheduled.
- SQLite: lifecycle, timed, configured, batch, dead letters, queue stats,
  scheduled, result store.
- PostgreSQL: lifecycle, timed, configured, batch, dead letters, queue stats,
  scheduled, result store.
- Redis: lifecycle, timed, configured, batch, dead letters, queue stats,
  scheduled, result store.

## Reading The Matrix

`Lifecycle` is the mandatory broker contract: enqueue, reserve, ack, retry,
defer, extend, fail, classify, lane isolation, uniqueness, receipt validation,
and concurrency semantics.

`Timed` covers deterministic clock scenarios such as retry delay, lease expiry,
lease extension, delayed enqueue, and ordering under controlled time.

`Configured` covers broker construction with bounded redelivery and dead-letter
retention policies.

`Batch`, `Dead letters`, `Queue stats`, and `Scheduled` are optional broker
capability suites. A lifecycle pass does not imply support for any of them.

`Result store` is storage-adjacent. It is verified by `result_store_contract!`
instead of the broker capability batteries because result storage is configured
beside a broker rather than through the core `Broker` trait.
