use crate::{Result, sql_err};
use rusqlite::Connection;
use worklane_core::Error;
use worklane_core::spi::{SCHEMA_VERSION, SchemaVersionCheck, check_schema_version};

/// The live job store. `seq` is the implicit rowid, giving a stable FIFO order for
/// `reserve`. `id` denormalizes the envelope's JobId for a by-id liveness lookup
/// without a scan (and is `UNIQUE` — enqueue is idempotent on JobId).
const JOBS_SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS jobs (
    seq          INTEGER PRIMARY KEY,
    id           TEXT    NOT NULL,
    receipt      TEXT,
    lane         TEXT    NOT NULL,
    priority     INTEGER NOT NULL DEFAULT 0,
    available_at INTEGER NOT NULL,
    leased_until INTEGER,
    envelope     BLOB    NOT NULL,
    deliveries   INTEGER NOT NULL DEFAULT 0
);";

/// The dead-letter store: `id` / `lane` denormalized off the envelope so reads are
/// lane-scoped and requeue is by id, both without a full scan.
const DEAD_SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS dead (
    seq        INTEGER PRIMARY KEY,
    id         TEXT NOT NULL,
    lane       TEXT NOT NULL,
    envelope   BLOB NOT NULL,
    error      TEXT NOT NULL,
    unique_key TEXT,
    dead_at    INTEGER NOT NULL DEFAULT 0
);";

/// Maps a live job's unique key to its row `seq`. A row exists only while its job
/// is live; `ack`/`fail` delete it.
const UNIQUE_KEYS_SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS unique_keys (
    unique_key TEXT    PRIMARY KEY,
    seq        INTEGER NOT NULL
);";

/// Opaque durable job results, keyed by JobId.
const RESULTS_SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS results (
    job_id TEXT PRIMARY KEY,
    blob   BLOB NOT NULL
);";

/// Last claimed occurrence per recurring schedule (HA scheduler coordination).
const SCHEDULES_SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS schedules (
    schedule_id TEXT    PRIMARY KEY,
    occurrence  INTEGER NOT NULL
);";

/// Backs `reserve`'s lane-scoped lookup. The column order mirrors `reserve`'s
/// `WHERE lane = ? ... ORDER BY priority DESC, available_at ASC, seq ASC` access
/// path exactly, so the highest priority is read first without a separate sort.
const JOBS_RESERVE_INDEX: &str =
    "CREATE INDEX IF NOT EXISTS jobs_reserve ON jobs (lane, priority DESC, available_at, seq);";

/// `UNIQUE(id)` on the live store: backs the by-id `is_live` point lookup AND makes
/// enqueue idempotent on JobId — a re-enqueue of an id a live job already holds is
/// a no-op (`ON CONFLICT(id)`), not a second job.
const JOBS_ID_INDEX: &str = "CREATE UNIQUE INDEX IF NOT EXISTS jobs_id ON jobs (id);";

/// Backs receipt-based resolution (`ack`/`retry`/`fail`/`extend`), which is on
/// the hot path after every reservation. Partial because only leased jobs have a
/// receipt.
const JOBS_RECEIPT_INDEX: &str =
    "CREATE INDEX IF NOT EXISTS jobs_receipt ON jobs (receipt) WHERE receipt IS NOT NULL;";

/// `UNIQUE(id)` on the dead-letter store. A defensive guard, not a constraint the
/// lifecycle can hit: job ids are random v4 UUIDs and a job is never simultaneously
/// live and dead (the dead row is written and the live row deleted in one
/// transaction; `requeue` deletes the dead row as it re-inserts the live one).
const DEAD_ID_INDEX: &str = "CREATE UNIQUE INDEX IF NOT EXISTS dead_id ON dead (id);";

/// Backs every lane-scoped dead-letter operation (`count`/`purge`/`read`/retention
/// prune, all `WHERE lane = ?`) with an index range instead of a table scan; `seq`
/// second so `read`'s `ORDER BY seq` and the count-prune selection are covered.
const DEAD_LANE_INDEX: &str = "CREATE INDEX IF NOT EXISTS dead_lane ON dead (lane, seq);";

/// Every DDL statement of the baseline schema, in dependency-free order. All are
/// `IF NOT EXISTS`, so applying the baseline is idempotent.
const BASELINE: [&str; 10] = [
    JOBS_SCHEMA,
    DEAD_SCHEMA,
    UNIQUE_KEYS_SCHEMA,
    RESULTS_SCHEMA,
    SCHEDULES_SCHEMA,
    JOBS_RESERVE_INDEX,
    JOBS_ID_INDEX,
    JOBS_RECEIPT_INDEX,
    DEAD_ID_INDEX,
    DEAD_LANE_INDEX,
];

/// Apply the per-connection settings every connection needs, returning the raw
/// `rusqlite` error so the r2d2 pool manager (whose `connect` must yield
/// `rusqlite::Error`) can run it on each connection it opens. WAL is a persistent
/// database property, but `synchronous`, `busy_timeout`, and the default
/// transaction behavior are per-connection and so must be set on every one.
/// `busy_timeout` lets a writer that meets another writer's WAL lock wait rather
/// than fail, which is what makes a multi-connection pool safe under SQLite's
/// single-writer rule.
pub(crate) fn init_connection(conn: &mut Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA busy_timeout = 5000;",
    )?;
    // `unchecked_transaction` (used throughout the broker) inherits this, so each
    // reserve/resolve takes the write lock at BEGIN — no lock-upgrade deadlock.
    conn.set_transaction_behavior(rusqlite::TransactionBehavior::Immediate);
    Ok(())
}

pub(crate) fn configure(conn: &mut Connection) -> Result<()> {
    init_connection(conn).map_err(sql_err)
}

/// Bring the database to the current schema.
///
/// A fresh database (`user_version` 0) is created at the [`SCHEMA_VERSION`]
/// baseline. A database already at the baseline is accepted as-is. Any other
/// stamped version belongs to a different schema generation; pre-1.0 there is no
/// in-place migration, so it is rejected rather than silently mis-read (see
/// [`SCHEMA_VERSION`]).
pub(crate) fn migrate(conn: &Connection) -> Result<()> {
    let version: i64 = conn
        .pragma_query_value(None, "user_version", |r| r.get(0))
        .map_err(sql_err)?;

    // `user_version` 0 is SQLite's unset sentinel — a fresh database with no
    // version stamped yet; map it to `None` for the shared decision.
    match check_schema_version((version != 0).then_some(version)) {
        SchemaVersionCheck::Fresh => {
            for ddl in BASELINE {
                conn.execute_batch(ddl).map_err(sql_err)?;
            }
            conn.pragma_update(None, "user_version", SCHEMA_VERSION)
                .map_err(sql_err)?;
            Ok(())
        }
        SchemaVersionCheck::Match => Ok(()),
        SchemaVersionCheck::Mismatch(v) => Err(Error::Broker(format!(
            "sqlite storage schema version {v} is not the supported baseline \
             {SCHEMA_VERSION}; worklane is pre-1.0 and does not migrate between schema \
             generations — drop and recreate the database"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(conn: &Connection) -> i64 {
        conn.pragma_query_value(None, "user_version", |r| r.get(0))
            .unwrap()
    }

    #[test]
    fn fresh_database_is_created_at_the_baseline() {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        assert_eq!(
            version(&conn),
            SCHEMA_VERSION,
            "fresh db is stamped the baseline"
        );
        // All baseline tables exist (a query against each succeeds).
        for table in ["jobs", "dead", "unique_keys", "results", "schedules"] {
            conn.execute_batch(&format!("SELECT 1 FROM {table} LIMIT 0;"))
                .unwrap_or_else(|e| panic!("table {table} should exist: {e}"));
        }
        for index in [
            "jobs_reserve",
            "jobs_id",
            "jobs_receipt",
            "dead_id",
            "dead_lane",
        ] {
            let exists: bool = conn
                .query_row(
                    "SELECT EXISTS(
                        SELECT 1 FROM sqlite_master
                        WHERE type = 'index' AND name = ?1
                    )",
                    [index],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(exists, "index {index} should exist");
        }
        // `jobs.id` is UNIQUE: a duplicate id is rejected.
        conn.execute_batch(
            "INSERT INTO jobs (id, lane, available_at, envelope) VALUES ('x','l',0,x'00');",
        )
        .unwrap();
        let dup = conn.execute_batch(
            "INSERT INTO jobs (id, lane, available_at, envelope) VALUES ('x','l',0,x'00');",
        );
        assert!(dup.is_err(), "UNIQUE(id) must reject a duplicate job id");
    }

    #[test]
    fn reopening_the_baseline_is_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        migrate(&conn).unwrap(); // no-op
        assert_eq!(version(&conn), SCHEMA_VERSION);
    }

    #[test]
    fn a_different_schema_generation_is_rejected() {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        // Simulate a database from another generation (e.g. a future version, or a
        // pre-1.0 ladder version): it must be rejected, not silently accepted.
        conn.pragma_update(None, "user_version", 99i64).unwrap();
        assert!(
            migrate(&conn).is_err(),
            "a non-baseline version is rejected"
        );
    }
}
