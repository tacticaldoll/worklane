use crate::{PostgresBroker, pg_err};
use worklane_core::{Error, Result};

/// The storage-schema version, stored in the `meta` table.
///
/// Version 1 is the **baseline** schema. worklane is pre-1.0 and has no stable
/// on-disk format yet: there is no in-place migration between schema generations.
/// A fresh database is created at the baseline; a database stamped with any other
/// version is rejected (drop and recreate it). Migration discipline begins at 1.0,
/// when the format is frozen.
const SCHEMA_VERSION: i64 = 1;

impl PostgresBroker {
    /// Create the baseline schema and stamp the version. A database already at the
    /// baseline is left as-is; one stamped with any other version belongs to a
    /// different schema generation and is rejected (pre-1.0: no in-place
    /// migration). All DDL is `IF NOT EXISTS`, so creating the baseline is
    /// idempotent.
    pub(crate) async fn init_schema(&self) -> Result<()> {
        let client = self.client().await?;
        let ddl = format!(
            "CREATE SCHEMA IF NOT EXISTS \"{s}\";
             CREATE TABLE IF NOT EXISTS {jobs} (
                 seq          BIGSERIAL PRIMARY KEY,
                 id           TEXT     NOT NULL,
                 receipt      TEXT,
                 lane         TEXT     NOT NULL,
                 priority     SMALLINT NOT NULL DEFAULT 0,
                 available_at BIGINT   NOT NULL,
                 leased_until BIGINT,
                 envelope     BYTEA    NOT NULL,
                 deliveries   BIGINT   NOT NULL DEFAULT 0
             );
             CREATE TABLE IF NOT EXISTS {dead} (
                 seq        BIGSERIAL PRIMARY KEY,
                 id         TEXT  NOT NULL,
                 lane       TEXT  NOT NULL,
                 envelope   BYTEA NOT NULL,
                 error      TEXT  NOT NULL,
                 unique_key TEXT,
                 dead_at    BIGINT NOT NULL DEFAULT 0
             );
             CREATE TABLE IF NOT EXISTS {meta} (
                 singleton      BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
                 schema_version BIGINT  NOT NULL
             );
             CREATE TABLE IF NOT EXISTS {unique_keys} (
                 unique_key TEXT   PRIMARY KEY,
                 seq        BIGINT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS {results} (
                 job_id TEXT  PRIMARY KEY,
                 blob   BYTEA NOT NULL
             );
             CREATE TABLE IF NOT EXISTS {schedules} (
                 schedule_id TEXT   PRIMARY KEY,
                 occurrence  BIGINT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS jobs_reserve \
                 ON {jobs} (lane, priority DESC, available_at, seq);
             CREATE INDEX IF NOT EXISTS jobs_receipt \
                 ON {jobs} (receipt) WHERE receipt IS NOT NULL;
             CREATE UNIQUE INDEX IF NOT EXISTS jobs_id \
                 ON {jobs} (id);
             CREATE UNIQUE INDEX IF NOT EXISTS dead_id \
                 ON {dead} (id);
             CREATE INDEX IF NOT EXISTS dead_lane \
                 ON {dead} (lane, seq);",
            s = self.schema.as_str(),
            jobs = self.table("jobs"),
            dead = self.table("dead"),
            meta = self.table("meta"),
            unique_keys = self.table("unique_keys"),
            results = self.table("results"),
            schedules = self.table("schedules"),
        );
        client.batch_execute(&ddl).await.map_err(pg_err)?;

        let current: Option<i64> = client
            .query_opt(
                &format!("SELECT schema_version FROM {} LIMIT 1", self.table("meta")),
                &[],
            )
            .await
            .map_err(pg_err)?
            .map(|row| row.get(0));
        match current {
            None => {
                // `meta` is a singleton (PK `singleton` defaults TRUE), so a second
                // broker initializing the same schema concurrently — both seeing an
                // empty table and both inserting — collides on the PK instead of
                // writing a duplicate version row. `ON CONFLICT DO NOTHING` makes the
                // loser a no-op; exactly one version row survives.
                client
                    .execute(
                        &format!(
                            "INSERT INTO {} (schema_version) VALUES ($1) \
                             ON CONFLICT (singleton) DO NOTHING",
                            self.table("meta")
                        ),
                        &[&SCHEMA_VERSION],
                    )
                    .await
                    .map_err(pg_err)?;
            }
            Some(v) if v == SCHEMA_VERSION => {}
            Some(v) => {
                return Err(Error::Broker(format!(
                    "postgres storage schema version {v} is not the supported baseline \
                     {SCHEMA_VERSION}; worklane is pre-1.0 and does not migrate between schema \
                     generations — drop and recreate the schema (or upgrade worklane if this \
                     database is newer)"
                )));
            }
        }
        Ok(())
    }
}
