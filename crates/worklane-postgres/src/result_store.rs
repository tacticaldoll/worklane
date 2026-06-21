use async_trait::async_trait;
use deadpool_postgres::Pool;
use worklane_core::{Error, JobId, Result, ResultStore};

use crate::ident::SafeSchema;

/// A PostgreSQL-backed durable result store.
#[derive(Clone)]
pub struct PostgresResultStore {
    pool: Pool,
    schema: SafeSchema,
}

impl PostgresResultStore {
    /// Create a new result store from an existing connection pool, scoped to
    /// `schema`. The schema is interpolated into table names, so it MUST be a
    /// safe SQL identifier; an invalid one is rejected here rather than flowing
    /// into a query as an injection vector. Construction via
    /// [`PostgresBroker::result_store`](crate::PostgresBroker::result_store) is
    /// always valid because the broker validated its schema at connect time.
    pub fn new(pool: Pool, schema: &str) -> Result<Self> {
        let schema = SafeSchema::new(schema)
            .ok_or_else(|| Error::ResultStore(format!("invalid schema name {schema:?}")))?;
        Ok(Self::from_safe(pool, schema))
    }

    /// Build a store from an already-validated schema, infallibly. For internal
    /// callers (the broker) that validated the schema at connect time.
    pub(crate) fn from_safe(pool: Pool, schema: SafeSchema) -> Self {
        Self { pool, schema }
    }

    /// `name` is a `'static` literal (see [`SafeSchema::qualify`]).
    fn table(&self, name: &'static str) -> String {
        self.schema.qualify(name)
    }

    async fn client(&self) -> Result<deadpool_postgres::Object> {
        self.pool.get().await.map_err(|e| {
            Error::ResultStore(worklane_core::redact_credentials(&format!(
                "pool checkout failed: {e}"
            )))
        })
    }
}

#[async_trait]
impl ResultStore for PostgresResultStore {
    async fn store(&self, job_id: &JobId, result: &[u8]) -> Result<()> {
        let client = self.client().await?;
        let key = job_id.to_string();
        let blob = result.to_vec();
        client
            .execute(
                &format!(
                    "INSERT INTO {} (job_id, blob) VALUES ($1, $2) \
                     ON CONFLICT (job_id) DO UPDATE SET blob = EXCLUDED.blob",
                    self.table("results")
                ),
                &[&key, &blob],
            )
            .await
            .map_err(|e| Error::ResultStore(worklane_core::redact_credentials(&e.to_string())))?;
        Ok(())
    }

    async fn get(&self, job_id: &JobId) -> Result<Option<Vec<u8>>> {
        let client = self.client().await?;
        let key = job_id.to_string();
        let row = client
            .query_opt(
                &format!(
                    "SELECT blob FROM {} WHERE job_id = $1",
                    self.table("results")
                ),
                &[&key],
            )
            .await
            .map_err(|e| Error::ResultStore(worklane_core::redact_credentials(&e.to_string())))?;

        match row {
            Some(r) => Ok(Some(r.get(0))),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PostgresBroker;

    #[tokio::test]
    async fn test_postgres_store_and_get() {
        let Some(url) = std::env::var("WORKLANE_POSTGRES_TEST_URL").ok() else {
            eprintln!(
                "SKIP test_postgres_store_and_get: set WORKLANE_POSTGRES_TEST_URL to run the postgres result-store test"
            );
            return;
        };

        // Use a unique schema
        let schema = format!(
            "wl_rs_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        );

        let broker = PostgresBroker::connect_with_schema(&url, &schema)
            .await
            .unwrap();
        let store = broker.result_store();

        let job_id = JobId::new();

        let retrieved = store.get(&job_id).await.unwrap();
        assert_eq!(retrieved, None);

        let data = b"hello pg";
        store.store(&job_id, data).await.unwrap();

        let retrieved = store.get(&job_id).await.unwrap();
        assert_eq!(retrieved.unwrap(), data);

        let new_data = b"new pg data";
        store.store(&job_id, new_data).await.unwrap();

        let retrieved = store.get(&job_id).await.unwrap();
        assert_eq!(retrieved.unwrap(), new_data);
    }
}
