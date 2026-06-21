use async_trait::async_trait;
use worklane_core::spi::nanos;
use worklane_core::{NewJob, Result};

use crate::{PostgresBroker, pg_err};

#[async_trait]
impl worklane_core::ScheduledStore for PostgresBroker {
    async fn enqueue_scheduled(
        &self,
        schedule_id: &str,
        occurrence: i64,
        job: NewJob,
    ) -> Result<bool> {
        let mut client = self.client().await?;
        let tx = Self::begin(&mut client).await?;
        let row = tx
            .query_opt(
                &format!(
                    "INSERT INTO {schedules} (schedule_id, occurrence) \
                     VALUES ($1, $2) \
                     ON CONFLICT (schedule_id) DO UPDATE \
                     SET occurrence = EXCLUDED.occurrence \
                     WHERE {schedules}.occurrence < EXCLUDED.occurrence \
                     RETURNING schedule_id",
                    schedules = self.table("schedules")
                ),
                &[&schedule_id, &occurrence],
            )
            .await
            .map_err(pg_err)?;

        if row.is_some() {
            let available_at = nanos(self.clock.now().saturating_add(job.delay));
            self.insert_job(&tx, job, available_at).await?;
            tx.commit().await.map_err(pg_err)?;
            Ok(true)
        } else {
            tx.commit().await.map_err(pg_err)?;
            Ok(false)
        }
    }

    async fn remove_schedule(&self, schedule_id: &str) -> Result<()> {
        let client = self.client().await?;
        client
            .execute(
                &format!(
                    "DELETE FROM {} WHERE schedule_id = $1",
                    self.table("schedules")
                ),
                &[&schedule_id],
            )
            .await
            .map_err(pg_err)?;
        Ok(())
    }
}
