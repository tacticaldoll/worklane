use async_trait::async_trait;
use rusqlite::params;
use worklane_core::spi::nanos;
use worklane_core::{NewJob, Result};

use crate::jobs::insert_job;
use crate::{SqliteBroker, sql_err};

#[async_trait]
impl worklane_core::ScheduledStore for SqliteBroker {
    async fn enqueue_scheduled(
        &self,
        schedule_id: &str,
        occurrence: i64,
        job: NewJob,
    ) -> Result<bool> {
        let schedule_id = schedule_id.to_string();
        let available_at = nanos(self.clock.now().saturating_add(job.delay));
        self.run(move |conn| {
            let tx = conn.unchecked_transaction().map_err(sql_err)?;
            let changed = tx
                .execute(
                    "INSERT INTO schedules (schedule_id, occurrence) \
                     VALUES (?1, ?2) \
                     ON CONFLICT(schedule_id) DO UPDATE \
                     SET occurrence = excluded.occurrence \
                     WHERE schedules.occurrence < excluded.occurrence",
                    params![schedule_id, occurrence],
                )
                .map_err(sql_err)?;
            if changed > 0 {
                insert_job(&tx, job, available_at)?;
                tx.commit().map_err(sql_err)?;
                Ok(true)
            } else {
                tx.commit().map_err(sql_err)?;
                Ok(false)
            }
        })
        .await
    }

    async fn remove_schedule(&self, schedule_id: &str) -> Result<()> {
        let schedule_id = schedule_id.to_string();
        self.run(move |conn| {
            conn.execute(
                "DELETE FROM schedules WHERE schedule_id = ?1",
                params![schedule_id],
            )
            .map_err(sql_err)?;
            Ok(())
        })
        .await
    }
}
