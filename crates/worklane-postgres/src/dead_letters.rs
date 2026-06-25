use worklane_core::Result;
use worklane_core::spi::decode_envelope;

use crate::{PostgresBroker, pg_err};

async fn prune_dead_letters(
    broker: &PostgresBroker,
    tx: &tokio_postgres::Transaction<'_>,
    lane: &str,
    now: i64,
) -> Result<()> {
    if broker.retention.is_unbounded() {
        return Ok(());
    }
    if let Some(cutoff) = broker.retention.age_cutoff_nanos(now) {
        tx.execute(
            &format!(
                "DELETE FROM {} WHERE lane = $1 AND dead_at < $2",
                broker.table("dead")
            ),
            &[&lane, &cutoff],
        )
        .await
        .map_err(pg_err)?;
    }
    if let Some(keep) = broker.retention.keep_count() {
        tx.execute(
            &format!(
                "DELETE FROM {dead} WHERE lane = $1 AND seq NOT IN \
                 (SELECT seq FROM {dead} WHERE lane = $1 ORDER BY seq DESC LIMIT $2)",
                dead = broker.table("dead")
            ),
            &[&lane, &keep],
        )
        .await
        .map_err(pg_err)?;
    }
    Ok(())
}

pub(crate) async fn dead_letter_seq(
    broker: &PostgresBroker,
    tx: &tokio_postgres::Transaction<'_>,
    seq: i64,
    blob: &[u8],
    error: String,
    now: i64,
) -> Result<()> {
    let envelope = decode_envelope(blob)?;
    let unique_key: Option<String> = tx
        .query_opt(
            &format!(
                "SELECT unique_key FROM {} WHERE seq = $1",
                broker.table("unique_keys")
            ),
            &[&seq],
        )
        .await
        .map_err(pg_err)?
        .map(|r| r.get(0));
    tx.execute(
        &format!(
            "INSERT INTO {} (id, lane, envelope, error, unique_key, dead_at) \
             VALUES ($1, $2, $3, $4, $5, $6)",
            broker.table("dead")
        ),
        &[
            &envelope.id.to_string(),
            &envelope.lane.as_str(),
            &blob,
            &error,
            &unique_key,
            &now,
        ],
    )
    .await
    .map_err(pg_err)?;
    tx.execute(&broker.queries.delete_unique_by_seq, &[&seq])
        .await
        .map_err(pg_err)?;
    tx.execute(&broker.queries.delete_job_by_seq, &[&seq])
        .await
        .map_err(pg_err)?;
    prune_dead_letters(broker, tx, envelope.lane.as_str(), now).await?;
    Ok(())
}
