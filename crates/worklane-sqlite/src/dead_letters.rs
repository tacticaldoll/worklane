use rusqlite::{Connection, OptionalExtension, params};
use worklane_core::spi::decode_envelope;
use worklane_core::{Result, RetentionPolicy};

use crate::sql_err;

pub(crate) fn free_unique_key(conn: &Connection, seq: i64) -> Result<()> {
    conn.execute("DELETE FROM unique_keys WHERE seq = ?1", params![seq])
        .map_err(sql_err)?;
    Ok(())
}

fn prune_dead_letters(
    conn: &Connection,
    lane: &str,
    policy: &RetentionPolicy,
    now: i64,
) -> Result<()> {
    if policy.is_unbounded() {
        return Ok(());
    }
    if let Some(cutoff) = policy.age_cutoff_nanos(now) {
        conn.execute(
            "DELETE FROM dead WHERE lane = ?1 AND dead_at < ?2",
            params![lane, cutoff],
        )
        .map_err(sql_err)?;
    }
    if let Some(keep) = policy.keep_count() {
        conn.execute(
            "DELETE FROM dead WHERE lane = ?1 AND seq NOT IN \
             (SELECT seq FROM dead WHERE lane = ?1 ORDER BY seq DESC LIMIT ?2)",
            params![lane, keep],
        )
        .map_err(sql_err)?;
    }
    Ok(())
}

pub(crate) fn dead_letter_seq(
    tx: &rusqlite::Transaction<'_>,
    seq: i64,
    blob: &[u8],
    error: String,
    now: i64,
    retention: &RetentionPolicy,
) -> Result<()> {
    let envelope = decode_envelope(blob)?;
    let unique_key: Option<String> = tx
        .query_row(
            "SELECT unique_key FROM unique_keys WHERE seq = ?1",
            params![seq],
            |r| r.get(0),
        )
        .optional()
        .map_err(sql_err)?;
    tx.execute(
        "INSERT INTO dead (id, lane, envelope, error, unique_key, dead_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            envelope.id.to_string(),
            envelope.lane.as_str(),
            blob,
            error,
            unique_key,
            now
        ],
    )
    .map_err(sql_err)?;
    free_unique_key(tx, seq)?;
    tx.execute("DELETE FROM jobs WHERE seq = ?1", params![seq])
        .map_err(sql_err)?;
    prune_dead_letters(tx, envelope.lane.as_str(), retention, now)?;
    Ok(())
}
