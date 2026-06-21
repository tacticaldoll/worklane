use rusqlite::{Connection, OptionalExtension, params};
use worklane_core::spi::{decode_envelope, encode_envelope, receipt_key, stale};
use worklane_core::{JobId, NewJob, ReservationReceipt, Result};

use crate::sql_err;

pub(crate) fn find_valid_row(
    conn: &Connection,
    receipt: ReservationReceipt,
    now: i64,
) -> Result<(i64, Vec<u8>)> {
    let key = receipt_key(&receipt)?;
    let row: Option<(i64, Option<i64>, Vec<u8>)> = conn
        .query_row(
            "SELECT seq, leased_until, envelope FROM jobs WHERE receipt = ?1",
            params![key],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(sql_err)?;
    match row {
        Some((seq, Some(leased_until), blob)) if leased_until > now => Ok((seq, blob)),
        _ => Err(stale(receipt)),
    }
}

pub(crate) fn insert_job(
    tx: &rusqlite::Transaction<'_>,
    job: NewJob,
    available_at: i64,
) -> Result<JobId> {
    let unique_key = job.unique_key.clone();
    if let Some(key) = &unique_key {
        let existing: Option<Vec<u8>> = tx
            .query_row(
                "SELECT j.envelope FROM unique_keys u \
                 JOIN jobs j ON j.seq = u.seq WHERE u.unique_key = ?1",
                params![key],
                |r| r.get(0),
            )
            .optional()
            .map_err(sql_err)?;
        if let Some(blob) = existing {
            return Ok(decode_envelope(&blob)?.id);
        }
    }

    let id = job.id;
    let envelope = job.into_envelope();
    let blob = encode_envelope(&envelope)?;
    let changed = tx
        .execute(
            "INSERT INTO jobs \
             (id, receipt, lane, priority, available_at, leased_until, envelope) \
             VALUES (?1, NULL, ?2, ?3, ?4, NULL, ?5) \
             ON CONFLICT(id) DO NOTHING",
            params![
                id.to_string(),
                envelope.lane.as_str(),
                envelope.priority,
                available_at,
                blob
            ],
        )
        .map_err(sql_err)?;
    if changed == 0 {
        return Ok(id);
    }
    if let Some(key) = &unique_key {
        let seq = tx.last_insert_rowid();
        tx.execute(
            "INSERT INTO unique_keys (unique_key, seq) VALUES (?1, ?2)",
            params![key, seq],
        )
        .map_err(sql_err)?;
    }
    Ok(id)
}
