use worklane_core::spi::{receipt_key, stale};
use worklane_core::{ReservationReceipt, Result};

use crate::{PostgresBroker, pg_err};

pub(crate) async fn find_valid_row_locked(
    tx: &tokio_postgres::Transaction<'_>,
    broker: &PostgresBroker,
    receipt: ReservationReceipt,
    now: i64,
) -> Result<(i64, Vec<u8>)> {
    let key = receipt_key(&receipt)?;
    let row = tx
        .query_opt(&broker.queries.find_valid_locked, &[&key])
        .await
        .map_err(pg_err)?;
    match row {
        Some(r) => {
            let seq: i64 = r.get(0);
            let leased_until: Option<i64> = r.get(1);
            let blob: Vec<u8> = r.get(2);
            match leased_until {
                Some(until) if until > now => Ok((seq, blob)),
                _ => Err(stale(receipt)),
            }
        }
        None => Err(stale(receipt)),
    }
}
