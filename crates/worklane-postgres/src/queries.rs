//! Precomputed SQL for the hot per-job-cycle statements.

/// SQL for the hot per-job-cycle statements (reserve and the resolutions), built
/// once at connect from the schema rather than re-`format!`-ed on every call.
/// The colder per-row enqueue and dead-letter-admin paths keep building SQL
/// inline — they are not in the throughput-critical loop, and the dedup-bearing
/// per-row insert is shaped by runtime data. The no-unique-key batch fast path,
/// by contrast, is a single fixed-shape multi-row `UNNEST` insert regardless of
/// batch size, so it *is* precomputed here (`enqueue_batch_unnest`).
pub(crate) struct Queries {
    pub(crate) reserve: String,
    pub(crate) extend: String,
    pub(crate) retry_update: String,
    pub(crate) ack_delete_returning_seq: String,
    pub(crate) find_valid_locked: String,
    pub(crate) delete_unique_by_seq: String,
    pub(crate) delete_job_by_seq: String,
    /// No-unique-key batch fast path: one multi-row insert for a whole batch.
    /// `WITH ORDINALITY … ORDER BY ord` pins `BIGSERIAL seq` assignment to input
    /// order so the batch reserves back strict-FIFO; a plain `UNNEST` makes no
    /// such ordering guarantee. `ON CONFLICT (id) DO NOTHING` preserves JobId
    /// idempotency exactly as the per-row `insert_job` path does.
    pub(crate) enqueue_batch_unnest: String,
}

impl Queries {
    /// Builds the per-schema query strings once. Taking a [`SafeSchema`] (rather
    /// than a `&str`) is the proof the schema was validated — it is interpolated
    /// into the table names exactly as `PostgresBroker::table` does.
    ///
    /// [`SafeSchema`]: crate::ident::SafeSchema
    pub(crate) fn new(schema: &crate::ident::SafeSchema) -> Self {
        let jobs = schema.qualify("jobs");
        let unique_keys = schema.qualify("unique_keys");
        Queries {
            reserve: format!(
                "UPDATE {jobs} SET receipt = $1, leased_until = $2, \
                 deliveries = deliveries + 1 \
                 WHERE seq = ( \
                     SELECT seq FROM {jobs} \
                     WHERE lane = $3 AND available_at <= $4 \
                       AND (leased_until IS NULL OR leased_until <= $4) \
                     ORDER BY priority DESC, available_at ASC, seq ASC \
                     FOR UPDATE SKIP LOCKED \
                     LIMIT 1 \
                 ) \
                 RETURNING envelope"
            ),
            extend: format!(
                "UPDATE {jobs} SET leased_until = $1 WHERE receipt = $2 AND leased_until > $3"
            ),
            retry_update: format!(
                "UPDATE {jobs} SET envelope = $1, available_at = $2, leased_until = NULL, \
                 receipt = NULL WHERE seq = $3"
            ),
            ack_delete_returning_seq: format!(
                "DELETE FROM {jobs} WHERE receipt = $1 AND leased_until > $2 RETURNING seq"
            ),
            find_valid_locked: format!(
                "SELECT seq, leased_until, envelope FROM {jobs} WHERE receipt = $1 FOR UPDATE"
            ),
            delete_unique_by_seq: format!("DELETE FROM {unique_keys} WHERE seq = $1"),
            delete_job_by_seq: format!("DELETE FROM {jobs} WHERE seq = $1"),
            enqueue_batch_unnest: format!(
                "INSERT INTO {jobs} \
                 (id, receipt, lane, priority, available_at, leased_until, envelope) \
                 SELECT id, NULL, lane, priority, available_at, NULL, envelope \
                 FROM UNNEST($1::text[], $2::text[], $3::int2[], $4::int8[], $5::bytea[]) \
                 WITH ORDINALITY AS t(id, lane, priority, available_at, envelope, ord) \
                 ORDER BY t.ord \
                 ON CONFLICT (id) DO NOTHING"
            ),
        }
    }
}
