//! Precomputed SQL for the hot per-job-cycle statements.

/// SQL for the hot per-job-cycle statements (reserve and the resolutions), built
/// once at connect from the schema rather than re-`format!`-ed on every call.
/// The colder enqueue and dead-letter-admin paths keep building SQL inline —
/// they are not in the throughput-critical loop, and some (batch insert) are
/// shaped by runtime data and so cannot be precomputed.
pub(crate) struct Queries {
    pub(crate) reserve: String,
    pub(crate) extend: String,
    pub(crate) retry_update: String,
    pub(crate) ack_delete_returning_seq: String,
    pub(crate) find_valid_locked: String,
    pub(crate) delete_unique_by_seq: String,
    pub(crate) delete_job_by_seq: String,
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
        }
    }
}
