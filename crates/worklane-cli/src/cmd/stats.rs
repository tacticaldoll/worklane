//! `stats <lane>` command.

use worklane_core::Broker;

/// Print lane statistics for `lane`.
pub async fn run(broker: &dyn Broker, lane: &str) -> Result<(), String> {
    let lane_val = lane
        .parse()
        .map_err(|e| format!("invalid lane '{lane}': {e}"))?;
    // A count primitive: it returns only a number and never materializes the
    // dead-letter payloads in the client. Sub-linear on every backend: `ZCARD`
    // on Redis, an index range on the SQL `dead(lane, seq)` index.
    let dead_count = broker
        .dead_letter_store()
        .ok_or_else(|| "this broker does not support dead-letter inspection".to_string())?
        .count_dead_letters(&lane_val)
        .await
        .map_err(|e| e.to_string())?;
    let pending_count = broker
        .queue_stats()
        .ok_or_else(|| "this broker does not support queue statistics".to_string())?
        .pending_count(&lane_val)
        .await
        .map_err(|e| e.to_string())?;

    println!("Lane:               {lane}");
    println!("Dead-letter count:  {dead_count}");
    println!("Pending job count:  {pending_count}");
    Ok(())
}
