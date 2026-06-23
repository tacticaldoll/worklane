//! `classify <job-id>` command.

use worklane_core::{Broker, JobId, JobState};

/// Report the lifecycle state of the job identified by `job_id`.
///
/// Uses the portable [`Broker::classify`] point lookup — `Live`, `DeadLettered`,
/// or `CompletedOrUnknown` — and never adds broker-native inspection. The id is
/// already parsed by the time it reaches here (clap rejects an invalid id before
/// a broker connection is opened).
pub async fn run(broker: &dyn Broker, job_id: JobId, format: &str) -> Result<(), String> {
    let state = broker.classify(job_id).await.map_err(|e| e.to_string())?;
    let name = state_name(state);
    match format {
        "text" => println!("{job_id}: {name}"),
        "json" => {
            let obj = serde_json::json!({ "job_id": job_id.to_string(), "state": name });
            let line =
                serde_json::to_string(&obj).map_err(|e| format!("json output failed: {e}"))?;
            println!("{line}");
        }
        other => {
            return Err(format!(
                "unknown --format '{other}' (expected 'text' or 'json')"
            ));
        }
    }
    Ok(())
}

/// The canonical name for a [`JobState`], shared by the text and JSON output so
/// both render the same three states verbatim from the enum.
fn state_name(state: JobState) -> &'static str {
    match state {
        JobState::Live => "Live",
        JobState::DeadLettered => "DeadLettered",
        JobState::CompletedOrUnknown => "CompletedOrUnknown",
    }
}
