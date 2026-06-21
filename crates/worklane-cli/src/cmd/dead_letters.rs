//! `dead-letters list`, `dead-letters requeue`, and `dead-letters purge`
//! commands.

use std::str::FromStr;

use serde::Serialize;
use worklane_core::{Broker, JobId};

/// The error shown when the selected broker has no dead-letter capability.
fn no_dead_letters() -> String {
    "this broker does not support dead-letter inspection".to_string()
}

/// List dead-letter records for `lane`, up to `limit` entries.
pub async fn list(
    broker: &dyn Broker,
    lane: &str,
    limit: usize,
    format: &str,
) -> Result<(), String> {
    let lane = lane
        .parse()
        .map_err(|e| format!("invalid lane '{lane}': {e}"))?;
    let records = broker
        .dead_letter_store()
        .ok_or_else(no_dead_letters)?
        .read_dead_letters(&lane, limit)
        .await
        .map_err(|e| e.to_string())?;

    if records.is_empty() {
        return Ok(());
    }

    match format {
        "table" => {
            let rows: Vec<DeadLetterRow> = records.into_iter().map(DeadLetterRow::from).collect();
            print!("{}", render_table(&rows));
        }
        _ => {
            // JSON lines (default)
            for dl in &records {
                let obj = serde_json::json!({
                    "id": dl.envelope.id.to_string(),
                    "kind": dl.envelope.kind,
                    "lane": dl.envelope.lane.as_str(),
                    "attempts": dl.envelope.attempts,
                    "max_attempts": dl.envelope.max_attempts,
                    "error": dl.error,
                });
                println!("{}", json_line(&obj)?);
            }
        }
    }
    Ok(())
}

fn json_line<T: Serialize>(value: &T) -> Result<String, String> {
    serde_json::to_string(value).map_err(|e| format!("json output failed: {e}"))
}

/// Requeue the dead-lettered job with the given `id` back to its original lane.
///
/// This moves the job out of the dead-letter store and back onto its lane — an
/// irreversible state change — so unless `yes` is set it prompts for
/// confirmation first and aborts on anything but an affirmative answer.
pub async fn requeue(broker: &dyn Broker, id: &str, yes: bool) -> Result<(), String> {
    let job_id =
        JobId::from_str(id).map_err(|_| format!("invalid job id '{id}' — expected a UUID"))?;

    if !yes
        && !confirm(&format!(
            "Requeue dead-lettered job {id} back to its lane? This cannot be undone. [y/N] "
        ))?
    {
        println!("Aborted.");
        return Ok(());
    }

    broker
        .dead_letter_store()
        .ok_or_else(no_dead_letters)?
        .requeue(job_id)
        .await
        .map_err(|e| format!("requeue failed: {e}"))?;
    println!("Requeued {id}");
    Ok(())
}

/// Permanently remove every dead-letter record for `lane`.
///
/// This is irreversible (the records are not requeued), so unless `yes` is set it
/// prompts for confirmation first and aborts on anything but an affirmative
/// answer.
pub async fn purge(broker: &dyn Broker, lane: &str, yes: bool) -> Result<(), String> {
    let parsed = lane
        .parse()
        .map_err(|e| format!("invalid lane '{lane}': {e}"))?;

    if !yes
        && !confirm(&format!(
            "Permanently delete ALL dead-lettered jobs on lane '{lane}'? This cannot be undone. [y/N] "
        ))?
    {
        println!("Aborted.");
        return Ok(());
    }

    let removed = broker
        .dead_letter_store()
        .ok_or_else(no_dead_letters)?
        .purge_dead_letters(&parsed)
        .await
        .map_err(|e| format!("purge failed: {e}"))?;
    println!("Purged {removed} dead-lettered job(s) from lane '{lane}'");
    Ok(())
}

/// Print `prompt` and read a yes/no answer from stdin. Returns `true` only on an
/// explicit `y`/`yes` (case-insensitive); end-of-input or anything else is `no`.
fn confirm(prompt: &str) -> Result<bool, String> {
    use std::io::{Write, stdin, stdout};
    print!("{prompt}");
    stdout().flush().map_err(|e| e.to_string())?;
    let mut answer = String::new();
    if stdin().read_line(&mut answer).map_err(|e| e.to_string())? == 0 {
        return Ok(false); // EOF / no TTY: default to the safe "no".
    }
    let answer = answer.trim().to_ascii_lowercase();
    Ok(answer == "y" || answer == "yes")
}

// ── table rendering helper ────────────────────────────────────────────────────

struct DeadLetterRow {
    id: String,
    kind: String,
    lane: String,
    attempts: u32,
    error: String,
}

/// Column headers, paired with the accessor that pulls each cell from a row.
const COLUMNS: [&str; 5] = ["id", "kind", "lane", "attempts", "error (truncated)"];

impl DeadLetterRow {
    /// The five cells of this row, in `COLUMNS` order.
    fn cells(&self) -> [String; 5] {
        [
            self.id.clone(),
            self.kind.clone(),
            self.lane.clone(),
            self.attempts.to_string(),
            self.error.clone(),
        ]
    }
}

/// Render `rows` as a left-aligned, space-padded text table with a header.
///
/// Column widths are measured in `char`s (not bytes), so multi-byte error
/// messages stay aligned. The output ends in a newline; the caller uses
/// `print!` (not `println!`) to avoid a trailing blank line.
fn render_table(rows: &[DeadLetterRow]) -> String {
    let mut widths: [usize; 5] = COLUMNS.map(|h| h.chars().count());
    let cells: Vec<[String; 5]> = rows.iter().map(DeadLetterRow::cells).collect();
    for row in &cells {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.chars().count());
        }
    }

    let mut out = String::new();
    let push_row = |out: &mut String, cols: &[String; 5]| {
        for (i, col) in cols.iter().enumerate() {
            if i > 0 {
                out.push_str("  ");
            }
            out.push_str(col);
            if i + 1 < cols.len() {
                // Pad all but the last column to its width.
                for _ in 0..widths[i] - col.chars().count() {
                    out.push(' ');
                }
            }
        }
        out.push('\n');
    };

    push_row(&mut out, &COLUMNS.map(str::to_owned));
    for row in &cells {
        push_row(&mut out, row);
    }
    out
}

impl From<worklane_core::DeadLetter> for DeadLetterRow {
    fn from(dl: worklane_core::DeadLetter) -> Self {
        // Truncate on a char boundary (chars, not bytes) so non-ASCII error
        // messages do not slice through a multi-byte code point and panic.
        // `nth(60).is_some()` tests "more than 60 chars" in one short-circuiting
        // pass instead of counting every char of a possibly-large message.
        let error = if dl.error.chars().nth(60).is_some() {
            let truncated: String = dl.error.chars().take(60).collect();
            format!("{truncated}…")
        } else {
            dl.error.clone()
        };
        DeadLetterRow {
            id: dl.envelope.id.to_string(),
            kind: dl.envelope.kind.clone(),
            lane: dl.envelope.lane.as_str().to_owned(),
            attempts: dl.envelope.attempts,
            error,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::ser::{Error as _, Serialize, Serializer};

    struct FailingJson;

    impl Serialize for FailingJson {
        fn serialize<S>(&self, _serializer: S) -> std::result::Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            Err(S::Error::custom("boom"))
        }
    }

    #[test]
    fn json_line_reports_serialization_errors() {
        let err = json_line(&FailingJson).expect_err("serialization failure is returned");
        assert!(err.contains("json output failed"));
        assert!(err.contains("boom"));
    }

    #[test]
    fn render_table_aligns_columns_with_a_header() {
        let rows = vec![
            DeadLetterRow {
                id: "abc".to_owned(),
                kind: "send_email".to_owned(),
                lane: "default".to_owned(),
                attempts: 3,
                error: "timeout".to_owned(),
            },
            DeadLetterRow {
                id: "d".to_owned(),
                kind: "x".to_owned(),
                lane: "l".to_owned(),
                attempts: 1,
                error: "boom".to_owned(),
            },
        ];
        let out = render_table(&rows);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 3, "header + two rows");
        assert!(
            lines[0].starts_with("id "),
            "header is padded: {:?}",
            lines[0]
        );
        assert!(lines[0].contains("error (truncated)"));
        // The `kind` column is padded to the width of the longest value.
        assert!(lines[1].contains("send_email"));
        assert!(
            lines[2].contains("x         "),
            "short kind padded: {:?}",
            lines[2]
        );
    }

    #[test]
    fn render_table_measures_width_in_chars_for_multibyte() {
        // A multi-byte lane must not break alignment (width is counted in chars).
        let rows = vec![DeadLetterRow {
            id: "i".to_owned(),
            kind: "k".to_owned(),
            lane: "café".to_owned(),
            attempts: 0,
            error: "é".to_owned(),
        }];
        let out = render_table(&rows);
        assert!(out.contains("café"));
        // Last column is not padded, so no panic and the row renders fully.
        assert!(out.lines().count() == 2);
    }
}
