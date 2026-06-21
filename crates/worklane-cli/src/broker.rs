//! Broker connection factory for the CLI.

use std::sync::Arc;

use worklane_core::{Broker, redact_credentials};
use worklane_postgres::PostgresBroker;
use worklane_redis::RedisBroker;
use worklane_sqlite::SqliteBroker;

use crate::Cli;

/// Connect to the broker specified by `cli` global flags.
///
/// Returns `Err` with a human-readable message when required flags are missing
/// or the connection fails. Connection-error text is redacted of credentials
/// (defence in depth — the backends also redact at the error boundary).
pub async fn connect(cli: &Cli) -> Result<Arc<dyn Broker>, String> {
    match cli.broker.as_str() {
        "sqlite" => {
            let path = cli
                .db
                .as_deref()
                .ok_or_else(|| "--db <PATH> is required for --broker sqlite".to_owned())?;
            let broker = SqliteBroker::open(path)
                .map_err(|e| format!("sqlite: failed to open '{path}': {e}"))?;
            Ok(Arc::new(broker))
        }
        "postgres" => {
            let url = resolve_url(cli, "DATABASE_URL", "postgres")?;
            let broker = PostgresBroker::connect(&url).await.map_err(|e| {
                format!(
                    "postgres: connection failed: {}",
                    redact_credentials(&e.to_string())
                )
            })?;
            Ok(Arc::new(broker))
        }
        "redis" => {
            let url = resolve_url(cli, "REDIS_URL", "redis")?;
            let broker = RedisBroker::connect(&url).await.map_err(|e| {
                format!(
                    "redis: connection failed: {}",
                    redact_credentials(&e.to_string())
                )
            })?;
            Ok(Arc::new(broker))
        }
        other => Err(format!(
            "unknown broker '{other}' — supported: sqlite, postgres, redis"
        )),
    }
}

/// Resolve the connection URL with one explicit, documented precedence —
/// `--url` flag, then `WORKLANE_URL`, then the backend's conventional variable
/// (`fallback_var`) — and announce the chosen source on stderr so the operator
/// is never silently pointed at the wrong cluster by a stray env var. The URL
/// itself is never printed (it may carry a password); only the source is.
fn resolve_url(cli: &Cli, fallback_var: &str, backend: &str) -> Result<String, String> {
    if let Some(url) = cli.url.clone() {
        eprintln!("{backend}: using --url");
        return Ok(url);
    }
    if let Ok(url) = std::env::var("WORKLANE_URL") {
        eprintln!("{backend}: using $WORKLANE_URL");
        return Ok(url);
    }
    if let Ok(url) = std::env::var(fallback_var) {
        eprintln!("{backend}: using ${fallback_var}");
        return Ok(url);
    }
    Err(format!(
        "--url <URL>, $WORKLANE_URL, or ${fallback_var} is required for --broker {backend}"
    ))
}
