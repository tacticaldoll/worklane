//! `worklane-cli` — operator CLI for inspecting and maintaining worklane
//! brokers.
//!
//! # Usage
//!
//! ```text
//! wl --broker sqlite --db ./jobs.db dead-letters list default
//! wl --broker postgres --url $DATABASE_URL dead-letters list critical --limit 20
//! wl --broker redis --url $REDIS_URL dead-letters requeue <id>
//! wl --broker sqlite --db ./jobs.db stats default
//! wl --broker sqlite --db ./jobs.db classify <job-id>
//! ```
#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod broker;
mod cmd;

use clap::{Parser, Subcommand};
use worklane_core::JobId;

/// Worklane operator CLI.
///
/// Connect to a durable broker and inspect or maintain lanes and dead-letters.
#[derive(Parser)]
#[command(name = "wl", version, about)]
struct Cli {
    /// Broker backend to connect to.
    #[arg(long, value_name = "TYPE", global = true, default_value = "sqlite")]
    pub broker: String,

    /// Path to the SQLite database file (required when --broker sqlite).
    #[arg(long, value_name = "PATH", global = true)]
    pub db: Option<String>,

    /// Connection URL for Postgres or Redis brokers.
    ///
    /// When omitted, the URL is resolved from the environment with this explicit
    /// precedence (see `broker::connect`): `WORKLANE_URL`, then the backend's
    /// conventional variable (`DATABASE_URL` for postgres, `REDIS_URL` for
    /// redis). The chosen source is printed to stderr so the target is never
    /// ambiguous. The env binding is resolved in `broker::connect`, not by clap,
    /// to keep all precedence in one place.
    #[arg(long, value_name = "URL", global = true)]
    pub url: Option<String>,

    #[command(subcommand)]
    /// Command to execute.
    pub command: Commands,
}

/// Top-level operator commands.
#[derive(Subcommand)]
enum Commands {
    /// Inspect and manage dead-lettered jobs.
    DeadLetters {
        /// Dead-letter operation to execute.
        #[command(subcommand)]
        action: DeadLetterAction,
    },
    /// Show lane health statistics.
    Stats {
        /// The lane to inspect.
        lane: String,
    },
    /// Classify a job's lifecycle state by id.
    Classify {
        /// The job ID to classify (UUID). Rejected before connecting if invalid.
        #[arg(value_parser = parse_job_id)]
        job_id: JobId,
        /// Output format: text (default) or json.
        #[arg(long, default_value = "text")]
        format: String,
    },
}

/// Parse a `<job-id>` argument into a [`JobId`] at the CLI layer, so an invalid
/// id fails fast with a non-zero exit before any broker connection is opened.
fn parse_job_id(s: &str) -> Result<JobId, String> {
    // clap already prefixes the bad value ("invalid value '<s>' for <JOB_ID>"),
    // and the inner error names the problem, so just surface that.
    s.parse::<JobId>().map_err(|e| e.to_string())
}

/// Dead-letter maintenance commands.
#[derive(Subcommand)]
enum DeadLetterAction {
    /// List dead-letter records for a lane.
    List {
        /// The lane to inspect.
        lane: String,
        /// Maximum number of records to return.
        #[arg(long, default_value_t = 50)]
        limit: usize,
        /// Output format: jsonl (default) or table.
        #[arg(long, default_value = "jsonl")]
        format: String,
    },
    /// Requeue a dead-lettered job back to its original lane.
    Requeue {
        /// The job ID to requeue (UUID).
        id: String,
        /// Skip the confirmation prompt.
        #[arg(long, short = 'y')]
        yes: bool,
    },
    /// Permanently delete all dead-lettered jobs for a lane.
    Purge {
        /// The lane whose dead-letter store to purge.
        lane: String,
        /// Skip the confirmation prompt.
        #[arg(long, short = 'y')]
        yes: bool,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let broker = match broker::connect(&cli).await {
        Ok(b) => b,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    };

    let result = match &cli.command {
        Commands::DeadLetters {
            action:
                DeadLetterAction::List {
                    lane,
                    limit,
                    format,
                },
        } => cmd::dead_letters::list(broker.as_ref(), lane, *limit, format).await,
        Commands::DeadLetters {
            action: DeadLetterAction::Requeue { id, yes },
        } => cmd::dead_letters::requeue(broker.as_ref(), id, *yes).await,
        Commands::DeadLetters {
            action: DeadLetterAction::Purge { lane, yes },
        } => cmd::dead_letters::purge(broker.as_ref(), lane, *yes).await,
        Commands::Stats { lane } => cmd::stats::run(broker.as_ref(), lane).await,
        Commands::Classify { job_id, format } => {
            cmd::classify::run(broker.as_ref(), *job_id, format).await
        }
    };

    if let Err(e) = result {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
