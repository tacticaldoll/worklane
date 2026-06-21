use std::path::PathBuf;
use std::process::Command;

use worklane_core::{Broker, Lane, NewJob};
use worklane_sqlite::SqliteBroker;

fn temp_db(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("wl-cli-test-{}-{}.db", name, std::process::id()));
    for ext in ["db", "db-wal", "db-shm"] {
        let _ = std::fs::remove_file(path.with_extension(ext));
    }
    let _ = std::fs::remove_file(&path);
    path
}

fn cli_command(db_path: &std::path::Path) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_wl"));
    cmd.arg("--broker").arg("sqlite").arg("--db").arg(db_path);
    cmd
}

#[tokio::test]
async fn dead_letters_list_and_requeue() {
    let path = temp_db("list-requeue");
    let broker = SqliteBroker::open(&path).unwrap();
    let lane = Lane::try_from("test_lane").unwrap();

    let job = NewJob::new(lane.clone(), "test_kind", b"{}".to_vec(), 3);
    let id = broker.enqueue(job).await.unwrap();

    let res = broker.reserve(&lane).await.unwrap().unwrap();
    broker
        .fail(res.receipt, "simulated error".to_owned())
        .await
        .unwrap();

    // 1. Check dead-letter count via stats
    let output = cli_command(&path)
        .arg("stats")
        .arg("test_lane")
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Dead-letter count:  1"));
    assert!(stdout.contains("Pending job count:  0"));

    // 2. List the dead letters (JSON lines)
    let output = cli_command(&path)
        .arg("dead-letters")
        .arg("list")
        .arg("test_lane")
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(&id.to_string()));
    assert!(stdout.contains("test_kind"));
    assert!(stdout.contains("simulated error"));

    // 3. List the dead letters (Table)
    let output = cli_command(&path)
        .arg("dead-letters")
        .arg("list")
        .arg("test_lane")
        .arg("--format")
        .arg("table")
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(&id.to_string()));
    assert!(stdout.contains("test_kind"));
    assert!(stdout.contains("simulated error"));

    // 4. Requeue the job (--yes skips the interactive confirmation prompt)
    let output = cli_command(&path)
        .arg("dead-letters")
        .arg("requeue")
        .arg(id.to_string())
        .arg("--yes")
        .output()
        .unwrap();
    assert!(output.status.success());

    // 5. Verify it's back in the live queue
    let res = broker.reserve(&lane).await.unwrap().unwrap();
    assert_eq!(res.envelope.id, id);
}

#[tokio::test]
async fn requeue_without_yes_aborts_on_no_input() {
    let path = temp_db("requeue-abort");
    let broker = SqliteBroker::open(&path).unwrap();
    let lane = Lane::try_from("test_lane").unwrap();

    let id = broker
        .enqueue(NewJob::new(lane.clone(), "test_kind", b"{}".to_vec(), 3))
        .await
        .unwrap();
    let res = broker.reserve(&lane).await.unwrap().unwrap();
    broker.fail(res.receipt, "boom".to_owned()).await.unwrap();

    // No --yes and no stdin (closed) → EOF → safe "no" → aborts without requeue.
    let output = cli_command(&path)
        .arg("dead-letters")
        .arg("requeue")
        .arg(id.to_string())
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Aborted."), "stdout was: {stdout}");

    // The job must still be dead-lettered, not back on its lane.
    assert!(broker.reserve(&lane).await.unwrap().is_none());
}

#[tokio::test]
async fn empty_stats_and_list() {
    let path = temp_db("empty-stats");
    let _broker = SqliteBroker::open(&path).unwrap();

    let output = cli_command(&path)
        .arg("stats")
        .arg("empty_lane")
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Dead-letter count:  0"));
    assert!(stdout.contains("Pending job count:  0"));

    let output = cli_command(&path)
        .arg("dead-letters")
        .arg("list")
        .arg("empty_lane")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(output.stdout.is_empty());
}
