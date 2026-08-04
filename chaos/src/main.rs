//! Chaos harness: kills real processes and restarts real infrastructure, then
//! checks that no job was lost.
//!
//! Every task execution appends its marker to a ledger file, so duplicates are
//! counted rather than assumed. At-least-once delivery permits duplicates; it
//! does not permit losses.
//!
//! A fault that lands after the work already finished proves nothing, so each
//! scenario records how much was still outstanding when it struck and reports
//! `inconclusive` rather than `pass` when the answer is nothing.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{Duration, Instant};

use gylo_pg::NewJob;
use sqlx::{PgPool, Row};
use tokio::process::{Child, Command};

const DSN: &str = "postgres://gylo:gylo@127.0.0.1:5442/gylo_dev";
const SETTLE_TIMEOUT: Duration = Duration::from_secs(120);
const BUSY_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Eq)]
enum Verdict {
    Pass,
    Fail,
    Inconclusive,
}

struct Outcome {
    scenario: &'static str,
    faults: usize,
    outstanding_at_fault: Vec<i64>,
    settled: bool,
    note: Option<String>,
}

struct Harness {
    pool: PgPool,
    ledger: PathBuf,
    root: PathBuf,
    jobs: usize,
}

impl Harness {
    async fn new(jobs: usize) -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .canonicalize()
            .unwrap();
        let ledger = std::env::temp_dir().join(format!("gylo-chaos-{}.ledger", std::process::id()));
        let pool = gylo_pg::connect(DSN, 8).await.unwrap();
        gylo_pg::migrate(&pool).await.unwrap();
        Self {
            pool,
            ledger,
            root,
            jobs,
        }
    }

    async fn reset(&self) {
        let _ = std::fs::remove_file(&self.ledger);
        std::fs::write(&self.ledger, b"").unwrap();
        sqlx::query("TRUNCATE gylo_job")
            .execute(&self.pool)
            .await
            .unwrap();
        for marker in 0..self.jobs {
            let payload = rmp_serde::to_vec(&(
                Vec::<i64>::new(),
                HashMap::from([("marker", marker as i64)]),
            ))
            .unwrap();
            gylo_pg::enqueue(&self.pool, &NewJob::new("record", payload))
                .await
                .unwrap();
        }
    }

    fn spawn_worker(&self) -> Child {
        Command::new(self.root.join("target/release/chaos-worker"))
            .env("DATABASE_URL", DSN)
            .env("GYLO_CHAOS_APP", "app:app")
            .env("GYLO_CHAOS_PYTHON", self.root.join(".venv/bin/python3"))
            .env(
                "GYLO_CHAOS_PYTHONPATH",
                format!(
                    "{}:{}",
                    self.root.join("python").display(),
                    self.root.join("chaos").display()
                ),
            )
            .env("GYLO_CHAOS_LEDGER", &self.ledger)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("spawning chaos-worker")
    }

    async fn unfinished(&self) -> i64 {
        sqlx::query("SELECT count(*) FROM gylo_job WHERE state IN ('available','running')")
            .fetch_one(&self.pool)
            .await
            .map(|row| row.get(0))
            .unwrap_or(-1)
    }

    /// Waits until the worker has started making progress but has not finished,
    /// so a fault injected now actually interrupts something.
    async fn wait_until_busy(&self) -> Option<i64> {
        let deadline = Instant::now() + BUSY_TIMEOUT;
        let total = self.jobs as i64;
        while Instant::now() < deadline {
            let left = self.unfinished().await;
            if left > 0 && left < total {
                return Some(left);
            }
            if left == 0 {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        None
    }

    async fn settle(&self) -> bool {
        let deadline = Instant::now() + SETTLE_TIMEOUT;
        while Instant::now() < deadline {
            if self.unfinished().await == 0 {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        false
    }

    fn ledger_counts(&self) -> HashMap<i64, usize> {
        let mut counts = HashMap::new();
        let contents = std::fs::read_to_string(&self.ledger).unwrap_or_default();
        for line in contents.lines() {
            if let Ok(marker) = line.trim().parse::<i64>() {
                *counts.entry(marker).or_insert(0) += 1;
            }
        }
        counts
    }

    async fn report(&self, outcome: Outcome) -> Verdict {
        let counts = self.ledger_counts();
        let executed = counts.len();
        let lost = self.jobs - executed;
        let duplicated = counts.values().filter(|count| **count > 1).count();
        let total_runs: usize = counts.values().sum();
        let interrupted = outcome
            .outstanding_at_fault
            .iter()
            .copied()
            .max()
            .unwrap_or(0);

        let completed: i64 = sqlx::query("SELECT count(*) FROM gylo_job WHERE state = 'completed'")
            .fetch_one(&self.pool)
            .await
            .map(|row| row.get(0))
            .unwrap_or(-1);

        let verdict = if !outcome.settled || lost > 0 {
            Verdict::Fail
        } else if outcome.faults == 0 || interrupted == 0 {
            Verdict::Inconclusive
        } else {
            Verdict::Pass
        };

        println!(
            "{:<20} {:>6}  {:>11}  {:>4}  {:>4}  {:>6.2}  {:>9}  {}",
            outcome.scenario,
            outcome.faults,
            interrupted,
            lost,
            duplicated,
            total_runs as f64 / self.jobs as f64,
            completed,
            match verdict {
                Verdict::Pass => "pass",
                Verdict::Fail => "FAIL",
                Verdict::Inconclusive => "inconclusive",
            }
        );
        if !outcome.settled {
            println!("    jobs never settled within {SETTLE_TIMEOUT:?}");
        }
        if let Some(note) = outcome.note {
            println!("    {note}");
        }
        verdict
    }
}

/// Polls rather than sampling once, because a respawn takes a moment and a
/// single sample taken during the gap reports a failure that did not happen.
async fn awaits_respawn(within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if python_children().await > 0 {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// Counts matching pids by line. `pgrep -c` is a Linux extension that macOS
/// does not have, and silently returns nothing there.
async fn python_children() -> usize {
    Command::new("pgrep")
        .args(["-f", "gylo._worker"])
        .output()
        .await
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|text| text.lines().filter(|line| !line.trim().is_empty()).count())
        .unwrap_or(0)
}

async fn kill_python_children() -> bool {
    Command::new("pkill")
        .args(["-9", "-f", "gylo._worker"])
        .status()
        .await
        .map(|status| status.success())
        .unwrap_or(false)
}

async fn scenario_child_kill(harness: &Harness) -> Verdict {
    harness.reset().await;
    let mut worker = harness.spawn_worker();

    let mut faults = 0;
    let mut outstanding = Vec::new();
    let mut respawned = None;

    for round in 0..4 {
        let Some(left) = harness.wait_until_busy().await else {
            break;
        };
        if kill_python_children().await {
            faults += 1;
            outstanding.push(left);
        }
        if round == 0 {
            respawned = Some(awaits_respawn(Duration::from_secs(5)).await);
        }
        tokio::time::sleep(Duration::from_millis(1200)).await;
    }

    let settled = harness.settle().await;
    let _ = worker.kill().await;
    kill_python_children().await;

    let note = match respawned {
        Some(false) => Some(
            "supervisor did not replace the killed child; recovery came only from lease expiry"
                .to_owned(),
        ),
        _ => None,
    };

    harness
        .report(Outcome {
            scenario: "python child kill -9",
            faults,
            outstanding_at_fault: outstanding,
            settled,
            note,
        })
        .await
}

async fn scenario_worker_kill(harness: &Harness) -> Verdict {
    harness.reset().await;
    let mut worker = harness.spawn_worker();
    let mut faults = 0;
    let mut outstanding = Vec::new();

    for _ in 0..3 {
        let Some(left) = harness.wait_until_busy().await else {
            break;
        };
        let _ = worker.kill().await;
        kill_python_children().await;
        faults += 1;
        outstanding.push(left);
        tokio::time::sleep(Duration::from_millis(300)).await;
        worker = harness.spawn_worker();
    }

    let settled = harness.settle().await;
    let _ = worker.kill().await;
    kill_python_children().await;

    harness
        .report(Outcome {
            scenario: "supervisor kill -9",
            faults,
            outstanding_at_fault: outstanding,
            settled,
            note: None,
        })
        .await
}

async fn scenario_postgres_restart(harness: &Harness) -> Verdict {
    harness.reset().await;
    let mut worker = harness.spawn_worker();
    let mut faults = 0;
    let mut outstanding = Vec::new();

    if let Some(left) = harness.wait_until_busy().await {
        let compose = harness.root.join(".claude/docker-compose.yml");
        let restarted = Command::new("docker")
            .args(["compose", "-f"])
            .arg(&compose)
            .args(["restart", "postgres"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .map(|status| status.success())
            .unwrap_or(false);
        if restarted {
            faults += 1;
            outstanding.push(left);
        }
    }

    let settled = harness.settle().await;
    let _ = worker.kill().await;
    kill_python_children().await;

    harness
        .report(Outcome {
            scenario: "postgres restart",
            faults,
            outstanding_at_fault: outstanding,
            settled,
            note: None,
        })
        .await
}

#[tokio::main]
async fn main() {
    let jobs: usize = std::env::var("JOBS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(2000);
    let only = std::env::args().nth(1);
    let harness = Harness::new(jobs).await;

    println!("{jobs} jobs per scenario\n");
    println!(
        "{:<20} {:>6}  {:>11}  {:>4}  {:>4}  {:>6}  {:>9}  verdict",
        "scenario", "faults", "interrupted", "lost", "dup", "runs", "completed"
    );
    println!("{:-<92}", "");

    let mut verdicts = Vec::new();
    let wanted = |name: &str| only.as_deref().is_none_or(|filter| filter == name);

    if wanted("child") {
        verdicts.push(scenario_child_kill(&harness).await);
    }
    if wanted("worker") {
        verdicts.push(scenario_worker_kill(&harness).await);
    }
    if wanted("postgres") {
        verdicts.push(scenario_postgres_restart(&harness).await);
    }

    let _ = std::fs::remove_file(&harness.ledger);
    if verdicts.iter().any(|verdict| *verdict != Verdict::Pass) {
        std::process::exit(1);
    }
}
