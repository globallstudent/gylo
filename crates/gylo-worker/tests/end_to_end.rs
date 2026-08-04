use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::PathBuf;
use std::time::Duration;

use gylo_pg::{NewJob, enqueue};
use gylo_worker::{Config, run};
use sqlx::{PgPool, Row};
use tokio_util::sync::CancellationToken;

const TIMEOUT: Duration = Duration::from_secs(30);

fn crate_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn workspace_root() -> PathBuf {
    crate_dir().join("../..").canonicalize().unwrap()
}

fn config() -> Config {
    let mut python_path = OsString::from(workspace_root().join("python"));
    python_path.push(":");
    python_path.push(crate_dir().join("tests/fixtures"));

    Config {
        app: "testapp:app".to_owned(),
        python: workspace_root().join(".venv/bin/python3"),
        python_path: Some(python_path),
        poll_interval: Duration::from_millis(10),
        ..Config::default()
    }
}

fn payload(args: &[i64], kwargs: &[(&str, &str)]) -> Vec<u8> {
    let kwargs: BTreeMap<&str, &str> = kwargs.iter().copied().collect();
    rmp_serde::to_vec(&(args, kwargs)).unwrap()
}

async fn state_of(pool: &PgPool, id: i64) -> String {
    sqlx::query("SELECT state::text FROM gylo_job WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
        .get(0)
}

async fn unfinished(pool: &PgPool) -> i64 {
    sqlx::query("SELECT count(*) FROM gylo_job WHERE state IN ('available', 'running')")
        .fetch_one(pool)
        .await
        .unwrap()
        .get(0)
}

async fn run_until_settled(pool: &PgPool) {
    let shutdown = CancellationToken::new();
    let worker = tokio::spawn(run(pool.clone(), config(), shutdown.clone()));

    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while unfinished(pool).await > 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "jobs did not settle within {TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    shutdown.cancel();
    worker
        .await
        .unwrap()
        .expect("supervisor exited with an error");
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_task_runs_in_python_and_is_marked_completed(pool: PgPool) {
    let id = enqueue(&pool, &NewJob::new("ok", Vec::new()))
        .await
        .unwrap();

    run_until_settled(&pool).await;

    assert_eq!(state_of(&pool, id).await, "completed");
}

#[sqlx::test(migrations = "../../migrations")]
async fn arguments_survive_the_round_trip(pool: PgPool) {
    let id = enqueue(
        &pool,
        &NewJob::new("expects", payload(&[1, 2], &[("label", "hi")])),
    )
    .await
    .unwrap();

    run_until_settled(&pool).await;

    assert_eq!(
        state_of(&pool, id).await,
        "completed",
        "the fixture raises when its arguments arrive wrong"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_synchronous_task_is_supported(pool: PgPool) {
    let id = enqueue(&pool, &NewJob::new("sync_ok", Vec::new()))
        .await
        .unwrap();

    run_until_settled(&pool).await;

    assert_eq!(state_of(&pool, id).await, "completed");
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_raising_task_is_discarded_with_its_traceback(pool: PgPool) {
    let id = enqueue(&pool, &NewJob::new("boom", Vec::new()))
        .await
        .unwrap();

    run_until_settled(&pool).await;

    assert_eq!(state_of(&pool, id).await, "discarded");

    let errors: serde_json::Value = sqlx::query("SELECT errors FROM gylo_job WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    let rendered = errors[0]["error"].as_str().unwrap();
    assert!(rendered.contains("ValueError: boom"), "got {rendered}");
    assert!(rendered.contains("Traceback"), "got {rendered}");
}

#[sqlx::test(migrations = "../../migrations")]
async fn an_unregistered_task_is_discarded(pool: PgPool) {
    let id = enqueue(&pool, &NewJob::new("nope.not_a_task", Vec::new()))
        .await
        .unwrap();

    run_until_settled(&pool).await;

    assert_eq!(state_of(&pool, id).await, "discarded");
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_batch_of_jobs_all_complete(pool: PgPool) {
    for _ in 0..250 {
        enqueue(&pool, &NewJob::new("ok", Vec::new()))
            .await
            .unwrap();
    }

    run_until_settled(&pool).await;

    let completed: i64 = sqlx::query("SELECT count(*) FROM gylo_job WHERE state = 'completed'")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    assert_eq!(completed, 250);
}
