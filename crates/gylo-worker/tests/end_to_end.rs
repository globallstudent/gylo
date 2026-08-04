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
    run_until_settled_with(pool, config()).await;
}

async fn run_until_settled_with(pool: &PgPool, config: Config) {
    let shutdown = CancellationToken::new();
    let worker = tokio::spawn(run(pool.clone(), config, shutdown.clone()));

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
    let mut job = NewJob::new("boom", Vec::new());
    job.max_attempts = 1;
    let id = enqueue(&pool, &job).await.unwrap();

    run_until_settled_with(&pool, quick_retries()).await;

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
async fn completions_flush_on_the_linger_timer(pool: PgPool) {
    for _ in 0..3 {
        enqueue(&pool, &NewJob::new("ok", Vec::new()))
            .await
            .unwrap();
    }

    run_until_settled_with(
        &pool,
        Config {
            completion_batch: 100_000,
            completion_linger: Duration::from_millis(20),
            ..config()
        },
    )
    .await;

    let completed: i64 = sqlx::query("SELECT count(*) FROM gylo_job WHERE state = 'completed'")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        completed, 3,
        "a batch far below the size trigger must still flush on the timer"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn one_batch_carrying_both_outcomes_finalises_each_correctly(pool: PgPool) {
    let mut expected_ok = Vec::new();
    let mut expected_boom = Vec::new();
    for i in 0..40 {
        let mut job = if i % 2 == 0 {
            NewJob::new("ok", Vec::new())
        } else {
            NewJob::new("boom", Vec::new())
        };
        job.max_attempts = 1;
        let id = enqueue(&pool, &job).await.unwrap();
        if i % 2 == 0 {
            expected_ok.push(id);
        } else {
            expected_boom.push(id);
        }
    }

    run_until_settled_with(&pool, quick_retries()).await;

    for id in expected_ok {
        assert_eq!(state_of(&pool, id).await, "completed");
    }
    for id in expected_boom {
        assert_eq!(state_of(&pool, id).await, "discarded");
        let errors: serde_json::Value = sqlx::query("SELECT errors FROM gylo_job WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap()
            .get(0);
        assert!(
            errors[0]["error"].as_str().unwrap().contains("ValueError"),
            "job {id} lost its traceback in a mixed batch"
        );
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_job_abandoned_by_a_dead_worker_is_recovered_and_run(pool: PgPool) {
    let id = enqueue(&pool, &NewJob::new("ok", Vec::new()))
        .await
        .unwrap();
    sqlx::query(
        "UPDATE gylo_job
         SET state = 'running',
             locked_by = gen_random_uuid(),
             lease_expires_at = now() - interval '1 second',
             started_at = now(),
             attempt = 1
         WHERE id = $1",
    )
    .bind(id)
    .execute(&pool)
    .await
    .unwrap();

    run_until_settled_with(
        &pool,
        Config {
            maintenance_interval: Duration::from_millis(50),
            ..config()
        },
    )
    .await;

    assert_eq!(
        state_of(&pool, id).await,
        "completed",
        "a lease abandoned by a dead worker must be recovered and the job run"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn lease_renewal_keeps_a_long_task_from_being_run_twice(pool: PgPool) {
    let id = enqueue(&pool, &NewJob::new("slow", Vec::new()))
        .await
        .unwrap();

    run_until_settled_with(
        &pool,
        Config {
            lease: Duration::from_secs(1),
            maintenance_interval: Duration::from_millis(200),
            ..config()
        },
    )
    .await;

    assert_eq!(state_of(&pool, id).await, "completed");
    let attempt: i16 = sqlx::query("SELECT attempt FROM gylo_job WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        attempt, 1,
        "the task outlives its lease, so without renewal it would be reclaimed and run again"
    );
}

async fn attempt_of(pool: &PgPool, id: i64) -> i16 {
    sqlx::query("SELECT attempt FROM gylo_job WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
        .get(0)
}

fn quick_retries() -> Config {
    Config {
        retry_base: Duration::from_millis(10),
        retry_cap: Duration::from_millis(50),
        ..config()
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_transient_failure_is_retried_and_then_succeeds(pool: PgPool) {
    let id = enqueue(
        &pool,
        &NewJob::new("flaky", payload(&[], &[("marker", "1")])),
    )
    .await
    .unwrap();

    run_until_settled_with(&pool, quick_retries()).await;

    assert_eq!(state_of(&pool, id).await, "completed");
    assert_eq!(
        attempt_of(&pool, id).await,
        2,
        "the job should have failed once and succeeded on the retry"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_retryable_failure_records_every_attempt(pool: PgPool) {
    let mut job = NewJob::new("boom", Vec::new());
    job.max_attempts = 3;
    let id = enqueue(&pool, &job).await.unwrap();

    run_until_settled_with(&pool, quick_retries()).await;

    assert_eq!(state_of(&pool, id).await, "discarded");
    assert_eq!(attempt_of(&pool, id).await, 3);

    let errors: serde_json::Value = sqlx::query("SELECT errors FROM gylo_job WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        errors.as_array().unwrap().len(),
        3,
        "every attempt should leave a record, not just the last"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn an_excluded_exception_type_is_not_retried(pool: PgPool) {
    let mut job = NewJob::new("fatal", Vec::new());
    job.max_attempts = 10;
    let id = enqueue(&pool, &job).await.unwrap();

    run_until_settled_with(&pool, quick_retries()).await;

    assert_eq!(state_of(&pool, id).await, "discarded");
    assert_eq!(
        attempt_of(&pool, id).await,
        1,
        "no_retry_on should end the job on its first attempt"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn raising_no_retry_ends_the_job_immediately(pool: PgPool) {
    let mut job = NewJob::new("refused", Vec::new());
    job.max_attempts = 10;
    let id = enqueue(&pool, &job).await.unwrap();

    run_until_settled_with(&pool, quick_retries()).await;

    assert_eq!(state_of(&pool, id).await, "discarded");
    assert_eq!(attempt_of(&pool, id).await, 1);
}

#[sqlx::test(migrations = "../../migrations")]
async fn an_undecodable_payload_is_not_retried(pool: PgPool) {
    let mut job = NewJob::new("ok", vec![0xC1]);
    job.max_attempts = 10;
    let id = enqueue(&pool, &job).await.unwrap();

    run_until_settled_with(&pool, quick_retries()).await;

    assert_eq!(state_of(&pool, id).await, "discarded");
    assert_eq!(
        attempt_of(&pool, id).await,
        1,
        "a payload that cannot decode will never decode, so retrying it burns attempts for nothing"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_lease_shorter_than_the_maintenance_interval_is_rejected(pool: PgPool) {
    let config = Config {
        lease: Duration::from_secs(5),
        maintenance_interval: Duration::from_secs(30),
        ..config()
    };

    let error = run(pool, config, CancellationToken::new())
        .await
        .expect_err("this config silently re-runs healthy jobs, so it must not start");

    assert!(
        error.to_string().contains("must be shorter than the lease"),
        "got {error}"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn an_empty_app_path_is_rejected(pool: PgPool) {
    let config = Config {
        app: String::new(),
        ..config()
    };

    assert!(
        run(pool, config, CancellationToken::new())
            .await
            .is_err_and(|error| error.to_string().contains("module:attribute"))
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_child_that_cannot_start_gives_up_with_its_error(pool: PgPool) {
    let config = Config {
        app: "no_such_module:app".to_owned(),
        max_restarts: 1,
        ..config()
    };

    let error = run(pool, config, CancellationToken::new())
        .await
        .expect_err("a worker whose app cannot be imported must not run forever");

    assert!(
        error.to_string().contains("python child exited"),
        "the operator needs the real cause, got {error}"
    );
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
