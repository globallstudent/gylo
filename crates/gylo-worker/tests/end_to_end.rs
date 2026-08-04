use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::PathBuf;
use std::time::Duration;

use gylo_pg::{NewJob, enqueue};
use gylo_worker::{Config, run};
use sqlx::{PgPool, Row};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const TIMEOUT: Duration = Duration::from_secs(30);

fn crate_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn workspace_root() -> PathBuf {
    crate_dir().join("../..").canonicalize().unwrap()
}

fn effects_log() -> PathBuf {
    std::env::temp_dir().join(format!("gylo-effects-{}.log", Uuid::new_v4()))
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
        // one child unless a test asks for more: several of these assert on
        // state a task kept in its own process, which only holds when every
        // attempt lands in the same one
        processes: 1,
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

/// Scoped to the queue the worker actually consumes. A schedule firing onto
/// another queue is not work this worker will ever settle.
async fn unfinished(pool: &PgPool) -> i64 {
    sqlx::query(
        "SELECT count(*) FROM gylo_job
         WHERE state IN ('available', 'running') AND queue = 'default'",
    )
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

    let errors: serde_json::Value = sqlx::query("SELECT errors FROM gylo_job WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        state_of(&pool, id).await,
        "completed",
        "errors were: {errors}"
    );
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

async fn schedules(pool: &PgPool) -> Vec<(String, String, String)> {
    sqlx::query("SELECT name, expression, timezone FROM gylo_cron ORDER BY name")
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect()
}

/// Runs until `done` holds, rather than for a fixed time: the child is a real
/// interpreter and its startup dominates anything worth sleeping for.
async fn run_until<F>(pool: &PgPool, config: Config, done: F)
where
    F: AsyncFn(&PgPool) -> bool,
{
    let shutdown = CancellationToken::new();
    let worker = tokio::spawn(run(pool.clone(), config, shutdown.clone()));

    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while !done(pool).await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition never held within {TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    shutdown.cancel();
    worker
        .await
        .unwrap()
        .expect("supervisor exited with an error");
}

async fn registered(pool: &PgPool) -> bool {
    !schedules(pool).await.is_empty()
}

async fn beat_jobs(pool: &PgPool) -> i64 {
    sqlx::query("SELECT count(*) FROM gylo_job WHERE queue = 'beat'")
        .fetch_one(pool)
        .await
        .unwrap()
        .get(0)
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_child_registers_its_schedules_on_connect(pool: PgPool) {
    run_until(
        &pool,
        Config {
            maintenance_interval: Duration::from_secs(25),
            ..config()
        },
        registered,
    )
    .await;

    assert_eq!(
        schedules(&pool).await,
        vec![
            (
                "every_second".to_owned(),
                "* * * * * *".to_owned(),
                "UTC".to_owned()
            ),
            (
                "nightly".to_owned(),
                "0 3 * * *".to_owned(),
                "Europe/London".to_owned()
            ),
        ]
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_due_schedule_enqueues_its_job(pool: PgPool) {
    run_until(
        &pool,
        Config {
            maintenance_interval: Duration::from_millis(100),
            ..config()
        },
        async |pool| beat_jobs(pool).await >= 1,
    )
    .await;

    assert!(beat_jobs(&pool).await >= 1);

    let nightly: i64 = sqlx::query("SELECT count(*) FROM gylo_job WHERE task = 'nightly'")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    assert_eq!(nightly, 0, "a nightly schedule is not due within the test");
}

#[sqlx::test(migrations = "../../migrations")]
async fn re_registering_keeps_the_pause_an_operator_set(pool: PgPool) {
    run_until(
        &pool,
        Config {
            maintenance_interval: Duration::from_secs(25),
            ..config()
        },
        registered,
    )
    .await;
    sqlx::query("UPDATE gylo_cron SET paused = true WHERE name = 'every_second'")
        .execute(&pool)
        .await
        .unwrap();

    let shutdown = CancellationToken::new();
    let worker = tokio::spawn(run(
        pool.clone(),
        Config {
            maintenance_interval: Duration::from_millis(100),
            ..config()
        },
        shutdown.clone(),
    ));
    tokio::time::sleep(Duration::from_millis(2500)).await;
    shutdown.cancel();
    let _ = worker.await;

    let paused: bool = sqlx::query("SELECT paused FROM gylo_cron WHERE name = 'every_second'")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    assert!(
        paused,
        "a deploy must not silently resume a paused schedule"
    );

    assert_eq!(beat_jobs(&pool).await, 0, "a paused schedule must not fire");
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

    let reported = error.to_string();
    assert!(
        reported.contains("python child exited"),
        "the operator needs the real cause, got {reported}"
    );
    assert!(
        reported.contains("No module named 'no_such_module'"),
        "an exit status alone does not say what is wrong; the child's own last \
         words are the only thing that names the misconfiguration, got {reported}"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_stored_result_survives_to_the_row(pool: PgPool) {
    let id = enqueue(&pool, &NewJob::new("adds", payload(&[2, 3], &[])))
        .await
        .unwrap();

    run_until_settled(&pool).await;

    assert_eq!(state_of(&pool, id).await, "completed");
    let stored: Option<Vec<u8>> = sqlx::query("SELECT result FROM gylo_job WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    let decoded: serde_json::Value =
        rmp_serde::from_slice(&stored.expect("a task with store_result must leave one")).unwrap();
    assert_eq!(decoded["sum"], 5);
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_task_without_store_result_leaves_none(pool: PgPool) {
    let id = enqueue(&pool, &NewJob::new("ok", Vec::new()))
        .await
        .unwrap();

    run_until_settled(&pool).await;

    let stored: Option<Vec<u8>> = sqlx::query("SELECT result FROM gylo_job WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    assert!(
        stored.is_none(),
        "storing what nobody asked for costs a write"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_result_that_cannot_be_encoded_is_not_retried(pool: PgPool) {
    let mut job = NewJob::new("unserialisable", Vec::new());
    job.max_attempts = 10;
    let id = enqueue(&pool, &job).await.unwrap();

    run_until_settled_with(&pool, quick_retries()).await;

    assert_eq!(state_of(&pool, id).await, "discarded");
    assert_eq!(
        attempt_of(&pool, id).await,
        1,
        "the return value will not encode on a second attempt either"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_completed_step_is_replayed_rather_than_repeated(pool: PgPool) {
    let log = effects_log();
    std::fs::write(&log, b"").unwrap();

    let mut job = NewJob::new("two_steps", payload(&[], &[("marker", "order-1")]));
    job.durable = true;
    job.max_attempts = 5;
    let id = enqueue(&pool, &job).await.unwrap();

    run_until_settled_with(
        &pool,
        Config {
            env: vec![("GYLO_TEST_EFFECTS".into(), log.clone().into())],
            ..quick_retries()
        },
    )
    .await;

    let errors: serde_json::Value = sqlx::query("SELECT errors FROM gylo_job WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        state_of(&pool, id).await,
        "completed",
        "errors were: {errors}"
    );
    assert_eq!(
        attempt_of(&pool, id).await,
        2,
        "the fixture fails once, so it must have taken a second attempt"
    );

    let effects = std::fs::read_to_string(&log).unwrap_or_default();
    let charges = effects.matches("order-1:charge").count();
    assert_eq!(
        charges, 1,
        "the charge step ran {charges} times; a replayed step must not repeat its \
         side effect, effects were:\n{effects}"
    );
    let _ = std::fs::remove_file(&log);
}

#[sqlx::test(migrations = "../../migrations")]
async fn steps_are_recorded_against_the_job(pool: PgPool) {
    let log = effects_log();
    std::fs::write(&log, b"").unwrap();

    let mut job = NewJob::new("two_steps", payload(&[], &[("marker", "order-2")]));
    job.durable = true;
    job.max_attempts = 5;
    let id = enqueue(&pool, &job).await.unwrap();

    run_until_settled_with(
        &pool,
        Config {
            env: vec![("GYLO_TEST_EFFECTS".into(), log.clone().into())],
            ..quick_retries()
        },
    )
    .await;

    let names: Vec<String> =
        sqlx::query("SELECT name FROM gylo_step WHERE job_id = $1 ORDER BY name")
            .bind(id)
            .fetch_all(&pool)
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.get(0))
            .collect();
    assert_eq!(names, vec!["charge".to_owned(), "finish".to_owned()]);
    let _ = std::fs::remove_file(&log);
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

#[sqlx::test(migrations = "../../migrations")]
async fn required_features_are_checked_before_anything_starts(pool: PgPool) {
    let shutdown = CancellationToken::new();
    shutdown.cancel();

    let config = Config {
        requires: vec![
            gylo_core::Feature::Workflows,
            gylo_core::Feature::DurableSteps,
            gylo_core::Feature::TransactionalEnqueue,
        ],
        ..config()
    };

    run(pool, config, shutdown)
        .await
        .expect("postgres provides everything gylo asks of it");
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_backlog_is_shared_across_children(pool: PgPool) {
    let log = effects_log();
    for _ in 0..2 {
        enqueue(&pool, &NewJob::new("rendezvous", payload(&[2], &[])))
            .await
            .unwrap();
    }

    run_until_settled_with(
        &pool,
        Config {
            processes: 2,
            // one job per child, so both jobs can only be running at once if
            // they are running in different children
            concurrency: 1,
            batch: 1,
            env: vec![("GYLO_TEST_EFFECTS".into(), log.clone().into())],
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
        completed, 2,
        "the tasks wait for each other, so neither completes unless two \
         children leased at the same time"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_task_that_overruns_its_timeout_is_failed(pool: PgPool) {
    let mut job = NewJob::new("hangs", Vec::new());
    job.max_attempts = 1;
    let id = enqueue(&pool, &job).await.unwrap();

    run_until_settled_with(&pool, quick_retries()).await;

    assert_eq!(
        state_of(&pool, id).await,
        "discarded",
        "a job with no deadline holds its lease for as long as the worker \
         lives, because maintenance keeps renewing it"
    );
    let errors: serde_json::Value = sqlx::query("SELECT errors FROM gylo_job WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    let rendered = errors[0]["error"].as_str().unwrap();
    assert!(rendered.contains("TimeoutError"), "got {rendered}");
}

#[sqlx::test(migrations = "../../migrations")]
async fn synchronous_tasks_do_not_hold_the_event_loop(pool: PgPool) {
    let log = effects_log();
    for _ in 0..2 {
        enqueue(&pool, &NewJob::new("sync_rendezvous", payload(&[2], &[])))
            .await
            .unwrap();
    }

    run_until_settled_with(
        &pool,
        Config {
            processes: 1,
            env: vec![("GYLO_TEST_EFFECTS".into(), log.clone().into())],
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
        completed, 2,
        "these wait for each other inside one child, so neither finishes \
         unless a synchronous body leaves the loop free to dispatch the next"
    );
}

/// Two workers, as a deployment actually runs them: separate supervisors with
/// their own lease identities, coordinating through nothing but the queue.
async fn drain_with_two_workers(pool: &PgPool, log: &PathBuf, config: Config) {
    let shutdown = CancellationToken::new();
    let first = tokio::spawn(run(pool.clone(), config.clone(), shutdown.clone()));
    let second = tokio::spawn(run(pool.clone(), config, shutdown.clone()));

    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while unfinished(pool).await > 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "two workers did not settle the queue within {TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    shutdown.cancel();
    first.await.unwrap().expect("first worker failed");
    second.await.unwrap().expect("second worker failed");
    let _ = log;
}

fn runs_recorded(log: &PathBuf) -> Vec<i64> {
    std::fs::read_to_string(log)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| line.trim().parse().ok())
        .collect()
}

#[sqlx::test(migrations = "../../migrations")]
async fn two_workers_on_one_queue_run_each_job_exactly_once(pool: PgPool) {
    let log = effects_log();
    let mut expected = Vec::new();
    for n in 0..200 {
        expected.push(n);
        enqueue(&pool, &NewJob::new("mark", payload(&[n], &[])))
            .await
            .unwrap();
    }

    drain_with_two_workers(
        &pool,
        &log,
        Config {
            processes: 2,
            env: vec![("GYLO_TEST_EFFECTS".into(), log.clone().into())],
            ..config()
        },
    )
    .await;

    let mut ran = runs_recorded(&log);
    ran.sort_unstable();
    assert_eq!(
        ran, expected,
        "leasing is only exactly-once if two workers never hand the same row \
         to both, and never leave one behind"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_key_limits_concurrency_across_workers_not_just_within_one(pool: PgPool) {
    let log = effects_log();
    for n in 0..12 {
        let mut job = NewJob::new("guarded", payload(&[n], &[]));
        job.concurrency = Some(("tenant".to_owned(), 1));
        enqueue(&pool, &job).await.unwrap();
    }

    drain_with_two_workers(
        &pool,
        &log,
        Config {
            processes: 2,
            env: vec![("GYLO_TEST_EFFECTS".into(), log.clone().into())],
            ..config()
        },
    )
    .await;

    // walking the bracket log rather than counting completions: every job
    // finishing proves only that nothing was lost, which a limit that never
    // applied would also satisfy
    let recorded = std::fs::read_to_string(&log).unwrap_or_default();
    let mut running = 0i32;
    let mut peak = 0i32;
    for line in recorded.lines() {
        match line.split_whitespace().next() {
            Some("start") => running += 1,
            Some("end") => running -= 1,
            _ => {}
        }
        peak = peak.max(running);
    }

    assert_eq!(
        recorded.lines().filter(|l| l.starts_with("end")).count(),
        12,
        "every job should still finish"
    );
    assert_eq!(
        peak, 1,
        "a key that only holds inside one worker is not admission control: \
         the tenant it protects is shared by every worker.\nlog was:\n{recorded}"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_key_limits_concurrency_within_one_worker(pool: PgPool) {
    let log = effects_log();
    for n in 0..12 {
        let mut job = NewJob::new("guarded", payload(&[n], &[]));
        job.concurrency = Some(("tenant".to_owned(), 1));
        enqueue(&pool, &job).await.unwrap();
    }

    run_until_settled_with(
        &pool,
        Config {
            processes: 4,
            env: vec![("GYLO_TEST_EFFECTS".into(), log.clone().into())],
            ..config()
        },
    )
    .await;

    let recorded = std::fs::read_to_string(&log).unwrap_or_default();
    let mut running = 0i32;
    let mut peak = 0i32;
    for line in recorded.lines() {
        match line.split_whitespace().next() {
            Some("start") => running += 1,
            Some("end") => running -= 1,
            _ => {}
        }
        peak = peak.max(running);
    }
    assert_eq!(peak, 1, "one worker, four children");
}
