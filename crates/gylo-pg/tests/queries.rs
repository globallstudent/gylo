use std::collections::HashSet;
use std::time::Duration;

use gylo_pg::{NewJob, complete_many, discard, discard_many, enqueue, fetch, reclaim_expired};
use sqlx::{PgPool, Row};
use uuid::Uuid;

const LEASE: Duration = Duration::from_secs(30);

fn worker() -> Uuid {
    Uuid::new_v4()
}

async fn state_of(pool: &PgPool, id: i64) -> String {
    sqlx::query("SELECT state::text FROM gylo_job WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
        .get::<String, _>(0)
}

#[sqlx::test(migrations = "../../migrations")]
async fn enqueued_job_comes_back_from_fetch(pool: PgPool) {
    let id = enqueue(&pool, &NewJob::new("billing.charge", vec![1, 2, 3]))
        .await
        .unwrap();

    let jobs = fetch(&pool, "default", 10, LEASE, worker()).await.unwrap();

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].id, id);
    assert_eq!(jobs[0].task, "billing.charge");
    assert_eq!(jobs[0].payload, vec![1, 2, 3]);
    assert_eq!(jobs[0].attempt, 1);
    assert_eq!(state_of(&pool, id).await, "running");
}

#[sqlx::test(migrations = "../../migrations")]
async fn fetch_honours_the_limit(pool: PgPool) {
    for _ in 0..10 {
        enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();
    }

    let jobs = fetch(&pool, "default", 4, LEASE, worker()).await.unwrap();

    assert_eq!(jobs.len(), 4);
}

#[sqlx::test(migrations = "../../migrations")]
async fn delayed_job_is_not_yet_eligible(pool: PgPool) {
    enqueue(
        &pool,
        &NewJob::new("t", Vec::new()).delayed_by(Duration::from_secs(3600)),
    )
    .await
    .unwrap();

    let jobs = fetch(&pool, "default", 10, LEASE, worker()).await.unwrap();

    assert!(jobs.is_empty());
}

#[sqlx::test(migrations = "../../migrations")]
async fn fetch_is_scoped_to_one_queue(pool: PgPool) {
    enqueue(&pool, &NewJob::new("t", Vec::new()).on_queue("emails"))
        .await
        .unwrap();

    assert!(
        fetch(&pool, "default", 10, LEASE, worker())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        fetch(&pool, "emails", 10, LEASE, worker())
            .await
            .unwrap()
            .len(),
        1
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn lower_priority_number_runs_first(pool: PgPool) {
    let low = enqueue(&pool, &NewJob::new("t", Vec::new()).with_priority(9))
        .await
        .unwrap();
    let high = enqueue(&pool, &NewJob::new("t", Vec::new()).with_priority(0))
        .await
        .unwrap();

    let jobs = fetch(&pool, "default", 2, LEASE, worker()).await.unwrap();

    assert_eq!(jobs[0].id, high);
    assert_eq!(jobs[1].id, low);
}

#[sqlx::test(migrations = "../../migrations")]
async fn concurrent_fetches_never_hand_out_the_same_job(pool: PgPool) {
    for _ in 0..20 {
        enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();
    }

    let mut first = pool.begin().await.unwrap();
    let mut second = pool.begin().await.unwrap();

    let claimed_first = fetch(&mut *first, "default", 10, LEASE, worker())
        .await
        .unwrap();
    let claimed_second = fetch(&mut *second, "default", 10, LEASE, worker())
        .await
        .unwrap();

    first.commit().await.unwrap();
    second.commit().await.unwrap();

    assert_eq!(claimed_first.len(), 10);
    assert_eq!(claimed_second.len(), 10);

    let ids_first: HashSet<i64> = claimed_first.iter().map(|job| job.id).collect();
    let ids_second: HashSet<i64> = claimed_second.iter().map(|job| job.id).collect();
    assert!(
        ids_first.is_disjoint(&ids_second),
        "SKIP LOCKED handed the same job to two workers"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn discarding_records_the_attempt_history(pool: PgPool) {
    let me = worker();
    let id = enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();
    fetch(&pool, "default", 1, LEASE, me).await.unwrap();

    assert!(discard(&pool, id, me, "ValueError: nope").await.unwrap());
    assert_eq!(state_of(&pool, id).await, "discarded");

    let errors: serde_json::Value = sqlx::query("SELECT errors FROM gylo_job WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);

    assert_eq!(errors.as_array().unwrap().len(), 1);
    assert_eq!(errors[0]["error"], "ValueError: nope");
    assert_eq!(errors[0]["attempt"], 1);
}

#[sqlx::test(migrations = "../../migrations")]
async fn complete_many_finalises_the_whole_batch(pool: PgPool) {
    for _ in 0..50 {
        enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();
    }
    let me = worker();
    let jobs = fetch(&pool, "default", 50, LEASE, me).await.unwrap();
    let ids: Vec<i64> = jobs.iter().map(|job| job.id).collect();

    assert_eq!(complete_many(&pool, &ids, me).await.unwrap(), 50);

    for id in &ids {
        assert_eq!(state_of(&pool, *id).await, "completed");
    }

    let row = sqlx::query(
        "SELECT count(*) FILTER (WHERE finalized_at IS NULL) AS unfinalised,
                count(*) FILTER (WHERE locked_by IS NOT NULL) AS still_locked,
                count(*) FILTER (WHERE lease_expires_at IS NOT NULL) AS still_leased
         FROM gylo_job WHERE id = ANY($1)",
    )
    .bind(&ids)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.get::<i64, _>("unfinalised"), 0);
    assert_eq!(row.get::<i64, _>("still_locked"), 0);
    assert_eq!(row.get::<i64, _>("still_leased"), 0);
}

#[sqlx::test(migrations = "../../migrations")]
async fn complete_many_skips_jobs_nobody_leased(pool: PgPool) {
    let me = worker();
    let leased = enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();
    let untouched = enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();
    fetch(&pool, "default", 1, LEASE, me).await.unwrap();

    assert_eq!(
        complete_many(&pool, &[leased, untouched], me)
            .await
            .unwrap(),
        1,
        "only the leased job should be finalised"
    );
    assert_eq!(state_of(&pool, leased).await, "completed");
    assert_eq!(state_of(&pool, untouched).await, "available");
}

#[sqlx::test(migrations = "../../migrations")]
async fn discard_many_pairs_each_error_with_its_own_job(pool: PgPool) {
    for _ in 0..5 {
        enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();
    }
    let me = worker();
    let jobs = fetch(&pool, "default", 5, LEASE, me).await.unwrap();
    let ids: Vec<i64> = jobs.iter().map(|job| job.id).collect();
    let errors: Vec<String> = ids.iter().map(|id| format!("failure for {id}")).collect();

    assert_eq!(discard_many(&pool, &ids, &errors, me).await.unwrap(), 5);

    for id in &ids {
        assert_eq!(state_of(&pool, *id).await, "discarded");
        let recorded: serde_json::Value = sqlx::query("SELECT errors FROM gylo_job WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap()
            .get(0);
        assert_eq!(recorded[0]["error"], format!("failure for {id}"));
        assert_eq!(recorded[0]["attempt"], 1);
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn empty_batches_are_a_no_op(pool: PgPool) {
    assert_eq!(complete_many(&pool, &[], worker()).await.unwrap(), 0);
    assert_eq!(discard_many(&pool, &[], &[], worker()).await.unwrap(), 0);
}

async fn scheduled_in(pool: &PgPool, id: i64) -> f64 {
    sqlx::query(
        "SELECT (EXTRACT(EPOCH FROM (scheduled_at - now())))::float8 FROM gylo_job WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .unwrap()
    .get(0)
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_failed_job_is_rescheduled_with_backoff(pool: PgPool) {
    let me = worker();
    let id = enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();
    fetch(&pool, "default", 1, LEASE, me).await.unwrap();

    let retried = gylo_pg::retry_many(
        &pool,
        &[id],
        &["boom".to_owned()],
        me,
        Duration::from_secs(10),
        Duration::from_secs(3600),
    )
    .await
    .unwrap();

    assert_eq!(retried.scheduled, 1);
    assert_eq!(retried.exhausted, 0);
    assert_eq!(state_of(&pool, id).await, "available");

    let delay = scheduled_in(&pool, id).await;
    assert!(
        (5.0..=10.0).contains(&delay),
        "first attempt should land within the jittered 50-100% band, got {delay}"
    );
    assert!(
        fetch(&pool, "default", 10, LEASE, worker())
            .await
            .unwrap()
            .is_empty(),
        "a job waiting out its backoff must not be eligible yet"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn backoff_grows_with_each_attempt(pool: PgPool) {
    let me = worker();
    let id = enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();
    let mut previous = 0.0;

    for _ in 0..4 {
        sqlx::query("UPDATE gylo_job SET scheduled_at = now() WHERE id = $1")
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
        fetch(&pool, "default", 1, LEASE, me).await.unwrap();
        gylo_pg::retry_many(
            &pool,
            &[id],
            &["boom".to_owned()],
            me,
            Duration::from_secs(1),
            Duration::from_secs(3600),
        )
        .await
        .unwrap();

        let delay = scheduled_in(&pool, id).await;
        assert!(
            delay > previous,
            "attempt delay {delay} did not exceed the previous {previous}"
        );
        previous = delay;
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn backoff_is_capped(pool: PgPool) {
    let me = worker();
    let id = enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();
    sqlx::query("UPDATE gylo_job SET attempt = 20, max_attempts = 100, state = 'running', locked_by = $2, lease_expires_at = now() + interval '1 minute' WHERE id = $1")
        .bind(id)
        .bind(me)
        .execute(&pool)
        .await
        .unwrap();

    gylo_pg::retry_many(
        &pool,
        &[id],
        &["boom".to_owned()],
        me,
        Duration::from_secs(1),
        Duration::from_secs(60),
    )
    .await
    .unwrap();

    let delay = scheduled_in(&pool, id).await;
    assert!(
        delay <= 60.0,
        "2^19 seconds must be clamped to the cap, got {delay}"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_failure_on_the_last_attempt_is_dead_lettered(pool: PgPool) {
    let me = worker();
    let mut job = NewJob::new("t", Vec::new());
    job.max_attempts = 1;
    let id = enqueue(&pool, &job).await.unwrap();
    fetch(&pool, "default", 1, LEASE, me).await.unwrap();

    let retried = gylo_pg::retry_many(
        &pool,
        &[id],
        &["boom".to_owned()],
        me,
        Duration::from_secs(1),
        Duration::from_secs(60),
    )
    .await
    .unwrap();

    assert_eq!(retried.scheduled, 0);
    assert_eq!(retried.exhausted, 1);
    assert_eq!(state_of(&pool, id).await, "discarded");
}

async fn expire_lease(pool: &PgPool, id: i64) {
    sqlx::query("UPDATE gylo_job SET lease_expires_at = now() - interval '1 second' WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
}

#[sqlx::test(migrations = "../../migrations")]
async fn an_expired_lease_returns_the_job_to_the_queue(pool: PgPool) {
    let id = enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();
    fetch(&pool, "default", 1, LEASE, worker()).await.unwrap();
    expire_lease(&pool, id).await;

    let reclaimed = reclaim_expired(&pool, 100).await.unwrap();

    assert_eq!(reclaimed.released, 1);
    assert_eq!(reclaimed.exhausted, 0);
    assert_eq!(state_of(&pool, id).await, "available");
    assert_eq!(
        fetch(&pool, "default", 10, LEASE, worker())
            .await
            .unwrap()
            .len(),
        1,
        "a reclaimed job must be fetchable again"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_live_lease_is_left_alone(pool: PgPool) {
    let id = enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();
    fetch(&pool, "default", 1, LEASE, worker()).await.unwrap();

    assert_eq!(
        reclaim_expired(&pool, 100).await.unwrap(),
        gylo_pg::Reclaimed::default()
    );
    assert_eq!(state_of(&pool, id).await, "running");
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_job_out_of_attempts_is_dead_lettered_rather_than_retried(pool: PgPool) {
    let mut job = NewJob::new("t", Vec::new());
    job.max_attempts = 1;
    let id = enqueue(&pool, &job).await.unwrap();
    fetch(&pool, "default", 1, LEASE, worker()).await.unwrap();
    expire_lease(&pool, id).await;

    let reclaimed = reclaim_expired(&pool, 100).await.unwrap();

    assert_eq!(reclaimed.released, 0);
    assert_eq!(reclaimed.exhausted, 1);
    assert_eq!(state_of(&pool, id).await, "discarded");

    let errors: serde_json::Value = sqlx::query("SELECT errors FROM gylo_job WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    assert!(
        errors[0]["error"]
            .as_str()
            .unwrap()
            .contains("lease expired")
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn reclaim_never_pushes_attempt_past_its_limit(pool: PgPool) {
    let mut job = NewJob::new("t", Vec::new());
    job.max_attempts = 3;
    let id = enqueue(&pool, &job).await.unwrap();

    for expected in 1..=3 {
        let jobs = fetch(&pool, "default", 1, LEASE, worker()).await.unwrap();
        assert_eq!(jobs[0].attempt, expected);
        expire_lease(&pool, id).await;
        reclaim_expired(&pool, 100).await.unwrap();
    }

    assert_eq!(
        state_of(&pool, id).await,
        "discarded",
        "the third expiry exhausts the job rather than violating the attempt bound"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn reclaim_honours_its_limit(pool: PgPool) {
    for _ in 0..10 {
        enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();
    }
    let jobs = fetch(&pool, "default", 10, LEASE, worker()).await.unwrap();
    for job in &jobs {
        expire_lease(&pool, job.id).await;
    }

    assert_eq!(reclaim_expired(&pool, 4).await.unwrap().released, 4);
    assert_eq!(reclaim_expired(&pool, 100).await.unwrap().released, 6);
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_leased_job_is_invisible_to_the_next_fetch(pool: PgPool) {
    enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();

    assert_eq!(
        fetch(&pool, "default", 10, LEASE, worker())
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        fetch(&pool, "default", 10, LEASE, worker())
            .await
            .unwrap()
            .is_empty()
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn one_worker_cannot_finalise_a_job_another_holds(pool: PgPool) {
    let holder = worker();
    let stranger = worker();
    let id = enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();
    fetch(&pool, "default", 1, LEASE, holder).await.unwrap();

    assert_eq!(complete_many(&pool, &[id], stranger).await.unwrap(), 0);
    assert_eq!(
        discard_many(&pool, &[id], &["nope".to_owned()], stranger)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        gylo_pg::retry_many(
            &pool,
            &[id],
            &["nope".to_owned()],
            stranger,
            Duration::from_secs(1),
            Duration::from_secs(60),
        )
        .await
        .unwrap(),
        gylo_pg::Retried::default()
    );
    assert_eq!(state_of(&pool, id).await, "running");

    assert_eq!(complete_many(&pool, &[id], holder).await.unwrap(), 1);
}

#[sqlx::test(migrations = "../../migrations")]
async fn one_worker_cannot_expire_or_extend_a_lease_another_holds(pool: PgPool) {
    let holder = worker();
    let stranger = worker();
    let id = enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();
    fetch(&pool, "default", 1, LEASE, holder).await.unwrap();

    assert_eq!(
        gylo_pg::abandon_leases(&pool, &[id], stranger)
            .await
            .unwrap(),
        0,
        "a stranger expiring this lease would hand a live job to a third worker"
    );
    assert_eq!(
        gylo_pg::renew_leases(&pool, &[id], stranger, LEASE)
            .await
            .unwrap(),
        0
    );

    assert_eq!(
        gylo_pg::abandon_leases(&pool, &[id], holder).await.unwrap(),
        1
    );
}
