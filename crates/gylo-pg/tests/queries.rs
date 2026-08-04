use std::collections::HashSet;
use std::time::Duration;

use gylo_pg::{NewJob, complete, discard, enqueue, fetch};
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
async fn completing_a_leased_job_finalises_it(pool: PgPool) {
    let id = enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();
    fetch(&pool, "default", 1, LEASE, worker()).await.unwrap();

    assert!(complete(&pool, id).await.unwrap());
    assert_eq!(state_of(&pool, id).await, "completed");

    let row = sqlx::query(
        "SELECT finalized_at IS NOT NULL AS done,
                locked_by IS NULL AS unlocked,
                lease_expires_at IS NULL AS unleased
         FROM gylo_job WHERE id = $1",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(row.get::<bool, _>("done"));
    assert!(row.get::<bool, _>("unlocked"));
    assert!(row.get::<bool, _>("unleased"));
}

#[sqlx::test(migrations = "../../migrations")]
async fn completing_a_job_nobody_leased_is_a_no_op(pool: PgPool) {
    let id = enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();

    assert!(!complete(&pool, id).await.unwrap());
    assert_eq!(state_of(&pool, id).await, "available");
}

#[sqlx::test(migrations = "../../migrations")]
async fn discarding_records_the_attempt_history(pool: PgPool) {
    let id = enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();
    fetch(&pool, "default", 1, LEASE, worker()).await.unwrap();

    assert!(discard(&pool, id, "ValueError: nope").await.unwrap());
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
