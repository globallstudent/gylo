use std::collections::HashSet;
use std::time::Duration;

use gylo_pg::{NewJob, complete_many, discard, discard_many, enqueue, fetch, reclaim_expired};
use sqlx::{PgPool, Row};
use uuid::Uuid;

const LEASE: Duration = Duration::from_secs(30);

fn worker() -> Uuid {
    Uuid::new_v4()
}

fn queues(names: &str) -> Vec<String> {
    names.split(',').map(str::to_owned).collect()
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

    let jobs = fetch(&pool, &queues("default"), 10, LEASE, worker())
        .await
        .unwrap();

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

    let jobs = fetch(&pool, &queues("default"), 4, LEASE, worker())
        .await
        .unwrap();

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

    let jobs = fetch(&pool, &queues("default"), 10, LEASE, worker())
        .await
        .unwrap();

    assert!(jobs.is_empty());
}

#[sqlx::test(migrations = "../../migrations")]
async fn fetch_is_scoped_to_one_queue(pool: PgPool) {
    enqueue(&pool, &NewJob::new("t", Vec::new()).on_queue("emails"))
        .await
        .unwrap();

    assert!(
        fetch(&pool, &queues("default"), 10, LEASE, worker())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        fetch(&pool, &queues("emails"), 10, LEASE, worker())
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

    let jobs = fetch(&pool, &queues("default"), 2, LEASE, worker())
        .await
        .unwrap();

    assert_eq!(jobs[0].id, high);
    assert_eq!(jobs[1].id, low);
}

#[sqlx::test(migrations = "../../migrations")]
async fn concurrent_fetches_never_hand_out_the_same_job(pool: PgPool) {
    for _ in 0..20 {
        enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();
    }

    // genuinely at the same time, through the pool, because that is how two
    // workers reach this and the path now owns its own transaction
    let names = queues("default");
    let (claimed_first, claimed_second) = tokio::join!(
        fetch(&pool, &names, 10, LEASE, worker()),
        fetch(&pool, &names, 10, LEASE, worker()),
    );
    let claimed_first = claimed_first.unwrap();
    let claimed_second = claimed_second.unwrap();

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
    fetch(&pool, &queues("default"), 1, LEASE, me)
        .await
        .unwrap();

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
    let jobs = fetch(&pool, &queues("default"), 50, LEASE, me)
        .await
        .unwrap();
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
    fetch(&pool, &queues("default"), 1, LEASE, me)
        .await
        .unwrap();

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
    let jobs = fetch(&pool, &queues("default"), 5, LEASE, me)
        .await
        .unwrap();
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
    fetch(&pool, &queues("default"), 1, LEASE, me)
        .await
        .unwrap();

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
        fetch(&pool, &queues("default"), 10, LEASE, worker())
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
        fetch(&pool, &queues("default"), 1, LEASE, me)
            .await
            .unwrap();
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
    fetch(&pool, &queues("default"), 1, LEASE, me)
        .await
        .unwrap();

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
    fetch(&pool, &queues("default"), 1, LEASE, worker())
        .await
        .unwrap();
    expire_lease(&pool, id).await;

    let reclaimed = reclaim_expired(&pool, 100).await.unwrap();

    assert_eq!(reclaimed.released, 1);
    assert_eq!(reclaimed.exhausted, 0);
    assert_eq!(state_of(&pool, id).await, "available");
    assert_eq!(
        fetch(&pool, &queues("default"), 10, LEASE, worker())
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
    fetch(&pool, &queues("default"), 1, LEASE, worker())
        .await
        .unwrap();

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
    fetch(&pool, &queues("default"), 1, LEASE, worker())
        .await
        .unwrap();
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
        let jobs = fetch(&pool, &queues("default"), 1, LEASE, worker())
            .await
            .unwrap();
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
    let jobs = fetch(&pool, &queues("default"), 10, LEASE, worker())
        .await
        .unwrap();
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
        fetch(&pool, &queues("default"), 10, LEASE, worker())
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        fetch(&pool, &queues("default"), 10, LEASE, worker())
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
    fetch(&pool, &queues("default"), 1, LEASE, holder)
        .await
        .unwrap();

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
    fetch(&pool, &queues("default"), 1, LEASE, holder)
        .await
        .unwrap();

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

#[sqlx::test(migrations = "../../migrations")]
async fn a_key_admits_only_its_limit(pool: PgPool) {
    for _ in 0..10 {
        enqueue(
            &pool,
            &NewJob::new("t", Vec::new()).limited_to("tenant-a", 2),
        )
        .await
        .unwrap();
    }

    let jobs = fetch(&pool, &queues("default"), 100, LEASE, worker())
        .await
        .unwrap();

    assert_eq!(
        jobs.len(),
        2,
        "a single batch must not exceed the limit either"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn keys_are_limited_independently(pool: PgPool) {
    for tenant in ["a", "b", "c"] {
        for _ in 0..5 {
            enqueue(
                &pool,
                &NewJob::new("t", Vec::new()).limited_to(format!("tenant-{tenant}"), 2),
            )
            .await
            .unwrap();
        }
    }

    let jobs = fetch(&pool, &queues("default"), 100, LEASE, worker())
        .await
        .unwrap();

    assert_eq!(jobs.len(), 6, "two each from three keys");
}

#[sqlx::test(migrations = "../../migrations")]
async fn unkeyed_jobs_are_never_held_back(pool: PgPool) {
    for _ in 0..5 {
        enqueue(
            &pool,
            &NewJob::new("t", Vec::new()).limited_to("tenant-a", 1),
        )
        .await
        .unwrap();
    }
    for _ in 0..5 {
        enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();
    }

    let jobs = fetch(&pool, &queues("default"), 100, LEASE, worker())
        .await
        .unwrap();

    assert_eq!(
        jobs.len(),
        6,
        "one from the limited key, plus all five free"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_finished_job_frees_its_slot(pool: PgPool) {
    let me = worker();
    for _ in 0..3 {
        enqueue(
            &pool,
            &NewJob::new("t", Vec::new()).limited_to("tenant-a", 1),
        )
        .await
        .unwrap();
    }

    let first = fetch(&pool, &queues("default"), 10, LEASE, me)
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    assert!(
        fetch(&pool, &queues("default"), 10, LEASE, worker())
            .await
            .unwrap()
            .is_empty(),
        "the slot is taken while the first job runs"
    );

    complete_many(&pool, &[first[0].id], me).await.unwrap();

    assert_eq!(
        fetch(&pool, &queues("default"), 10, LEASE, worker())
            .await
            .unwrap()
            .len(),
        1,
        "finishing one lets the next in"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn concurrent_workers_respect_the_same_key(pool: PgPool) {
    for _ in 0..20 {
        enqueue(
            &pool,
            &NewJob::new("t", Vec::new()).limited_to("tenant-a", 3),
        )
        .await
        .unwrap();
    }

    let names = queues("default");
    let (a, b) = tokio::join!(
        fetch(&pool, &names, 20, LEASE, worker()),
        fetch(&pool, &names, 20, LEASE, worker()),
    );
    let (a, b) = (a.unwrap(), b.unwrap());

    assert!(
        a.len() + b.len() <= 3,
        "two workers claimed {} together, over the limit of 3",
        a.len() + b.len()
    );
}

async fn workflow_of(pool: &PgPool, tasks: &[&str]) -> Vec<i64> {
    let workflow: i64 = sqlx::query("INSERT INTO gylo_workflow DEFAULT VALUES RETURNING id")
        .fetch_one(pool)
        .await
        .unwrap()
        .get(0);
    let mut ids = Vec::new();
    for (index, task) in tasks.iter().enumerate() {
        let pending = i32::from(index > 0);
        let id: i64 = sqlx::query(
            "INSERT INTO gylo_job (workflow_id, queue, task, payload, pending_deps, scheduled_at)
             VALUES ($1, 'default', $2, ''::bytea, $3,
                     CASE WHEN $3 > 0 THEN 'infinity' ELSE now() END)
             RETURNING id",
        )
        .bind(workflow)
        .bind(task)
        .bind(pending)
        .fetch_one(pool)
        .await
        .unwrap()
        .get(0);
        if let Some(parent) = ids.last() {
            sqlx::query("INSERT INTO gylo_edge (workflow_id, parent, child) VALUES ($1, $2, $3)")
                .bind(workflow)
                .bind(parent)
                .bind(id)
                .execute(pool)
                .await
                .unwrap();
        }
        ids.push(id);
    }
    ids
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_chain_releases_the_next_step_only_when_the_first_finishes(pool: PgPool) {
    let me = worker();
    let ids = workflow_of(&pool, &["first", "second"]).await;

    let claimed = fetch(&pool, &queues("default"), 10, LEASE, me)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1, "the second step is not runnable yet");
    assert_eq!(claimed[0].id, ids[0]);

    complete_many(&pool, &[ids[0]], me).await.unwrap();

    let next = fetch(&pool, &queues("default"), 10, LEASE, worker())
        .await
        .unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(
        next[0].id, ids[1],
        "finishing the first releases the second"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_fan_in_waits_for_every_parent(pool: PgPool) {
    let me = worker();
    let workflow: i64 = sqlx::query("INSERT INTO gylo_workflow DEFAULT VALUES RETURNING id")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    let mut parents = Vec::new();
    for _ in 0..2 {
        let id = enqueue(&pool, &NewJob::new("branch", Vec::new()))
            .await
            .unwrap();
        parents.push(id);
    }
    let callback: i64 = sqlx::query(
        "INSERT INTO gylo_job (workflow_id, queue, task, payload, pending_deps, scheduled_at)
         VALUES ($1, 'default', 'join', ''::bytea, 2, 'infinity') RETURNING id",
    )
    .bind(workflow)
    .fetch_one(&pool)
    .await
    .unwrap()
    .get(0);
    for parent in &parents {
        sqlx::query("INSERT INTO gylo_edge (workflow_id, parent, child) VALUES ($1, $2, $3)")
            .bind(workflow)
            .bind(parent)
            .bind(callback)
            .execute(&pool)
            .await
            .unwrap();
    }

    fetch(&pool, &queues("default"), 10, LEASE, me)
        .await
        .unwrap();
    complete_many(&pool, &[parents[0]], me).await.unwrap();
    assert!(
        fetch(&pool, &queues("default"), 10, LEASE, worker())
            .await
            .unwrap()
            .is_empty(),
        "one parent finishing is not enough"
    );

    complete_many(&pool, &[parents[1]], me).await.unwrap();
    let released = fetch(&pool, &queues("default"), 10, LEASE, worker())
        .await
        .unwrap();
    assert_eq!(released.len(), 1);
    assert_eq!(released[0].id, callback);
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_dead_parent_cancels_everything_downstream(pool: PgPool) {
    let me = worker();
    let ids = workflow_of(&pool, &["first", "second", "third"]).await;

    fetch(&pool, &queues("default"), 10, LEASE, me)
        .await
        .unwrap();
    discard_many(&pool, &[ids[0]], &["boom".to_owned()], me)
        .await
        .unwrap();

    assert_eq!(state_of(&pool, ids[0]).await, "discarded");
    assert_eq!(state_of(&pool, ids[1]).await, "cancelled");
    assert_eq!(
        state_of(&pool, ids[2]).await,
        "cancelled",
        "cancellation must reach the whole tail, not just the next step"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn depth_separates_what_an_operator_can_act_on(pool: PgPool) {
    enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();
    enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();
    enqueue(
        &pool,
        &NewJob::new("t", Vec::new()).delayed_by(Duration::from_secs(3600)),
    )
    .await
    .unwrap();
    let blocked = enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();
    sqlx::query("UPDATE gylo_job SET scheduled_at = 'infinity' WHERE id = $1")
        .bind(blocked)
        .execute(&pool)
        .await
        .unwrap();
    enqueue(&pool, &NewJob::new("t", Vec::new()).on_queue("elsewhere"))
        .await
        .unwrap();

    fetch(&pool, &queues("default"), 1, LEASE, worker())
        .await
        .unwrap();

    let depth = gylo_pg::depth(&pool, "default").await.unwrap();

    assert_eq!(
        depth,
        gylo_pg::Depth {
            ready: 1,
            scheduled: 1,
            blocked: 1,
            running: 1,
        },
        "a backlog gauge that counts jobs held for later, or parked on a \
         dependency, reports work nothing is waiting on"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn fetch_spans_every_named_queue_in_priority_order(pool: PgPool) {
    let low = enqueue(
        &pool,
        &NewJob::new("low", Vec::new())
            .on_queue("alpha")
            .with_priority(9),
    )
    .await
    .unwrap();
    let high = enqueue(
        &pool,
        &NewJob::new("high", Vec::new())
            .on_queue("beta")
            .with_priority(0),
    )
    .await
    .unwrap();
    enqueue(&pool, &NewJob::new("other", Vec::new()).on_queue("gamma"))
        .await
        .unwrap();

    let jobs = fetch(&pool, &queues("alpha,beta"), 10, LEASE, worker())
        .await
        .unwrap();

    let ids: Vec<i64> = jobs.iter().map(|job| job.id).collect();
    assert_eq!(
        ids,
        vec![high, low],
        "priority must mean the same thing wherever a job was placed, or a \
         high one in a quiet queue loses to a low one in a busy neighbour"
    );
}

async fn dead_letter(pool: &PgPool, queue: &str, error: &str) -> i64 {
    let mut job = NewJob::new("t", Vec::new()).on_queue(queue);
    job.max_attempts = 1;
    let id = enqueue(pool, &job).await.unwrap();
    let holder = worker();
    fetch(pool, &queues(queue), 10, LEASE, holder)
        .await
        .unwrap();
    discard(pool, id, holder, error).await.unwrap();
    id
}

#[sqlx::test(migrations = "../../migrations")]
async fn dead_lettered_jobs_are_listed_with_their_last_error(pool: PgPool) {
    let id = dead_letter(&pool, "payments", "ValueError: boom").await;
    dead_letter(&pool, "reports", "other").await;

    let failed = gylo_pg::list_discarded(&pool, Some("payments"), 20)
        .await
        .unwrap();

    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].id, id);
    assert_eq!(failed[0].queue, "payments");
    assert_eq!(failed[0].error.as_deref(), Some("ValueError: boom"));
}

#[sqlx::test(migrations = "../../migrations")]
async fn retrying_a_dead_lettered_job_resets_its_attempts(pool: PgPool) {
    let id = dead_letter(&pool, "payments", "boom").await;

    let retried = gylo_pg::retry_discarded(&pool, Some(&[id]), None)
        .await
        .unwrap();

    assert_eq!(retried, vec![id]);
    assert_eq!(state_of(&pool, id).await, "available");

    let jobs = fetch(&pool, &queues("payments"), 10, LEASE, worker())
        .await
        .unwrap();
    assert_eq!(
        jobs.len(),
        1,
        "a job is dead-lettered for reaching max_attempts, so requeueing \
         without resetting the counter produces one that cannot be fetched"
    );
    assert_eq!(jobs[0].attempt, 1);
}

#[sqlx::test(migrations = "../../migrations")]
async fn purging_is_scoped_to_the_queue_asked_for(pool: PgPool) {
    dead_letter(&pool, "payments", "boom").await;
    let kept = dead_letter(&pool, "reports", "boom").await;

    let removed = gylo_pg::purge_discarded(&pool, Some("payments"))
        .await
        .unwrap();

    assert_eq!(removed, 1);
    assert_eq!(state_of(&pool, kept).await, "discarded");
}

#[sqlx::test(migrations = "../../migrations")]
async fn queues_lists_only_those_holding_live_work(pool: PgPool) {
    enqueue(&pool, &NewJob::new("t", Vec::new()).on_queue("busy"))
        .await
        .unwrap();
    dead_letter(&pool, "settled", "boom").await;

    assert_eq!(gylo_pg::queues(&pool).await.unwrap(), vec!["busy"]);
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_full_unkeyed_batch_does_not_starve_keyed_work(pool: PgPool) {
    for _ in 0..130 {
        enqueue(&pool, &NewJob::new("t", Vec::new())).await.unwrap();
    }
    let keyed = enqueue(&pool, &NewJob::new("k", Vec::new()).limited_to("tenant", 1))
        .await
        .unwrap();

    let jobs = fetch(&pool, &queues("default"), 128, LEASE, worker())
        .await
        .unwrap();

    assert!(
        jobs.iter().any(|job| job.id == keyed),
        "a backlog of unkeyed jobs one batch deep fills every fetch, and \
         without a floor the keyed pass never runs at all"
    );
}
