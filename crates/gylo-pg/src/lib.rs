//! Postgres backend for gylo.
//!
//! Queries take any executor so callers can run them inside their own
//! transaction. Fetching leases jobs with `FOR UPDATE SKIP LOCKED`, so
//! concurrent workers never block one another.

use std::time::Duration;

use gylo_core::Job;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{PgExecutor, Row};
use uuid::Uuid;

/// Channel a statement-level trigger notifies on insert, carrying the queue
/// name. `NOTIFY` is transactional, so a worker is woken at commit and never
/// before the row it is being told about is visible.
pub const AVAILABLE_CHANNEL: &str = "gylo_available";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Migration(#[from] sqlx::migrate::MigrateError),
}

#[derive(Debug, Clone)]
pub struct NewJob {
    pub queue: String,
    pub task: String,
    pub payload: Vec<u8>,
    /// Lower runs first.
    pub priority: i16,
    pub max_attempts: i16,
    pub delay: Duration,
}

impl NewJob {
    pub fn new(task: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            queue: "default".to_owned(),
            task: task.into(),
            payload,
            priority: 0,
            max_attempts: 20,
            delay: Duration::ZERO,
        }
    }

    #[must_use]
    pub fn on_queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = queue.into();
        self
    }

    #[must_use]
    pub fn delayed_by(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    #[must_use]
    pub fn with_priority(mut self, priority: i16) -> Self {
        self.priority = priority;
        self
    }
}

const ENQUEUE: &str = "
    INSERT INTO gylo_job (queue, task, payload, priority, max_attempts, scheduled_at)
    VALUES ($1, $2, $3, $4, $5, now() + make_interval(secs => $6))
    RETURNING id
";

const FETCH: &str = "
    WITH candidate AS (
        SELECT id FROM gylo_job
        WHERE state = 'available' AND queue = $1 AND scheduled_at <= now()
        ORDER BY priority, scheduled_at, id
        LIMIT $2
        FOR UPDATE SKIP LOCKED
    )
    UPDATE gylo_job j
    SET state = 'running',
        locked_by = $3,
        lease_expires_at = now() + make_interval(secs => $4),
        started_at = now(),
        attempt = j.attempt + 1
    FROM candidate c
    WHERE j.id = c.id
    RETURNING j.id, j.task, j.payload, j.attempt, j.max_attempts
";

const COMPLETE_MANY: &str = "
    UPDATE gylo_job
    SET state = 'completed',
        finalized_at = now(),
        locked_by = NULL,
        lease_expires_at = NULL
    WHERE id = ANY($1) AND locked_by = $2 AND state = 'running'
";

const DISCARD_MANY: &str = "
    UPDATE gylo_job j
    SET state = 'discarded',
        finalized_at = now(),
        locked_by = NULL,
        lease_expires_at = NULL,
        errors = j.errors || jsonb_build_object(
            'attempt', j.attempt,
            'at', now(),
            'error', v.error
        )
    FROM unnest($1::bigint[], $2::text[]) AS v(id, error)
    WHERE j.id = v.id AND j.locked_by = $3 AND j.state = 'running'
";

const DISCARD: &str = "
    UPDATE gylo_job
    SET state = 'discarded',
        finalized_at = now(),
        locked_by = NULL,
        lease_expires_at = NULL,
        errors = errors || jsonb_build_object(
            'attempt', attempt,
            'at', now(),
            'error', $3::text
        )
    WHERE id = $1 AND locked_by = $2 AND state = 'running'
";

const RECLAIM: &str = "
    WITH expired AS (
        SELECT id, queue, attempt >= max_attempts AS exhausted
        FROM gylo_job
        WHERE state = 'running' AND lease_expires_at < now()
        LIMIT $1
    ),
    released AS (
        UPDATE gylo_job j
        SET state = 'available', locked_by = NULL, lease_expires_at = NULL
        FROM expired e
        WHERE j.id = e.id AND NOT e.exhausted AND j.state = 'running'
        RETURNING j.queue
    ),
    exhausted AS (
        UPDATE gylo_job j
        SET state = 'discarded',
            finalized_at = now(),
            locked_by = NULL,
            lease_expires_at = NULL,
            errors = j.errors || jsonb_build_object(
                'attempt', j.attempt,
                'at', now(),
                'error', 'lease expired before the worker reported an outcome'
            )
        FROM expired e
        WHERE j.id = e.id AND e.exhausted AND j.state = 'running'
        RETURNING j.id
    ),
    woken AS (
        SELECT pg_notify('gylo_available', ready.queue)
        FROM (SELECT DISTINCT queue FROM released) AS ready
    )
    SELECT
        (SELECT count(*) FROM released)  AS released,
        (SELECT count(*) FROM exhausted) AS exhausted,
        (SELECT count(*) FROM woken)     AS woken
";

const RENEW: &str = "
    UPDATE gylo_job
    SET lease_expires_at = now() + make_interval(secs => $3)
    WHERE id = ANY($1) AND locked_by = $2 AND state = 'running'
";

/// Extends the lease on jobs still being worked, so a task that outlives its
/// original lease is not reclaimed and run a second time.
///
/// Scoped to the holder: a worker whose lease already lapsed must not extend
/// the one another worker has since taken.
pub async fn renew_leases<'e, E: PgExecutor<'e>>(
    executor: E,
    ids: &[i64],
    worker: Uuid,
    lease: Duration,
) -> Result<u64, Error> {
    if ids.is_empty() {
        return Ok(0);
    }
    let result = sqlx::query(RENEW)
        .bind(ids)
        .bind(worker)
        .bind(lease.as_secs_f64())
        .execute(executor)
        .await?;
    Ok(result.rows_affected())
}

const RETRY_MANY: &str = "
    WITH input AS (
        SELECT * FROM unnest($1::bigint[], $2::text[]) AS t(id, error)
    ),
    scheduled AS (
        UPDATE gylo_job j
        SET state = 'available',
            locked_by = NULL,
            lease_expires_at = NULL,
            scheduled_at = now() + make_interval(secs =>
                least($3::float8 * power(2, j.attempt - 1), $4::float8)
                * (0.5 + random() * 0.5)
            ),
            errors = j.errors || jsonb_build_object(
                'attempt', j.attempt,
                'at', now(),
                'error', i.error
            )
        FROM input i
        WHERE j.id = i.id AND j.locked_by = $5 AND j.state = 'running'
              AND j.attempt < j.max_attempts
        RETURNING j.id
    ),
    exhausted AS (
        UPDATE gylo_job j
        SET state = 'discarded',
            finalized_at = now(),
            locked_by = NULL,
            lease_expires_at = NULL,
            errors = j.errors || jsonb_build_object(
                'attempt', j.attempt,
                'at', now(),
                'error', i.error
            )
        FROM input i
        WHERE j.id = i.id AND j.locked_by = $5 AND j.state = 'running'
              AND j.attempt >= j.max_attempts
        RETURNING j.id
    )
    SELECT
        (SELECT count(*) FROM scheduled)  AS scheduled,
        (SELECT count(*) FROM exhausted)  AS exhausted
";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Retried {
    /// Scheduled for another attempt.
    pub scheduled: i64,
    /// Out of attempts, so dead-lettered instead.
    pub exhausted: i64,
}

/// Reschedules failed jobs with exponential backoff, dead-lettering those out
/// of attempts.
///
/// The delay is computed per row from the job's own `attempt`, so the caller
/// never has to carry attempt counts alongside its completion batch. Jitter is
/// applied at 50–100% of nominal, which keeps a burst of simultaneous failures
/// from retrying in lockstep.
pub async fn retry_many<'e, E: PgExecutor<'e>>(
    executor: E,
    ids: &[i64],
    errors: &[String],
    worker: Uuid,
    base: Duration,
    cap: Duration,
) -> Result<Retried, Error> {
    debug_assert_eq!(ids.len(), errors.len());
    if ids.is_empty() {
        return Ok(Retried::default());
    }
    let row = sqlx::query(RETRY_MANY)
        .bind(ids)
        .bind(errors)
        .bind(base.as_secs_f64())
        .bind(cap.as_secs_f64())
        .bind(worker)
        .fetch_one(executor)
        .await?;
    Ok(Retried {
        scheduled: row.try_get("scheduled")?,
        exhausted: row.try_get("exhausted")?,
    })
}

const ABANDON: &str = "
    UPDATE gylo_job
    SET lease_expires_at = now() - interval '1 microsecond'
    WHERE id = ANY($1) AND locked_by = $2 AND state = 'running'
";

/// Expires leases the caller knows it can no longer honour, so the next sweep
/// recovers them immediately instead of after the full lease duration.
pub async fn abandon_leases<'e, E: PgExecutor<'e>>(
    executor: E,
    ids: &[i64],
    worker: Uuid,
) -> Result<u64, Error> {
    if ids.is_empty() {
        return Ok(0);
    }
    let result = sqlx::query(ABANDON)
        .bind(ids)
        .bind(worker)
        .execute(executor)
        .await?;
    Ok(result.rows_affected())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Reclaimed {
    /// Returned to `available` for another worker to pick up.
    pub released: i64,
    /// Out of attempts, so dead-lettered instead of retried.
    pub exhausted: i64,
}

/// A job with attempts left goes back to `available`; one without is
/// dead-lettered. That split keeps `attempt <= max_attempts` true, since every
/// fetch increments and the constraint would otherwise reject a whole batch.
pub async fn reclaim_expired<'e, E: PgExecutor<'e>>(
    executor: E,
    limit: i64,
) -> Result<Reclaimed, Error> {
    let row = sqlx::query(RECLAIM).bind(limit).fetch_one(executor).await?;
    Ok(Reclaimed {
        released: row.try_get("released")?,
        exhausted: row.try_get("exhausted")?,
    })
}

pub async fn connect(url: &str, max_connections: u32) -> Result<PgPool, Error> {
    Ok(PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(url)
        .await?)
}

pub async fn migrate(pool: &PgPool) -> Result<(), Error> {
    sqlx::migrate!("../../migrations").run(pool).await?;
    Ok(())
}

pub async fn enqueue<'e, E: PgExecutor<'e>>(executor: E, job: &NewJob) -> Result<i64, Error> {
    let row = sqlx::query(ENQUEUE)
        .bind(&job.queue)
        .bind(&job.task)
        .bind(&job.payload)
        .bind(job.priority)
        .bind(job.max_attempts)
        .bind(job.delay.as_secs_f64())
        .fetch_one(executor)
        .await?;
    Ok(row.try_get("id")?)
}

/// Leases up to `limit` eligible jobs, skipping rows another worker holds.
pub async fn fetch<'e, E: PgExecutor<'e>>(
    executor: E,
    queue: &str,
    limit: i64,
    lease: Duration,
    worker: Uuid,
) -> Result<Vec<Job>, Error> {
    let rows = sqlx::query(FETCH)
        .bind(queue)
        .bind(limit)
        .bind(worker)
        .bind(lease.as_secs_f64())
        .fetch_all(executor)
        .await?;

    rows.into_iter()
        .map(|row| {
            Ok(Job {
                id: row.try_get("id")?,
                task: row.try_get("task")?,
                payload: row.try_get("payload")?,
                attempt: row.try_get("attempt")?,
                max_attempts: row.try_get("max_attempts")?,
            })
        })
        .collect()
}

/// Every failure is terminal until retry scheduling exists; this always moves
/// the job to the dead-letter state rather than back to `available`.
pub async fn discard<'e, E: PgExecutor<'e>>(
    executor: E,
    id: i64,
    worker: Uuid,
    error: &str,
) -> Result<bool, Error> {
    let result = sqlx::query(DISCARD)
        .bind(id)
        .bind(worker)
        .bind(error)
        .execute(executor)
        .await?;
    Ok(result.rows_affected() == 1)
}

/// Returns how many were still leased, and so actually finalised. One
/// statement and one commit, rather than one of each per job.
pub async fn complete_many<'e, E: PgExecutor<'e>>(
    executor: E,
    ids: &[i64],
    worker: Uuid,
) -> Result<u64, Error> {
    if ids.is_empty() {
        return Ok(0);
    }
    let result = sqlx::query(COMPLETE_MANY)
        .bind(ids)
        .bind(worker)
        .execute(executor)
        .await?;
    Ok(result.rows_affected())
}

/// Batched [`discard`]. `ids` and `errors` are zipped positionally, so they
/// must be the same length.
pub async fn discard_many<'e, E: PgExecutor<'e>>(
    executor: E,
    ids: &[i64],
    errors: &[String],
    worker: Uuid,
) -> Result<u64, Error> {
    debug_assert_eq!(ids.len(), errors.len());
    if ids.is_empty() {
        return Ok(0);
    }
    let result = sqlx::query(DISCARD_MANY)
        .bind(ids)
        .bind(errors)
        .bind(worker)
        .execute(executor)
        .await?;
    Ok(result.rows_affected())
}
