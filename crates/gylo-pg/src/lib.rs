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
    /// Delay before the job becomes eligible to run.
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

const COMPLETE: &str = "
    UPDATE gylo_job
    SET state = 'completed',
        finalized_at = now(),
        locked_by = NULL,
        lease_expires_at = NULL
    WHERE id = $1 AND state = 'running'
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
            'error', $2::text
        )
    WHERE id = $1 AND state = 'running'
";

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

/// Returns whether the job was still leased, and so actually finalised.
pub async fn complete<'e, E: PgExecutor<'e>>(executor: E, id: i64) -> Result<bool, Error> {
    let result = sqlx::query(COMPLETE).bind(id).execute(executor).await?;
    Ok(result.rows_affected() == 1)
}

/// Every failure is terminal until retry scheduling exists; this always moves
/// the job to the dead-letter state rather than back to `available`.
pub async fn discard<'e, E: PgExecutor<'e>>(
    executor: E,
    id: i64,
    error: &str,
) -> Result<bool, Error> {
    let result = sqlx::query(DISCARD)
        .bind(id)
        .bind(error)
        .execute(executor)
        .await?;
    Ok(result.rows_affected() == 1)
}
