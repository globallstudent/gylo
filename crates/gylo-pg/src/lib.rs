//! Postgres backend for gylo.
//!
//! Queries take any executor so callers can run them inside their own
//! transaction. Fetching leases jobs with `FOR UPDATE SKIP LOCKED`, so
//! concurrent workers never block one another.

use std::time::Duration;

use chrono::{DateTime, Utc};
use gylo_core::Job;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::{PgExecutor, Row};
use uuid::Uuid;

/// Channel a statement-level trigger notifies on insert, carrying the queue
/// name. `NOTIFY` is transactional, so a worker is woken at commit and never
/// before the row it is being told about is visible.
pub const AVAILABLE_CHANNEL: &str = "gylo_available";

/// Everything gylo does, since Postgres is where every feature was designed.
pub const CAPABILITIES: gylo_core::Capabilities = gylo_core::Capabilities {
    backend: "postgres",
    durable_acknowledgement: true,
    transactional_enqueue: true,
    priorities: true,
    delayed_jobs: true,
    unique_jobs: true,
    keyed_concurrency: true,
    workflows: true,
    durable_steps: true,
    cron: true,
    results: true,
};

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
    /// Set together: at most `max_concurrency` jobs sharing this key run at once.
    pub concurrency: Option<(String, i32)>,
    /// Whether completed steps are kept so a retry can replay them.
    pub durable: bool,
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
            concurrency: None,
            durable: false,
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

    #[must_use]
    pub fn limited_to(mut self, key: impl Into<String>, at_once: i32) -> Self {
        self.concurrency = Some((key.into(), at_once));
        self
    }
}

const ENQUEUE: &str = "
    INSERT INTO gylo_job (queue, task, payload, priority, max_attempts, scheduled_at,
                          concurrency_key, max_concurrency, durable)
    VALUES ($1, $2, $3, $4, $5, now() + make_interval(secs => $6), $7, $8, $9)
    RETURNING id
";

const FETCH: &str = "
    WITH active AS (
        SELECT concurrency_key, count(*) AS running
        FROM gylo_job
        WHERE state = 'running' AND concurrency_key IS NOT NULL
        GROUP BY concurrency_key
    ),
    locked AS (
        SELECT j.id, j.priority, j.scheduled_at, j.concurrency_key, j.max_concurrency,
               COALESCE(a.running, 0) AS already
        FROM gylo_job j
        LEFT JOIN active a ON a.concurrency_key = j.concurrency_key
        WHERE j.state = 'available' AND j.queue = ANY($1) AND j.scheduled_at <= now()
          AND (j.concurrency_key IS NULL OR COALESCE(a.running, 0) < j.max_concurrency)
        ORDER BY j.priority, j.scheduled_at, j.id
        LIMIT $2
        FOR UPDATE OF j SKIP LOCKED
    ),
    admitted AS (
        SELECT id FROM (
            SELECT id, concurrency_key, max_concurrency, already,
                   row_number() OVER (PARTITION BY concurrency_key
                                      ORDER BY priority, scheduled_at, id) AS n
            FROM locked
        ) ranked
        WHERE concurrency_key IS NULL OR already + n <= max_concurrency
    )
    UPDATE gylo_job j
    SET state = 'running',
        locked_by = $3,
        lease_expires_at = now() + make_interval(secs => $4),
        started_at = now(),
        attempt = j.attempt + 1
    FROM admitted c
    WHERE j.id = c.id
    RETURNING j.id, j.task, j.payload, j.attempt, j.max_attempts, j.durable
";

const COMPLETE_WITH_RESULTS: &str = "
    WITH done AS (
        UPDATE gylo_job j
        SET state = 'completed',
            finalized_at = now(),
            locked_by = NULL,
            lease_expires_at = NULL,
            result = v.result
        FROM unnest($1::bigint[], $2::bytea[]) AS v(id, result)
        WHERE j.id = v.id AND j.locked_by = $3 AND j.state = 'running'
        RETURNING j.id
    ),
    counted AS (
        SELECT e.child, count(*) AS n
        FROM gylo_edge e JOIN done ON done.id = e.parent
        GROUP BY e.child
    ),
    released AS (
        UPDATE gylo_job child
        SET pending_deps = child.pending_deps - counted.n,
            scheduled_at = CASE
                WHEN child.pending_deps - counted.n = 0 THEN now()
                ELSE child.scheduled_at
            END
        FROM counted
        WHERE child.id = counted.child
        RETURNING child.queue, child.pending_deps
    ),
    woken AS (
        SELECT pg_notify('gylo_available', ready.queue)
        FROM (SELECT DISTINCT queue FROM released WHERE pending_deps = 0) AS ready
    )
    SELECT (SELECT count(*) FROM done) AS settled, (SELECT count(*) FROM woken) AS woken
";

const CANCEL: &str = "
    UPDATE gylo_job
    SET state = 'cancelled',
        finalized_at = now(),
        locked_by = NULL,
        lease_expires_at = NULL
    WHERE id = ANY($1) AND state = 'available'
";

const RESULT: &str = "
    SELECT state::text AS state, result, errors
    FROM gylo_job WHERE id = $1
";

const COMPLETE_MANY: &str = "
    WITH done AS (
        UPDATE gylo_job
        SET state = 'completed',
            finalized_at = now(),
            locked_by = NULL,
            lease_expires_at = NULL
        WHERE id = ANY($1) AND locked_by = $2 AND state = 'running'
        RETURNING id
    ),
    counted AS (
        SELECT e.child, count(*) AS n
        FROM gylo_edge e JOIN done ON done.id = e.parent
        GROUP BY e.child
    ),
    released AS (
        UPDATE gylo_job child
        SET pending_deps = child.pending_deps - counted.n,
            scheduled_at = CASE
                WHEN child.pending_deps - counted.n = 0 THEN now()
                ELSE child.scheduled_at
            END
        FROM counted
        WHERE child.id = counted.child
        RETURNING child.queue, child.pending_deps
    ),
    woken AS (
        SELECT pg_notify('gylo_available', ready.queue)
        FROM (SELECT DISTINCT queue FROM released WHERE pending_deps = 0) AS ready
    )
    SELECT (SELECT count(*) FROM done) AS settled, (SELECT count(*) FROM woken) AS woken
";

const DISCARD_MANY: &str = "
    WITH RECURSIVE dead AS (
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
        RETURNING j.id
    ),
    walk AS (
        SELECT e.child FROM gylo_edge e WHERE e.parent IN (SELECT id FROM dead)
        UNION
        SELECT e.child FROM gylo_edge e JOIN walk ON e.parent = walk.child
    ),
    cancelled AS (
        UPDATE gylo_job
        SET state = 'cancelled', finalized_at = now()
        WHERE id IN (SELECT child FROM walk) AND state = 'available'
        RETURNING id
    )
    SELECT (SELECT count(*) FROM dead) AS settled,
           (SELECT count(*) FROM cancelled) AS cancelled
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

const RECORD_STEP: &str = "
    INSERT INTO gylo_step (job_id, name, result)
    VALUES ($1, $2, $3)
    ON CONFLICT (job_id, name) DO NOTHING
";

const STEPS_FOR: &str = "
    SELECT name, result FROM gylo_step WHERE job_id = $1 ORDER BY created_at, name
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
        .bind(job.concurrency.as_ref().map(|(key, _)| key.as_str()))
        .bind(job.concurrency.as_ref().map(|(_, at_once)| *at_once))
        .bind(job.durable)
        .fetch_one(executor)
        .await?;
    Ok(row.try_get("id")?)
}

/// Leases up to `limit` eligible jobs, skipping rows another worker holds.
///
/// A job carrying a concurrency key is admitted only while fewer than
/// `max_concurrency` of that key are running. The count is taken before
/// locking and the batch is then ranked per key, because Postgres will not
/// accept `FOR UPDATE` beside a window function — so the limit has to be
/// applied in two passes rather than one.
/// Leases across every named queue in one round trip.
///
/// Ordering is global rather than per queue, so a job's priority means the same
/// thing wherever it was placed. Round-robin between queues would make a high
/// priority in a quiet queue lose to a low one in a busy neighbour.
pub async fn fetch<'e, E: PgExecutor<'e>>(
    executor: E,
    queues: &[String],
    limit: i64,
    lease: Duration,
    worker: Uuid,
) -> Result<Vec<Job>, Error> {
    let rows = sqlx::query(FETCH)
        .bind(queues)
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
                durable: row.try_get("durable")?,
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
///
/// Completing a job also releases whatever depended on it: dependants have
/// their outstanding count decremented and, at zero, become runnable. Doing
/// that in the same statement is what makes fan-in safe when several parents
/// of one child finish at once.
pub async fn complete_many<'e, E: PgExecutor<'e>>(
    executor: E,
    ids: &[i64],
    worker: Uuid,
) -> Result<u64, Error> {
    if ids.is_empty() {
        return Ok(0);
    }
    let row = sqlx::query(COMPLETE_MANY)
        .bind(ids)
        .bind(worker)
        .fetch_one(executor)
        .await?;
    Ok(row.try_get::<i64, _>("settled")? as u64)
}

/// Batched [`discard`]. `ids` and `errors` are zipped positionally, so they
/// must be the same length.
///
/// Everything downstream of a dead job is cancelled, transitively. Without
/// that, a failed step in a chain leaves the rest waiting on a parent that
/// will never finish.
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
    let row = sqlx::query(DISCARD_MANY)
        .bind(ids)
        .bind(errors)
        .bind(worker)
        .fetch_one(executor)
        .await?;
    Ok(row.try_get::<i64, _>("settled")? as u64)
}

const UPSERT_CRON: &str = "
    INSERT INTO gylo_cron (name, queue, task, payload, expression, timezone, next_run_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (name) DO UPDATE SET
        queue = EXCLUDED.queue,
        task = EXCLUDED.task,
        payload = EXCLUDED.payload,
        expression = EXCLUDED.expression,
        timezone = EXCLUDED.timezone,
        next_run_at = CASE
            WHEN gylo_cron.expression IS DISTINCT FROM EXCLUDED.expression
              OR gylo_cron.timezone IS DISTINCT FROM EXCLUDED.timezone
            THEN EXCLUDED.next_run_at
            ELSE gylo_cron.next_run_at
        END,
        updated_at = now()
";

const DUE_CRON: &str = "
    SELECT name, expression, timezone
    FROM gylo_cron
    WHERE NOT paused AND next_run_at <= now()
    ORDER BY next_run_at
    LIMIT $1
";

const FIRE_CRON: &str = "
    WITH won AS (
        UPDATE gylo_cron
        SET next_run_at = $2, last_run_at = now()
        WHERE name = $1 AND NOT paused AND next_run_at <= now()
        RETURNING queue, task, payload
    )
    INSERT INTO gylo_job (queue, task, payload)
    SELECT queue, task, payload FROM won
    RETURNING id
";

/// A schedule registered by the tasks a worker loaded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronEntry {
    pub name: String,
    pub queue: String,
    pub task: String,
    pub payload: Vec<u8>,
    pub expression: String,
    pub timezone: String,
}

/// A schedule that has come due, with what it needs to compute the next one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueCron {
    pub name: String,
    pub expression: String,
    pub timezone: String,
}

/// Records a schedule, leaving `next_run_at` alone unless the expression or
/// zone actually changed, and never touching `paused`.
///
/// Both matter across a restart: resetting the clock on every boot would let a
/// frequently-restarted fleet skip runs, and clearing `paused` would undo an
/// operator every time someone deploys.
pub async fn upsert_cron<'e, E: PgExecutor<'e>>(
    executor: E,
    entry: &CronEntry,
    next_run_at: DateTime<Utc>,
) -> Result<(), Error> {
    sqlx::query(UPSERT_CRON)
        .bind(&entry.name)
        .bind(&entry.queue)
        .bind(&entry.task)
        .bind(&entry.payload)
        .bind(&entry.expression)
        .bind(&entry.timezone)
        .bind(next_run_at)
        .execute(executor)
        .await?;
    Ok(())
}

pub async fn due_cron<'e, E: PgExecutor<'e>>(
    executor: E,
    limit: i64,
) -> Result<Vec<DueCron>, Error> {
    let rows = sqlx::query(DUE_CRON)
        .bind(limit)
        .fetch_all(executor)
        .await?;
    rows.into_iter()
        .map(|row| {
            Ok(DueCron {
                name: row.try_get("name")?,
                expression: row.try_get("expression")?,
                timezone: row.try_get("timezone")?,
            })
        })
        .collect()
}

/// Advances the schedule and enqueues its job in one statement, returning the
/// job id if this caller was the one that won the row.
///
/// Exactly-once comes from the row lock Postgres takes for the `UPDATE`: only
/// one caller can match `next_run_at <= now()` before it moves. No leader
/// election sits on top of this; see ADR 0007.
pub async fn fire_cron<'e, E: PgExecutor<'e>>(
    executor: E,
    name: &str,
    next_run_at: DateTime<Utc>,
) -> Result<Option<i64>, Error> {
    let row = sqlx::query(FIRE_CRON)
        .bind(name)
        .bind(next_run_at)
        .fetch_optional(executor)
        .await?;
    row.map(|row| row.try_get("id"))
        .transpose()
        .map_err(Error::from)
}

/// Finalises jobs that produced a return value, storing each alongside its own
/// job. Kept separate from [`complete_many`] so the common case — a task that
/// stores nothing — does not carry an array of nulls.
pub async fn complete_many_with_results<'e, E: PgExecutor<'e>>(
    executor: E,
    ids: &[i64],
    results: &[Vec<u8>],
    worker: Uuid,
) -> Result<u64, Error> {
    debug_assert_eq!(ids.len(), results.len());
    if ids.is_empty() {
        return Ok(0);
    }
    let row = sqlx::query(COMPLETE_WITH_RESULTS)
        .bind(ids)
        .bind(results)
        .bind(worker)
        .fetch_one(executor)
        .await?;
    Ok(row.try_get::<i64, _>("settled")? as u64)
}

/// Cancels jobs that have not started, returning how many were still waiting.
///
/// A running job is left alone: interrupting Python mid-task would mean
/// killing the child and taking every sibling job with it. Cancelling what has
/// not begun is the part that can be done honestly.
pub async fn cancel<'e, E: PgExecutor<'e>>(executor: E, ids: &[i64]) -> Result<u64, Error> {
    if ids.is_empty() {
        return Ok(0);
    }
    let result = sqlx::query(CANCEL).bind(ids).execute(executor).await?;
    Ok(result.rows_affected())
}

/// How a job ended, for a caller waiting on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobOutcome {
    pub state: String,
    pub result: Option<Vec<u8>>,
    pub errors: serde_json::Value,
}

pub async fn outcome<'e, E: PgExecutor<'e>>(
    executor: E,
    id: i64,
) -> Result<Option<JobOutcome>, Error> {
    let row = sqlx::query(RESULT)
        .bind(id)
        .fetch_optional(executor)
        .await?;
    row.map(|row| {
        Ok(JobOutcome {
            state: row.try_get("state")?,
            result: row.try_get("result")?,
            errors: row.try_get("errors")?,
        })
    })
    .transpose()
}

/// Records a completed step. Ignores a repeat, so a step reported twice after
/// a crash between the write and its acknowledgement stays a single record.
pub async fn record_step<'e, E: PgExecutor<'e>>(
    executor: E,
    job: i64,
    name: &str,
    result: &[u8],
) -> Result<(), Error> {
    sqlx::query(RECORD_STEP)
        .bind(job)
        .bind(name)
        .bind(result)
        .execute(executor)
        .await?;
    Ok(())
}

pub async fn steps_for<'e, E: PgExecutor<'e>>(
    executor: E,
    job: i64,
) -> Result<Vec<(String, Vec<u8>)>, Error> {
    let rows = sqlx::query(STEPS_FOR).bind(job).fetch_all(executor).await?;
    rows.into_iter()
        .map(|row| Ok((row.try_get("name")?, row.try_get("result")?)))
        .collect()
}

/// What a queue is holding, split by what an operator can act on.
///
/// `available` alone is not a backlog: it also covers jobs deliberately held
/// for later, and workflow jobs parked at `scheduled_at = 'infinity'` waiting
/// on a dependency. Only `ready` is work nothing is stopping.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Depth {
    pub ready: i64,
    pub scheduled: i64,
    pub blocked: i64,
    pub running: i64,
}

const DEPTH: &str = "
    SELECT
        count(*) FILTER (
            WHERE state = 'available' AND scheduled_at <= now()
        ) AS ready,
        count(*) FILTER (
            WHERE state = 'available'
              AND scheduled_at > now()
              AND scheduled_at <> 'infinity'
        ) AS scheduled,
        count(*) FILTER (
            WHERE state = 'available' AND scheduled_at = 'infinity'
        ) AS blocked,
        count(*) FILTER (WHERE state = 'running') AS running
    FROM gylo_job
    WHERE queue = $1
";

pub async fn depth<'e, E: PgExecutor<'e>>(executor: E, queue: &str) -> Result<Depth, Error> {
    let row = sqlx::query(DEPTH).bind(queue).fetch_one(executor).await?;
    Ok(Depth {
        ready: row.get("ready"),
        scheduled: row.get("scheduled"),
        blocked: row.get("blocked"),
        running: row.get("running"),
    })
}

/// One dead-lettered job, enough to decide whether to retry it.
#[derive(Debug, Clone)]
pub struct Discarded {
    pub id: i64,
    pub queue: String,
    pub task: String,
    pub attempt: i16,
    pub finalized_at: Option<chrono::DateTime<Utc>>,
    /// The last one only. The whole history is on the row for anyone who needs
    /// it, and a listing that printed every attempt would bury the failure.
    pub error: Option<String>,
}

const LIST_DISCARDED: &str = "
    SELECT id, queue, task, attempt, finalized_at,
           errors -> -1 ->> 'error' AS error
    FROM gylo_job
    WHERE state = 'discarded' AND ($1::text IS NULL OR queue = $1)
    ORDER BY finalized_at DESC NULLS LAST, id DESC
    LIMIT $2
";

pub async fn list_discarded<'e, E: PgExecutor<'e>>(
    executor: E,
    queue: Option<&str>,
    limit: i64,
) -> Result<Vec<Discarded>, Error> {
    let rows = sqlx::query(LIST_DISCARDED)
        .bind(queue)
        .bind(limit)
        .fetch_all(executor)
        .await?;
    Ok(rows
        .into_iter()
        .map(|row| Discarded {
            id: row.get("id"),
            queue: row.get("queue"),
            task: row.get("task"),
            attempt: row.get("attempt"),
            finalized_at: row.get("finalized_at"),
            error: row.get("error"),
        })
        .collect())
}

/// Returns dead-lettered jobs to the queue with their attempts reset.
///
/// The attempt counter has to go back to zero: a job is discarded precisely
/// because it reached `max_attempts`, so requeueing without resetting produces
/// a job that is immediately discarded again by the same rule.
const RETRY_DISCARDED: &str = "
    UPDATE gylo_job
    SET state = 'available',
        attempt = 0,
        scheduled_at = now(),
        locked_by = NULL,
        lease_expires_at = NULL,
        finalized_at = NULL
    WHERE state = 'discarded'
      AND ($1::bigint[] IS NULL OR id = ANY($1))
      AND ($2::text IS NULL OR queue = $2)
    RETURNING id
";

pub async fn retry_discarded<'e, E: PgExecutor<'e>>(
    executor: E,
    ids: Option<&[i64]>,
    queue: Option<&str>,
) -> Result<Vec<i64>, Error> {
    let rows = sqlx::query(RETRY_DISCARDED)
        .bind(ids)
        .bind(queue)
        .fetch_all(executor)
        .await?;
    Ok(rows.into_iter().map(|row| row.get("id")).collect())
}

const PURGE_DISCARDED: &str = "
    DELETE FROM gylo_job
    WHERE state = 'discarded' AND ($1::text IS NULL OR queue = $1)
";

pub async fn purge_discarded<'e, E: PgExecutor<'e>>(
    executor: E,
    queue: Option<&str>,
) -> Result<u64, Error> {
    Ok(sqlx::query(PURGE_DISCARDED)
        .bind(queue)
        .execute(executor)
        .await?
        .rows_affected())
}

const QUEUE_NAMES: &str = "
    SELECT DISTINCT queue FROM gylo_job
    WHERE state IN ('available', 'running')
    ORDER BY queue
";

pub async fn queues<'e, E: PgExecutor<'e>>(executor: E) -> Result<Vec<String>, Error> {
    let rows = sqlx::query(QUEUE_NAMES).fetch_all(executor).await?;
    Ok(rows.into_iter().map(|row| row.get("queue")).collect())
}
