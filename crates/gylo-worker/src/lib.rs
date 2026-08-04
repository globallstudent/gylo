//! Supervisor process: leases jobs from Postgres and dispatches them to a
//! Python child over the gylo wire protocol.
//!
//! Dispatch and completion run as independent tasks over the same socket, so
//! neither direction waits on a round trip.

use std::collections::HashSet;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

mod observe;

use chrono::Utc;
use gylo_core::{Decoder, Message, Outcome, Schedule, encode};
use sqlx::PgPool;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::process::{Child, Command};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const READ_BUFFER: usize = 1 << 16;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const LISTEN_RETRY: Duration = Duration::from_secs(1);
const RESTART_DELAY: Duration = Duration::from_millis(250);
const RESTART_CEILING: Duration = Duration::from_secs(30);
const HEALTHY_SESSION: Duration = Duration::from_secs(30);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
const DATABASE_RETRY: Duration = Duration::from_millis(250);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Database(#[from] gylo_pg::Error),
    #[error(transparent)]
    Protocol(#[from] gylo_core::ProtocolError),
    #[error("python child did not connect within {0:?}")]
    HandshakeTimeout(Duration),
    #[error("python child exited: {0}")]
    ChildExited(String),
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error(transparent)]
    Unsupported(#[from] gylo_core::Unsupported),
}

#[derive(Debug, Clone)]
pub struct Config {
    /// Queues this worker consumes, in no significant order: a job's priority
    /// is compared across all of them at once rather than per queue.
    pub queues: Vec<String>,
    /// Python child processes to run. Task code holds the interpreter lock, so
    /// one child uses one core however high `concurrency` goes; this is the
    /// setting that spends the rest of the machine.
    pub processes: usize,
    /// Jobs in flight in one child at once; also caps how far ahead of that
    /// child its supervisor will lease.
    pub concurrency: usize,
    /// Jobs leased per round trip. Throughput keeps climbing well past the
    /// default, but every leased job is one that waits for lease expiry if the
    /// worker dies, so this is bounded for recovery latency rather than speed.
    pub batch: i64,
    /// How long a lease is held before maintenance may reclaim it. Renewed
    /// every `maintenance_interval` while a job is still running, so this
    /// bounds recovery time rather than task duration.
    pub lease: Duration,
    pub poll_interval: Duration,
    pub completion_batch: usize,
    /// How long a partial completion batch waits for company before flushing.
    pub completion_linger: Duration,
    /// Cadence for renewing held leases and reclaiming abandoned ones. Must
    /// stay comfortably below `lease` or live jobs will be reclaimed.
    pub maintenance_interval: Duration,
    pub reclaim_limit: i64,
    /// First retry lands after roughly this long, doubling per attempt.
    pub retry_base: Duration,
    /// Ceiling on the doubling, before jitter.
    pub retry_cap: Duration,
    /// Consecutive failed sessions tolerated before the worker gives up. A
    /// session that stayed healthy resets the count, so this only catches a
    /// child that cannot stay up at all — usually a misconfigured `app`.
    pub max_restarts: u32,
    /// Schedules examined per maintenance tick.
    pub cron_limit: i64,
    pub python: PathBuf,
    /// `module:attribute` path to the user's app object.
    pub app: String,
    pub python_path: Option<OsString>,
    /// Extra environment for the child, on top of what the worker inherited.
    pub env: Vec<(OsString, OsString)>,
    /// Features this deployment uses. A backend that cannot provide one of
    /// them stops the worker at startup rather than quietly doing less.
    pub requires: Vec<gylo_core::Feature>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            queues: vec!["default".to_owned()],
            processes: std::thread::available_parallelism().map_or(1, |cores| cores.get()),
            concurrency: 256,
            batch: 128,
            lease: Duration::from_secs(30),
            poll_interval: Duration::from_millis(200),
            completion_batch: 100,
            completion_linger: Duration::from_millis(5),
            maintenance_interval: Duration::from_secs(10),
            reclaim_limit: 1000,
            retry_base: Duration::from_secs(1),
            retry_cap: Duration::from_secs(3600),
            max_restarts: 8,
            cron_limit: 100,
            python: PathBuf::from("python3"),
            app: String::new(),
            python_path: None,
            env: Vec::new(),
            requires: Vec::new(),
        }
    }
}

impl Config {
    /// Rejects settings whose effect would be silent rather than obvious. A
    /// maintenance interval at or above the lease is the dangerous one: leases
    /// expire before they are renewed, so healthy jobs are reclaimed and run a
    /// second time while everything appears to be working.
    fn validate(&self) -> Result<(), Error> {
        let invalid = if self.app.is_empty() {
            Some("app must be set to a module:attribute path".to_owned())
        } else if self.queues.is_empty() {
            Some("at least one queue must be given".to_owned())
        } else if self.processes == 0 {
            Some("processes must be at least 1".to_owned())
        } else if self.concurrency == 0 {
            Some("concurrency must be at least 1".to_owned())
        } else if self.batch < 1 {
            Some(format!("batch must be at least 1, got {}", self.batch))
        } else if self.maintenance_interval >= self.lease {
            Some(format!(
                "maintenance interval {:?} must be shorter than the lease {:?}, \
                 or running jobs will be reclaimed and run twice",
                self.maintenance_interval, self.lease
            ))
        } else {
            None
        };

        invalid.map_or(Ok(()), |reason| Err(Error::Config(reason)))
    }
}

struct SocketGuard(PathBuf);

impl Drop for SocketGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Jobs handed to the child but not yet finalised.
///
/// Holds ids rather than a count so maintenance can renew exactly the leases
/// this worker still owns, and so a failed session can hand back exactly what
/// it was holding.
struct InFlight {
    ids: Mutex<HashSet<i64>>,
    limit: usize,
    freed: Notify,
    drained: Notify,
}

impl InFlight {
    fn new(limit: usize) -> Self {
        Self {
            ids: Mutex::new(HashSet::with_capacity(limit)),
            limit,
            freed: Notify::new(),
            drained: Notify::new(),
        }
    }

    fn held(&self) -> usize {
        self.ids
            .lock()
            .expect("in-flight set is not poisoned")
            .len()
    }

    fn available(&self) -> usize {
        self.limit.saturating_sub(self.held())
    }

    fn is_idle(&self) -> bool {
        self.held() == 0
    }

    fn snapshot(&self) -> Vec<i64> {
        self.ids
            .lock()
            .expect("in-flight set is not poisoned")
            .iter()
            .copied()
            .collect()
    }

    fn take(&self) -> Vec<i64> {
        let mut held = self.ids.lock().expect("in-flight set is not poisoned");
        let ids = held.iter().copied().collect();
        held.clear();
        ids
    }

    fn reserve(&self, ids: &[i64]) {
        let mut held = self.ids.lock().expect("in-flight set is not poisoned");
        held.extend(ids.iter().copied());
    }

    fn release(&self, ids: &[i64]) {
        if ids.is_empty() {
            return;
        }
        let empty = {
            let mut held = self.ids.lock().expect("in-flight set is not poisoned");
            for id in ids {
                held.remove(id);
            }
            held.is_empty()
        };
        if empty {
            self.drained.notify_waiters();
        }
        self.freed.notify_one();
    }
}

/// Run until `shutdown` is cancelled, then drain in-flight work and stop.
///
/// A dying child ends its session but not the worker: the session is rebuilt
/// around a fresh child and whatever it held is handed back for another
/// attempt. Surviving that is the reason task code runs in its own process.
pub async fn run(pool: PgPool, config: Config, shutdown: CancellationToken) -> Result<(), Error> {
    config.validate()?;
    gylo_pg::CAPABILITIES.require(&config.requires)?;

    let wakeup = Arc::new(Notify::new());
    // cancelled on our own initiative when a child gives up for good, which
    // the caller's token must not be, since it is theirs
    let internal = shutdown.child_token();

    let listening = tokio::spawn(listen(
        pool.clone(),
        config.queues.clone(),
        Arc::clone(&wakeup),
        internal.clone(),
    ));
    let recovering = tokio::spawn(recover(pool.clone(), config.clone(), internal.clone()));

    observe::children(config.processes);
    let mut children = tokio::task::JoinSet::new();
    for _ in 0..config.processes {
        children.spawn(supervise(
            pool.clone(),
            config.clone(),
            Arc::clone(&wakeup),
            internal.clone(),
        ));
    }

    let mut outcome = Ok(());
    while let Some(joined) = children.join_next().await {
        if let Ok(Err(error)) = joined
            && outcome.is_ok()
        {
            // a child that cannot stay up stops the worker rather than leaving
            // a process that looks healthy and runs at a fraction of capacity
            outcome = Err(error);
            internal.cancel();
        }
    }

    listening.abort();
    recovering.abort();
    outcome
}

/// One Python child, restarted as needed, with its own lease identity.
///
/// Children coordinate through the queue and nothing else: each leases under
/// its own id and `SKIP LOCKED` keeps them off each other's rows, so adding
/// one costs a connection and no contention.
async fn supervise(
    pool: PgPool,
    config: Config,
    wakeup: Arc<Notify>,
    shutdown: CancellationToken,
) -> Result<(), Error> {
    let worker = Uuid::new_v4();
    let inflight = Arc::new(InFlight::new(config.concurrency));
    let renewing = tokio::spawn(renew(
        pool.clone(),
        config.clone(),
        worker,
        Arc::clone(&inflight),
        shutdown.clone(),
    ));

    let mut consecutive: u32 = 0;
    let mut outcome = Ok(());

    while !shutdown.is_cancelled() {
        let started = tokio::time::Instant::now();
        match session(&pool, &config, worker, &inflight, &wakeup, &shutdown).await {
            Ok(()) => break,
            Err(error) => {
                if started.elapsed() >= HEALTHY_SESSION {
                    consecutive = 0;
                }
                consecutive += 1;
                observe::child_restarted();
                hand_back(&pool, &inflight, &config, worker).await;

                if consecutive > config.max_restarts {
                    tracing::error!(
                        %error,
                        restarts = consecutive - 1,
                        "python child keeps failing to stay up, giving up"
                    );
                    outcome = Err(error);
                    break;
                }

                let backoff = RESTART_DELAY
                    .saturating_mul(1 << (consecutive - 1).min(16))
                    .min(RESTART_CEILING);
                tracing::error!(%error, ?backoff, "worker session ended, restarting");
                tokio::select! {
                    () = shutdown.cancelled() => break,
                    () = tokio::time::sleep(backoff) => {}
                }
            }
        }
    }

    renewing.abort();
    outcome
}

/// Releases whatever the dead session was holding so another attempt can start
/// now rather than after the lease runs out.
async fn hand_back(pool: &PgPool, inflight: &InFlight, config: &Config, worker: Uuid) {
    let held = inflight.take();
    if held.is_empty() {
        return;
    }
    if let Err(error) = gylo_pg::abandon_leases(pool, &held, worker).await {
        tracing::error!(%error, jobs = held.len(), "could not release abandoned jobs");
        return;
    }
    match gylo_pg::reclaim_expired(pool, config.reclaim_limit).await {
        Ok(reclaimed) => tracing::info!(
            released = reclaimed.released,
            exhausted = reclaimed.exhausted,
            "handed back jobs from the failed session"
        ),
        Err(error) => tracing::error!(%error, "reclaiming after a failed session failed"),
    }
}

/// One child process, from spawn until it or the socket fails.
async fn session(
    pool: &PgPool,
    config: &Config,
    worker: Uuid,
    inflight: &Arc<InFlight>,
    wakeup: &Notify,
    shutdown: &CancellationToken,
) -> Result<(), Error> {
    let path = socket_path();
    let guard = SocketGuard(path.clone());
    let listener = UnixListener::bind(&path)?;
    let mut child = spawn_child(config, &path)?;

    let accepted = tokio::select! {
        result = tokio::time::timeout(HANDSHAKE_TIMEOUT, listener.accept()) => {
            result.map_err(|_| Error::HandshakeTimeout(HANDSHAKE_TIMEOUT))??
        }
        status = child.wait() => {
            return Err(Error::ChildExited(match status {
                Ok(status) => status.to_string(),
                Err(error) => error.to_string(),
            }));
        }
    };
    let (reader, writer) = accepted.0.into_split();

    let (acks, ack_rx) = tokio::sync::mpsc::unbounded_channel();
    let completions = tokio::spawn(collect_completions(
        reader,
        pool.clone(),
        Arc::clone(inflight),
        config.clone(),
        worker,
        acks,
    ));

    let outcome = tokio::select! {
        result = dispatch(
            writer,
            Dispatching {
                pool: pool.clone(),
                config,
                worker,
                inflight,
                wakeup,
                shutdown,
            },
            ack_rx,
        ) => result,
        status = child.wait() => Err(Error::ChildExited(match status {
            Ok(status) => status.to_string(),
            Err(error) => error.to_string(),
        })),
    };

    if shutdown.is_cancelled() {
        let deadline = tokio::time::Instant::now() + DRAIN_TIMEOUT;
        while !inflight.is_idle() && tokio::time::Instant::now() < deadline {
            tokio::select! {
                () = inflight.drained.notified() => {}
                () = tokio::time::sleep(Duration::from_millis(20)) => {}
            }
        }
        if !inflight.is_idle() {
            tracing::warn!(
                jobs = inflight.held(),
                "drain timed out; leases will be reclaimed by maintenance"
            );
        }
    }

    drop(guard);
    let _ = child.kill().await;
    let _ = completions.await;
    outcome
}

/// Wakes the dispatcher when a job lands on its queue. Failures here are
/// survivable: the dispatcher still polls, so losing the listener costs
/// latency rather than correctness.
async fn listen(
    pool: PgPool,
    queues: Vec<String>,
    wakeup: Arc<Notify>,
    shutdown: CancellationToken,
) {
    let queues: HashSet<String> = queues.into_iter().collect();
    loop {
        match sqlx::postgres::PgListener::connect_with(&pool).await {
            Ok(mut listener) => {
                if let Err(error) = listener.listen(gylo_pg::AVAILABLE_CHANNEL).await {
                    tracing::warn!(%error, "could not listen for job notifications");
                } else {
                    loop {
                        tokio::select! {
                            () = shutdown.cancelled() => return,
                            received = listener.recv() => match received {
                                Ok(notification) if queues.contains(notification.payload()) => {
                                    // every idle child, not one: a child that
                                    // is not waiting here is already fetching
                                    // and will see the job without being told
                                    wakeup.notify_waiters();
                                }
                                Ok(_) => {}
                                Err(error) => {
                                    tracing::warn!(%error, "job notification stream ended");
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            Err(error) => tracing::warn!(%error, "could not open a notification connection"),
        }

        tokio::select! {
            () = shutdown.cancelled() => return,
            () = tokio::time::sleep(LISTEN_RETRY) => {}
        }
    }
}

/// Keeps one child's leases alive while its jobs run.
async fn renew(
    pool: PgPool,
    config: Config,
    worker: Uuid,
    inflight: Arc<InFlight>,
    shutdown: CancellationToken,
) {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = tokio::time::sleep(config.maintenance_interval) => {}
        }

        let held = inflight.snapshot();
        if held.is_empty() {
            continue;
        }
        if let Err(error) = gylo_pg::renew_leases(&pool, &held, worker, config.lease).await {
            tracing::error!(%error, jobs = held.len(), "renewing leases failed");
        }
    }
}

/// Fires due schedules and recovers leases whose worker died.
///
/// Once per worker process rather than once per child: both are queue-wide, so
/// running them per child would multiply the work without finding anything a
/// single pass would miss. Shares the renewal cadence, being bounded by the
/// same lease.
async fn recover(pool: PgPool, config: Config, shutdown: CancellationToken) {
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            () = tokio::time::sleep(config.maintenance_interval) => {}
        }

        for queue in &config.queues {
            match gylo_pg::depth(&pool, queue).await {
                Ok(depth) => observe::depth(queue, depth),
                Err(error) => tracing::warn!(%error, %queue, "sampling queue depth failed"),
            }
        }

        fire_due_schedules(&pool, config.cron_limit).await;

        match gylo_pg::reclaim_expired(&pool, config.reclaim_limit).await {
            Ok(reclaimed) if reclaimed != gylo_pg::Reclaimed::default() => {
                observe::reclaimed(reclaimed.released, reclaimed.exhausted);
                tracing::info!(
                    released = reclaimed.released,
                    exhausted = reclaimed.exhausted,
                    "recovered abandoned leases"
                );
            }
            Ok(_) => {}
            Err(error) => tracing::error!(%error, "reclaiming abandoned leases failed"),
        }
    }
}

/// Records the schedules a child declared. A bad expression disables that one
/// schedule loudly rather than stopping the worker, since the rest of its tasks
/// are unaffected.
async fn register_schedules(pool: &PgPool, entries: &[gylo_core::CronRegistration]) {
    let now = Utc::now();
    for entry in entries {
        let schedule = match Schedule::parse(&entry.expression, &entry.timezone) {
            Ok(schedule) => schedule,
            Err(error) => {
                tracing::error!(schedule = %entry.name, %error, "schedule not registered");
                continue;
            }
        };
        let next = match schedule.next_after(now) {
            Ok(next) => next,
            Err(error) => {
                tracing::error!(schedule = %entry.name, %error, "schedule has no next run");
                continue;
            }
        };
        let record = gylo_pg::CronEntry {
            name: entry.name.clone(),
            queue: entry.queue.clone(),
            task: entry.task.clone(),
            payload: entry.payload.clone(),
            expression: entry.expression.clone(),
            timezone: entry.timezone.clone(),
        };
        if let Err(error) = gylo_pg::upsert_cron(pool, &record, next).await {
            tracing::error!(schedule = %entry.name, %error, "recording the schedule failed");
        }
    }
}

/// Enqueues whatever has come due. Every worker attempts this; the row lock
/// Postgres takes for the update means only one of them wins each occurrence.
async fn fire_due_schedules(pool: &PgPool, limit: i64) {
    let due = match gylo_pg::due_cron(pool, limit).await {
        Ok(due) => due,
        Err(error) => {
            tracing::error!(%error, "looking for due schedules failed");
            return;
        }
    };

    let now = Utc::now();
    for entry in due {
        let next = match Schedule::parse(&entry.expression, &entry.timezone)
            .and_then(|schedule| schedule.next_after(now))
        {
            Ok(next) => next,
            Err(error) => {
                tracing::error!(schedule = %entry.name, %error, "cannot advance schedule");
                continue;
            }
        };
        match gylo_pg::fire_cron(pool, &entry.name, next).await {
            Ok(Some(job)) => tracing::info!(schedule = %entry.name, job, "schedule fired"),
            Ok(None) => {}
            Err(error) => tracing::error!(schedule = %entry.name, %error, "firing failed"),
        }
    }
}

/// What the dispatch loop needs from the session around it.
struct Dispatching<'a> {
    pool: PgPool,
    config: &'a Config,
    worker: Uuid,
    inflight: &'a InFlight,
    wakeup: &'a Notify,
    shutdown: &'a CancellationToken,
}

async fn dispatch(
    mut writer: OwnedWriteHalf,
    context: Dispatching<'_>,
    mut acks: tokio::sync::mpsc::UnboundedReceiver<Message>,
) -> Result<(), Error> {
    let Dispatching {
        pool,
        config,
        worker,
        inflight,
        wakeup,
        shutdown,
    } = context;
    let mut buf = Vec::with_capacity(READ_BUFFER);
    let mut reserved = Vec::with_capacity(config.batch as usize);

    while !shutdown.is_cancelled() {
        while let Ok(ack) = acks.try_recv() {
            buf.clear();
            if encode(&ack, &mut buf).is_ok() {
                writer.write_all(&buf).await?;
            }
        }

        let available = inflight.available();
        if available == 0 {
            tokio::select! {
                ack = acks.recv() => {
                    if let Some(ack) = ack {
                        buf.clear();
                        if encode(&ack, &mut buf).is_ok() {
                            writer.write_all(&buf).await?;
                        }
                    }
                }
                () = inflight.freed.notified() => {}
                () = shutdown.cancelled() => break,
            }
            continue;
        }

        let limit = config.batch.min(available as i64);
        let jobs = match gylo_pg::fetch(&pool, &config.queues, limit, config.lease, worker).await {
            Ok(jobs) => {
                observe::leased(jobs.len());
                jobs
            }
            Err(error) => {
                tracing::warn!(%error, "leasing jobs failed, retrying");
                tokio::select! {
                    () = tokio::time::sleep(DATABASE_RETRY) => {}
                    () = shutdown.cancelled() => break,
                }
                continue;
            }
        };

        if jobs.is_empty() {
            tokio::select! {
                ack = acks.recv() => {
                    if let Some(ack) = ack {
                        buf.clear();
                        if encode(&ack, &mut buf).is_ok() {
                            writer.write_all(&buf).await?;
                        }
                    }
                }
                () = wakeup.notified() => {}
                () = tokio::time::sleep(config.poll_interval) => {}
                () = shutdown.cancelled() => break,
            }
            continue;
        }

        buf.clear();
        reserved.clear();
        for job in jobs {
            let id = job.id;
            if job.durable && job.attempt > 1 {
                match gylo_pg::steps_for(&pool, id).await {
                    Ok(steps) if !steps.is_empty() => {
                        if encode(&Message::Steps { id, steps }, &mut buf).is_err() {
                            tracing::error!(job = id, "could not send prior steps");
                        }
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::error!(job = id, %error, "loading prior steps failed");
                    }
                }
            }
            let message = Message::Dispatch {
                id,
                task: job.task,
                payload: job.payload,
            };
            match encode(&message, &mut buf) {
                Ok(()) => reserved.push(id),
                Err(error) => {
                    tracing::error!(job = id, %error, "job could not be encoded, discarding");
                    if let Err(error) =
                        gylo_pg::discard(&pool, id, worker, &error.to_string()).await
                    {
                        tracing::error!(job = id, %error, "discarding the job failed");
                    }
                }
            }
        }

        if !reserved.is_empty() {
            inflight.reserve(&reserved);
            writer.write_all(&buf).await?;
        }
    }

    Ok(())
}

#[derive(Default)]
struct Pending {
    completed: Vec<i64>,
    with_result: Vec<i64>,
    results: Vec<Vec<u8>>,
    retry: Vec<i64>,
    retry_errors: Vec<String>,
    terminal: Vec<i64>,
    terminal_errors: Vec<String>,
}

impl Pending {
    fn push(&mut self, id: i64, outcome: Outcome) {
        match outcome {
            Outcome::Success { result } if result.is_empty() => self.completed.push(id),
            Outcome::Success { result } => {
                self.with_result.push(id);
                self.results.push(result);
            }
            Outcome::Failure { error, retry: true } => {
                self.retry.push(id);
                self.retry_errors.push(error);
            }
            Outcome::Failure {
                error,
                retry: false,
            } => {
                self.terminal.push(id);
                self.terminal_errors.push(error);
            }
        }
    }

    fn len(&self) -> usize {
        self.completed.len() + self.with_result.len() + self.retry.len() + self.terminal.len()
    }

    async fn flush(&mut self, pool: &PgPool, inflight: &InFlight, config: &Config, worker: Uuid) {
        if self.len() == 0 {
            return;
        }
        let started = tokio::time::Instant::now();
        if let Err(error) = gylo_pg::complete_many(pool, &self.completed, worker).await {
            tracing::error!(%error, jobs = self.completed.len(), "recording completions failed");
        }
        if let Err(error) =
            gylo_pg::complete_many_with_results(pool, &self.with_result, &self.results, worker)
                .await
        {
            tracing::error!(%error, jobs = self.with_result.len(), "recording results failed");
        }
        if let Err(error) = gylo_pg::retry_many(
            pool,
            &self.retry,
            &self.retry_errors,
            worker,
            config.retry_base,
            config.retry_cap,
        )
        .await
        {
            tracing::error!(%error, jobs = self.retry.len(), "scheduling retries failed");
        }
        if let Err(error) =
            gylo_pg::discard_many(pool, &self.terminal, &self.terminal_errors, worker).await
        {
            tracing::error!(%error, jobs = self.terminal.len(), "recording failures failed");
        }

        observe::settled(
            self.completed.len() + self.with_result.len(),
            self.retry.len(),
            self.terminal.len(),
        );
        observe::flushed(started.elapsed().as_secs_f64());

        let mut settled = Vec::with_capacity(self.len());
        settled.extend_from_slice(&self.completed);
        settled.extend_from_slice(&self.with_result);
        settled.extend_from_slice(&self.retry);
        settled.extend_from_slice(&self.terminal);
        self.completed.clear();
        self.with_result.clear();
        self.results.clear();
        self.retry.clear();
        self.retry_errors.clear();
        self.terminal.clear();
        self.terminal_errors.clear();
        inflight.release(&settled);
    }
}

/// Jobs are released only once their batch is durable, so the pending buffer
/// stays bounded by the concurrency limit and cannot outrun Postgres.
async fn collect_completions(
    mut reader: OwnedReadHalf,
    pool: PgPool,
    inflight: Arc<InFlight>,
    config: Config,
    worker: Uuid,
    acks: tokio::sync::mpsc::UnboundedSender<Message>,
) {
    let batch = config.completion_batch;
    let linger = config.completion_linger;
    let mut decoder = Decoder::new();
    let mut chunk = vec![0u8; READ_BUFFER];
    let mut pending = Pending::default();
    let mut deadline: Option<tokio::time::Instant> = None;

    loop {
        let timer = async {
            match deadline {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        };

        tokio::select! {
            read = reader.read(&mut chunk) => {
                match read {
                    Ok(0) => break,
                    Err(error) => {
                        tracing::error!(%error, "reading from python child failed");
                        break;
                    }
                    Ok(read) => decoder.extend(&chunk[..read]),
                }

                loop {
                    match decoder.next_message() {
                        Ok(None) => break,
                        Ok(Some(Message::Complete { id, outcome })) => {
                            pending.push(id, outcome);
                            if deadline.is_none() {
                                deadline = Some(tokio::time::Instant::now() + linger);
                            }
                        }
                        Ok(Some(Message::Register(entries))) => {
                            register_schedules(&pool, &entries).await;
                        }
                        Ok(Some(Message::Record { id, name, result })) => {
                            match gylo_pg::record_step(&pool, id, &name, &result).await {
                                Ok(()) => {
                                    let _ = acks.send(Message::Stored { id, name });
                                }
                                Err(error) => {
                                    tracing::error!(job = id, step = %name, %error,
                                        "recording a step failed; the child will wait");
                                }
                            }
                        }
                        Ok(Some(Message::Steps { .. } | Message::Stored { .. })) => {
                            tracing::error!("python child sent a supervisor-only frame");
                        }
                        Ok(Some(Message::Dispatch { .. })) => {
                            tracing::error!("python child sent a dispatch frame");
                        }
                        Err(error) => {
                            tracing::error!(%error, "python child sent an unreadable frame");
                            pending.flush(&pool, &inflight, &config, worker).await;
                            return;
                        }
                    }
                }

                if pending.len() >= batch {
                    pending.flush(&pool, &inflight, &config, worker).await;
                    deadline = None;
                }
            }
            () = timer => {
                pending.flush(&pool, &inflight, &config, worker).await;
                deadline = None;
            }
        }
    }

    pending.flush(&pool, &inflight, &config, worker).await;
}

fn spawn_child(config: &Config, socket: &Path) -> std::io::Result<Child> {
    let mut command = Command::new(&config.python);
    command
        .arg("-m")
        .arg("gylo._worker")
        .arg("--socket")
        .arg(socket)
        .arg("--app")
        .arg(&config.app)
        .kill_on_drop(true);

    if let Some(path) = &config.python_path {
        command.env("PYTHONPATH", path);
    }
    for (key, value) in &config.env {
        command.env(key, value);
    }
    command.spawn()
}

fn socket_path() -> PathBuf {
    std::env::temp_dir().join(format!("gylo-{}.sock", Uuid::new_v4()))
}
