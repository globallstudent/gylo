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
}

#[derive(Debug, Clone)]
pub struct Config {
    pub queue: String,
    /// Jobs in flight in the child at once; also caps how far ahead of the
    /// child the supervisor will lease.
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
}

impl Default for Config {
    fn default() -> Self {
        Self {
            queue: "default".to_owned(),
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

    let worker = Uuid::new_v4();
    let inflight = Arc::new(InFlight::new(config.concurrency));
    let wakeup = Arc::new(Notify::new());

    let listening = tokio::spawn(listen(
        pool.clone(),
        config.queue.clone(),
        Arc::clone(&wakeup),
        shutdown.clone(),
    ));
    let maintaining = tokio::spawn(maintain(
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

    listening.abort();
    maintaining.abort();
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

    let completions = tokio::spawn(collect_completions(
        reader,
        pool.clone(),
        Arc::clone(inflight),
        config.clone(),
        worker,
    ));

    let outcome = tokio::select! {
        result = dispatch(writer, pool.clone(), config, worker, inflight, wakeup, shutdown) => result,
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
async fn listen(pool: PgPool, queue: String, wakeup: Arc<Notify>, shutdown: CancellationToken) {
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
                                Ok(notification) if notification.payload() == queue => {
                                    wakeup.notify_one();
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

/// Renewal and recovery share a cadence because both are bounded by the lease.
async fn maintain(
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
        if let Err(error) = gylo_pg::renew_leases(&pool, &held, worker, config.lease).await {
            tracing::error!(%error, jobs = held.len(), "renewing leases failed");
        }

        fire_due_schedules(&pool, config.cron_limit).await;

        match gylo_pg::reclaim_expired(&pool, config.reclaim_limit).await {
            Ok(reclaimed) if reclaimed != gylo_pg::Reclaimed::default() => {
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

async fn dispatch(
    mut writer: OwnedWriteHalf,
    pool: PgPool,
    config: &Config,
    worker: Uuid,
    inflight: &InFlight,
    wakeup: &Notify,
    shutdown: &CancellationToken,
) -> Result<(), Error> {
    let mut buf = Vec::with_capacity(READ_BUFFER);
    let mut reserved = Vec::with_capacity(config.batch as usize);

    while !shutdown.is_cancelled() {
        let available = inflight.available();
        if available == 0 {
            tokio::select! {
                () = inflight.freed.notified() => {}
                () = shutdown.cancelled() => break,
            }
            continue;
        }

        let limit = config.batch.min(available as i64);
        let jobs = match gylo_pg::fetch(&pool, &config.queue, limit, config.lease, worker).await {
            Ok(jobs) => jobs,
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
    command.spawn()
}

fn socket_path() -> PathBuf {
    std::env::temp_dir().join(format!("gylo-{}.sock", Uuid::new_v4()))
}
