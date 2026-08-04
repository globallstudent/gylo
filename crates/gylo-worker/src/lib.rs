//! Supervisor process: leases jobs from Postgres and dispatches them to a
//! Python child over the gylo wire protocol.
//!
//! Dispatch and completion run as independent tasks over the same socket, so
//! neither direction waits on a round trip.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use gylo_core::{Decoder, Message, Outcome, encode};
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
}

#[derive(Debug, Clone)]
pub struct Config {
    pub queue: String,
    /// Jobs in flight in the child at once; also caps how far ahead of the
    /// child the supervisor will lease.
    pub concurrency: usize,
    pub batch: i64,
    /// How long a lease is held before maintenance may reclaim it.
    pub lease: Duration,
    pub poll_interval: Duration,
    pub python: PathBuf,
    /// `module:attribute` path to the user's app object.
    pub app: String,
    pub python_path: Option<OsString>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            queue: "default".to_owned(),
            concurrency: 64,
            batch: 64,
            lease: Duration::from_secs(30),
            poll_interval: Duration::from_millis(200),
            python: PathBuf::from("python3"),
            app: String::new(),
            python_path: None,
        }
    }
}

struct SocketGuard(PathBuf);

impl Drop for SocketGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

struct Capacity {
    inflight: AtomicUsize,
    limit: usize,
    freed: Notify,
    drained: Notify,
}

impl Capacity {
    fn new(limit: usize) -> Self {
        Self {
            inflight: AtomicUsize::new(0),
            limit,
            freed: Notify::new(),
            drained: Notify::new(),
        }
    }

    fn available(&self) -> usize {
        self.limit
            .saturating_sub(self.inflight.load(Ordering::Acquire))
    }

    fn reserve(&self, count: usize) {
        self.inflight.fetch_add(count, Ordering::AcqRel);
    }

    fn release(&self) {
        if self.inflight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.drained.notify_waiters();
        }
        self.freed.notify_one();
    }

    fn is_idle(&self) -> bool {
        self.inflight.load(Ordering::Acquire) == 0
    }
}

/// Run until `shutdown` is cancelled, then drain in-flight work and stop.
pub async fn run(pool: PgPool, config: Config, shutdown: CancellationToken) -> Result<(), Error> {
    let path = socket_path();
    let _guard = SocketGuard(path.clone());
    let listener = UnixListener::bind(&path)?;
    let mut child = spawn_child(&config, &path)?;

    let handshake = Duration::from_secs(30);
    let accepted = tokio::time::timeout(handshake, listener.accept())
        .await
        .map_err(|_| Error::HandshakeTimeout(handshake))??;
    let (reader, writer) = accepted.0.into_split();

    let capacity = Arc::new(Capacity::new(config.concurrency));
    let completions = tokio::spawn(collect_completions(
        reader,
        pool.clone(),
        Arc::clone(&capacity),
    ));

    let outcome = dispatch(writer, pool, &config, &capacity, &shutdown).await;

    let deadline = tokio::time::Instant::now() + DRAIN_TIMEOUT;
    while !capacity.is_idle() && tokio::time::Instant::now() < deadline {
        tokio::select! {
            () = capacity.drained.notified() => {}
            () = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
    }
    if !capacity.is_idle() {
        tracing::warn!("drain timed out; leases will be reclaimed by maintenance");
    }

    drop(_guard);
    let _ = child.kill().await;
    let _ = completions.await;
    outcome
}

async fn dispatch(
    mut writer: OwnedWriteHalf,
    pool: PgPool,
    config: &Config,
    capacity: &Capacity,
    shutdown: &CancellationToken,
) -> Result<(), Error> {
    let worker = Uuid::new_v4();
    let mut buf = Vec::with_capacity(READ_BUFFER);

    while !shutdown.is_cancelled() {
        let available = capacity.available();
        if available == 0 {
            tokio::select! {
                () = capacity.freed.notified() => {}
                () = shutdown.cancelled() => break,
            }
            continue;
        }

        let limit = config.batch.min(available as i64);
        let jobs = gylo_pg::fetch(&pool, &config.queue, limit, config.lease, worker).await?;
        if jobs.is_empty() {
            tokio::select! {
                () = tokio::time::sleep(config.poll_interval) => {}
                () = shutdown.cancelled() => break,
            }
            continue;
        }

        buf.clear();
        let mut reserved = 0usize;
        for job in jobs {
            let message = Message::Dispatch {
                id: job.id,
                task: job.task,
                payload: job.payload,
            };
            match encode(&message, &mut buf) {
                Ok(()) => reserved += 1,
                Err(error) => {
                    let Message::Dispatch { id, .. } = message else {
                        unreachable!("message was constructed as a dispatch")
                    };
                    tracing::error!(job = id, %error, "job could not be encoded, discarding");
                    gylo_pg::discard(&pool, id, &error.to_string()).await?;
                }
            }
        }

        if reserved > 0 {
            capacity.reserve(reserved);
            writer.write_all(&buf).await?;
        }
    }

    Ok(())
}

async fn collect_completions(mut reader: OwnedReadHalf, pool: PgPool, capacity: Arc<Capacity>) {
    let mut decoder = Decoder::new();
    let mut chunk = vec![0u8; READ_BUFFER];

    loop {
        let read = match reader.read(&mut chunk).await {
            Ok(0) => break,
            Ok(read) => read,
            Err(error) => {
                tracing::error!(%error, "reading from python child failed");
                break;
            }
        };
        decoder.extend(&chunk[..read]);

        loop {
            match decoder.next_message() {
                Ok(None) => break,
                Ok(Some(message)) => apply(&pool, &capacity, message).await,
                Err(error) => {
                    tracing::error!(%error, "python child sent an unreadable frame");
                    return;
                }
            }
        }
    }
}

async fn apply(pool: &PgPool, capacity: &Capacity, message: Message) {
    let Message::Complete { id, outcome } = message else {
        tracing::error!("python child sent a dispatch frame");
        return;
    };

    let result = match &outcome {
        Outcome::Success => gylo_pg::complete(pool, id).await,
        Outcome::Failure(error) => gylo_pg::discard(pool, id, error).await,
    };

    match result {
        Ok(true) => {}
        Ok(false) => tracing::warn!(job = id, "completion arrived for a job we no longer hold"),
        Err(error) => tracing::error!(job = id, %error, "recording completion failed"),
    }
    capacity.release();
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
