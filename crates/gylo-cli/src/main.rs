use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};
use axum::Router;
use axum::routing::get;
use clap::{Parser, Subcommand};
use gylo_worker::Config;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(name = "gylo", version, about = "A distributed task queue for Python")]
struct Cli {
    /// Postgres connection string.
    #[arg(long, env = "DATABASE_URL", global = true)]
    database_url: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Apply any migrations the database is missing.
    Migrate,
    /// Run a worker until interrupted.
    Worker(Box<WorkerArgs>),
    /// Show what each queue is holding.
    Queue {
        /// Only this queue. Every queue with work on it when unset.
        #[arg(long)]
        queue: Option<String>,
    },
    /// Inspect and act on jobs that ran out of attempts.
    Jobs {
        #[command(subcommand)]
        command: JobsCommand,
    },
}

#[derive(Subcommand)]
enum JobsCommand {
    /// List dead-lettered jobs, most recently failed first.
    Failed {
        #[arg(long)]
        queue: Option<String>,

        #[arg(long, default_value_t = 20)]
        limit: i64,
    },
    /// Return dead-lettered jobs to their queue with attempts reset.
    Retry {
        /// Job ids. Every dead-lettered job in scope when none are given.
        ids: Vec<i64>,

        #[arg(long)]
        queue: Option<String>,
    },
    /// Delete dead-lettered jobs for good.
    Purge {
        #[arg(long)]
        queue: Option<String>,

        /// Required, because this cannot be undone.
        #[arg(long)]
        yes: bool,
    },
}

/// Boxed into its own type: inline, these fields make the `Worker` variant
/// vastly larger than `Migrate`, so every `Command` would carry the worker's
/// full configuration around.
#[derive(clap::Args)]
struct WorkerArgs {
    /// `module:attribute` path to your Gylo app.
    #[arg(long, env = "GYLO_APP")]
    app: String,

    /// Queues to consume. Repeat or comma-separate for several; a job's
    /// priority is compared across all of them at once rather than per queue.
    #[arg(long = "queue", default_value = "default", value_delimiter = ',')]
    queues: Vec<String>,

    /// Python child processes to run. Defaults to the core count, since
    /// task code holds the interpreter lock and one child uses one core.
    #[arg(long)]
    processes: Option<usize>,

    /// Jobs one Python child may have in flight at once.
    #[arg(long, default_value_t = 256)]
    concurrency: usize,

    /// Jobs leased per round trip.
    #[arg(long, default_value_t = 128)]
    batch: i64,

    /// How long a lease is held before another worker may reclaim it.
    #[arg(long, default_value = "30s")]
    lease: humantime::Duration,

    /// How long to wait before polling again when the queue is empty.
    /// Notifications normally arrive first, so this is a safety net.
    #[arg(long, default_value = "200ms")]
    poll_interval: humantime::Duration,

    /// How often to renew held leases and reclaim abandoned ones. Must
    /// stay well below the lease.
    #[arg(long, default_value = "10s")]
    maintenance_interval: humantime::Duration,

    /// Interpreter to run task code with. Defaults to the one beside this
    /// executable, falling back to `python3` on PATH.
    #[arg(long, env = "GYLO_PYTHON")]
    python: Option<PathBuf>,

    /// Value for the child's PYTHONPATH.
    #[arg(long, env = "PYTHONPATH")]
    python_path: Option<OsString>,

    /// Defaults to what the configured children can actually use.
    #[arg(long)]
    pool_size: Option<u32>,

    /// Address to serve `/metrics` and `/healthz` on. Off when unset.
    #[arg(long, env = "GYLO_OBSERVE_ADDRESS")]
    observe: Option<SocketAddr>,
}

/// The interpreter that owns this installation.
///
/// A wheel puts the `gylo` binary in the same directory as the environment's
/// own interpreter, which is the one that can import the user's tasks. Taking
/// `python3` off PATH instead picks up whichever interpreter happens to come
/// first, so an activated environment or a system Python shadowing the venv
/// silently runs task code somewhere the dependencies are not installed.
fn sibling_python() -> PathBuf {
    let name = "python3";
    std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|bin| bin.join(name)))
        .filter(|python| python.is_file())
        .unwrap_or_else(|| PathBuf::from(name))
}

/// Stop signals, listened for from the moment this is built.
///
/// Unix only, like the rest of the worker: the supervisor reaches its children
/// over a Unix domain socket, so there is no build of this for a platform
/// without one.
///
/// Container runtimes and init systems ask for shutdown with SIGTERM and only
/// escalate to SIGKILL once a grace period runs out. A worker listening for
/// interrupts alone never hears the request, so it is killed outright and its
/// in-flight jobs sit unavailable until their leases expire.
///
/// Registration is separate from waiting, and happens before the worker
/// connects to anything: a signal arriving in the second or so a pool takes to
/// come up would otherwise find no handler installed and kill the process,
/// which is exactly when a rollout is most likely to send one.
struct StopSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

impl StopSignals {
    fn listen() -> Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};

        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    async fn received(&mut self) -> &'static str {
        tokio::select! {
            _ = self.interrupt.recv() => "SIGINT",
            _ = self.terminate.recv() => "SIGTERM",
        }
    }
}

/// Serves `/metrics` for Prometheus and `/healthz` for a container probe.
///
/// Health is deliberately shallow: it answers whether this process is alive
/// and able to serve, not whether Postgres is reachable. A liveness probe that
/// fails on a database blip restarts every worker at once, turning a recovery
/// into an outage, and the queue depth metrics already say when work is not
/// moving.
async fn serve_observability(
    address: SocketAddr,
    prometheus: PrometheusHandle,
    shutdown: CancellationToken,
) -> Result<()> {
    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route(
            "/metrics",
            get(move || {
                let rendered = prometheus.render();
                async move { rendered }
            }),
        );

    let listener = tokio::net::TcpListener::bind(address)
        .await
        .with_context(|| format!("binding the metrics endpoint on {address}"))?;
    tracing::info!(%address, "serving /metrics and /healthz");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
        .context("serving the metrics endpoint")
}

async fn show_queues(pool: &PgPool, queue: Option<String>) -> Result<()> {
    let names = match queue {
        Some(one) => vec![one],
        None => gylo_pg::queues(pool).await.context("listing queues")?,
    };
    if names.is_empty() {
        println!("no queue is holding anything");
        return Ok(());
    }

    println!(
        "{:<20} {:>9} {:>10} {:>9} {:>9}",
        "queue", "ready", "scheduled", "blocked", "running"
    );
    for name in names {
        let depth = gylo_pg::depth(pool, &name)
            .await
            .with_context(|| format!("reading the depth of {name}"))?;
        println!(
            "{:<20} {:>9} {:>10} {:>9} {:>9}",
            name, depth.ready, depth.scheduled, depth.blocked, depth.running
        );
    }
    Ok(())
}

async fn show_failed(pool: &PgPool, queue: Option<String>, limit: i64) -> Result<()> {
    let failed = gylo_pg::list_discarded(pool, queue.as_deref(), limit)
        .await
        .context("listing dead-lettered jobs")?;
    if failed.is_empty() {
        println!("nothing has been dead-lettered");
        return Ok(());
    }

    for job in failed {
        let when = job.finalized_at.map_or_else(
            || "-".to_owned(),
            |at| at.format("%Y-%m-%d %H:%M:%S").to_string(),
        );
        // one line each, since the point of the listing is to find the id
        // worth looking at rather than to read the tracebacks
        let error = job.error.unwrap_or_else(|| "-".to_owned());
        let first = error.lines().next_back().unwrap_or("-");
        println!(
            "{:<10} {:<16} {:<24} attempt {:<4} {when}  {first}",
            job.id, job.queue, job.task, job.attempt
        );
    }
    Ok(())
}

async fn retry_failed(pool: &PgPool, ids: Vec<i64>, queue: Option<String>) -> Result<()> {
    let selected = (!ids.is_empty()).then_some(ids);
    let retried = gylo_pg::retry_discarded(pool, selected.as_deref(), queue.as_deref())
        .await
        .context("returning dead-lettered jobs to their queue")?;
    println!("{} job(s) queued again", retried.len());
    Ok(())
}

async fn purge_failed(pool: &PgPool, queue: Option<String>, confirmed: bool) -> Result<()> {
    if !confirmed {
        anyhow::bail!("pass --yes to delete dead-lettered jobs; this cannot be undone");
    }
    let removed = gylo_pg::purge_discarded(pool, queue.as_deref())
        .await
        .context("deleting dead-lettered jobs")?;
    println!("{removed} job(s) deleted");
    Ok(())
}

async fn connect(url: Option<String>, size: u32) -> Result<PgPool> {
    let url = url.context("set DATABASE_URL or pass --database-url")?;
    gylo_pg::connect(&url, size)
        .await
        .context("connecting to postgres")
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "gylo=info,warn".into()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::Migrate => {
            let pool = connect(cli.database_url, 2).await?;
            gylo_pg::migrate(&pool)
                .await
                .context("applying migrations")?;
            println!("migrations applied");
            Ok(())
        }
        Command::Queue { queue } => {
            let pool = connect(cli.database_url, 2).await?;
            show_queues(&pool, queue).await
        }
        Command::Jobs { command } => {
            let pool = connect(cli.database_url, 2).await?;
            match command {
                JobsCommand::Failed { queue, limit } => show_failed(&pool, queue, limit).await,
                JobsCommand::Retry { ids, queue } => retry_failed(&pool, ids, queue).await,
                JobsCommand::Purge { queue, yes } => purge_failed(&pool, queue, yes).await,
            }
        }
        Command::Worker(args) => {
            let WorkerArgs {
                app,
                queues,
                processes,
                concurrency,
                batch,
                lease,
                poll_interval,
                maintenance_interval,
                python,
                python_path,
                pool_size,
                observe,
            } = *args;
            let mut signals = StopSignals::listen()?;
            // installed before the worker runs so no metric is recorded into a
            // registry that does not exist yet
            let prometheus = observe
                .map(|_| {
                    PrometheusBuilder::new()
                        .install_recorder()
                        .context("installing the metrics recorder")
                })
                .transpose()?;
            let processes = processes.unwrap_or_else(|| Config::default().processes);
            // three at once per child — leasing, finalising, renewing — plus
            // the queue-wide listener and recovery
            let size = pool_size.unwrap_or(processes as u32 * 3 + 4);
            let pool = connect(cli.database_url, size).await?;
            let config = Config {
                queues,
                processes,
                concurrency,
                batch,
                lease: lease.into(),
                poll_interval: poll_interval.into(),
                maintenance_interval: maintenance_interval.into(),
                python: python.unwrap_or_else(sibling_python),
                app,
                python_path,
                ..Config::default()
            };

            let shutdown = CancellationToken::new();
            let signal = shutdown.clone();
            tokio::spawn(async move {
                let received = signals.received().await;
                tracing::info!(
                    signal = received,
                    "draining in-flight jobs, signal again to stop now"
                );
                signal.cancel();
            });

            let serving = match (observe, prometheus) {
                (Some(address), Some(handle)) => Some(tokio::spawn(serve_observability(
                    address,
                    handle,
                    shutdown.clone(),
                ))),
                _ => None,
            };

            tracing::info!(queues = ?config.queues, "worker starting");
            let outcome = gylo_worker::run(pool, config, shutdown.clone())
                .await
                .context("running the worker");
            shutdown.cancel();
            if let Some(serving) = serving
                && let Ok(Err(error)) = serving.await
            {
                tracing::error!(%error, "metrics endpoint failed");
            }
            outcome?;
            tracing::info!("worker stopped");
            Ok(())
        }
    }
}
