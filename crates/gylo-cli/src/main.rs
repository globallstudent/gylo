use std::ffi::OsString;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use gylo_worker::Config;
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
    Worker {
        /// `module:attribute` path to your Gylo app.
        #[arg(long, env = "GYLO_APP")]
        app: String,

        #[arg(long, default_value = "default")]
        queue: String,

        /// Jobs the Python child may have in flight at once.
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

        /// Interpreter to run task code with.
        #[arg(long, default_value = "python3", env = "GYLO_PYTHON")]
        python: PathBuf,

        /// Value for the child's PYTHONPATH.
        #[arg(long, env = "PYTHONPATH")]
        python_path: Option<OsString>,

        #[arg(long, default_value_t = 8)]
        pool_size: u32,
    },
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
        Command::Worker {
            app,
            queue,
            concurrency,
            batch,
            lease,
            poll_interval,
            maintenance_interval,
            python,
            python_path,
            pool_size,
        } => {
            let pool = connect(cli.database_url, pool_size).await?;
            let config = Config {
                queue,
                concurrency,
                batch,
                lease: lease.into(),
                poll_interval: poll_interval.into(),
                maintenance_interval: maintenance_interval.into(),
                python,
                app,
                python_path,
                ..Config::default()
            };

            let shutdown = CancellationToken::new();
            let signal = shutdown.clone();
            tokio::spawn(async move {
                if tokio::signal::ctrl_c().await.is_ok() {
                    tracing::info!("draining in-flight jobs, interrupt again to stop now");
                    signal.cancel();
                }
            });

            tracing::info!(queue = %config.queue, "worker starting");
            gylo_worker::run(pool, config, shutdown)
                .await
                .context("running the worker")?;
            tracing::info!("worker stopped");
            Ok(())
        }
    }
}
