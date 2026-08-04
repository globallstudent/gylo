use std::ffi::OsString;
use std::path::PathBuf;
use std::time::Duration;

use gylo_worker::Config;
use tokio_util::sync::CancellationToken;

fn env(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| panic!("{key} must be set"))
}

fn millis(key: &str, fallback: u64) -> Duration {
    Duration::from_millis(
        std::env::var(key)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(fallback),
    )
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .init();

    let pool = gylo_pg::connect(&env("DATABASE_URL"), 8)
        .await
        .expect("connecting to postgres");

    let config = Config {
        app: env("GYLO_CHAOS_APP"),
        python: PathBuf::from(env("GYLO_CHAOS_PYTHON")),
        python_path: Some(OsString::from(env("GYLO_CHAOS_PYTHONPATH"))),
        lease: millis("GYLO_CHAOS_LEASE_MS", 2000),
        maintenance_interval: millis("GYLO_CHAOS_MAINTENANCE_MS", 400),
        poll_interval: millis("GYLO_CHAOS_POLL_MS", 100),
        concurrency: 16,
        batch: 8,
        ..Config::default()
    };

    let shutdown = CancellationToken::new();
    let signal = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        signal.cancel();
    });

    if let Err(error) = gylo_worker::run(pool, config, shutdown).await {
        eprintln!("worker stopped: {error}");
        std::process::exit(1);
    }
}
