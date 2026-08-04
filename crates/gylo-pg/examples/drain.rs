//! How fast Postgres alone can hand out jobs and take them back.
//!
//! The end-to-end benchmark plateaus around ninety thousand a second and that
//! could be the database, the children, or the IPC between them. This runs the
//! same lease-and-finalise cycle with no Python anywhere, so whatever it
//! reaches is the ceiling the rest of the worker is working under.

use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use gylo_pg::{NewJob, complete_many, connect, enqueue, fetch};
use uuid::Uuid;

const QUEUE: &str = "drainbench";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = env::var("DATABASE_URL").expect("set DATABASE_URL");
    let jobs: usize = env::var("JOBS").map_or(50_000, |v| v.parse().unwrap());
    let batch: i64 = env::var("BATCH").map_or(128, |v| v.parse().unwrap());

    for leasers in [1, 2, 4, 6, 8, 12] {
        let pool = connect(&url, leasers as u32 + 4).await?;
        sqlx::query("DELETE FROM gylo_job WHERE queue = $1")
            .bind(QUEUE)
            .execute(&pool)
            .await?;

        let job = NewJob::new("bench.noop", Vec::new()).on_queue(QUEUE);
        let mut tx = pool.begin().await?;
        for _ in 0..jobs {
            enqueue(&mut *tx, &job).await?;
        }
        tx.commit().await?;

        let drained = Arc::new(AtomicU64::new(0));
        let started = Instant::now();
        let mut tasks = Vec::new();
        for _ in 0..leasers {
            let pool = pool.clone();
            let drained = Arc::clone(&drained);
            let worker = Uuid::new_v4();
            tasks.push(tokio::spawn(async move {
                loop {
                    let leased = fetch(&pool, QUEUE, batch, Duration::from_secs(30), worker)
                        .await
                        .expect("leasing");
                    if leased.is_empty() {
                        return;
                    }
                    let ids: Vec<i64> = leased.iter().map(|job| job.id).collect();
                    complete_many(&pool, &ids, worker)
                        .await
                        .expect("finalising");
                    drained.fetch_add(ids.len() as u64, Ordering::Relaxed);
                }
            }));
        }
        for task in tasks {
            task.await?;
        }

        let elapsed = started.elapsed().as_secs_f64();
        let total = drained.load(Ordering::Relaxed);
        println!(
            "{leasers:>2} leasers  {:>10.0}/s  {total} jobs in {elapsed:.2}s",
            total as f64 / elapsed
        );
        pool.close().await;
    }

    Ok(())
}
