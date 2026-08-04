use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use gylo_core::Feature;
use gylo_redis::Backend;
use tokio::sync::Mutex;

const URL: &str = "redis://127.0.0.1:6389/1";
const LEASE: Duration = Duration::from_secs(30);

/// Each test gets its own namespace, so the suite can run in parallel against
/// one server without tests clearing each other's jobs.
async fn fresh(namespace: &str) -> Backend {
    let mut backend = Backend::connect_namespaced(URL, namespace)
        .await
        .expect("redis is not running");
    backend.clear().await.unwrap();
    backend
}

#[tokio::test]
async fn an_enqueued_job_comes_back() {
    let mut backend = fresh("t:an_enqueued_job_comes_back").await;
    backend
        .enqueue(1, "billing.charge", vec![1, 2, 3], 0, 20, Duration::ZERO)
        .await
        .unwrap();

    let jobs = backend.fetch(10, LEASE).await.unwrap();

    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].id, 1);
    assert_eq!(jobs[0].task, "billing.charge");
    assert_eq!(jobs[0].payload, vec![1, 2, 3]);
    assert_eq!(jobs[0].attempt, 1);
}

#[tokio::test]
async fn a_delayed_job_is_not_yet_due() {
    let mut backend = fresh("t:a_delayed_job_is_not_yet_due").await;
    backend
        .enqueue(1, "t", Vec::new(), 0, 20, Duration::from_secs(3600))
        .await
        .unwrap();

    assert!(backend.fetch(10, LEASE).await.unwrap().is_empty());
}

#[tokio::test]
async fn lower_priority_number_runs_first() {
    let mut backend = fresh("t:lower_priority_number_runs_first").await;
    backend
        .enqueue(1, "low", Vec::new(), 9, 20, Duration::ZERO)
        .await
        .unwrap();
    backend
        .enqueue(2, "high", Vec::new(), 0, 20, Duration::ZERO)
        .await
        .unwrap();

    let jobs = backend.fetch(10, LEASE).await.unwrap();

    assert_eq!(jobs[0].task, "high");
    assert_eq!(jobs[1].task, "low");
}

#[tokio::test]
async fn a_leased_job_is_invisible_to_the_next_fetch() {
    let mut backend = fresh("t:a_leased_job_is_invisible_to_the_next_fetch").await;
    backend
        .enqueue(1, "t", Vec::new(), 0, 20, Duration::ZERO)
        .await
        .unwrap();

    assert_eq!(backend.fetch(10, LEASE).await.unwrap().len(), 1);
    assert!(backend.fetch(10, LEASE).await.unwrap().is_empty());
}

#[tokio::test]
async fn an_expired_lease_is_reclaimed() {
    let mut backend = fresh("t:an_expired_lease_is_reclaimed").await;
    backend
        .enqueue(1, "t", Vec::new(), 0, 20, Duration::ZERO)
        .await
        .unwrap();
    backend.fetch(10, Duration::ZERO).await.unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(backend.reclaim_expired(100).await.unwrap(), 1);

    let again = backend.fetch(10, LEASE).await.unwrap();
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].attempt, 2, "the second claim is a second attempt");
}

#[tokio::test]
async fn completing_removes_the_job_entirely() {
    let mut backend = fresh("t:completing_removes_the_job_entirely").await;
    backend
        .enqueue(1, "t", Vec::new(), 0, 20, Duration::ZERO)
        .await
        .unwrap();
    backend.fetch(10, Duration::ZERO).await.unwrap();

    assert_eq!(backend.complete(&[1]).await.unwrap(), 1);
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert_eq!(
        backend.reclaim_expired(100).await.unwrap(),
        0,
        "a finished job must not come back when its lease lapses"
    );
}

#[tokio::test]
async fn a_retry_becomes_due_again_after_its_delay() {
    let mut backend = fresh("t:a_retry_becomes_due_again_after_its_delay").await;
    backend
        .enqueue(1, "t", Vec::new(), 0, 20, Duration::ZERO)
        .await
        .unwrap();
    backend.fetch(10, LEASE).await.unwrap();

    backend
        .retry(1, 0, Duration::from_millis(50))
        .await
        .unwrap();
    assert!(backend.fetch(10, LEASE).await.unwrap().is_empty());

    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(backend.fetch(10, LEASE).await.unwrap().len(), 1);
}

#[tokio::test]
async fn concurrent_consumers_never_get_the_same_job() {
    let mut seeder = fresh("t:concurrent_consumers_never_get_the_same_job").await;
    for id in 0..2_000 {
        seeder
            .enqueue(id, "t", Vec::new(), 0, 20, Duration::ZERO)
            .await
            .unwrap();
    }

    let seen = Arc::new(Mutex::new(HashSet::new()));
    let counted = Arc::new(Mutex::new(0usize));
    let mut handles = Vec::new();
    for _ in 0..8 {
        let seen = Arc::clone(&seen);
        let counted = Arc::clone(&counted);
        handles.push(tokio::spawn(async move {
            let mut backend =
                Backend::connect_namespaced(URL, "t:concurrent_consumers_never_get_the_same_job")
                    .await
                    .unwrap();
            loop {
                let jobs = backend.fetch(50, LEASE).await.unwrap();
                if jobs.is_empty() {
                    break;
                }
                *counted.lock().await += jobs.len();
                let mut seen = seen.lock().await;
                for job in jobs {
                    seen.insert(job.id);
                }
            }
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }

    let delivered = *counted.lock().await;
    assert_eq!(delivered, 2_000);
    assert_eq!(
        delivered,
        seen.lock().await.len(),
        "the Lua claim runs to completion without interleaving, so no two \
         consumers can take the same job"
    );
}

#[tokio::test]
async fn the_backend_refuses_what_it_cannot_do() {
    let backend = fresh("t:the_backend_refuses_what_it_cannot_do").await;
    let capabilities = backend.capabilities();

    for feature in [
        Feature::TransactionalEnqueue,
        Feature::Workflows,
        Feature::DurableSteps,
        Feature::KeyedConcurrency,
        Feature::UniqueJobs,
        Feature::Results,
        Feature::Cron,
    ] {
        let refused = capabilities
            .require(&[feature])
            .expect_err("redis cannot support this and must say so");
        assert_eq!(refused.backend, "redis");
        assert_eq!(refused.feature, feature);
    }

    capabilities
        .require(&[Feature::Priorities, Feature::DelayedJobs])
        .expect("these it can do");
}

#[tokio::test]
async fn durability_reflects_the_running_server() {
    let backend = fresh("t:durability_reflects_the_running_server").await;

    assert!(
        !backend.capabilities().durable_acknowledgement,
        "this test server does not fsync every write, so the backend must \
         report that jobs it accepts can be lost"
    );
}

#[tokio::test]
async fn renewing_only_touches_leases_still_held() {
    let mut backend = fresh("renew").await;
    let held = backend
        .enqueue(1, "t", Vec::new(), 0, 20, Duration::ZERO)
        .await
        .unwrap();
    backend
        .enqueue(2, "t", Vec::new(), 0, 20, Duration::ZERO)
        .await
        .unwrap();
    backend.fetch(10, Duration::from_millis(50)).await.unwrap();
    backend.complete(&[2]).await.unwrap();

    let renewed = backend
        .renew(&[held, 2], Duration::from_secs(60))
        .await
        .unwrap();

    assert_eq!(
        renewed, 1,
        "a job another worker has already taken back must not have its lease \
         extended by the worker that lost it"
    );
}

#[tokio::test]
async fn abandoning_returns_work_without_waiting_for_expiry() {
    let mut backend = fresh("abandon").await;
    backend
        .enqueue(1, "t", Vec::new(), 0, 20, Duration::ZERO)
        .await
        .unwrap();
    let leased = backend.fetch(10, Duration::from_secs(600)).await.unwrap();
    assert_eq!(leased.len(), 1);
    assert_eq!(backend.depth().await.unwrap(), 0);

    backend.abandon(&[1]).await.unwrap();

    assert_eq!(backend.depth().await.unwrap(), 1);
    assert_eq!(
        backend
            .fetch(10, Duration::from_secs(60))
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn cron_is_not_claimed_without_an_implementation() {
    let backend = fresh("caps").await;

    assert!(
        !backend.capabilities().supports(Feature::Cron),
        "declaring a feature this backend has no code for is worse than \
         declaring none: the capability model exists to refuse, not to promise"
    );
}
