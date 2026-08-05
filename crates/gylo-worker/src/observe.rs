//! What the worker reports about itself.
//!
//! Recording goes through the `metrics` facade, so a process that installs no
//! recorder pays nothing and a library user is not forced into an exporter.
//! The binary decides how these are exposed.
//!
//! Names are gathered here rather than written at each call site because a
//! typo in a metric name is invisible until someone builds a dashboard on the
//! one that never appears.

use metrics::{counter, gauge, histogram};

pub const LEASED: &str = "gylo_jobs_leased_total";
pub const COMPLETED: &str = "gylo_jobs_completed_total";
pub const RETRIED: &str = "gylo_jobs_retried_total";
pub const DISCARDED: &str = "gylo_jobs_discarded_total";
pub const RECLAIMED: &str = "gylo_leases_reclaimed_total";
pub const EXHAUSTED: &str = "gylo_leases_exhausted_total";
pub const RESTARTS: &str = "gylo_child_restarts_total";
pub const CHILDREN: &str = "gylo_children_running";
pub const READY: &str = "gylo_queue_ready";
pub const SCHEDULED: &str = "gylo_queue_scheduled";
pub const BLOCKED: &str = "gylo_queue_blocked";
pub const RUNNING: &str = "gylo_queue_running";
pub const FLUSH_SECONDS: &str = "gylo_completion_flush_seconds";
pub const PRUNED: &str = "gylo_jobs_pruned_total";

pub fn leased(count: usize) {
    counter!(LEASED).increment(count as u64);
}

pub fn settled(completed: usize, retried: usize, discarded: usize) {
    counter!(COMPLETED).increment(completed as u64);
    counter!(RETRIED).increment(retried as u64);
    counter!(DISCARDED).increment(discarded as u64);
}

pub fn flushed(seconds: f64) {
    histogram!(FLUSH_SECONDS).record(seconds);
}

pub fn reclaimed(released: i64, exhausted: i64) {
    counter!(RECLAIMED).increment(released.max(0) as u64);
    counter!(EXHAUSTED).increment(exhausted.max(0) as u64);
}

pub fn child_restarted() {
    counter!(RESTARTS).increment(1);
}

pub fn children(count: usize) {
    gauge!(CHILDREN).set(count as f64);
}

pub fn depth(queue: &str, depth: gylo_pg::Depth) {
    let owned = queue.to_owned();
    gauge!(READY, "queue" => owned.clone()).set(depth.ready as f64);
    gauge!(SCHEDULED, "queue" => owned.clone()).set(depth.scheduled as f64);
    gauge!(BLOCKED, "queue" => owned.clone()).set(depth.blocked as f64);
    gauge!(RUNNING, "queue" => owned).set(depth.running as f64);
}

pub fn pruned(count: u64) {
    counter!(PRUNED).increment(count);
}
