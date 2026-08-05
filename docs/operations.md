# Operations

## Seeing the queue

```console
$ gylo queue
queue                    ready  scheduled   blocked   running
default                    312          8         2          64
mail                         0          0         0           3
```

Four numbers instead of one, because "how many jobs" is four different
questions:

- **ready** — runnable now; the only number that is a backlog
- **scheduled** — deliberately later: delays, retry backoff
- **blocked** — workflow members waiting on parents
- **running** — leased right now

Alert on *ready* sustained above what your workers clear in an acceptable
delay. Alerting on the total pages you for retries backing off and workflows
waiting — both of which are the system working correctly.

## Dead letters

```console
$ gylo jobs failed --queue default
1450363    default    billing.charge    attempt 20   2026-08-06 09:14:02  ConnectionError: api.stripe.com timed out
$ gylo jobs retry 1450363          # attempts reset; ids or --queue for all
$ gylo jobs purge --queue default --yes
```

A job out of attempts keeps its complete error history — every attempt's
traceback, timestamped, on the row — for `--retain-discarded` (7 days). The
listing shows each job's last error; the full history is in the `errors`
column. `retry` resets the attempt counter, because a job dead-lettered *for*
exhausting attempts would otherwise dead-letter again untouched.

## Metrics

`--observe 127.0.0.1:9464` serves Prometheus at `/metrics` and a liveness
probe at `/healthz`.

| Metric | Reading it |
|---|---|
| `gylo_queue_ready` | the backlog; the first thing to alert on |
| `gylo_jobs_completed_total` / `retried_total` / `discarded_total` | rising `discarded` = a task is failing permanently; rising `retried` alone = something flaky but surviving |
| `gylo_leases_reclaimed_total` | jobs recovered from dead workers — occasional is life, steady means something keeps dying; pair with `gylo_child_restarts_total` |
| `gylo_completion_flush_seconds` | the durability write path (p50 ~5ms); climbing means Postgres is struggling before anything else says so |
| `gylo_jobs_pruned_total` | retention working; flat-at-zero with traffic means the table is growing |

## The worker's own logs

Structured `tracing` on stderr; `RUST_LOG=gylo=debug` for more. Task tracebacks
and prints appear under `gylo::child`, attributed per child. When a child dies,
the supervisor's error carries the child's final stderr line — an import
failure names the missing module in the message, not in a log you have to go
find.

## Postgres, for DBAs

One hot table (`gylo_job`) with partial indexes per access path; fetches are
short autocommit statements via `SKIP LOCKED` — worker count does not create
lock contention. Wakeups ride `LISTEN/NOTIFY` with polling as fallback, so
notification loss costs latency, never correctness. Connections per worker:
roughly `3 × processes + 4`, sized automatically unless `--pool-size` says
otherwise. PgBouncer in transaction mode is fine for *your* enqueue traffic,
but workers should connect directly — they hold session state (`LISTEN`).
