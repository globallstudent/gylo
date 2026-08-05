# gylo

A distributed task queue for Python with a Rust core. Postgres-first,
async-native, one runtime dependency.

```bash
pip install --pre gylo
```

!!! warning "These docs track `main`"
    The published `0.1.0a1` pre-release predates much of what is documented
    here — synchronous enqueue, task context, typed enqueue, retention. Until
    the next pre-release ships, build from source to follow along exactly.

## The pitch, honestly

**The job commits with your data or not at all.** `enqueue` takes the database
connection you are already holding, so the insert joins your transaction. If
your order insert rolls back, the receipt job vanishes with it. Every queue
that talks to a separate broker has a window where one of the two survives
alone — gylo removes the window by construction, not by retry.

**Nothing degrades quietly.** A backend declares its capabilities, and asking
for a feature it cannot back stops the worker at startup with the feature and
backend named. There is no configuration in which gylo silently does less than
you asked.

**Crash-safety is measured, not promised.** Under `kill -9` of workers, their
children, or Postgres itself, gylo loses nothing: leases are reclaimed and the
work runs again. A four-hour soak of 448,736 mixed jobs — retries, timeouts,
workflows, cron — accounted for every single run with flat memory and a clean
shutdown at the end.

**Fast where it counts.** Enqueue is the fastest in the field (27–37µs
pipelined; the transactional form beats most competitors' *non*-transactional
sends). Drain throughput runs at whatever Postgres itself allows — measured
within noise of the database's own ceiling, meaning the worker adds
approximately nothing.

## What it does

Retries with exponential backoff and jitter · per-task timeouts by default ·
scheduled and delayed jobs · cron without leader election · priorities ·
unique jobs · keyed concurrency for multi-tenant fairness · DAG workflows ·
durable steps that replay instead of repeating side effects · results ·
cancellation · task self-context (attempt numbers) · sync **and** async
enqueue · SQLAlchemy and Django connection support · Prometheus metrics ·
graceful SIGTERM drain · dead-letter inspection and retry from the CLI ·
bounded tables via retention.

## What it requires

Python 3.11+ and PostgreSQL 14+, on Linux or macOS. One runtime dependency:
`msgspec`. Windows is not supported — the supervisor reaches its Python
children over a Unix domain socket.

## Where to start

- [Getting started](quickstart.md) — running in five minutes
- [Tasks and options](tasks.md) — the API you will actually use
- [Deployment](deployment.md) — processes, leases, shutdown, what to alert on
- Migrating from [Celery](migrate-celery.md), [Dramatiq](migrate-dramatiq.md),
  [arq](migrate-arq.md) or [TaskIQ](migrate-taskiq.md) — concept by concept,
  sharp edges included
