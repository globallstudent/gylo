# Coming from TaskIQ

TaskIQ is a toolkit — you choose a broker, a result backend, a scheduler and
middlewares, and wire them together. gylo is the assembled machine: Postgres
is the broker and the result store, the scheduler runs inside the worker, and
the middleware jobs are built in.

## The structural difference

A TaskIQ deployment composes several moving parts:

```python
broker = ListQueueBroker("redis://…").with_result_backend(
    RedisAsyncResultBackend("redis://…")
)
scheduler = TaskiqScheduler(broker, [LabelScheduleSource(broker)])
```

gylo has one part, and enqueueing takes the database connection you already
hold, so the job commits with your data:

```python
import gylo

app = gylo.Gylo()


@app.task
async def resize(image_id: int) -> None: ...


async def upload(conn, image_id: int) -> None:
    async with conn.transaction():
        await mark_uploaded(conn, image_id)
        await resize.enqueue(conn, image_id)
```

No separate scheduler process, no result backend to pick, no broker/worker
version skew — and no enqueue that succeeds while the transaction that
motivated it rolls back.

## Concept map

| TaskIQ | gylo |
|---|---|
| broker + `result_backend` | `gylo.Gylo()` — Postgres is both |
| `@broker.task` | `@app.task` |
| `await my_task.kiq(x)` | `await task.enqueue(conn, x)` |
| `.kicker().with_labels(…)` | `.options(queue=…, priority=…, delay=…, …)` |
| `schedule=[{"cron": "*/5 * * * *"}]` label + scheduler process | `@app.cron("*/5 * * * *")`, inside the worker |
| `await handle.wait_result(timeout=…)` | poll `await gylo.outcome(conn, job_id)` |
| `NoResultError` / opt-out of results | results are opt-in: `store_result=True` |
| retry middleware (`SimpleRetryMiddleware`) | built in — exponential backoff with jitter, `retry_on` / `no_retry_on` |
| `TaskiqDepends` | nothing — see below |
| `InMemoryBroker` for tests | not yet; today tests run against a real database |
| taskiq-pipelines | `gylo.chain` / `group` / `chord` — no result piping |

## What gylo will not do the same way

**No dependency injection.** `TaskiqDepends` is TaskIQ's marquee feature —
FastAPI-style parameters resolved at execution. gylo tasks are plain
functions: they take arguments from the enqueue and reach shared resources
(pools, clients) as module state. If your task graph leans heavily on DI,
this is the largest rewrite item — and the part of TaskIQ gylo deliberately
does not copy, because implicit parameters are the part that resists typing
and testing. Framework integration helpers are a known gap.

**No pluggable brokers or backends.** TaskIQ's strength is choice — NATS,
RabbitMQ, Redis, Kafka. gylo's position is that a task queue's reliability
*is* its storage, so it ships Postgres and makes it excellent. If you need a
specific broker for organisational reasons, TaskIQ is genuinely the better
fit.

**No blocking result wait.** `wait_result` polls under the hood; gylo asks
you to poll `outcome` yourself so the cost is visible — and results expire
with the completed-job retention window (24h default). See
[Results and cancellation](results.md).

**No middleware API.** Custom middlewares have no seam to attach to. The
built-ins cover retries, timeouts, dead letters, metrics and graceful
shutdown; anything beyond that is a feature request, not a plugin today.

**Cron does not backfill.** A schedule that came due while the worker fleet
was down advances to its next occurrence — documented in
[Scheduling](scheduling.md), stated here because separate-scheduler setups
sometimes queue missed runs on restart.
