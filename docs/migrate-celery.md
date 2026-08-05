# Coming from Celery

Most Celery concepts have a direct counterpart. The differences that remain
are deliberate, and they are listed at the bottom rather than discovered in
production.

## The structural difference

Celery enqueues into a broker through an ambient app object; the send is
invisible to your database transaction. gylo enqueues **into your database,
on a connection you pass**, so the job commits with your data or not at all:

```python
import gylo

app = gylo.Gylo()


@app.task(retry_on=(TimeoutError,))
async def send_receipt(order_id: int) -> None: ...


async def place_order(conn) -> None:
    async with conn.transaction():
        order = await create_order(conn)
        await send_receipt.options(delay=60, queue="emails").enqueue(
            conn, order.id
        )
```

If the order rolls back, so does the receipt — the dual-write window every
broker-backed queue lives with does not exist. Where no connection is at
hand, `await send_receipt.submit(order_id)` borrows one from the app's pool,
with the weaker promise that entails.

The other split worth internalising: **failure policy lives on the task**
(`retry_on`, `no_retry_on`, `timeout`, `store_result`), **placement lives at
enqueue** (`queue`, `priority`, `delay`, `max_attempts`, `unique`,
concurrency caps — all via `.options()`). There is no routing table in
config; the call site says where the job goes.

## Concept map

| Celery | gylo |
|---|---|
| `Celery(broker=…, backend=…)` | `gylo.Gylo()` — Postgres is broker and backend |
| `@app.task` | `@app.task` |
| `task.delay(x)` | `await task.enqueue(conn, x)` |
| `apply_async(countdown=60)` | `task.options(delay=60).enqueue(conn, …)` |
| `apply_async(queue=…, priority=…)` | `task.options(queue=…, priority=…)` — lower number runs first |
| `bind=True`, `self` | `context=True`, a `gylo.JobContext` first parameter |
| `autoretry_for=(…)` | `retry_on=(…)` — already the default for `Exception` |
| `retry_backoff`, `retry_jitter` | on by default; shaped worker-wide by `--retry-base` / `--retry-cap` |
| `max_retries=3` | `options(max_attempts=4)` — attempts, not retries |
| `self.retry()` | just raise — the policy decides |
| `Reject` / `Ignore` | `raise gylo.NoRetryError(…)` |
| `acks_late=True` | always true — a job is acknowledged by finishing |
| `time_limit` / `soft_time_limit` | `timeout` — on by default at 300s |
| `chain`, `group`, `chord` | `gylo.chain`, `gylo.group`, `gylo.chord` |
| `task.s(x)` / `task.si(x)` | `task.signature(x)` — every signature is immutable |
| beat + `beat_schedule` | `@app.cron("*/5 * * * *")` — no beat process, no leader |
| `AsyncResult.get()` | `await gylo.outcome(conn, job_id)` — poll, never block |
| `revoke()` | `gylo.cancel(conn, *ids)` — not-yet-started jobs only |
| celery-once and friends | `options(unique=True)` or `unique="your-key"`, built in |
| Flower | `gylo queue`, `gylo jobs`, Prometheus metrics |

## What gylo will not do the same way

**Chains do not pass results.** A Celery chain feeds each task's return value
to the next; every gylo signature behaves like `si()` — a child receives its
own arguments, never its parents' output. Share state through your own tables
keyed by something the tasks agree on. This is scope, not a gap: implicit
result piping is how a queue becomes a badly specified dataflow engine.
[Workflows](workflows.md) explains the model.

**Running jobs cannot be revoked.** `gylo.cancel` cancels what has not
started and reports how many that was. Celery's `revoke(terminate=True)`
kills a worker process mid-task, taking every sibling job with it; gylo
declines to offer the footgun and tells you exactly what it did instead.
Cooperative cancellation and timeouts cover the rest —
[Results and cancellation](results.md).

**There is no `rate_limit`.** Keyed concurrency bounds how many jobs run *at
once* per key — the multi-tenant fairness problem — but nothing bounds jobs
per second yet.

**No absolute ETA.** `delay` is seconds from now. For "run at 9am", compute
the difference, or give the task a cron schedule.

**Attempts are counted, not retries.** `ctx.attempt` starts at 1 and
`max_attempts` is the total budget. Celery's `max_retries=3` means four runs;
say `max_attempts=4`.

**Results are opt-in and expire.** `store_result=True`, and the retrieval
window is the completed-job retention window (24h by default). There is no
blocking `.get()`; a caller that needs the answer synchronously usually
wanted a function call.

**One backend.** Postgres. No RabbitMQ, no Redis, no SQS. If the job must
survive, it goes where your data already lives.
