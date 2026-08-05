# Coming from Dramatiq

Dramatiq and gylo agree on a lot: sane defaults, retries with backoff out of
the box, lower priority number runs first. The differences are where the jobs
live and what is built in rather than assembled from middleware.

## The structural difference

`actor.send()` posts to RabbitMQ or Redis, outside your database transaction.
gylo enqueues on a connection you pass, so the job commits atomically with
the state change that caused it:

```python
import gylo

app = gylo.Gylo()


@app.task
async def send_email(user_id: int) -> None: ...


async def register(conn, email: str) -> None:
    async with conn.transaction():
        user_id = await insert_user(conn, email)
        await send_email.enqueue(conn, user_id)
```

What Dramatiq composes from middleware — Retries, TimeLimit, Results,
ShutdownNotifications — is core behaviour here: backoff with jitter by
default, a 300s timeout on every task, `store_result=True` when you want the
return value, and a SIGTERM drain that finishes in-flight work. There is no
middleware stack to order correctly.

## Concept map

| Dramatiq | gylo |
|---|---|
| `@dramatiq.actor` | `@app.task` |
| `actor.send(x)` | `await task.enqueue(conn, x)` |
| `send_with_options(delay=60_000)` — milliseconds | `task.options(delay=60).enqueue(conn, …)` — seconds |
| `queue_name="emails"` | `options(queue="emails")` |
| `priority=0` runs before `priority=10` | same — lower number first |
| `max_retries=3` | `options(max_attempts=4)` — total attempts |
| `min_backoff` / `max_backoff` — per actor, milliseconds | `--retry-base` / `--retry-cap` — worker-wide, seconds, jitter always on |
| `throws=(ValueError,)` | `no_retry_on=(ValueError,)` |
| TimeLimit middleware | `timeout`, on by default |
| Results middleware + result backend | `store_result=True`, `await gylo.outcome(conn, job_id)` |
| `message.get_result(block=True)` | poll `outcome` — nothing blocks |
| `group` | `gylo.group` |
| `pipeline` | `gylo.chain` — but see below |
| periodiq / APScheduler | `@app.cron("0 9 * * *")`, runs inside the worker |
| RabbitMQ / Redis broker | Postgres |
| processes × threads | one Python process per core, coordinating through the queue |

## What gylo will not do the same way

**Chains do not pipe results.** A Dramatiq pipeline hands each actor's return
value to the next; a gylo chain only orders execution — every step receives
its own arguments. Pass work through your own tables. The reasoning is in
[Workflows](workflows.md).

**No middleware hooks.** Dramatiq's middleware API is a genuine extension
point; gylo does not expose one. What the middlewares are usually *for* —
retries, time limits, results, graceful shutdown, dead letters — is built in,
but if you have a custom middleware doing something else, there is no
equivalent seam today.

**No rate limiter.** Dramatiq ships backend-backed rate limiters; gylo's
keyed concurrency bounds simultaneous jobs per key, not jobs per second.

**Threads are not a knob.** Dramatiq runs processes × threads;
gylo runs one child process per core and dispatches the async event loop
inside each. Synchronous tasks run on a thread pool automatically — but
concurrency is configured as one number, not two.

**Results expire with the job row.** The retrieval window is the completed
retention window, 24 hours by default. Results that must outlive it belong in
your own tables — see [Results and cancellation](results.md).
