# Coming from arq

arq and gylo share a temperament — async-first, small API, no ceremony. The
move is mostly mechanical: tasks stop being strings, Redis becomes Postgres,
and a few of arq's per-call knobs become policy.

## The structural difference

arq enqueues by name into Redis: `await redis.enqueue_job("send_report", 42)`
— a typo in the string is discovered at run time, and the enqueue happens
outside any transaction. gylo enqueues **through the task object, on a
database connection you pass**:

```python
import gylo

app = gylo.Gylo()


@app.task
async def send_report(user_id: int) -> None: ...


async def nightly(conn) -> None:
    await send_report.enqueue(conn, 42)
```

The task is an object with the function's real signature, so a wrong argument
fails the type check, and the insert joins whatever transaction the
connection holds. Workers keep their own registry the same way arq's
`WorkerSettings.functions` does — a name no worker recognises dead-letters
rather than retrying forever.

## Concept map

| arq | gylo |
|---|---|
| `WorkerSettings.functions = […]` | tasks register by decoration on the app |
| `await redis.enqueue_job("name", x)` | `await task.enqueue(conn, x)` — object, not string |
| `_defer_by=60` / `_defer_until=dt` | `options(delay=60)` — seconds from now only |
| `_queue_name` | `options(queue=…)` |
| `_job_id` for deduplication | `options(unique=True)` on arguments, or `unique="your-key"` |
| `ctx` as mandatory first parameter | `context=True` opts in, `gylo.JobContext` |
| `ctx["job_try"]` | `ctx.attempt`, with `ctx.final` for the last one |
| `raise Retry(defer=…)` | just raise — policy retries with exponential backoff |
| `max_tries` | `options(max_attempts=…)` |
| `job_timeout` | `timeout` — on by default at 300s |
| `keep_result=3600` | `store_result=True`; kept for the retention window |
| `await job.result(timeout=…)` | `await gylo.outcome(conn, job_id)` — poll, no blocking wait |
| `abort()` | `gylo.cancel(conn, *ids)` — not-yet-started jobs only |
| `cron(func, hour=9, minute=0)` in settings | `@app.cron("0 9 * * *")` on the task itself |
| Redis | Postgres |

## What gylo will not do the same way

**The task cannot choose its own retry delay.** arq's `Retry(defer=10)` lets
a failure pick when to come back — useful for honouring a `Retry-After`
header. In gylo backoff is policy: exponential from `--retry-base` to
`--retry-cap` with jitter, computed in the database. A task that must wait a
specific interval re-enqueues itself with `options(delay=…)` and raises
`gylo.NoRetryError` — explicit, but a real difference.

**No `on_startup` / `on_shutdown` hooks.** arq gives the worker a lifespan
for building HTTP sessions and closing them. gylo's children import your
module and go; per-process setup happens at import time or lazily in tasks.
A worker lifespan hook is a known gap.

**No absolute scheduling at enqueue.** `_defer_until` with a datetime has no
equivalent — `delay` is seconds from now, and recurring absolute times are
cron's job.

**Results expire on the retention clock, not per job.** arq's `keep_result`
is per-call; gylo's window is the completed-job retention setting (24h
default) for everything. Results that must outlive it belong in your own
tables, written by the task — ideally as a [durable step](durable-steps.md).

**More machinery is available when you want it.** Workflows (`chain`,
`group`, `chord`), durable steps, keyed concurrency and priorities have no
arq counterpart — nothing to migrate, but worth knowing they exist before
building them by hand. Start at [Workflows](workflows.md).
