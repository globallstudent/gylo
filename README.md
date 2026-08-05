# gylo

A distributed task queue for Python with a Rust core. Postgres-first,
async-native, one runtime dependency.

```bash
pip install --pre gylo
```

```python
import gylo

app = gylo.Gylo()


@app.task
async def send_receipt(order_id: int) -> None:
    ...


async with pool.acquire() as conn, conn.transaction():
    order = await create_order(conn, ...)
    await send_receipt.enqueue(conn, order_id=order.id)
```

```bash
gylo migrate
gylo worker --app myapp:app --queue default
```

Full documentation: **[globallstudent.github.io/gylo](https://globallstudent.github.io/gylo/)**

## Why

**The job commits with your data or not at all.** `enqueue` takes the
connection you are already holding, so the insert joins your transaction. If
the order rolls back, so does the receipt. Every queue that talks to a separate
broker has a window where one of the two survives and the other does not.

**Nothing degrades quietly.** A backend declares what it supports, and asking
for something it cannot do stops the worker at startup rather than silently
doing less. "Redis supports priorities" is technically true and practically
false; gylo will not let that become your problem at 3am.

**One process per core, by default.** Task code holds the interpreter lock, so
a single child uses a single core no matter how high concurrency goes. gylo
runs one child per core and they coordinate through nothing but the queue.

## What it does

Retries with exponential backoff and jitter · scheduled and delayed jobs ·
cron without leader election · priorities · unique jobs · keyed concurrency for
multi-tenant fairness · results · cancellation · DAG workflows · durable steps,
so a retry replays completed steps instead of repeating their side effects ·
per-task timeouts · graceful shutdown on SIGTERM · Prometheus metrics and a
health endpoint.

## Numbers

Measured on one 8-core machine with Postgres and Redis in a VM, against each
library's own worker at its own defaults, 20,000 jobs. Reproduce with
`benches/end_to_end.py`.

| | Drain | Enqueue |
|---|---|---|
| dramatiq | 99,272/s | 104–119µs |
| **gylo** | **74,250/s** | **27–37µs** |
| taskiq | 6,747/s | 191–202µs |
| celery | 2,021/s | 243–260µs |
| arq | 200/s (defaults) | 351–383µs |

Two honest caveats. dramatiq wins the drain because a list pop from a Redis
that acknowledges before it persists is a cheaper operation than a durable
state transition — the gap is in the storage layer, not the worker. And arq's
default is a rate cap (100 jobs per 0.5s poll); opened up it reaches 1,784/s.

Under `kill -9`, gylo loses nothing: leases are reclaimed and the work runs
again. That result, not the throughput, is the one worth caring about.

## Status

Alpha, and released as a pre-release on purpose: `pip install gylo` will not
pick this up without `--pre`, so nothing reaches a production deployment by
accident. The engine is tested — 116 Rust and 61 Python tests, plus a chaos
harness that kills workers and restarts Postgres — and the semantics are
settled, but it has not run in production anywhere yet.

The Redis backend exists and is not yet reachable from the worker; use
Postgres.

## Requirements

Python 3.11+ and PostgreSQL 14+, on Linux or macOS.

Windows is not supported. The supervisor talks to its Python children over a
Unix domain socket, and giving it a named-pipe transport is a piece of work
rather than a build flag.

## Licence

Apache-2.0.
