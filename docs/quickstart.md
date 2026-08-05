# Getting started

## Install

```bash
pip install --pre gylo
```

The wheel carries both the Python package and the `gylo` binary — worker,
migrations, and operations tooling in one install. `msgspec` is the only
runtime dependency; your database driver is yours to choose:

```bash
pip install --pre "gylo[asyncpg]"    # or [psycopg]
```

## A database and its schema

gylo needs PostgreSQL 14+. Apply the schema with the bundled binary:

```bash
export DATABASE_URL=postgres://user:pass@localhost/myapp
gylo migrate
```

Migrations are embedded in the binary, additive, and safe to re-run.

## Define a task

```python title="myapp.py"
--8<-- "examples/quickstart_app.py"
```

A task is a plain function with a decorator. It stays callable directly —
`await send_receipt(1, email="a@b.c")` runs it inline, which is how you unit
test it.

## Enqueue — inside your own transaction

```python
async with pool.acquire() as conn, conn.transaction():
    order_id = await create_order(conn, ...)
    await send_receipt.enqueue(conn, order_id, email="a@b.c")
```

The connection is explicit and the point: the job commits atomically with the
order. Roll back, and the job was never enqueued. From synchronous code — a
Django view, a Flask handler — use `send_receipt.enqueue_sync(conn, ...)`;
see [Synchronous code](sync.md).

Enqueue is fully typed: `send_receipt.enqueue(conn, "one", emial=...)` fails
your type checker with the task's real signature in the error.

## Run a worker

```bash
gylo worker --app myapp:app
```

That is the whole deployment: one process that spawns a Python child per core,
leases jobs in batches, and finalises them durably. `Ctrl-C` or SIGTERM drains
in-flight work before exiting.

## See it

```bash
gylo queue            # depth per queue: ready, scheduled, blocked, running
gylo jobs failed      # dead-lettered jobs with their last error
```

From here: [Tasks and options](tasks.md) for the full API, or
[Deployment](deployment.md) before anything faces traffic.
