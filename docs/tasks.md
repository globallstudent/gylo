# Tasks and options

```python
--8<-- "examples/tasks_options.py"
```

## Defining tasks

`@app.task` registers a function under `module.qualname`, or under `name=`
when you want a stable identifier that survives refactors — recommended for
anything long-lived, since the name is what lives in the database.

Arguments are encoded with MessagePack. Anything `msgspec` can encode travels:
the usual scalars, lists, dicts, `datetime`. Anything it cannot raises at
enqueue, while you are still on the stack — as does a payload too large for a
dispatch frame to ever carry (16MB), because failing later would dead-letter a
job nobody can fix.

Tasks may be `async def` or plain `def`. Synchronous bodies run on a thread so
they never stall the event loop the other jobs in that child share.

## Options ride the task, not the call

Enqueue options live on `.options(...)` rather than on `enqueue` itself, so
they can never collide with your task's own parameters — a task is free to
take an argument named `queue` or `priority`.

| Option | Meaning |
|---|---|
| `queue` | Which queue the job lands on (`"default"`) |
| `priority` | Lower runs first; compared across every queue a worker consumes |
| `delay` | Seconds before the job becomes runnable |
| `max_attempts` | Total attempts before dead-lettering (20) |
| `unique` | `True` to dedup on arguments, a string to dedup on your own key |
| `concurrency_key` + `max_concurrency` | See [Keyed concurrency](keyed-concurrency.md) |

A `BoundTask` from `.options()` is reusable — build it once, enqueue many
times.

## Uniqueness

`unique=True` digests the task name, queue, and arguments; `unique="customer-7"`
uses your key instead. Either way, while a matching job is waiting or running,
a second enqueue inserts nothing and returns the id of the job already there.
Once the first finishes, the key frees. Deduplication is a database constraint,
not a racy check — two concurrent enqueues cannot both win.

## Batching

`enqueue_many` inserts a list in one round trip: tuples for positional
arguments, `gylo.call(...)` when keywords are needed. It deliberately returns
no ids — reporting them per row would cost the pipelining that makes it worth
using.

## Task self-context

`context=True` passes a `JobContext` first argument carrying `job_id`,
`attempt`, `max_attempts`, and `final` — true on the last attempt, which is
the moment to alert a human rather than fail into the void. Durable tasks'
step context carries the same fields.

## `submit` — when you have no connection

`app.bind(pool)` once at startup, then `await task.submit(...)` borrows a
connection per call. It is the weaker promise, on purpose: the job commits on
its own, so it survives a transaction of yours that later rolls back. When
that distinction matters, you want `enqueue`.
