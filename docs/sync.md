# Synchronous code

Most deployed Python is synchronous — Django views, Flask handlers, scripts.
gylo's *worker* is async because that is how it goes fast, but **enqueueing
never requires your application to be**. Every client operation has a `_sync`
counterpart: `enqueue_sync`, `enqueue_many_sync`, `outcome_sync`,
`cancel_sync`.

```python
--8<-- "examples/sync_usage.py"
```

The transactional guarantee is identical: the insert joins whatever
transaction the connection holds, and rolls back with it.

## Connections that work

Hand any of these to either flavour of the API — gylo picks the adapter from
the connection itself:

| You hold | It works because |
|---|---|
| `asyncpg` connection or pool | native async adapter |
| `psycopg` async connection | native async adapter |
| `psycopg` sync connection | native sync adapter |
| SQLAlchemy `Connection` / `AsyncConnection` | unwrapped to the driver connection underneath |
| Django's `connection.connection` | the psycopg connection under Django's wrapper |

Unwrapping matters more than it sounds: the driver connection *is* the
wrapper's connection, so an enqueue through SQLAlchemy lands inside the
SQLAlchemy transaction — `session.rollback()` takes the job with it. The
tests prove exactly that.

Handing a sync connection to the async API (or the reverse) raises
`WrongConnectionError` naming the variant you wanted, rather than whatever
confusing thing the driver would have said.

## What has no sync form

Workflow enqueue and `submit` are async-only today. For workflows from sync
code, `asyncio.run(flow.enqueue(conn))` with an async connection works; if
this is a real limitation for you, it is the kind of feedback worth filing.
