"""Per-driver insert statements.

Placeholder syntax is driver-specific, so each adapter owns its own SQL rather
than a shared string being rewritten. The Rust core is not involved: it cannot
join a transaction owned by a Python driver, which is the whole reason enqueue
runs here.

The statements are generated rather than written out. The column list has grown
several times, and hand-numbering `$1` through `$10` twice per driver is a
defect waiting to happen.

Synchronous connections get their own adapter and their own entry points,
because most production Python is still synchronous and a queue that only
speaks `await` excludes it. SQLAlchemy and Django connections are unwrapped to
the driver connection underneath, which shares their transaction — the insert
still commits or rolls back with the caller's own work.
"""

from __future__ import annotations

import inspect
from typing import Any, Protocol

import msgspec

__all__ = ["UnsupportedDriverError", "WrongFlavourError", "resolve"]

_COLUMNS = (
    "queue",
    "task",
    "payload",
    "priority",
    "max_attempts",
    "scheduled_at",
    "concurrency_key",
    "max_concurrency",
    "durable",
)
_DELAY = _COLUMNS.index("scheduled_at")
_UNIQUE_PREDICATE = "unique_key IS NOT NULL AND state IN ('available', 'running')"
_LIVE = "state IN ('available', 'running')"


class UnsupportedDriverError(TypeError):
    """No adapter recognises the given connection object."""


class WrongFlavourError(TypeError):
    """A sync connection reached the async API, or the other way around."""


def _statements(marks: list[str]) -> tuple[str, str, str, str, str]:
    """Builds every statement a driver needs from its placeholder style."""
    columns = ", ".join(_COLUMNS)
    values = list(marks[: len(_COLUMNS)])
    values[_DELAY] = f"now() + make_interval(secs => {values[_DELAY]})"
    plain = ", ".join(values)
    key_mark = marks[len(_COLUMNS)]
    keyed = ", ".join([*values, key_mark])
    tail = marks[len(_COLUMNS) + 1] if len(marks) > len(_COLUMNS) + 1 else key_mark

    return (
        f"INSERT INTO gylo_job ({columns}) VALUES ({plain}) RETURNING id",
        f"WITH new AS (INSERT INTO gylo_job ({columns}, unique_key) VALUES ({keyed}) "
        f"ON CONFLICT (unique_key) WHERE {_UNIQUE_PREDICATE} DO NOTHING RETURNING id) "
        f"SELECT id FROM new UNION ALL "
        f"SELECT id FROM gylo_job WHERE unique_key = {tail} AND {_LIVE} LIMIT 1",
        f"INSERT INTO gylo_job ({columns}, unique_key) VALUES ({keyed}) "
        f"ON CONFLICT (unique_key) WHERE {_UNIQUE_PREDICATE} DO NOTHING",
        f"SELECT state::text, result, errors FROM gylo_job WHERE id = {marks[0]}",
        "UPDATE gylo_job SET state = 'cancelled', finalized_at = now(), "
        "locked_by = NULL, lease_expires_at = NULL "
        f"WHERE id = ANY({marks[0]}) AND state = 'available'",
    )


def _node_statement(numbered: bool) -> str:
    """The insert for one workflow node.

    A node with unmet dependencies is scheduled at infinity, which the ordinary
    fetch predicate already excludes — so a blocked job needs no state of its
    own and the hot query is untouched.
    """
    marks = (
        [f"${n}" for n in range(1, len(_COLUMNS) + 3)]
        if numbered
        else ["%s"] * (len(_COLUMNS) + 2)
    )
    workflow, rest, pending = marks[0], marks[1 : len(_COLUMNS) + 1], marks[-1]
    values = list(rest)
    values[_DELAY] = (
        f"CASE WHEN {pending} > 0 THEN 'infinity' "
        f"ELSE now() + make_interval(secs => {rest[_DELAY]}) END"
    )
    columns = ", ".join(_COLUMNS)
    return (
        f"INSERT INTO gylo_job (workflow_id, {columns}, pending_deps) "
        f"VALUES ({workflow}, {', '.join(values)}, {pending}) RETURNING id"
    )


_NEW_WORKFLOW = "INSERT INTO gylo_workflow DEFAULT VALUES RETURNING id"
_NEW_EDGE = "INSERT INTO gylo_edge (workflow_id, parent, child) VALUES ($1, $2, $3)"


class SyncPsycopgAdapter:
    IS_ASYNC = False
    INSERT, INSERT_UNIQUE, INSERT_MANY_UNIQUE, OUTCOME, CANCEL = _statements(
        ["%s"] * (len(_COLUMNS) + 2)
    )
    INSERT_NODE = _node_statement(numbered=False)
    NEW_EDGE = _NEW_EDGE.replace("$1", "%s").replace("$2", "%s").replace("$3", "%s")

    @classmethod
    def insert(cls, conn: Any, params: tuple[Any, ...]) -> int:
        with conn.cursor() as cursor:
            cursor.execute(cls.INSERT, params)
            return cursor.fetchone()[0]

    @classmethod
    def insert_unique(cls, conn: Any, params: tuple[Any, ...]) -> int:
        with conn.cursor() as cursor:
            cursor.execute(cls.INSERT_UNIQUE, (*params, params[-1]))
            return cursor.fetchone()[0]

    @classmethod
    def insert_many(
        cls, conn: Any, rows: list[tuple[Any, ...]], *, unique: bool
    ) -> None:
        with conn.cursor() as cursor:
            cursor.executemany(cls.INSERT_MANY_UNIQUE if unique else cls.INSERT, rows)

    @classmethod
    def outcome(cls, conn: Any, job_id: int) -> tuple[str, bytes | None, Any] | None:
        with conn.cursor() as cursor:
            cursor.execute(cls.OUTCOME, (job_id,))
            row = cursor.fetchone()
        if row is None:
            return None
        return row[0], row[1], row[2] or []

    @classmethod
    def cancel(cls, conn: Any, job_ids: list[int]) -> int:
        with conn.cursor() as cursor:
            cursor.execute(cls.CANCEL, (job_ids,))
            return cursor.rowcount

    @classmethod
    def insert_workflow(
        cls, conn: Any, nodes: list[tuple[Any, ...]], edges: list[tuple[int, int]]
    ) -> list[int]:
        with conn.transaction(), conn.cursor() as cursor:
            cursor.execute(_NEW_WORKFLOW)
            workflow = cursor.fetchone()[0]
            ids = []
            for node in nodes:
                cursor.execute(cls.INSERT_NODE, (workflow, *node))
                ids.append(cursor.fetchone()[0])
            if edges:
                cursor.executemany(
                    cls.NEW_EDGE, [(workflow, ids[p], ids[c]) for p, c in edges]
                )
        return ids


class Adapter(Protocol):
    IS_ASYNC: bool
    INSERT: str
    INSERT_UNIQUE: str
    INSERT_MANY_UNIQUE: str
    OUTCOME: str
    CANCEL: str

    @classmethod
    async def insert(cls, conn: Any, params: tuple[Any, ...]) -> int: ...

    @classmethod
    async def insert_unique(cls, conn: Any, params: tuple[Any, ...]) -> int: ...

    @classmethod
    async def insert_many(
        cls, conn: Any, rows: list[tuple[Any, ...]], *, unique: bool
    ) -> None: ...

    @classmethod
    async def outcome(
        cls, conn: Any, job_id: int
    ) -> tuple[str, bytes | None, Any] | None: ...

    @classmethod
    async def cancel(cls, conn: Any, job_ids: list[int]) -> int: ...

    @classmethod
    async def insert_workflow(
        cls, conn: Any, nodes: list[tuple[Any, ...]], edges: list[tuple[int, int]]
    ) -> list[int]: ...


class AsyncpgAdapter:
    IS_ASYNC = True
    INSERT, INSERT_UNIQUE, INSERT_MANY_UNIQUE, OUTCOME, CANCEL = _statements(
        [f"${n}" for n in range(1, len(_COLUMNS) + 2)]
    )
    INSERT_NODE = _node_statement(numbered=True)
    NEW_EDGE = _NEW_EDGE

    @classmethod
    async def insert(cls, conn: Any, params: tuple[Any, ...]) -> int:
        return await conn.fetchval(cls.INSERT, *params)

    @classmethod
    async def insert_unique(cls, conn: Any, params: tuple[Any, ...]) -> int:
        return await conn.fetchval(cls.INSERT_UNIQUE, *params)

    @classmethod
    async def insert_many(
        cls, conn: Any, rows: list[tuple[Any, ...]], *, unique: bool
    ) -> None:
        await conn.executemany(cls.INSERT_MANY_UNIQUE if unique else cls.INSERT, rows)

    @classmethod
    async def outcome(
        cls, conn: Any, job_id: int
    ) -> tuple[str, bytes | None, Any] | None:
        row = await conn.fetchrow(cls.OUTCOME, job_id)
        if row is None:
            return None
        return row[0], row[1], msgspec.json.decode(row[2]) if row[2] else []

    @classmethod
    async def cancel(cls, conn: Any, job_ids: list[int]) -> int:
        tag = await conn.execute(cls.CANCEL, job_ids)
        return int(tag.rsplit(" ", 1)[-1])

    @classmethod
    async def insert_workflow(
        cls, conn: Any, nodes: list[tuple[Any, ...]], edges: list[tuple[int, int]]
    ) -> list[int]:
        # a worker can lease a root the moment its row commits, and on an
        # autocommit connection that is before the edges exist — fan-in then
        # finds nothing to decrement and the graph parks forever. The driver's
        # transaction (a savepoint, when the caller already holds one) makes
        # the graph visible whole or not at all
        async with conn.transaction():
            workflow = await conn.fetchval(_NEW_WORKFLOW)
            ids = [
                await conn.fetchval(cls.INSERT_NODE, workflow, *node) for node in nodes
            ]
            if edges:
                await conn.executemany(
                    cls.NEW_EDGE, [(workflow, ids[p], ids[c]) for p, c in edges]
                )
        return ids


class PsycopgAdapter:
    IS_ASYNC = True
    INSERT, INSERT_UNIQUE, INSERT_MANY_UNIQUE, OUTCOME, CANCEL = _statements(
        ["%s"] * (len(_COLUMNS) + 2)
    )
    INSERT_NODE = _node_statement(numbered=False)
    NEW_EDGE = _NEW_EDGE.replace("$1", "%s").replace("$2", "%s").replace("$3", "%s")

    @classmethod
    async def insert(cls, conn: Any, params: tuple[Any, ...]) -> int:
        async with conn.cursor() as cursor:
            await cursor.execute(cls.INSERT, params)
            row = await cursor.fetchone()
        return row[0]

    @classmethod
    async def insert_unique(cls, conn: Any, params: tuple[Any, ...]) -> int:
        async with conn.cursor() as cursor:
            await cursor.execute(cls.INSERT_UNIQUE, (*params, params[-1]))
            row = await cursor.fetchone()
        return row[0]

    @classmethod
    async def insert_many(
        cls, conn: Any, rows: list[tuple[Any, ...]], *, unique: bool
    ) -> None:
        async with conn.cursor() as cursor:
            await cursor.executemany(
                cls.INSERT_MANY_UNIQUE if unique else cls.INSERT, rows
            )

    @classmethod
    async def outcome(
        cls, conn: Any, job_id: int
    ) -> tuple[str, bytes | None, Any] | None:
        async with conn.cursor() as cursor:
            await cursor.execute(cls.OUTCOME, (job_id,))
            row = await cursor.fetchone()
        if row is None:
            return None
        return row[0], row[1], row[2] or []

    @classmethod
    async def cancel(cls, conn: Any, job_ids: list[int]) -> int:
        async with conn.cursor() as cursor:
            await cursor.execute(cls.CANCEL, (job_ids,))
            return cursor.rowcount

    @classmethod
    async def insert_workflow(
        cls, conn: Any, nodes: list[tuple[Any, ...]], edges: list[tuple[int, int]]
    ) -> list[int]:
        async with conn.transaction(), conn.cursor() as cursor:
            await cursor.execute(_NEW_WORKFLOW)
            workflow = (await cursor.fetchone())[0]
            ids = []
            for node in nodes:
                await cursor.execute(cls.INSERT_NODE, (workflow, *node))
                ids.append((await cursor.fetchone())[0])
            if edges:
                await cursor.executemany(
                    cls.NEW_EDGE, [(workflow, ids[p], ids[c]) for p, c in edges]
                )
        return ids


def _psycopg_flavour(conn: Any) -> type[Adapter]:
    # one module ships both flavours; what tells them apart is whether
    # execute is a coroutine function
    if inspect.iscoroutinefunction(type(conn).execute):
        return PsycopgAdapter
    return SyncPsycopgAdapter


def _unwrap(conn: Any) -> Any:
    """The driver connection under an ORM's wrapper.

    Writing on it lands inside the wrapper's own transaction, because it is
    the same wire connection — the insert still commits or rolls back with
    the caller's other work.
    """
    root = type(conn).__module__.split(".", 1)[0]
    if root == "sqlalchemy":
        sync = getattr(conn, "sync_connection", None) or conn
        fairy = getattr(sync, "connection", None)
        driver = getattr(fairy, "driver_connection", None)
        if driver is not None:
            return driver
    if root == "django":
        driver = getattr(conn, "connection", None)
        if driver is not None:
            return driver
    return conn


_BY_MODULE: dict[str, Any] = {
    "asyncpg": lambda _conn: AsyncpgAdapter,
    "psycopg": _psycopg_flavour,
}


def resolve(conn: Any) -> tuple[type[Adapter], Any]:
    """The adapter for a connection, and the connection to actually use.

    Matching on the module rather than the exact class keeps pools, connection
    proxies, and driver subclasses working without an explicit registry entry
    for each. A SQLAlchemy or Django wrapper is unwrapped to the driver
    connection underneath, and that unwrapped connection is what the adapter
    must be handed — the wrapper does not speak the driver's own methods.
    """
    conn = _unwrap(conn)
    for base in type(conn).__mro__:
        root = base.__module__.split(".", 1)[0]
        if root in _BY_MODULE:
            return _BY_MODULE[root](conn), conn
    raise UnsupportedDriverError(
        f"no gylo adapter for {type(conn).__module__}.{type(conn).__qualname__}; "
        f"supported drivers are {', '.join(sorted(_BY_MODULE))}"
    )
