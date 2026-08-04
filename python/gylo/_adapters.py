"""Per-driver insert statements.

Placeholder syntax is driver-specific, so each adapter owns its own SQL rather
than a shared string being rewritten. The Rust core is not involved: it cannot
join a transaction owned by a Python driver, which is the whole reason enqueue
runs here.
"""

from __future__ import annotations

from typing import Any, Protocol

import msgspec

__all__ = ["UnsupportedDriverError", "adapter_for"]

_COLUMNS = "queue, task, payload, priority, max_attempts, scheduled_at"
_UNIQUE_PREDICATE = "unique_key IS NOT NULL AND state IN ('available', 'running')"


class UnsupportedDriverError(TypeError):
    """No adapter recognises the given connection object."""


class Adapter(Protocol):
    INSERT: str
    INSERT_UNIQUE: str
    INSERT_MANY_UNIQUE: str

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


class AsyncpgAdapter:
    INSERT = (
        f"INSERT INTO gylo_job ({_COLUMNS}) "
        "VALUES ($1, $2, $3, $4, $5, now() + make_interval(secs => $6)) "
        "RETURNING id"
    )
    INSERT_UNIQUE = (
        f"WITH new AS (INSERT INTO gylo_job ({_COLUMNS}, unique_key) "
        "VALUES ($1, $2, $3, $4, $5, now() + make_interval(secs => $6), $7) "
        f"ON CONFLICT (unique_key) WHERE {_UNIQUE_PREDICATE} DO NOTHING "
        "RETURNING id) "
        "SELECT id FROM new UNION ALL "
        "SELECT id FROM gylo_job "
        "WHERE unique_key = $7 AND state IN ('available', 'running') LIMIT 1"
    )
    INSERT_MANY_UNIQUE = (
        f"INSERT INTO gylo_job ({_COLUMNS}, unique_key) "
        "VALUES ($1, $2, $3, $4, $5, now() + make_interval(secs => $6), $7) "
        f"ON CONFLICT (unique_key) WHERE {_UNIQUE_PREDICATE} DO NOTHING"
    )

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
        row = await conn.fetchrow(_OUTCOME.format("$1"), job_id)
        if row is None:
            return None
        return row[0], row[1], msgspec.json.decode(row[2]) if row[2] else []

    @classmethod
    async def cancel(cls, conn: Any, job_ids: list[int]) -> int:
        tag = await conn.execute(_CANCEL.format("$1"), job_ids)
        return int(tag.rsplit(" ", 1)[-1])


class PsycopgAdapter:
    INSERT = (
        f"INSERT INTO gylo_job ({_COLUMNS}) "
        "VALUES (%s, %s, %s, %s, %s, now() + make_interval(secs => %s)) "
        "RETURNING id"
    )
    INSERT_UNIQUE = (
        f"WITH new AS (INSERT INTO gylo_job ({_COLUMNS}, unique_key) "
        "VALUES (%s, %s, %s, %s, %s, now() + make_interval(secs => %s), %s) "
        f"ON CONFLICT (unique_key) WHERE {_UNIQUE_PREDICATE} DO NOTHING "
        "RETURNING id) "
        "SELECT id FROM new UNION ALL "
        "SELECT id FROM gylo_job "
        "WHERE unique_key = %s AND state IN ('available', 'running') LIMIT 1"
    )
    INSERT_MANY_UNIQUE = (
        f"INSERT INTO gylo_job ({_COLUMNS}, unique_key) "
        "VALUES (%s, %s, %s, %s, %s, now() + make_interval(secs => %s), %s) "
        f"ON CONFLICT (unique_key) WHERE {_UNIQUE_PREDICATE} DO NOTHING"
    )

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
            await cursor.execute(_OUTCOME.format("%s"), (job_id,))
            row = await cursor.fetchone()
        if row is None:
            return None
        return row[0], row[1], row[2] or []

    @classmethod
    async def cancel(cls, conn: Any, job_ids: list[int]) -> int:
        async with conn.cursor() as cursor:
            await cursor.execute(_CANCEL.format("%s"), (job_ids,))
            return cursor.rowcount


_OUTCOME = "SELECT state::text, result, errors FROM gylo_job WHERE id = {}"
_CANCEL = (
    "UPDATE gylo_job SET state = 'cancelled', finalized_at = now(), "
    "locked_by = NULL, lease_expires_at = NULL "
    "WHERE id = ANY({}) AND state = 'available'"
)


_BY_MODULE: dict[str, type[Adapter]] = {
    "asyncpg": AsyncpgAdapter,
    "psycopg": PsycopgAdapter,
}


def adapter_for(conn: Any) -> type[Adapter]:
    """Pick an adapter from the connection's own module.

    Matching on the module rather than the exact class keeps pools, connection
    proxies, and driver subclasses working without an explicit registry entry
    for each.
    """
    for base in type(conn).__mro__:
        root = base.__module__.split(".", 1)[0]
        if root in _BY_MODULE:
            return _BY_MODULE[root]
    raise UnsupportedDriverError(
        f"no gylo adapter for {type(conn).__module__}.{type(conn).__qualname__}; "
        f"supported drivers are {', '.join(sorted(_BY_MODULE))}"
    )
