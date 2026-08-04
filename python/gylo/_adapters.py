"""Per-driver insert statements.

Placeholder syntax is driver-specific, so each adapter owns its own SQL rather
than a shared string being rewritten. The Rust core is not involved: it cannot
join a transaction owned by a Python driver, which is the whole reason enqueue
runs here.
"""

from __future__ import annotations

from typing import Any, Protocol

__all__ = ["UnsupportedDriverError", "adapter_for"]

_COLUMNS = "queue, task, payload, priority, max_attempts, scheduled_at"


class UnsupportedDriverError(TypeError):
    """No adapter recognises the given connection object."""


class Adapter(Protocol):
    INSERT: str

    @classmethod
    async def insert(cls, conn: Any, params: tuple[Any, ...]) -> int: ...

    @classmethod
    async def insert_many(cls, conn: Any, rows: list[tuple[Any, ...]]) -> None: ...


class AsyncpgAdapter:
    INSERT = (
        f"INSERT INTO gylo_job ({_COLUMNS}) "
        "VALUES ($1, $2, $3, $4, $5, now() + make_interval(secs => $6)) "
        "RETURNING id"
    )

    @classmethod
    async def insert(cls, conn: Any, params: tuple[Any, ...]) -> int:
        return await conn.fetchval(cls.INSERT, *params)

    @classmethod
    async def insert_many(cls, conn: Any, rows: list[tuple[Any, ...]]) -> None:
        await conn.executemany(cls.INSERT, rows)


class PsycopgAdapter:
    INSERT = (
        f"INSERT INTO gylo_job ({_COLUMNS}) "
        "VALUES (%s, %s, %s, %s, %s, now() + make_interval(secs => %s)) "
        "RETURNING id"
    )

    @classmethod
    async def insert(cls, conn: Any, params: tuple[Any, ...]) -> int:
        async with conn.cursor() as cursor:
            await cursor.execute(cls.INSERT, params)
            row = await cursor.fetchone()
        return row[0]

    @classmethod
    async def insert_many(cls, conn: Any, rows: list[tuple[Any, ...]]) -> None:
        async with conn.cursor() as cursor:
            await cursor.executemany(cls.INSERT, rows)


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
