"""The enqueue surface production code actually reaches for.

Most deployed Python is synchronous, and much of it holds a SQLAlchemy or
Django connection rather than a bare driver. Each path here goes to a real
database: what is being proven is that the insert lands, in the caller's own
transaction, whatever wrapper the connection arrived in.
"""

from __future__ import annotations

import os

import psycopg
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine

import gylo

DSN = os.environ.get(
    "GYLO_TEST_DATABASE_URL",
    "postgres://gylo:gylo@127.0.0.1:5442/gylo_test",
)
PG_DSN = DSN.replace("postgres://", "postgresql://")

app = gylo.Gylo()


@app.task(name="sync.work")
def work(n: int) -> None: ...


@pytest.fixture
def sync_conn():
    with psycopg.connect(PG_DSN, autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE gylo_job CASCADE")
        yield conn


def state_of(conn, job_id: int) -> str:
    with conn.cursor() as cursor:
        cursor.execute("SELECT state::text FROM gylo_job WHERE id = %s", (job_id,))
        return cursor.fetchone()[0]


def test_sync_enqueue_lands_the_job(sync_conn):
    job = work.enqueue_sync(sync_conn, 1)

    assert state_of(sync_conn, job) == "available"


def test_sync_enqueue_joins_the_callers_transaction(sync_conn):
    with psycopg.connect(PG_DSN) as tx_conn:
        job = work.enqueue_sync(tx_conn, 1)
        tx_conn.rollback()

    with sync_conn.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM gylo_job WHERE id = %s", (job,))
        assert cursor.fetchone()[0] == 0, (
            "the whole point of taking the caller's connection is that a "
            "rolled-back transaction takes the job with it"
        )


def test_sync_enqueue_many_and_outcome(sync_conn):
    work.enqueue_many_sync(sync_conn, [((n,), {}) for n in range(5)])

    with sync_conn.cursor() as cursor:
        cursor.execute("SELECT count(*), min(id) FROM gylo_job")
        count, first = cursor.fetchone()
    assert count == 5
    assert gylo.outcome_sync(sync_conn, first).state == "available"
    assert gylo.cancel_sync(sync_conn, first) == 1


def test_a_sync_connection_is_refused_by_the_async_api(sync_conn):
    with pytest.raises(gylo.WrongFlavourError, match="_sync variant"):
        work.enqueue(sync_conn, 1).send(None)


@pytest.mark.asyncio
async def test_an_async_connection_is_refused_by_the_sync_api(pool):
    async with pool.acquire() as conn:
        with pytest.raises(gylo.WrongFlavourError, match="await the plain"):
            work.enqueue_sync(conn, 1)


def test_sqlalchemy_sync_connection_is_unwrapped(sync_conn):
    engine = create_engine(f"postgresql+psycopg://{PG_DSN.split('://', 1)[1]}")
    with engine.begin() as sa_conn:
        job = work.enqueue_sync(sa_conn, 1)
        sa_conn.execute(text("SELECT 1"))
    engine.dispose()

    assert state_of(sync_conn, job) == "available"


def test_sqlalchemy_rollback_takes_the_job_with_it(sync_conn):
    engine = create_engine(f"postgresql+psycopg://{PG_DSN.split('://', 1)[1]}")
    with engine.connect() as sa_conn:
        job = work.enqueue_sync(sa_conn, 1)
        sa_conn.rollback()
    engine.dispose()

    with sync_conn.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM gylo_job WHERE id = %s", (job,))
        assert cursor.fetchone()[0] == 0


@pytest.mark.asyncio
async def test_sqlalchemy_async_connection_is_unwrapped(sync_conn):
    engine = create_async_engine(f"postgresql+asyncpg://{PG_DSN.split('://', 1)[1]}")
    async with engine.begin() as sa_conn:
        job = await work.enqueue(sa_conn, 1)
    await engine.dispose()

    assert state_of(sync_conn, job) == "available"


def test_an_oversized_payload_is_rejected_at_enqueue(sync_conn):
    with pytest.raises(ValueError, match="dispatch frame can carry"):
        work.enqueue_sync(sync_conn, "x" * (17 * 1024 * 1024))
