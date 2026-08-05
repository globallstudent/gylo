import os

import asyncpg
import pytest

DSN = os.environ.get(
    "GYLO_TEST_DATABASE_URL",
    "postgres://gylo:gylo@127.0.0.1:5442/gylo_test",
)


@pytest.fixture
async def conn():
    connection = await asyncpg.connect(DSN)
    await connection.execute("TRUNCATE gylo_job CASCADE")
    try:
        yield connection
    finally:
        await connection.close()


@pytest.fixture
async def pool():
    created = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
    async with created.acquire() as conn:
        await conn.execute("TRUNCATE gylo_job CASCADE")
    try:
        yield created
    finally:
        await created.close()


PG_DSN = DSN.replace("postgres://", "postgresql://")


@pytest.fixture
def sync_conn():
    psycopg = pytest.importorskip("psycopg")
    with psycopg.connect(PG_DSN, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE gylo_job CASCADE")
        yield connection
