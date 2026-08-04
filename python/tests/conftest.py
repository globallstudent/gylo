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
