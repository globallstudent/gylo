import msgspec
import pytest

import gylo

app = gylo.Gylo()


@app.task(name="notify")
async def notify(who: str) -> None:
    pass


@pytest.fixture
def bound(pool):
    app.bind(pool)
    yield app
    app.bind(None)


async def test_submit_enqueues_without_a_connection(bound, conn):
    job_id = await notify.submit("ops")

    row = await conn.fetchrow("SELECT * FROM gylo_job WHERE id = $1", job_id)
    assert row["task"] == "notify"
    args, _ = msgspec.msgpack.decode(row["payload"])
    assert args == ["ops"]


async def test_submit_carries_options(bound, conn):
    job_id = await notify.options(queue="alerts", priority=2).submit("ops")

    row = await conn.fetchrow("SELECT * FROM gylo_job WHERE id = $1", job_id)
    assert row["queue"] == "alerts"
    assert row["priority"] == 2


async def test_submit_does_not_join_your_transaction(bound, conn):
    """The trade `submit` makes, pinned so it cannot change silently."""

    class Boom(Exception):
        pass

    with pytest.raises(Boom):
        async with conn.transaction():
            await notify.submit("ops")
            raise Boom

    surviving = await conn.fetchval("SELECT count(*) FROM gylo_job")
    assert surviving == 1, (
        "submit commits on its own connection, so a rollback of yours does "
        "not take the job with it; enqueue(conn, ...) is the one that does"
    )


async def test_submit_without_a_pool_says_what_to_do():
    lonely = gylo.Gylo()

    @lonely.task(name="orphan")
    async def orphan() -> None:
        pass

    with pytest.raises(gylo.UnboundAppError, match="enqueue"):
        await orphan.submit()
