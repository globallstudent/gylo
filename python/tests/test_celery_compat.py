import datetime as dt

import msgspec
import pytest

from gylo.compat.celery import AsyncResult, Celery, chain, group

celery = Celery("myapp")


@celery.task
async def send_email(to: str) -> str:
    return f"sent:{to}"


@celery.task(name="explicit.name", max_retries=2)
async def with_options() -> None:
    pass


@pytest.fixture
def configured(pool):
    celery.configure(pool)
    return celery


async def test_delay_enqueues(configured, conn):
    result = await send_email.delay("a@b.c")

    assert isinstance(result, AsyncResult)
    row = await conn.fetchrow("SELECT * FROM gylo_job WHERE id = $1", result.id)
    assert row["task"] == "test_celery_compat.send_email"
    args, _ = msgspec.msgpack.decode(row["payload"])
    assert args == ["a@b.c"]


async def test_apply_async_maps_celery_options(configured, conn):
    result = await send_email.apply_async(
        args=["a@b.c"], queue="mail", priority=4, max_retries=2
    )

    row = await conn.fetchrow("SELECT * FROM gylo_job WHERE id = $1", result.id)
    assert row["queue"] == "mail"
    assert row["priority"] == 4
    assert row["max_attempts"] == 3, "celery counts retries, gylo counts attempts"


async def test_countdown_becomes_a_delay(configured, conn):
    result = await send_email.apply_async(args=["a@b.c"], countdown=90)

    ahead = await conn.fetchval(
        "SELECT EXTRACT(EPOCH FROM (scheduled_at - now())) FROM gylo_job WHERE id = $1",
        result.id,
    )
    assert 85 < float(ahead) <= 90


async def test_eta_becomes_a_delay(configured, conn):
    when = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=120)

    result = await send_email.apply_async(args=["a@b.c"], eta=when)

    ahead = await conn.fetchval(
        "SELECT EXTRACT(EPOCH FROM (scheduled_at - now())) FROM gylo_job WHERE id = $1",
        result.id,
    )
    assert 110 < float(ahead) <= 120


async def test_an_eta_in_the_past_runs_now(configured, conn):
    when = dt.datetime.now(dt.UTC) - dt.timedelta(hours=1)

    result = await send_email.apply_async(args=["a@b.c"], eta=when)

    ahead = await conn.fetchval(
        "SELECT EXTRACT(EPOCH FROM (scheduled_at - now())) FROM gylo_job WHERE id = $1",
        result.id,
    )
    assert float(ahead) <= 0


async def test_a_named_task_keeps_its_name(configured, conn):
    result = await with_options.delay()

    task = await conn.fetchval("SELECT task FROM gylo_job WHERE id = $1", result.id)
    assert task == "explicit.name"


async def test_status_maps_to_celery_vocabulary(configured, conn):
    result = await send_email.delay("a@b.c")
    assert await result.status() == "PENDING"

    await conn.execute(
        "UPDATE gylo_job SET state = 'completed', finalized_at = now() WHERE id = $1",
        result.id,
    )
    assert await result.status() == "SUCCESS"
    assert await result.ready()
    assert await result.successful()


async def test_a_cancelled_job_reads_as_revoked(configured, conn):
    result = await send_email.delay("a@b.c")
    await conn.execute(
        "UPDATE gylo_job SET state = 'cancelled', finalized_at = now() WHERE id = $1",
        result.id,
    )

    assert await result.status() == "REVOKED"
    assert not await result.successful()


async def test_signatures_compose_into_workflows(configured, conn):
    flow = chain(send_email.s("first@b.c"), send_email.s("second@b.c"))

    ids = await flow.enqueue(conn)

    assert len(ids) == 2
    pending = await conn.fetchval(
        "SELECT pending_deps FROM gylo_job WHERE id = $1", ids[1]
    )
    assert pending == 1


async def test_group_composes(configured, conn):
    ids = await group(send_email.s("a@b.c"), send_email.s("c@d.e")).enqueue(conn)

    assert len(ids) == 2


async def test_enqueueing_without_a_pool_is_refused():
    lonely = Celery()

    @lonely.task
    async def orphan() -> None:
        pass

    with pytest.raises(RuntimeError, match="configure"):
        await orphan.delay()


async def test_the_task_is_still_directly_callable(configured):
    assert await send_email("a@b.c") == "sent:a@b.c"
