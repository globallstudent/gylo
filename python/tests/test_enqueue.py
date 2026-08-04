import msgspec
import pytest

from gylo import Gylo, UnsupportedDriverError

app = Gylo()


@app.task(name="send_receipt")
async def send_receipt(order_id: int, *, email: str) -> None:
    pass


async def row(conn, job_id):
    return await conn.fetchrow("SELECT * FROM gylo_job WHERE id = $1", job_id)


async def count(conn):
    return await conn.fetchval("SELECT count(*) FROM gylo_job")


async def test_enqueue_inserts_the_job(conn):
    job_id = await send_receipt.enqueue(conn, 42, email="a@b.c")

    record = await row(conn, job_id)
    assert record["task"] == "send_receipt"
    assert record["queue"] == "default"
    assert record["state"] == "available"
    assert record["priority"] == 0
    assert record["attempt"] == 0


async def test_arguments_round_trip_through_the_payload(conn):
    job_id = await send_receipt.enqueue(conn, 42, email="a@b.c")

    record = await row(conn, job_id)
    args, kwargs = msgspec.msgpack.decode(record["payload"])
    assert args == [42]
    assert kwargs == {"email": "a@b.c"}


async def test_enqueue_commits_with_the_caller_transaction(conn):
    async with conn.transaction():
        await conn.execute("SELECT 1")
        job_id = await send_receipt.enqueue(conn, 1, email="a@b.c")

    assert await row(conn, job_id) is not None


async def test_enqueue_is_rolled_back_with_the_caller_transaction(conn):
    class Boom(Exception):
        pass

    with pytest.raises(Boom):
        async with conn.transaction():
            await send_receipt.enqueue(conn, 1, email="a@b.c")
            raise Boom

    assert await count(conn) == 0, (
        "the job must not survive a transaction that rolled back"
    )


async def test_options_reach_the_row(conn):
    job_id = await send_receipt.options(
        queue="receipts", priority=3, max_attempts=5
    ).enqueue(conn, 1, email="a@b.c")

    record = await row(conn, job_id)
    assert record["queue"] == "receipts"
    assert record["priority"] == 3
    assert record["max_attempts"] == 5


async def test_delay_pushes_the_schedule_out(conn):
    job_id = await send_receipt.options(delay=60).enqueue(conn, 1, email="a@b.c")

    ahead = await conn.fetchval(
        "SELECT EXTRACT(EPOCH FROM (scheduled_at - now())) FROM gylo_job WHERE id = $1",
        job_id,
    )
    assert 55 < float(ahead) <= 60


async def test_enqueue_many_inserts_every_call(conn):
    await send_receipt.options(queue="bulk").enqueue_many(
        conn,
        [((i,), {"email": f"user{i}@example.com"}) for i in range(25)],
    )

    assert await count(conn) == 25
    payloads = await conn.fetch("SELECT payload FROM gylo_job ORDER BY id")
    first_args, first_kwargs = msgspec.msgpack.decode(payloads[0]["payload"])
    assert first_args == [0]
    assert first_kwargs == {"email": "user0@example.com"}


async def test_enqueue_many_with_nothing_to_do_is_a_no_op(conn):
    await send_receipt.enqueue_many(conn, [])

    assert await count(conn) == 0


async def test_an_unrecognised_connection_is_rejected():
    with pytest.raises(UnsupportedDriverError, match="no gylo adapter"):
        await send_receipt.enqueue(object(), 1, email="a@b.c")


async def test_options_do_not_collide_with_task_arguments(conn):
    collide = Gylo()

    @collide.task(name="collide")
    async def ship(*, queue: str, priority: int) -> None:
        pass

    job_id = await ship.options(queue="shipping").enqueue(
        conn, queue="ground", priority=9
    )

    record = await row(conn, job_id)
    assert record["queue"] == "shipping", "the option should route the job"
    assert record["priority"] == 0, "the task argument must not become the option"

    _, kwargs = msgspec.msgpack.decode(record["payload"])
    assert kwargs == {"queue": "ground", "priority": 9}


async def test_options_leave_the_task_defaults_alone(conn):
    await send_receipt.options(queue="once").enqueue(conn, 1, email="a@b.c")
    job_id = await send_receipt.enqueue(conn, 2, email="a@b.c")

    record = await row(conn, job_id)
    assert record["queue"] == "default"


@app.task(name="rebuild_index")
async def rebuild_index(tenant: str, *, full: bool = False) -> None:
    pass


async def test_a_unique_job_is_only_queued_once(conn):
    first = await rebuild_index.options(unique=True).enqueue(conn, "acme")
    second = await rebuild_index.options(unique=True).enqueue(conn, "acme")

    assert first == second, "the second enqueue should return the job already queued"
    assert await count(conn) == 1


async def test_uniqueness_is_per_argument_set(conn):
    await rebuild_index.options(unique=True).enqueue(conn, "acme")
    await rebuild_index.options(unique=True).enqueue(conn, "other")

    assert await count(conn) == 2


async def test_keyword_order_does_not_change_the_key(conn):
    first = await rebuild_index.options(unique=True).enqueue(conn, "acme", full=True)

    kwargs = {"full": True}
    second = await rebuild_index.options(unique=True).enqueue(conn, "acme", **kwargs)

    assert first == second
    assert await count(conn) == 1


async def test_an_explicit_key_deduplicates_regardless_of_arguments(conn):
    first = await rebuild_index.options(unique="nightly").enqueue(conn, "acme")
    second = await rebuild_index.options(unique="nightly").enqueue(conn, "different")

    assert first == second
    assert await count(conn) == 1


async def test_the_same_key_on_another_task_does_not_collide(conn):
    await rebuild_index.options(unique="shared").enqueue(conn, "acme")
    await send_receipt.options(unique="shared").enqueue(conn, 1, email="a@b.c")

    assert await count(conn) == 2


async def test_uniqueness_is_scoped_to_the_queue(conn):
    await rebuild_index.options(unique=True, queue="a").enqueue(conn, "acme")
    await rebuild_index.options(unique=True, queue="b").enqueue(conn, "acme")

    assert await count(conn) == 2


async def test_a_finished_job_frees_its_key(conn):
    first = await rebuild_index.options(unique=True).enqueue(conn, "acme")
    await conn.execute(
        "UPDATE gylo_job SET state = 'completed', finalized_at = now() WHERE id = $1",
        first,
    )

    second = await rebuild_index.options(unique=True).enqueue(conn, "acme")

    assert second != first, "once the first run finished, the job may be queued again"
    assert await count(conn) == 2


async def test_a_non_unique_job_is_never_deduplicated(conn):
    await rebuild_index.enqueue(conn, "acme")
    await rebuild_index.enqueue(conn, "acme")

    assert await count(conn) == 2


async def test_enqueue_many_deduplicates_within_the_batch(conn):
    await rebuild_index.options(unique=True).enqueue_many(
        conn, [(("acme",), {}), (("acme",), {}), (("other",), {})]
    )

    assert await count(conn) == 2
