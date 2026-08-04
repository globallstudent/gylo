"""Enqueue cost, measured against the libraries gylo means to replace.

What this measures is one axis: how long a producer waits to submit a job.
It is not an end-to-end throughput comparison, and it is not free of
confounds — gylo talks to Postgres because that is what gylo is, while the
others talk to Redis because that is what they are. That difference is real
and is part of the choice, but it means a slower number here is not on its
own evidence of a slower library.

Two figures are reported for gylo. The transactional one is the feature: the
job commits with your business write. The pipelined one is what to compare
against a fire-and-forget `delay()`, which is what the others are doing.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Callable

import asyncpg

DSN = os.environ.get("DATABASE_URL", "postgres://gylo:gylo@127.0.0.1:5442/gylo_dev")
REDIS = os.environ.get("GYLO_BENCH_REDIS", "redis://127.0.0.1:6389/0")
ROUNDS = int(os.environ.get("ROUNDS", "2000"))

ARGS = ("customer-42",)
KWARGS = {"amount": 1999, "currency": "GBP"}


def report(label: str, note: str, seconds: float, rounds: int = ROUNDS) -> None:
    per = seconds / rounds * 1e6
    print(f"{label:<26} {rounds / seconds:>10,.0f}/s  {per:>9.1f}µs   {note}")


async def bench_gylo_transactional() -> None:
    import gylo

    app = gylo.Gylo()

    @app.task(name="bench.charge")
    async def charge(customer: str, *, amount: int, currency: str) -> None:
        pass

    conn = await asyncpg.connect(DSN)
    await conn.execute("TRUNCATE gylo_job CASCADE")
    for _ in range(50):
        await charge.enqueue(conn, *ARGS, **KWARGS)

    start = time.perf_counter()
    for _ in range(ROUNDS):
        async with conn.transaction():
            await charge.enqueue(conn, *ARGS, **KWARGS)
    report(
        "gylo (in a transaction)",
        "commits with your write",
        time.perf_counter() - start,
    )

    await conn.execute("TRUNCATE gylo_job CASCADE")
    start = time.perf_counter()
    await charge.enqueue_many(conn, [(ARGS, KWARGS)] * ROUNDS)
    report("gylo (pipelined)", "fire and forget", time.perf_counter() - start)

    await conn.execute("TRUNCATE gylo_job CASCADE")
    await conn.close()


def bench_celery() -> None:
    from celery import Celery

    app = Celery("bench", broker=REDIS, backend=None)
    app.conf.task_ignore_result = True

    @app.task(name="bench.charge")
    def charge(customer: str, amount: int = 0, currency: str = "") -> None:
        pass

    for _ in range(50):
        charge.apply_async(args=ARGS, kwargs=KWARGS)

    start = time.perf_counter()
    for _ in range(ROUNDS):
        charge.apply_async(args=ARGS, kwargs=KWARGS)
    report("celery", "redis broker", time.perf_counter() - start)


def bench_dramatiq() -> None:
    import dramatiq
    from dramatiq.brokers.redis import RedisBroker

    broker = RedisBroker(url=REDIS)
    dramatiq.set_broker(broker)

    @dramatiq.actor(queue_name="bench")
    def charge(customer: str, amount: int = 0, currency: str = "") -> None:
        pass

    for _ in range(50):
        charge.send(*ARGS, **KWARGS)

    start = time.perf_counter()
    for _ in range(ROUNDS):
        charge.send(*ARGS, **KWARGS)
    report("dramatiq", "redis broker", time.perf_counter() - start)


async def bench_arq() -> None:
    from arq import create_pool
    from arq.connections import RedisSettings

    settings = RedisSettings.from_dsn(REDIS)
    pool = await create_pool(settings)
    for _ in range(50):
        await pool.enqueue_job("charge", *ARGS, **KWARGS)

    start = time.perf_counter()
    for _ in range(ROUNDS):
        await pool.enqueue_job("charge", *ARGS, **KWARGS)
    report("arq", "redis broker", time.perf_counter() - start)
    await pool.aclose()


async def bench_taskiq() -> None:
    from taskiq_redis import ListQueueBroker

    broker = ListQueueBroker(url=REDIS)
    await broker.startup()

    @broker.task(task_name="bench.charge")
    async def charge(customer: str, amount: int = 0, currency: str = "") -> None:
        pass

    for _ in range(50):
        await charge.kiq(*ARGS, **KWARGS)

    start = time.perf_counter()
    for _ in range(ROUNDS):
        await charge.kiq(*ARGS, **KWARGS)
    report("taskiq", "redis broker", time.perf_counter() - start)
    await broker.shutdown()


def guarded(name: str, run: Callable[[], None]) -> None:
    try:
        run()
    except Exception as error:
        print(
            f"{name:<26} {'skipped':>10}              {type(error).__name__}: {error}"
        )


def main() -> None:
    print(f"{ROUNDS} enqueues each, one producer, warm\n")
    print(f"{'library':<26} {'rate':>10}  {'per call':>9}   note")
    print("-" * 78)

    guarded("gylo", lambda: asyncio.run(bench_gylo_transactional()))
    guarded("celery", bench_celery)
    guarded("dramatiq", bench_dramatiq)
    guarded("arq", lambda: asyncio.run(bench_arq()))
    guarded("taskiq", lambda: asyncio.run(bench_taskiq()))


if __name__ == "__main__":
    main()
