"""Enqueue cost, measured against the libraries gylo means to replace.

What this measures is one axis: how long a producer waits to submit a job.
It is not an end-to-end throughput comparison, and it is not free of
confounds — gylo talks to Postgres because that is what gylo is, while the
others talk to Redis because that is what they are. That difference is real
and is part of the choice, but it means a slower number here is not on its
own evidence of a slower library.

Three figures are reported for gylo. The marginal one is the honest
comparison for the transactional path: a caller enqueueing inside a
transaction they already have, since that is the situation the feature exists
for, and the added latency is what a competitor's `delay()` is also adding.
Giving the enqueue a transaction of its own charges it for a BEGIN and COMMIT
the caller already pays, which flatters nobody and describes no real use.
"""

from __future__ import annotations

import asyncio
import os
import statistics
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
    await conn.execute("DROP TABLE IF EXISTS bench_order")
    await conn.execute(
        "CREATE TABLE bench_order (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,"
        " total int NOT NULL)"
    )

    async def business_only() -> None:
        async with conn.transaction():
            await conn.fetchval(
                "INSERT INTO bench_order (total) VALUES ($1) RETURNING id", 1999
            )

    async def business_and_job() -> None:
        async with conn.transaction():
            await conn.fetchval(
                "INSERT INTO bench_order (total) VALUES ($1) RETURNING id", 1999
            )
            await charge.enqueue(conn, *ARGS, **KWARGS)

    for _ in range(50):
        await business_only()
        await business_and_job()

    # medians of per-operation samples, not a difference of two totals: a
    # subtraction of totals compounds the noise in both and produced swings of
    # more than 2x between runs
    async def samples(run) -> list[float]:
        taken = []
        for _ in range(ROUNDS):
            start = time.perf_counter()
            await run()
            taken.append(time.perf_counter() - start)
        return taken

    baseline = statistics.median(await samples(business_only))
    together = statistics.median(await samples(business_and_job))
    marginal = max(together - baseline, 1e-9)
    print(
        f"{'gylo (transactional)':<26} {1 / marginal:>10,.0f}/s  "
        f"{marginal * 1e6:>9.1f}µs   added to a transaction you already had"
    )
    await conn.execute("DROP TABLE bench_order")

    await conn.execute("TRUNCATE gylo_job CASCADE")
    start = time.perf_counter()
    await charge.enqueue_many(conn, [gylo.call(*ARGS, **KWARGS)] * ROUNDS)
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
