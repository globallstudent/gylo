from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import time
from pathlib import Path

WORKERS = Path(__file__).parent / "workers"
ROOT = Path(__file__).resolve().parents[1]
REDIS = os.environ.get("GYLO_BENCH_REDIS", "redis://127.0.0.1:6389/9")
DSN = os.environ.get("DATABASE_URL", "postgres://gylo:gylo@127.0.0.1:5442/gylo_dev")
JOBS = int(os.environ.get("JOBS", "20000"))
REPEATS = int(os.environ.get("REPEATS", "3"))
TIMEOUT = float(os.environ.get("TIMEOUT", "120"))
OPENS, CLOSES = int(JOBS * 0.2), int(JOBS * 0.8)

sys.path.insert(0, str(WORKERS))
sys.path.insert(0, str(ROOT / "python"))
import shared  # noqa: E402


def progress(line: str) -> None:
    """A slow library can take minutes per round, and a benchmark that prints
    nothing until it finishes is indistinguishable from one that has hung.
    Skipped when redirected, where a carriage return is just a character."""
    if sys.stdout.isatty():
        print(line.ljust(46), end="\r", flush=True)


def environment() -> dict[str, str]:
    env = dict(os.environ)
    env["GYLO_BENCH_REDIS"] = REDIS
    env["DATABASE_URL"] = DSN
    env["GYLO_PYTHON"] = str(ROOT / ".venv" / "bin" / "python3")
    env["PYTHONPATH"] = f"{WORKERS}:{ROOT / 'python'}"
    return env


def drain(command: list[str]) -> tuple[float, float] | None:
    process = subprocess.Popen(
        command,
        cwd=WORKERS,
        env=environment(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    launched = time.perf_counter()
    deadline = launched + TIMEOUT
    try:
        while shared.completed() < 1 and time.perf_counter() < deadline:
            time.sleep(0.002)
        startup = time.perf_counter() - launched

        while (opened := shared.completed()) < OPENS and time.perf_counter() < deadline:
            time.sleep(0.001)
        start = time.perf_counter()
        while (
            closed := shared.completed()
        ) < CLOSES and time.perf_counter() < deadline:
            time.sleep(0.001)
        elapsed = time.perf_counter() - start
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()

    if closed < CLOSES or elapsed <= 0:
        return None
    # counter values at both ends, not the marks: a buffered counter overshoots
    # a mark by however much was in flight, and the overshoot is not uniform
    return (closed - opened) / elapsed, startup


def seed_gylo() -> None:
    import asyncpg
    import gylo_app

    async def run() -> None:
        conn = await asyncpg.connect(DSN)
        await conn.execute("TRUNCATE gylo_job CASCADE")
        await gylo_app.work.options(queue="bench").enqueue_many(
            conn, [((n,), {}) for n in range(JOBS)]
        )
        await conn.close()

    asyncio.run(run())


def seed_celery() -> None:
    import celery_app

    for n in range(JOBS):
        celery_app.work.apply_async(args=(n,))


def seed_dramatiq() -> None:
    import dramatiq_app

    for n in range(JOBS):
        dramatiq_app.work.send(n)


def seed_arq() -> None:
    from arq import create_pool
    from arq.connections import RedisSettings

    async def run() -> None:
        pool = await create_pool(RedisSettings.from_dsn(REDIS))
        for n in range(JOBS):
            await pool.enqueue_job("work", n)
        await pool.aclose()

    asyncio.run(run())


def seed_taskiq() -> None:
    import taskiq_app

    async def run() -> None:
        await taskiq_app.broker.startup()
        for n in range(JOBS):
            await taskiq_app.work.kiq(n)
        await taskiq_app.broker.shutdown()

    asyncio.run(run())


LIBRARIES = [
    (
        "gylo",
        seed_gylo,
        lambda: [
            str(ROOT / "target" / "release" / "gylo"),
            "worker",
            "--app",
            "gylo_app:app",
            "--queue",
            "bench",
        ],
    ),
    (
        "celery",
        seed_celery,
        lambda: [
            str(ROOT / ".venv" / "bin" / "celery"),
            "-A",
            "celery_app",
            "worker",
            "--loglevel",
            "critical",
        ],
    ),
    (
        "dramatiq",
        seed_dramatiq,
        lambda: [
            str(ROOT / ".venv" / "bin" / "dramatiq"),
            "dramatiq_app",
            "--queues",
            "bench",
        ],
    ),
    (
        "arq",
        seed_arq,
        lambda: [str(ROOT / ".venv" / "bin" / "arq"), "arq_app.WorkerSettings"],
    ),
    # arq's default is a rate cap rather than a resource limit — at most
    # queue_read_limit jobs every poll_delay, which is 100 every 0.5s however
    # idle the worker is. Reporting only that would describe the setting
    # instead of the library, so it is measured opened up as well.
    (
        "arq (tuned)",
        seed_arq,
        lambda: [str(ROOT / ".venv" / "bin" / "arq"), "arq_tuned_app.WorkerSettings"],
    ),
    (
        "taskiq",
        seed_taskiq,
        lambda: [
            str(ROOT / ".venv" / "bin" / "taskiq"),
            "worker",
            "taskiq_app:broker",
        ],
    ),
]


def main() -> None:
    print(f"{JOBS} jobs, best of {REPEATS}, every library at its own defaults")
    print(f"rate measured between job {OPENS:,} and job {CLOSES:,}\n")
    print(f"{'library':<10} {'rate':>12}  {'backlog':>9}  {'startup':>9}")
    print("-" * 46)

    for name, seed, command in LIBRARIES:
        try:
            results = []
            for round_ in range(REPEATS):
                progress(f"{name:<10} run {round_ + 1}/{REPEATS}")
                shared.reset()
                seed()
                result = drain(command())
                if result is not None:
                    results.append(result)
            if not results:
                progress("")
                print(f"{name:<10} {'incomplete':>12}")
                continue
            rate, startup = max(results, key=lambda pair: pair[0])
            progress("")
            print(
                f"{name:<10} {rate:>10,.0f}/s  {JOBS / rate:>8.2f}s  "
                f"{startup * 1000:>7.0f}ms"
            )
        except Exception as error:
            print(f"{name:<10} {'skipped':>12}   {type(error).__name__}: {error}")


if __name__ == "__main__":
    main()
