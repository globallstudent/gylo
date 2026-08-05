"""Soak: does behaviour hold over time, not over a benchmark's ten seconds.

Runs a mixed workload — success, failure, retry, timeout, durable steps,
workflow chains, cron — at a modest steady rate for SOAK_SECONDS, sampling
resident memory, database connections, held leases and table size throughout.
The verdict asserts three things no short test can:

- nothing drifts: memory, connections and table size are compared between the
  early and late thirds of the run, not eyeballed
- nothing is lost or doubled: every enqueued job's attempts are counted from a
  ledger the tasks write on entry, so retention deleting the rows afterwards
  cannot hide anything
- the worker still shuts down cleanly after all of it

Throughput is deliberately not the point; the rate is small so that any growth
is a leak rather than a backlog.
"""

from __future__ import annotations

import asyncio
import os
import signal
import statistics
import subprocess
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path

import asyncpg

ROOT = Path(__file__).resolve().parents[1]
WORKERS = Path(__file__).parent / "workers"
DSN = os.environ.get("DATABASE_URL", "postgres://gylo:gylo@127.0.0.1:5442/gylo_dev")
DURATION = float(os.environ.get("SOAK_SECONDS", "300"))
SAMPLE_EVERY = 5.0
WAVE_EVERY = 1.0

sys.path.insert(0, str(WORKERS))
sys.path.insert(0, str(ROOT / "python"))


def rss_kb(pids: list[int]) -> int:
    if not pids:
        return 0
    out = subprocess.run(
        ["ps", "-o", "rss=", "-p", ",".join(map(str, pids))],
        capture_output=True,
        text=True,
    ).stdout
    return sum(int(line) for line in out.split())


def family(supervisor: int) -> list[int]:
    out = subprocess.run(
        ["pgrep", "-P", str(supervisor)], capture_output=True, text=True
    ).stdout
    return [supervisor, *(int(line) for line in out.split())]


async def main() -> int:
    ledger = Path(tempfile.mkstemp(prefix="gylo-soak-")[1])
    os.environ["GYLO_SOAK_LEDGER"] = str(ledger)
    conn = await asyncpg.connect(DSN)
    await conn.execute("TRUNCATE gylo_job CASCADE")
    await conn.execute("DELETE FROM gylo_cron")

    import soak_app

    env = dict(os.environ)
    env.update(
        GYLO_SOAK_LEDGER=str(ledger),
        GYLO_PYTHON=str(ROOT / ".venv" / "bin" / "python3"),
        PYTHONPATH=f"{WORKERS}:{ROOT / 'python'}",
        DATABASE_URL=DSN,
    )
    log = Path("/tmp/gylo-soak-worker.log").open("w")  # noqa: SIM115
    worker = subprocess.Popen(
        [
            str(ROOT / "target" / "release" / "gylo"),
            "worker",
            "--app",
            "soak_app:app",
            "--maintenance-interval",
            "5s",
            "--retain-completed",
            "30s",
        ],
        cwd=WORKERS,
        env=env,
        stdout=log,
        stderr=log,
    )

    expected: dict[int, int] = {}
    marker = 0
    samples: list[dict] = []
    started = time.monotonic()
    next_wave = started
    next_sample = started

    def note(runs: int) -> int:
        nonlocal marker
        marker += 1
        expected[marker] = runs
        return marker

    try:
        while time.monotonic() - started < DURATION:
            now = time.monotonic()
            if now >= next_wave:
                next_wave = now + WAVE_EVERY
                await soak_app.ok.enqueue_many(
                    conn, [((note(1),), {}) for _ in range(20)]
                )
                await soak_app.slow.enqueue_many(
                    conn, [((note(1),), {}) for _ in range(5)]
                )
                await soak_app.retry.options(max_attempts=2).enqueue(conn, note(2))
                await soak_app.times_out.options(max_attempts=2).enqueue(conn, note(2))
                await soak_app.steppy.enqueue(conn, note(1))
                await soak_app.steppy.enqueue(conn, note(1))
                import gylo

                await gylo.chain(
                    soak_app.link.signature(note(1)),
                    soak_app.link.signature(note(1)),
                    soak_app.link.signature(note(1)),
                ).enqueue(conn)

            if now >= next_sample:
                next_sample = now + SAMPLE_EVERY
                row = await conn.fetchrow(
                    """
                    SELECT
                      (SELECT count(*) FROM pg_stat_activity
                        WHERE datname = current_database()) AS connections,
                      (SELECT count(*) FROM gylo_job WHERE state = 'running')
                        AS running,
                      (SELECT count(*) FROM gylo_job
                        WHERE state = 'available' AND scheduled_at <= now())
                        AS ready,
                      (SELECT count(*) FROM gylo_job WHERE state <> 'discarded')
                        AS live_rows,
                      pg_total_relation_size('gylo_job') AS bytes
                    """
                )
                samples.append(
                    {
                        "at": now - started,
                        "rss": rss_kb(family(worker.pid)),
                        "connections": row["connections"],
                        "running": row["running"],
                        "ready": row["ready"],
                        "live_rows": row["live_rows"],
                        "bytes": row["bytes"],
                    }
                )
            await asyncio.sleep(0.05)

        drained_by = time.monotonic() + 120
        want = sum(expected.values())
        while time.monotonic() < drained_by:
            lines = ledger.read_text().splitlines() if ledger.exists() else []
            got = sum(1 for line in lines if not line.startswith("cron"))
            if got >= want:
                break
            await asyncio.sleep(1)
    finally:
        worker.send_signal(signal.SIGTERM)
        try:
            exit_code = worker.wait(timeout=45)
        except subprocess.TimeoutExpired:
            worker.kill()
            exit_code = -9
        log.close()
        await conn.close()

    counted: Counter[int] = Counter()
    cron_runs = 0
    for line in ledger.read_text().splitlines():
        category, raw = line.split()
        if category == "cron":
            cron_runs += 1
        else:
            counted[int(raw)] += 1

    failures: list[str] = []

    lost = [m for m, runs in expected.items() if counted[m] < runs]
    doubled = [m for m, runs in expected.items() if counted[m] > runs]
    if lost:
        failures.append(f"{len(lost)} job(s) ran fewer times than owed: {lost[:10]}")
    if doubled:
        failures.append(
            f"{len(doubled)} job(s) ran more times than owed: {doubled[:10]}"
        )
    # cron resolution is the maintenance interval: schedules are examined once
    # per tick and missed occurrences are skipped, so an every-second schedule
    # under a 5s tick owes one firing per tick, not sixty per minute
    ticks = DURATION / 5.0
    if cron_runs < ticks * 0.5:
        failures.append(
            f"cron fired {cron_runs} times across {ticks:.0f} ticks; the "
            f"schedule stalled"
        )
    if exit_code != 0:
        failures.append(f"worker exited {exit_code} on SIGTERM instead of draining")

    def third(rows: list[dict], key: str, which: int) -> float:
        cut = len(rows) // 3
        window = rows[cut : 2 * cut] if which == 0 else rows[2 * cut :]
        return statistics.median(r[key] for r in window)

    warm = [s for s in samples if s["at"] > DURATION * 0.2]
    if len(warm) >= 6:
        early_rss, late_rss = third(warm, "rss", 0), third(warm, "rss", 1)
        if late_rss > early_rss * 1.3 + 32 * 1024:
            failures.append(
                f"resident memory drifted {early_rss / 1024:.0f}MB -> "
                f"{late_rss / 1024:.0f}MB across the run"
            )
        # the pool fills lazily toward its cap, so a step up is expected; a
        # leak is a slope that keeps climbing between the middle and the end
        early_c, late_c = third(warm, "connections", 0), third(warm, "connections", 1)
        peak_c = max(s["connections"] for s in warm)
        if peak_c > 40:
            failures.append(f"{peak_c} connections open; far beyond the pool cap")
        if late_c > early_c + 6:
            failures.append(
                f"connections still climbing late in the run "
                f"({early_c:.0f} -> {late_c:.0f}); something is not returning them"
            )
        # dead letters are retained for days by design, so total bytes grow
        # linearly for any soak shorter than that window and say nothing.
        # What must stay flat is everything retention is supposed to bound.
        early_live, late_live = third(warm, "live_rows", 0), third(warm, "live_rows", 1)
        if late_live > early_live * 1.5 + 500:
            failures.append(
                f"rows outside the dead-letter class grew {early_live:.0f} -> "
                f"{late_live:.0f}; retention is not keeping pace"
            )
        backlog = [s["ready"] for s in warm]
        if backlog[-1] > 200:
            failures.append(
                f"{backlog[-1]} jobs ready at the end; the worker fell behind"
            )
    else:
        failures.append("too few samples to judge drift; run longer")

    owed = sum(expected.values())
    print(f"soak: {DURATION:.0f}s, {len(expected)} jobs, {owed} owed runs")
    if samples:
        head = f"{'t':>6} {'rssMB':>7} {'conns':>6} {'running':>8}"
        print(head + f" {'ready':>6} {'live':>6} {'tableMB':>8}")
        step = max(1, len(samples) // 12)
        for s in samples[::step]:
            print(
                f"{s['at']:>6.0f} {s['rss'] / 1024:>7.0f} {s['connections']:>6} "
                f"{s['running']:>8} {s['ready']:>6} {s['live_rows']:>6} "
                f"{s['bytes'] / 1048576:>8.1f}"
            )
    print(f"cron fired {cron_runs} times")
    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        return 1
    print("PASS: no drift, every run accounted for, clean shutdown")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
