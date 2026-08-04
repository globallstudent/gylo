from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

WORKERS = Path(__file__).parent / "workers"
ROOT = Path(__file__).resolve().parents[1]
REDIS = os.environ.get("GYLO_BENCH_REDIS", "redis://127.0.0.1:6389/9")
DSN = os.environ.get("DATABASE_URL", "postgres://gylo:gylo@127.0.0.1:5442/gylo_dev")
JOBS = int(os.environ.get("JOBS", "2000"))
KILL_AT = float(os.environ.get("KILL_AT", "0.25"))
RECOVERY = float(os.environ.get("RECOVERY", "120"))
TIMEOUT = float(os.environ.get("TIMEOUT", "90"))

os.environ["GYLO_BENCH_MODE"] = "conformance"
os.environ["GYLO_BENCH_REDIS"] = REDIS
sys.path.insert(0, str(WORKERS))
sys.path.insert(0, str(ROOT / "python"))
import shared  # noqa: E402
from end_to_end import (  # noqa: E402
    LIBRARIES,
    seed_arq,
    seed_celery,
    seed_dramatiq,
    seed_gylo,
    seed_taskiq,
)


def environment() -> dict[str, str]:
    env = dict(os.environ)
    env["GYLO_BENCH_REDIS"] = REDIS
    env["GYLO_BENCH_MODE"] = "conformance"
    env["DATABASE_URL"] = DSN
    env["GYLO_PYTHON"] = str(ROOT / ".venv" / "bin" / "python3")
    env["PYTHONPATH"] = f"{WORKERS}:{ROOT / 'python'}"
    return env


def purge() -> None:

    import redis

    redis.Redis.from_url(REDIS).flushdb()


def spawn(command: list[str]) -> subprocess.Popen[bytes]:
    # its own process group, so the kill reaches the pool a prefork worker
    # spawned rather than only the parent that would have reaped them
    return subprocess.Popen(
        command,
        cwd=WORKERS,
        env=environment(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def annihilate(process: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(os.getpgid(process.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        process.kill()
    process.wait(timeout=30)


def wait_for(target: int, deadline: float) -> bool:
    while time.perf_counter() < deadline:
        if shared.completed() >= target:
            return True
        time.sleep(0.02)
    return False


def scenario(name: str, seed, command: list[str]) -> None:
    purge()
    shared.reset()
    seed()

    worker = spawn(command)
    if not wait_for(int(JOBS * KILL_AT), time.perf_counter() + TIMEOUT):
        annihilate(worker)
        print(f"{name:<12} {'never got going':>50}")
        return

    at_kill = shared.completed()
    annihilate(worker)

    recovered = spawn(command)
    started = time.perf_counter()
    whole = wait_for(JOBS, started + RECOVERY)
    took = time.perf_counter() - started
    annihilate(recovered)

    counted = shared.runs()
    missing = JOBS - len(counted)
    duplicated = sum(count - 1 for count in counted.values() if count > 1)
    verdict = (
        f"missing {missing / JOBS:.1%}"
        if missing
        else ("at-least-once" if duplicated else "exactly-once")
    )
    recovery = f"{took:>6.1f}s" if whole else f">{RECOVERY:.0f}s"
    print(
        f"{name:<12} {at_kill:>8} {missing:>7} {duplicated:>6} {recovery:>8}  {verdict}"
    )


SEEDS = {
    "gylo": seed_gylo,
    "celery": seed_celery,
    "dramatiq": seed_dramatiq,
    "arq": seed_arq,
    "taskiq": seed_taskiq,
}


def main() -> None:
    print(
        f"{JOBS} jobs, SIGKILL to the worker at {KILL_AT:.0%} drained, then restarted"
    )
    print(f"up to {RECOVERY:.0f}s allowed to make good afterwards\n")
    columns = f"{'library':<12} {'at kill':>8} {'missing':>7} {'dup':>6}"
    print(f"{columns} {'recovery':>8}  verdict")
    print("-" * 64)

    for name, _, command in LIBRARIES:
        if name not in SEEDS:
            continue
        try:
            scenario(name, SEEDS[name], command())
        except Exception as error:
            print(f"{name:<12} {'skipped':>8}   {type(error).__name__}: {error}")


if __name__ == "__main__":
    main()
