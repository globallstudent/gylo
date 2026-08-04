import os

import redis
import redis.asyncio

URL = os.environ.setdefault("GYLO_BENCH_REDIS", "redis://127.0.0.1:6389/9")
CONFORMANCE = os.environ.get("GYLO_BENCH_MODE") == "conformance"
COUNTER = "bench:done"
RUNS = "bench:runs"
FLUSH = 100

_sync = redis.Redis.from_url(URL)
_async: redis.asyncio.Redis | None = None
_pending = 0


def done(n: int) -> None:
    """For synchronous workers."""
    global _pending
    if CONFORMANCE:
        _sync.hincrby(RUNS, n, 1)
        return
    _pending += 1
    if _pending >= FLUSH:
        _sync.incrby(COUNTER, _pending)
        _pending = 0


async def adone(n: int) -> None:
    """For coroutine workers, so the loop is never blocked."""
    global _async, _pending
    if _async is None:
        _async = redis.asyncio.Redis.from_url(URL)
    if CONFORMANCE:
        await _async.hincrby(RUNS, n, 1)
        return
    _pending += 1
    if _pending >= FLUSH:
        flushing, _pending = _pending, 0
        await _async.incrby(COUNTER, flushing)


def reset() -> None:
    global _pending
    _pending = 0
    _sync.delete(COUNTER, RUNS)


def completed() -> int:
    if CONFORMANCE:
        return _sync.hlen(RUNS)
    return int(_sync.get(COUNTER) or 0)


def runs() -> dict[int, int]:
    """How many times each job ran, for the conformance harness."""
    return {int(k): int(v) for k, v in _sync.hgetall(RUNS).items()}
