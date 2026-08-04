"""Every library's task does the same thing: count itself.

A shared counter is the only completion signal five runtimes agree on, but a
round trip per job caps the whole benchmark at roughly twelve thousand a second
and two libraries already reach that. So each worker process counts locally and
flushes every hundred, which puts the harness two orders of magnitude clear of
anything it is measuring.

The flush granularity means the counter is only ever approximately current,
which is why the benchmark times a window in the middle of the drain and reads
the counter at both ends rather than assuming it stops exactly on the target.
"""

import os

import redis
import redis.asyncio

URL = os.environ.setdefault("GYLO_BENCH_REDIS", "redis://127.0.0.1:6389/0")
COUNTER = "bench:done"
FLUSH = 100

_sync = redis.Redis.from_url(URL)
_async: redis.asyncio.Redis | None = None
_pending = 0


def done() -> None:
    """For synchronous workers."""
    global _pending
    _pending += 1
    if _pending >= FLUSH:
        _sync.incrby(COUNTER, _pending)
        _pending = 0


async def adone() -> None:
    """For coroutine workers, so the loop is never blocked."""
    global _async, _pending
    _pending += 1
    if _pending >= FLUSH:
        flushing, _pending = _pending, 0
        if _async is None:
            _async = redis.asyncio.Redis.from_url(URL)
        await _async.incrby(COUNTER, flushing)


def reset() -> None:
    global _pending
    _pending = 0
    _sync.delete(COUNTER)


def completed() -> int:
    return int(_sync.get(COUNTER) or 0)
