import asyncio

import gylo

app = gylo.Gylo()


@app.task(name="slow")
async def slow() -> None:
    await asyncio.sleep(3)


@app.task(name="ok")
async def ok() -> None:
    pass


@app.task(name="boom")
async def boom() -> None:
    raise ValueError("boom")


@app.task(name="expects")
async def expects(first: int, second: int, *, label: str) -> None:
    got = [first, second, label]
    if got != [1, 2, "hi"]:
        raise AssertionError(f"arguments did not survive the wire: {got!r}")


@app.task(name="sync_ok")
def sync_ok() -> None:
    pass


_attempts: dict[int, int] = {}


@app.task(name="flaky")
async def flaky(marker: int) -> None:
    """Fails once per marker, then succeeds.

    The counter lives in the child process, which survives retries, so the
    second attempt sees the first one's state.
    """
    _attempts[marker] = _attempts.get(marker, 0) + 1
    if _attempts[marker] < 2:
        raise RuntimeError("transient")


@app.task(name="fatal", no_retry_on=(ValueError,))
async def fatal() -> None:
    raise ValueError("permanent")


@app.task(name="refused")
async def refused() -> None:
    raise gylo.NoRetryError("give up immediately")
