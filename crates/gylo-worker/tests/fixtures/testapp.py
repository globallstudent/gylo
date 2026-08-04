import asyncio
import os
from pathlib import Path

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


@app.cron("* * * * * *", name="every_second", queue="beat")
async def every_second() -> None:
    pass


@app.cron("0 3 * * *", name="nightly", timezone="Europe/London")
async def nightly() -> None:
    pass


@app.task(name="adds", store_result=True)
async def adds(left: int, right: int) -> dict[str, int]:
    return {"sum": left + right}


@app.task(name="unserialisable", store_result=True)
async def unserialisable() -> object:
    return object()


SIDE_EFFECTS = Path(os.environ.get("GYLO_TEST_EFFECTS", "/tmp/gylo-effects.log"))


@app.task(name="two_steps", durable=True)
async def two_steps(ctx, marker: str) -> None:
    """Charges once as a step, then fails on its first attempt only."""

    async def charge() -> str:
        with SIDE_EFFECTS.open("a") as log:
            log.write(f"{marker}:charge\n")
        return f"{marker}-charged"

    charged = await ctx.step("charge", charge)

    with SIDE_EFFECTS.open("a") as log:
        log.write(f"{marker}:attempt\n")
    if SIDE_EFFECTS.read_text().count(f"{marker}:attempt") < 2:
        raise RuntimeError("transient after the first step")

    async def finish() -> str:
        return charged

    await ctx.step("finish", finish)


@app.task(name="rendezvous")
async def rendezvous(wanted: int) -> None:
    """Completes only once `wanted` children are running this at the same time.

    A child limited to one job at a time cannot satisfy this alone, so the task
    finishing at all is the evidence that dispatch reached separate processes.
    """
    with SIDE_EFFECTS.open("a") as log:
        log.write(f"{os.getpid()}\n")

    for _ in range(200):
        arrived = set(SIDE_EFFECTS.read_text().split())
        if len(arrived) >= wanted:
            return
        await asyncio.sleep(0.05)
    raise RuntimeError(f"only {len(arrived)} of {wanted} children arrived")
