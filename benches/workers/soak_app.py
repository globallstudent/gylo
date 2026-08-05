"""Mixed workload for the soak harness.

Every path through the worker at once — success, failure, retry, timeout,
durable steps, workflow links, cron — because slow leaks live in the
interactions, not in any one path exercised alone.

Each attempt appends one ledger line at entry, so the harness counts what
actually ran rather than what the database says survived retention.
"""

import asyncio
import os
from pathlib import Path

import gylo

app = gylo.Gylo()

LEDGER = Path(os.environ["GYLO_SOAK_LEDGER"])


def record(category: str, marker: int) -> None:
    with LEDGER.open("a") as ledger:
        ledger.write(f"{category} {marker}\n")
        ledger.flush()
        os.fsync(ledger.fileno())


@app.task(name="ok")
async def ok(marker: int) -> None:
    record("ok", marker)


@app.task(name="slow")
async def slow(marker: int) -> None:
    record("slow", marker)
    await asyncio.sleep(0.1)


@app.task(name="retry")
async def retry(marker: int) -> None:
    record("retry", marker)
    raise RuntimeError("always fails; enqueued with max_attempts=2")


@app.task(name="times_out", timeout=0.2)
async def times_out(marker: int) -> None:
    record("timeout", marker)
    await asyncio.sleep(3)


@app.task(name="steppy", durable=True)
async def steppy(ctx, marker: int) -> None:
    record("steppy", marker)

    for n in range(3):

        async def step(n: int = n) -> int:
            return n

        await ctx.step(f"s{n}", step)


@app.task(name="link")
async def link(marker: int) -> None:
    record("link", marker)


@app.cron("* * * * * *", name="pulse")
async def pulse() -> None:
    record("cron", 0)
