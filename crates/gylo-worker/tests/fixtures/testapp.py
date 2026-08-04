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
