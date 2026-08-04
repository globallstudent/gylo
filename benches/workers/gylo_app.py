from shared import adone

import gylo

app = gylo.Gylo()


@app.task(name="bench.work")
async def work(n: int) -> None:
    await adone()
