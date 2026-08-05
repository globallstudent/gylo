import gylo

app = gylo.Gylo()


@app.task
async def remind(user_id: int) -> None: ...


async def defer(conn) -> None:
    await remind.options(delay=3600.0).enqueue(conn, 7)


@app.cron("0 3 * * *", name="nightly-report", timezone="Europe/London")
async def nightly_report() -> None: ...


@app.cron("*/5 * * * *", name="sync-inventory", queue="sync")
async def sync_inventory() -> None: ...
