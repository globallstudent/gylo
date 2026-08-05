import gylo

app = gylo.Gylo()


@app.task
async def export_report(tenant: str, report_id: int) -> None: ...


async def enqueue_exports(conn, tenant: str, report_ids: list[int]) -> None:
    bounded = export_report.options(
        concurrency_key=f"tenant:{tenant}",
        max_concurrency=2,
    )
    await bounded.enqueue_many(conn, [(tenant, r) for r in report_ids])
