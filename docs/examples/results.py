import gylo

app = gylo.Gylo()


@app.task(store_result=True)
async def analyse(document_id: int) -> dict:
    return {"score": 0.97}


async def check_on(conn, job_id: int) -> None:
    got = await gylo.outcome(conn, job_id)
    if got is None:
        ...  # no such job, or already pruned by retention
    elif got.succeeded:
        print(got.result)
    elif got.finished:
        print(got.state, got.errors[-1] if got.errors else None)


async def call_off(conn, job_ids: list[int]) -> None:
    cancelled = await gylo.cancel(conn, *job_ids)
    print(f"{cancelled} had not started and were cancelled")
