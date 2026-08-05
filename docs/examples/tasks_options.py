import gylo

app = gylo.Gylo()


@app.task
async def send_receipt(order_id: int, *, email: str) -> None: ...


@app.task(name="billing.charge", store_result=True)
async def charge(customer: str, amount_cents: int) -> dict:
    return {"charged": amount_cents}


@app.task(context=True)
async def flaky_import(ctx: gylo.JobContext, source: str) -> None:
    if ctx.final:
        # last attempt: page a human instead of failing into the void
        ...


async def enqueue_all(conn) -> None:
    await send_receipt.enqueue(conn, 42, email="a@b.c")

    await send_receipt.options(queue="mail", delay=30.0).enqueue(
        conn, 43, email="c@d.e"
    )

    await send_receipt.options(unique=True).enqueue(conn, 42, email="a@b.c")

    await send_receipt.enqueue_many(
        conn,
        [gylo.call(n, email=f"user{n}@example.com") for n in range(100)],
    )
