import gylo

app = gylo.Gylo(default_timeout=300.0)


class InvalidAddressError(Exception): ...


@app.task(retry_on=(ConnectionError, TimeoutError))
async def deliver(parcel_id: int) -> None: ...


@app.task(no_retry_on=(InvalidAddressError,))
async def geocode(address: str) -> None: ...


@app.task(timeout=30)
async def call_slow_api(query: str) -> None: ...


@app.task(timeout=None)
async def overnight_export(day: str) -> None: ...


@app.task
async def validate(document: str) -> None:
    if not document:
        raise gylo.NoRetryError("an empty document will never validate")
