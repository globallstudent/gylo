import gylo

app = gylo.Gylo()


@app.task
async def extract(source: str) -> None: ...


@app.task
async def transform(source: str) -> None: ...


@app.task
async def load(source: str) -> None: ...


@app.task
async def notify(source: str) -> None: ...


async def run_pipeline(conn) -> None:
    await gylo.chain(
        extract.signature("s3://bucket"),
        transform.signature("s3://bucket"),
        load.signature("s3://bucket"),
    ).enqueue(conn)

    await gylo.chord(
        gylo.group(
            transform.signature("shard-1"),
            transform.signature("shard-2"),
            transform.signature("shard-3"),
        ),
        notify.signature("all-shards"),
    ).enqueue(conn)
