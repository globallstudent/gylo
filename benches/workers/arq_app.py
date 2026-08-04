import os
from typing import ClassVar

from arq.connections import RedisSettings
from shared import adone


async def work(ctx, n: int) -> None:
    await adone()


class WorkerSettings:
    functions: ClassVar = [work]
    redis_settings = RedisSettings.from_dsn(
        os.environ.setdefault("GYLO_BENCH_REDIS", "redis://127.0.0.1:6389/0")
    )
    keep_result = 0
