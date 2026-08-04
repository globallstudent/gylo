import os

from shared import adone
from taskiq_redis import ListQueueBroker

broker = ListQueueBroker(
    url=os.environ.setdefault("GYLO_BENCH_REDIS", "redis://127.0.0.1:6389/0")
)


@broker.task(task_name="bench.work")
async def work(n: int) -> None:
    await adone(n)
