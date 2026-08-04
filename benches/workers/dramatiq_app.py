import os

import dramatiq
from dramatiq.brokers.redis import RedisBroker
from shared import done

dramatiq.set_broker(
    RedisBroker(
        url=os.environ.setdefault("GYLO_BENCH_REDIS", "redis://127.0.0.1:6389/0")
    )
)


@dramatiq.actor(queue_name="bench", max_retries=0)
def work(n: int) -> None:
    done(n)
