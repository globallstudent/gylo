import os

from celery import Celery
from shared import done

app = Celery(
    "bench",
    broker=os.environ.setdefault("GYLO_BENCH_REDIS", "redis://127.0.0.1:6389/0"),
)
app.conf.task_ignore_result = True


@app.task(name="bench.work")
def work(n: int) -> None:
    done()
