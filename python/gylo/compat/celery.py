"""A Celery-shaped surface over gylo.

The point is an incremental migration: existing call sites keep working while
tasks move over one at a time. It is not a goal to reimplement Celery, and two
differences are deliberate rather than incidental.

`delay` and `apply_async` return awaitables. Celery's are synchronous, and
making these block would mean running an event loop inside a call that may
already be inside one. Adding `await` is the one mechanical change a migration
needs.

Enqueueing through this layer does not join your transaction, because Celery's
API has nowhere to put a connection. That is the guarantee gylo exists to give,
so a task that matters should move to `gylo`'s own `enqueue(conn, ...)` rather
than stay here.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Sequence
from typing import Any

from .. import DEFAULT_MAX_ATTEMPTS, Gylo, JobOutcome
from .. import outcome as _outcome
from .._workflow import Signature, Workflow, chain, chord, group

__all__ = ["AsyncResult", "Celery", "chain", "chord", "group"]


class AsyncResult:
    """A handle on a job, mirroring the part of Celery's that people use."""

    __slots__ = ("_app", "id")

    def __init__(self, job_id: int, app: Celery) -> None:
        self.id = job_id
        self._app = app

    async def outcome(self) -> JobOutcome | None:
        async with self._app.connection() as conn:
            return await _outcome(conn, self.id)

    async def status(self) -> str:
        found = await self.outcome()
        return _CELERY_STATES.get(found.state, "PENDING") if found else "PENDING"

    async def ready(self) -> bool:
        found = await self.outcome()
        return bool(found and found.finished)

    async def successful(self) -> bool:
        found = await self.outcome()
        return bool(found and found.succeeded)

    async def get(self) -> Any:
        """The stored return value, if the task kept one.

        Celery blocks until the job finishes. Doing that here would mean
        polling inside the caller's event loop, so this reports what is known
        now and leaves waiting to the caller.
        """
        found = await self.outcome()
        return found.result if found else None


_CELERY_STATES = {
    "available": "PENDING",
    "running": "STARTED",
    "completed": "SUCCESS",
    "discarded": "FAILURE",
    "cancelled": "REVOKED",
}


class CeleryTask:
    """A registered task carrying Celery's calling conventions."""

    __slots__ = ("_app", "_defaults", "_task", "name")

    def __init__(self, app: Celery, task: Any, defaults: dict[str, Any]) -> None:
        self._app = app
        self._task = task
        self._defaults = defaults
        self.name = task.name

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._task(*args, **kwargs)

    def _bound(self, **overrides: Any) -> Any:
        return self._task.options(**{**self._defaults, **overrides})

    async def delay(self, *args: Any, **kwargs: Any) -> AsyncResult:
        return await self.apply_async(args=args, kwargs=kwargs)

    async def apply_async(
        self,
        args: Sequence[Any] = (),
        kwargs: dict[str, Any] | None = None,
        *,
        queue: str | None = None,
        countdown: float | None = None,
        eta: dt.datetime | None = None,
        priority: int | None = None,
        max_retries: int | None = None,
    ) -> AsyncResult:
        options: dict[str, Any] = {}
        if queue is not None:
            options["queue"] = queue
        if priority is not None:
            options["priority"] = priority
        if max_retries is not None:
            options["max_attempts"] = max_retries + 1
        delay = _delay_from(countdown, eta)
        if delay is not None:
            options["delay"] = delay

        target = self._bound(**options)
        async with self._app.connection() as conn:
            job_id = await target.enqueue(conn, *args, **(kwargs or {}))
        return AsyncResult(job_id, self._app)

    def s(self, *args: Any, **kwargs: Any) -> Signature:
        """Celery's shorthand for a signature."""
        return self._bound().signature(*args, **kwargs)

    signature = s


def _delay_from(countdown: float | None, eta: dt.datetime | None) -> float | None:
    if countdown is not None:
        return float(countdown)
    if eta is None:
        return None
    now = dt.datetime.now(eta.tzinfo or dt.UTC)
    return max(0.0, (eta - now).total_seconds())


class Celery:
    """Stands in for `celery.Celery`.

    A pool must be attached before anything is enqueued, because Celery's API
    has no place to pass a connection.
    """

    __slots__ = ("_pool", "app")

    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        self.app = Gylo()
        self._pool: Any = None

    def configure(self, pool: Any) -> None:
        """Attach the connection pool that enqueues will borrow from."""
        self._pool = pool

    def connection(self) -> Any:
        if self._pool is None:
            raise RuntimeError(
                "call configure(pool) before enqueueing through the celery layer"
            )
        return self._pool.acquire()

    def task(
        self,
        fn: Callable[..., Any] | None = None,
        *,
        name: str | None = None,
        max_retries: int = DEFAULT_MAX_ATTEMPTS - 1,
        **_ignored: Any,
    ) -> Any:
        """Register a task, accepting and ignoring Celery options gylo has no
        equivalent for rather than failing on them."""

        def register(func: Callable[..., Any]) -> CeleryTask:
            task = self.app.task(func, name=name)
            return CeleryTask(self, task, {"max_attempts": max_retries + 1})

        return register(fn) if fn is not None else register

    def workflow(self, flow: Workflow) -> Workflow:
        return flow
