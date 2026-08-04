"""gylo — a distributed task queue for Python with a Rust core."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

import msgspec

from ._adapters import UnsupportedDriverError, adapter_for
from ._steps import StepContext
from ._workflow import Signature, Workflow, chain, chord, group

__all__ = [
    "BoundTask",
    "CronEntry",
    "Gylo",
    "JobOutcome",
    "NoRetryError",
    "Options",
    "Signature",
    "StepContext",
    "Task",
    "UnboundAppError",
    "UnknownTaskError",
    "UnsupportedDriverError",
    "Workflow",
    "cancel",
    "chain",
    "chord",
    "group",
    "outcome",
]

DEFAULT_QUEUE = "default"
DEFAULT_MAX_ATTEMPTS = 20

_encode = msgspec.msgpack.Encoder().encode


class UnknownTaskError(LookupError):
    """No task is registered under the requested name."""


class UnboundAppError(RuntimeError):
    """`submit` was used on an app with no pool attached."""


class NoRetryError(Exception):
    """Raise to fail a job permanently regardless of its retry policy."""


class Task:
    """A registered task.

    Calling the instance runs the wrapped function directly, so a task stays
    usable as an ordinary function in tests and from other tasks.
    """

    __slots__ = (
        "_app",
        "durable",
        "fn",
        "name",
        "no_retry_on",
        "retry_on",
        "store_result",
    )

    def __init__(
        self,
        app: Gylo,
        name: str,
        fn: Callable[..., Any],
        retry_on: tuple[type[BaseException], ...] = (Exception,),
        no_retry_on: tuple[type[BaseException], ...] = (),
        store_result: bool = False,
        durable: bool = False,
    ) -> None:
        self._app = app
        self.name = name
        self.fn = fn
        self.retry_on = retry_on
        self.no_retry_on = no_retry_on
        self.store_result = store_result
        self.durable = durable

    def should_retry(self, error: BaseException) -> bool:
        """Whether `error` earns another attempt.

        Exclusions win over inclusions, so a broad `retry_on` can be narrowed
        without restating it.
        """
        if isinstance(error, NoRetryError):
            return False
        if self.no_retry_on and isinstance(error, self.no_retry_on):
            return False
        return isinstance(error, self.retry_on)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.fn(*args, **kwargs)

    def __repr__(self) -> str:
        return f"Task({self.name!r})"

    def options(
        self,
        *,
        queue: str | None = None,
        priority: int | None = None,
        delay: float | None = None,
        max_attempts: int | None = None,
        unique: bool | str | None = None,
        concurrency_key: str | None = None,
        max_concurrency: int | None = None,
    ) -> BoundTask:
        """Bind enqueue options for the next call.

        Options live here rather than on `enqueue` so they cannot collide with
        the task's own parameters — a task is free to take an argument called
        `queue` or `priority`.

        `unique=True` deduplicates on the arguments; `unique="key"` on a key
        you choose. Either way a job already waiting or running blocks a second
        one, and enqueue returns the id of the job that is already there.

        `concurrency_key` with `max_concurrency` caps how many jobs sharing
        that key run at once, which is how one tenant is stopped from starving
        the others.
        """
        if (concurrency_key is None) != (max_concurrency is None):
            raise ValueError(
                "concurrency_key and max_concurrency must be given together"
            )
        if max_concurrency is not None and max_concurrency < 1:
            raise ValueError("max_concurrency must be at least 1")
        given = {
            "queue": queue,
            "priority": priority,
            "delay": delay,
            "max_attempts": max_attempts,
            "unique": unique,
            "concurrency_key": concurrency_key,
            "max_concurrency": max_concurrency,
        }
        return BoundTask(
            self, Options(**{k: v for k, v in given.items() if v is not None})
        )

    async def enqueue(self, conn: Any, /, *args: Any, **kwargs: Any) -> int:
        """Insert the job on `conn`, returning its id.

        The connection is explicit so the insert lands in the caller's own
        transaction and commits atomically with whatever else it is doing.
        """
        return await self.options().enqueue(conn, *args, **kwargs)

    async def submit(self, *args: Any, **kwargs: Any) -> int:
        """Enqueue on a connection borrowed from the app's pool.

        Convenient where no connection is at hand, and a weaker promise: the
        job is committed on its own, so it can survive a transaction of yours
        that later rolls back.
        """
        return await self.options().submit(*args, **kwargs)

    async def enqueue_many(
        self,
        conn: Any,
        /,
        calls: Sequence[tuple[Sequence[Any], dict[str, Any]]],
    ) -> None:
        return await self.options().enqueue_many(conn, calls)

    def signature(self, *args: Any, **kwargs: Any) -> Signature:
        """The task and these arguments, for placing in a workflow."""
        return self.options().signature(*args, **kwargs)


@dataclass(frozen=True, slots=True)
class CronEntry:
    """A schedule declared alongside a task."""

    name: str
    queue: str
    task: str
    expression: str
    timezone: str
    payload: bytes

    def as_wire(self) -> tuple[str, str, str, str, str, bytes]:
        return (
            self.name,
            self.queue,
            self.task,
            self.expression,
            self.timezone,
            self.payload,
        )


@dataclass(frozen=True, slots=True)
class Options:
    queue: str = DEFAULT_QUEUE
    priority: int = 0
    delay: float = 0.0
    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    unique: bool | str = False
    concurrency_key: str | None = None
    max_concurrency: int | None = None


def _unique_key(
    task: str, options: Options, args: Sequence[Any], kwargs: dict
) -> bytes:
    """Digest identifying a job for deduplication.

    Keyword arguments are sorted, because dictionaries encode in insertion
    order and callers should not have to pass them in a fixed one. The task
    name and queue are always included, so an explicit key given to two
    different tasks does not collide.
    """
    identity: Any = (
        options.unique
        if isinstance(options.unique, str)
        else (tuple(args), sorted(kwargs.items()))
    )
    return hashlib.blake2b(
        _encode((task, options.queue, identity)), digest_size=32
    ).digest()


class BoundTask:
    """A task with the options its next enqueue will use."""

    __slots__ = ("options", "task")

    def __init__(self, task: Task, options: Options) -> None:
        self.task = task
        self.options = options

    def _row(self, args: Sequence[Any], kwargs: dict[str, Any]) -> tuple[Any, ...]:
        row = (
            self.options.queue,
            self.task.name,
            _encode((tuple(args), kwargs)),
            self.options.priority,
            self.options.max_attempts,
            float(self.options.delay),
            self.options.concurrency_key,
            self.options.max_concurrency,
            self.task.durable,
        )
        if self.options.unique is False:
            return row
        return (*row, _unique_key(self.task.name, self.options, args, kwargs))

    def signature(self, *args: Any, **kwargs: Any) -> Signature:
        """The task and these arguments, for placing in a workflow."""
        return Signature(
            task=self.task.name,
            args=args,
            kwargs=kwargs,
            options=self.options,
            durable=self.task.durable,
        )

    async def submit(self, *args: Any, **kwargs: Any) -> int:
        """Enqueue on a connection borrowed from the app's pool."""
        async with self.task._app.borrow() as conn:
            return await self.enqueue(conn, *args, **kwargs)

    async def enqueue(self, conn: Any, /, *args: Any, **kwargs: Any) -> int:
        """Insert the job, returning its id.

        With `unique` set, the id may belong to a job that was already queued.
        """
        adapter = adapter_for(conn)
        row = self._row(args, kwargs)
        if self.options.unique is False:
            return await adapter.insert(conn, row)
        return await adapter.insert_unique(conn, row)

    async def enqueue_many(
        self,
        conn: Any,
        /,
        calls: Sequence[tuple[Sequence[Any], dict[str, Any]]],
    ) -> None:
        """Insert many jobs in one round trip.

        Ids are not returned, because reporting them per row would cost the
        pipelining that makes this worth using over a loop of `enqueue`.
        """
        if not calls:
            return
        rows = [self._row(call_args, call_kwargs) for call_args, call_kwargs in calls]
        await adapter_for(conn).insert_many(
            conn, rows, unique=self.options.unique is not False
        )


class Gylo:
    """Registry of the tasks a worker can run."""

    __slots__ = ("_crons", "_pool", "_tasks")

    def __init__(self) -> None:
        self._tasks: dict[str, Task] = {}
        self._crons: dict[str, CronEntry] = {}
        self._pool: Any = None

    def bind(self, pool: Any) -> None:
        """Attach a connection pool for `submit`.

        Only needed by callers that do not have a connection to hand. Anything
        that should commit with your own write wants `enqueue(conn, ...)`.
        """
        self._pool = pool

    def borrow(self) -> Any:
        if self._pool is None:
            raise UnboundAppError(
                "call bind(pool) before submit, or use enqueue(conn, ...) to "
                "place the job in a transaction you control"
            )
        return self._pool.acquire()

    def task(
        self,
        fn: Callable[..., Any] | None = None,
        *,
        name: str | None = None,
        retry_on: tuple[type[BaseException], ...] = (Exception,),
        no_retry_on: tuple[type[BaseException], ...] = (),
        store_result: bool = False,
        durable: bool = False,
    ) -> Any:
        """Register a function as a task, bare or called with arguments.

        `durable` gives the task a step context as its first argument. Steps it
        completes are recorded, and a retry replays them rather than repeating
        their side effects.

        `store_result` keeps the return value for later retrieval. It is off by
        default because most jobs are run for their effects, and storing what
        nobody reads costs a write and a row that cannot be pruned.
        """

        def register(func: Callable[..., Any]) -> Task:
            task_name = name or f"{func.__module__}.{func.__qualname__}"
            if task_name in self._tasks:
                raise ValueError(f"task {task_name!r} is already registered")
            task = Task(
                self, task_name, func, retry_on, no_retry_on, store_result, durable
            )
            self._tasks[task_name] = task
            return task

        return register(fn) if fn is not None else register

    def cron(
        self,
        expression: str,
        *,
        name: str | None = None,
        timezone: str = "UTC",
        queue: str = DEFAULT_QUEUE,
        args: Sequence[Any] = (),
        kwargs: dict[str, Any] | None = None,
    ) -> Any:
        """Register a task that also runs on a schedule.

        The schedule travels to the supervisor when a worker starts, so the
        code that defines it is the only place it is defined. Missed runs are
        not backfilled: a schedule that came due while the fleet was down
        advances to its next occurrence.
        """

        def register(func: Callable[..., Any]) -> Task:
            task = self.task(func, name=name)
            schedule_name = name or task.name
            if schedule_name in self._crons:
                raise ValueError(f"schedule {schedule_name!r} is already registered")
            self._crons[schedule_name] = CronEntry(
                name=schedule_name,
                queue=queue,
                task=task.name,
                expression=expression,
                timezone=timezone,
                payload=_encode((tuple(args), kwargs or {})),
            )
            return task

        return register

    @property
    def crons(self) -> tuple[CronEntry, ...]:
        """Every schedule registered, in declaration order."""
        return tuple(self._crons.values())

    def get(self, name: str) -> Task:
        try:
            return self._tasks[name]
        except KeyError:
            raise UnknownTaskError(name) from None

    @property
    def names(self) -> frozenset[str]:
        return frozenset(self._tasks)


@dataclass(frozen=True, slots=True)
class JobOutcome:
    """How a job ended, for a caller that went looking."""

    state: str
    result: Any = None
    errors: list[dict[str, Any]] = field(default_factory=list)

    @property
    def finished(self) -> bool:
        return self.state in {"completed", "discarded", "cancelled"}

    @property
    def succeeded(self) -> bool:
        return self.state == "completed"


async def outcome(conn: Any, job_id: int) -> JobOutcome | None:
    """Look up how a job ended, or None if no such job exists.

    `result` is only populated for tasks registered with `store_result=True`.
    """
    row = await adapter_for(conn).outcome(conn, job_id)
    if row is None:
        return None
    state, stored, errors = row
    return JobOutcome(
        state=state,
        result=None if stored is None else msgspec.msgpack.decode(stored),
        errors=errors or [],
    )


async def cancel(conn: Any, *job_ids: int) -> int:
    """Cancel jobs that have not started, returning how many were still waiting.

    A job already running is left alone: stopping Python mid-task would mean
    killing the worker's child and every sibling job with it.
    """
    if not job_ids:
        return 0
    return await adapter_for(conn).cancel(conn, list(job_ids))
