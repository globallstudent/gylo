"""gylo — a distributed task queue for Python with a Rust core."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

import msgspec

from ._adapters import UnsupportedDriverError, adapter_for

__all__ = [
    "BoundTask",
    "Gylo",
    "NoRetryError",
    "Options",
    "Task",
    "UnknownTaskError",
    "UnsupportedDriverError",
]

DEFAULT_QUEUE = "default"
DEFAULT_MAX_ATTEMPTS = 20

_encode = msgspec.msgpack.Encoder().encode


class UnknownTaskError(LookupError):
    """No task is registered under the requested name."""


class NoRetryError(Exception):
    """Raise to fail a job permanently regardless of its retry policy."""


class Task:
    """A registered task.

    Calling the instance runs the wrapped function directly, so a task stays
    usable as an ordinary function in tests and from other tasks.
    """

    __slots__ = ("fn", "name", "no_retry_on", "options_default", "retry_on")

    def __init__(
        self,
        name: str,
        fn: Callable[..., Any],
        retry_on: tuple[type[BaseException], ...] = (Exception,),
        no_retry_on: tuple[type[BaseException], ...] = (),
    ) -> None:
        self.name = name
        self.fn = fn
        self.retry_on = retry_on
        self.no_retry_on = no_retry_on
        self.options_default = Options()

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
    ) -> BoundTask:
        """Bind enqueue options for the next call.

        Options live here rather than on `enqueue` so they cannot collide with
        the task's own parameters — a task is free to take an argument called
        `queue` or `priority`.
        """
        return BoundTask(
            self,
            Options(
                queue=self.options_default.queue if queue is None else queue,
                priority=(
                    self.options_default.priority if priority is None else priority
                ),
                delay=self.options_default.delay if delay is None else delay,
                max_attempts=(
                    self.options_default.max_attempts
                    if max_attempts is None
                    else max_attempts
                ),
            ),
        )

    async def enqueue(self, conn: Any, /, *args: Any, **kwargs: Any) -> int:
        """Insert the job on `conn`, returning its id.

        The connection is explicit so the insert lands in the caller's own
        transaction and commits atomically with whatever else it is doing.
        """
        return await BoundTask(self, self.options_default).enqueue(
            conn, *args, **kwargs
        )

    async def enqueue_many(
        self,
        conn: Any,
        /,
        calls: Sequence[tuple[Sequence[Any], dict[str, Any]]],
    ) -> None:
        """Insert many jobs in one round trip."""
        return await BoundTask(self, self.options_default).enqueue_many(conn, calls)


@dataclass(frozen=True, slots=True)
class Options:
    queue: str = DEFAULT_QUEUE
    priority: int = 0
    delay: float = 0.0
    max_attempts: int = DEFAULT_MAX_ATTEMPTS


class BoundTask:
    """A task with the options its next enqueue will use."""

    __slots__ = ("options", "task")

    def __init__(self, task: Task, options: Options) -> None:
        self.task = task
        self.options = options

    def _row(self, args: Sequence[Any], kwargs: dict[str, Any]) -> tuple[Any, ...]:
        return (
            self.options.queue,
            self.task.name,
            _encode((tuple(args), kwargs)),
            self.options.priority,
            self.options.max_attempts,
            float(self.options.delay),
        )

    async def enqueue(self, conn: Any, /, *args: Any, **kwargs: Any) -> int:
        return await adapter_for(conn).insert(conn, self._row(args, kwargs))

    async def enqueue_many(
        self,
        conn: Any,
        /,
        calls: Sequence[tuple[Sequence[Any], dict[str, Any]]],
    ) -> None:
        """Ids are not returned: reporting them per row would cost the
        pipelining that makes this worth using over a loop of `enqueue`."""
        if not calls:
            return
        rows = [self._row(call_args, call_kwargs) for call_args, call_kwargs in calls]
        await adapter_for(conn).insert_many(conn, rows)


class Gylo:
    """Registry of the tasks a worker can run."""

    __slots__ = ("_tasks",)

    def __init__(self) -> None:
        self._tasks: dict[str, Task] = {}

    def task(
        self,
        fn: Callable[..., Any] | None = None,
        *,
        name: str | None = None,
        retry_on: tuple[type[BaseException], ...] = (Exception,),
        no_retry_on: tuple[type[BaseException], ...] = (),
    ) -> Any:
        """Register a function as a task, bare or called with arguments."""

        def register(func: Callable[..., Any]) -> Task:
            task_name = name or f"{func.__module__}.{func.__qualname__}"
            if task_name in self._tasks:
                raise ValueError(f"task {task_name!r} is already registered")
            task = Task(task_name, func, retry_on, no_retry_on)
            self._tasks[task_name] = task
            return task

        return register(fn) if fn is not None else register

    def get(self, name: str) -> Task:
        try:
            return self._tasks[name]
        except KeyError:
            raise UnknownTaskError(name) from None

    @property
    def names(self) -> frozenset[str]:
        return frozenset(self._tasks)
