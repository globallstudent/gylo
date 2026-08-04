"""gylo — a distributed task queue for Python with a Rust core."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

__all__ = ["Gylo", "Task", "UnknownTaskError"]


class UnknownTaskError(LookupError):
    """No task is registered under the requested name."""


class Task:
    """A registered task.

    Calling the instance runs the wrapped function directly, so a task stays
    usable as an ordinary function in tests and from other tasks.
    """

    __slots__ = ("fn", "name")

    def __init__(self, name: str, fn: Callable[..., Any]) -> None:
        self.name = name
        self.fn = fn

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.fn(*args, **kwargs)

    def __repr__(self) -> str:
        return f"Task({self.name!r})"


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
    ) -> Any:
        """Register a function as a task, bare or called with arguments."""

        def register(func: Callable[..., Any]) -> Task:
            task_name = name or f"{func.__module__}.{func.__qualname__}"
            if task_name in self._tasks:
                raise ValueError(f"task {task_name!r} is already registered")
            task = Task(task_name, func)
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
