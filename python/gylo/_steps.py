"""Checkpointing inside a task.

A durable task receives a context whose `step` runs a piece of work once and
remembers what it returned. On a retry the step is replayed from that record
instead of being run again, so a task that charged a card and then failed to
send the receipt does not charge the card twice.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from typing import Any

import msgspec


class StepContext:
    """Passed as the first argument to a task registered with `durable=True`."""

    __slots__ = ("_ack", "_completed", "_job_id", "_record")

    def __init__(
        self,
        job_id: int,
        completed: dict[str, bytes],
        record: Callable[[int, str, bytes], None],
        ack: Callable[[int, str], Any],
    ) -> None:
        self._job_id = job_id
        self._completed = completed
        self._record = record
        self._ack = ack

    async def step(self, name: str, work: Callable[[], Any]) -> Any:
        """Run `work` once, or return what it returned on an earlier attempt.

        Control does not pass to the next step until this one is durable. A
        step recorded but not yet acknowledged would be repeated after a crash,
        which is the whole thing being avoided.
        """
        if name in self._completed:
            return msgspec.msgpack.decode(self._completed[name])

        result = work()
        if inspect.isawaitable(result):
            result = await result

        encoded = msgspec.msgpack.encode(result)
        self._record(self._job_id, name, encoded)
        await self._ack(self._job_id, name)
        self._completed[name] = encoded
        return result

    @property
    def completed(self) -> frozenset[str]:
        """Steps already recorded for this job."""
        return frozenset(self._completed)


class StepAcks:
    """Waits for the supervisor to confirm each step is durable."""

    __slots__ = ("_waiting",)

    def __init__(self) -> None:
        self._waiting: dict[tuple[int, str], asyncio.Future[None]] = {}

    def expect(self, job_id: int, name: str) -> asyncio.Future[None]:
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiting[(job_id, name)] = future
        return future

    def resolve(self, job_id: int, name: str) -> None:
        future = self._waiting.pop((job_id, name), None)
        if future is not None and not future.done():
            future.set_result(None)

    def abandon(self) -> None:
        """Fail everything still waiting, so a lost connection does not hang."""
        for future in self._waiting.values():
            if not future.done():
                future.set_exception(ConnectionError("supervisor went away"))
        self._waiting.clear()
