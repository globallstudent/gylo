"""Child process that runs task code.

Launched by the Rust supervisor, never directly by users. Each dispatch becomes
its own task and writes its completion as it finishes, so completions are not
ordered by dispatch and nothing blocks on a round trip.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import inspect
import sys
import traceback
from typing import Any

import msgspec

from . import Gylo, UnknownTaskError
from ._protocol import (
    Decoder,
    Dispatch,
    Steps,
    Stored,
    encode_failure,
    encode_record,
    encode_register,
    encode_success,
)
from ._steps import StepAcks, StepContext

READ_BYTES = 1 << 16
_decode_payload = msgspec.msgpack.Decoder().decode


def load_app(path: str) -> Gylo:
    module_name, separator, attribute = path.partition(":")
    if not separator:
        raise ValueError(f"expected module:attribute, got {path!r}")
    module = importlib.import_module(module_name)
    try:
        return getattr(module, attribute)
    except AttributeError:
        raise ValueError(f"{module_name!r} has no attribute {attribute!r}") from None


async def _fail(writer: asyncio.StreamWriter, job_id: int, *, retry: bool) -> None:
    writer.write(encode_failure(job_id, traceback.format_exc(), retry=retry))
    await writer.drain()


async def _execute(
    app: Gylo,
    message: Dispatch,
    writer: asyncio.StreamWriter,
    context: StepContext | None,
) -> None:
    """Run one job and report how it went.

    Only a failure raised by the task body consults the retry policy. An
    unregistered name and an undecodable payload are settled permanently,
    because neither can come out differently on another attempt.
    """
    try:
        task = app.get(message.task)
    except UnknownTaskError:
        await _fail(writer, message.id, retry=False)
        return

    try:
        args: list[Any]
        kwargs: dict[str, Any]
        args, kwargs = _decode_payload(message.payload) if message.payload else ([], {})
    except Exception:
        await _fail(writer, message.id, retry=False)
        return

    try:
        if context is not None:
            returned = task.fn(context, *args, **kwargs)
        else:
            returned = task.fn(*args, **kwargs)
        if inspect.isawaitable(returned):
            returned = await returned
    except Exception as error:
        await _fail(writer, message.id, retry=task.should_retry(error))
        return

    stored = b""
    if task.store_result:
        try:
            stored = msgspec.msgpack.encode(returned)
        except TypeError, ValueError:
            await _fail(writer, message.id, retry=False)
            return

    writer.write(encode_success(message.id, stored))
    await writer.drain()


async def serve(app: Gylo, socket: str) -> None:
    reader, writer = await asyncio.open_unix_connection(socket)
    writer.write(encode_register([entry.as_wire() for entry in app.crons]))
    await writer.drain()

    decoder = Decoder()
    running: set[asyncio.Task[None]] = set()
    acks = StepAcks()
    replays: dict[int, dict[str, bytes]] = {}

    def record(job_id: int, name: str, result: bytes) -> None:
        writer.write(encode_record(job_id, name, result))

    async def confirm(job_id: int, name: str) -> None:
        await acks.expect(job_id, name)

    while True:
        chunk = await reader.read(READ_BYTES)
        if not chunk:
            break
        decoder.extend(chunk)
        for message in decoder.drain():
            if isinstance(message, Steps):
                replays[message.id] = dict(message.steps)
                continue
            if isinstance(message, Stored):
                acks.resolve(message.id, message.name)
                continue

            task = app.get(message.task) if message.task in app.names else None
            context = (
                StepContext(message.id, replays.pop(message.id, {}), record, confirm)
                if task is not None and task.durable
                else None
            )
            job = asyncio.ensure_future(_execute(app, message, writer, context))
            running.add(job)
            job.add_done_callback(running.discard)

    acks.abandon()
    if running:
        await asyncio.gather(*running, return_exceptions=True)
    writer.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="gylo._worker")
    parser.add_argument("--socket", required=True)
    parser.add_argument("--app", required=True)
    arguments = parser.parse_args(argv)

    asyncio.run(serve(load_app(arguments.app), arguments.socket))
    return 0


if __name__ == "__main__":
    sys.exit(main())
