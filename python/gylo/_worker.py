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
from ._protocol import Decoder, Dispatch, encode_failure, encode_success

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


async def _execute(app: Gylo, message: Dispatch, writer: asyncio.StreamWriter) -> None:
    task = None
    try:
        task = app.get(message.task)
        args: list[Any]
        kwargs: dict[str, Any]
        if message.payload:
            args, kwargs = _decode_payload(message.payload)
        else:
            args, kwargs = [], {}

        result = task.fn(*args, **kwargs)
        if inspect.isawaitable(result):
            await result
    except UnknownTaskError:
        writer.write(encode_failure(message.id, traceback.format_exc(), retry=False))
    except Exception as error:
        retry = task.should_retry(error) if task is not None else False
        writer.write(encode_failure(message.id, traceback.format_exc(), retry=retry))
    else:
        writer.write(encode_success(message.id))
    await writer.drain()


async def serve(app: Gylo, socket: str) -> None:
    reader, writer = await asyncio.open_unix_connection(socket)
    decoder = Decoder()
    running: set[asyncio.Task[None]] = set()

    while True:
        chunk = await reader.read(READ_BYTES)
        if not chunk:
            break
        decoder.extend(chunk)
        for message in decoder.drain():
            job = asyncio.ensure_future(_execute(app, message, writer))
            running.add(job)
            job.add_done_callback(running.discard)

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
