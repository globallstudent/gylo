"""Wire protocol shared with the Rust supervisor.

Mirrors `crates/gylo-core/src/protocol.rs`; the two must change together.

    frame    = u32 body_len (LE) || body
    body     = u8 kind || kind-specific

    dispatch = 0x00 || i64 job_id || u16 task_len || task_utf8 || payload
    complete = 0x01 || i64 job_id || u8 outcome || error_utf8
    register = 0x02 || messagepack [[name, queue, task, expr, tz, payload], ..]
    steps    = 0x03 || messagepack [job_id, [[name, result], ..]]
    record   = 0x04 || messagepack [job_id, name, result]
    stored   = 0x05 || messagepack [job_id, name]

    outcome  = 0x00 success | 0x01 failed, retryable | 0x02 failed, terminal
"""

from __future__ import annotations

import struct
from dataclasses import dataclass

import msgspec

HEADER_BYTES = 4
KIND_DISPATCH = 0x00
KIND_COMPLETE = 0x01
KIND_REGISTER = 0x02
KIND_STEPS = 0x03
KIND_RECORD = 0x04
KIND_STORED = 0x05
OUTCOME_SUCCESS = 0x00
OUTCOME_RETRY = 0x01
OUTCOME_TERMINAL = 0x02
MAX_FRAME_BYTES = 16 * 1024 * 1024

_LENGTH = struct.Struct("<I")
_COMPLETE = struct.Struct("<IBqB")
_DISPATCH_HEAD = struct.Struct("<qH")
_COMPLETE_BODY_BYTES = 10
_DISPATCH_HEAD_BYTES = 11
MAX_ERROR_BYTES = 64 * 1024


class ProtocolError(Exception):
    """A frame that could not be interpreted."""


@dataclass(frozen=True, slots=True)
class Dispatch:
    id: int
    task: str
    payload: bytes


@dataclass(frozen=True, slots=True)
class Steps:
    """Steps a durable job already completed, replayed instead of repeated."""

    id: int
    steps: list[tuple[str, bytes]]


@dataclass(frozen=True, slots=True)
class Stored:
    """The supervisor confirming a step is durable."""

    id: int
    name: str


def encode_register(entries: list[tuple[str, str, str, str, str, bytes]]) -> bytes:
    """Frame the schedules this child declares, sent once on connect."""
    body = bytes([KIND_REGISTER]) + msgspec.msgpack.encode(entries)
    return _LENGTH.pack(len(body)) + body


def encode_record(job_id: int, name: str, result: bytes) -> bytes:
    """Frame a completed step for the supervisor to make durable."""
    body = bytes([KIND_RECORD]) + msgspec.msgpack.encode((job_id, name, result))
    return _LENGTH.pack(len(body)) + body


def encode_success(job_id: int, result: bytes = b"") -> bytes:
    return (
        _COMPLETE.pack(
            _COMPLETE_BODY_BYTES + len(result), KIND_COMPLETE, job_id, OUTCOME_SUCCESS
        )
        + result
    )


def encode_failure(job_id: int, error: str, *, retry: bool) -> bytes:
    """Frame a failure, trimming the rendered error to a size the supervisor
    will accept. An oversized frame would be rejected on the far side, killing
    the session and redelivering the job to fail exactly the same way."""
    rendered = error.encode("utf-8", "replace")
    if len(rendered) > MAX_ERROR_BYTES:
        rendered = rendered[:MAX_ERROR_BYTES] + b"\n... truncated"
    return (
        _COMPLETE.pack(
            _COMPLETE_BODY_BYTES + len(rendered),
            KIND_COMPLETE,
            job_id,
            OUTCOME_RETRY if retry else OUTCOME_TERMINAL,
        )
        + rendered
    )


class Decoder:
    """Reassembles dispatches from arbitrarily chunked reads."""

    __slots__ = ("_buf",)

    def __init__(self) -> None:
        self._buf = bytearray()

    def extend(self, data: bytes) -> None:
        self._buf += data

    def drain(self) -> list[Dispatch | Steps | Stored]:
        buf = self._buf
        size = len(buf)
        pos = 0
        out: list[Dispatch | Steps | Stored] = []

        while size - pos >= HEADER_BYTES:
            (body_len,) = _LENGTH.unpack_from(buf, pos)
            if body_len > MAX_FRAME_BYTES:
                raise ProtocolError(
                    f"frame body of {body_len} bytes exceeds the "
                    f"{MAX_FRAME_BYTES} byte limit"
                )
            if size - pos - HEADER_BYTES < body_len:
                break

            body = pos + HEADER_BYTES
            end = body + body_len
            kind = buf[body]

            if kind in (KIND_STEPS, KIND_STORED):
                unpacked = msgspec.msgpack.decode(bytes(buf[body + 1 : end]))
                out.append(
                    Steps(id=unpacked[0], steps=[tuple(s) for s in unpacked[1]])
                    if kind == KIND_STEPS
                    else Stored(id=unpacked[0], name=unpacked[1])
                )
                pos = end
                continue

            if kind != KIND_DISPATCH:
                raise ProtocolError(f"unexpected message kind {kind:#04x}")
            if body_len < _DISPATCH_HEAD_BYTES:
                raise ProtocolError("frame body is truncated")

            job_id, name_len = _DISPATCH_HEAD.unpack_from(buf, body + 1)
            name_at = body + 11
            payload_at = name_at + name_len
            if payload_at > end:
                raise ProtocolError("frame body is truncated")

            out.append(
                Dispatch(
                    id=job_id,
                    task=bytes(buf[name_at:payload_at]).decode("utf-8"),
                    payload=bytes(buf[payload_at:end]),
                )
            )
            pos = end

        if pos:
            del buf[:pos]
        return out
