"""The type checker's view of the API, enforced like any other behaviour.

Typed enqueue is a named differentiator, and nothing but this file stops a
refactor from quietly widening everything back to Any — which is exactly what
the task decorator once did.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

pytest.importorskip("mypy")

CHECKED = textwrap.dedent(
    """
    import gylo

    app = gylo.Gylo()

    @app.task
    async def send(order_id: int, *, email: str) -> None: ...

    @app.task(durable=True)
    async def steppy(ctx: gylo.StepContext, amount: int) -> None: ...

    @app.task(context=True)
    async def aware(ctx: gylo.JobContext, n: int) -> None: ...

    async def good(conn: object) -> None:
        await send.enqueue(conn, 1, email="a@b.c")
        send.enqueue_sync(conn, 1, email="a@b.c")
        # the context argument belongs to the worker, not the caller
        await steppy.enqueue(conn, 5)
        await aware.enqueue(conn, 5)

    async def bad(conn: object) -> None:
        await send.enqueue(conn, "not-an-int", email="a@b.c")  # E: arg-type
        await send.enqueue(conn, 1, emial="typo@b.c")  # E: call-arg
        await send.enqueue(conn, 1)  # E: call-arg
        await steppy.enqueue(conn, "wrong")  # E: arg-type
    """
)


def test_the_checker_accepts_right_calls_and_rejects_wrong_ones(
    tmp_path: Path,
) -> None:
    source = tmp_path / "checked.py"
    source.write_text(CHECKED)
    package = Path(__file__).resolve().parents[1]

    result = subprocess.run(
        [sys.executable, "-m", "mypy", "--no-error-summary", str(source)],
        capture_output=True,
        text=True,
        env={"PYTHONPATH": str(package), "PATH": "/usr/bin:/bin"},
    )

    findings = result.stdout
    for line_hint in ("arg-type", "call-arg"):
        assert line_hint in findings, (
            f"mypy should reject the deliberate mistakes; output was:\n{findings}"
        )
    wrong_lines = [
        line
        for line in findings.splitlines()
        if "error" in line
        and "# E:" not in CHECKED.splitlines()[int(line.split(":", 2)[1]) - 1]
    ]
    assert not wrong_lines, "mypy rejected lines that must type-check:\n" + "\n".join(
        wrong_lines
    )
