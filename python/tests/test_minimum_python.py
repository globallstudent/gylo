"""The package must parse on the oldest interpreter it claims to support.

A single unparenthesised `except A, B:` compiled happily on the development
interpreter and silently made the whole package require 3.14, which would have
shipped as a `requires-python` almost no deployment could satisfy. `ruff`'s
target version was set to match, so the formatter rewrote the compatible form
back into the incompatible one on every run.

`feature_version` gates syntax rather than library calls, so this catches new
grammar and not a new stdlib function. Running the suite against the minimum
itself is the other half, and belongs in CI.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

MINIMUM = (3, 11)
PACKAGE = Path(__file__).resolve().parents[1] / "gylo"


@pytest.mark.parametrize(
    "source", sorted(PACKAGE.rglob("*.py")), ids=lambda path: path.name
)
def test_parses_on_the_minimum_supported_python(source: Path) -> None:
    try:
        ast.parse(source.read_text(), filename=str(source), feature_version=MINIMUM)
    except SyntaxError as error:
        pytest.fail(
            f"{source.name} needs syntax newer than "
            f"{MINIMUM[0]}.{MINIMUM[1]}: {error.msg}"
        )


def test_the_typing_marker_ships() -> None:
    import gylo

    marker = Path(gylo.__file__).parent / "py.typed"

    assert marker.is_file(), (
        f"{marker} is missing, so `Typing :: Typed` in pyproject.toml is a "
        f"promise the distribution does not keep"
    )
