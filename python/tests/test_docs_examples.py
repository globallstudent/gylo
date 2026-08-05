"""The documentation's code, run rather than trusted.

Samples rot silently: an API rename passes every test while every page still
shows the old spelling. So each example file is imported and its entry points
run against a real database, and every fenced Python block in the pages must
at least compile.
"""

from __future__ import annotations

import ast
import importlib.util
import re
import sys
from pathlib import Path

import pytest

DOCS = Path(__file__).resolve().parents[2] / "docs"
EXAMPLES = DOCS / "examples"

pytestmark = pytest.mark.skipif(not DOCS.is_dir(), reason="docs not present")


def load(name: str):
    spec = importlib.util.spec_from_file_location(
        f"docs_example_{name}", EXAMPLES / f"{name}.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "name",
    sorted(p.stem for p in EXAMPLES.glob("*.py")),
)
def test_every_example_imports(name: str) -> None:
    load(name)


@pytest.mark.asyncio
async def test_examples_run_against_a_real_database(conn) -> None:
    await load("tasks_options").enqueue_all(conn)
    await load("scheduling").defer(conn)
    await load("keyed").enqueue_exports(conn, "acme", [1, 2, 3])
    await load("workflows").run_pipeline(conn)

    results = load("results")
    job = await results.analyse.enqueue(conn, 7)
    await results.check_on(conn, job)
    await results.call_off(conn, [job])


def test_sync_example_runs(sync_conn) -> None:
    module = load("sync_usage")
    job = module.send_welcome.enqueue_sync(sync_conn, 1)
    module.check_and_cancel(sync_conn, job)


@pytest.mark.parametrize(
    "page",
    sorted(p.name for p in DOCS.glob("*.md")),
)
def test_every_fenced_block_compiles(page: str) -> None:
    text = (DOCS / page).read_text()
    for i, block in enumerate(re.findall(r"```python[^\n]*\n(.*?)```", text, re.S)):
        if "--8<--" in block:
            continue
        try:
            ast.parse(block)
        except SyntaxError as error:
            pytest.fail(f"{page} block {i}: {error}")
