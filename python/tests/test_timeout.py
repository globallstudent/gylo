from __future__ import annotations

import pytest

import gylo


def test_a_task_inherits_the_apps_default() -> None:
    app = gylo.Gylo()

    @app.task(name="inherits")
    async def inherits() -> None: ...

    assert inherits.timeout == gylo.DEFAULT_TIMEOUT


def test_a_task_can_set_its_own() -> None:
    app = gylo.Gylo()

    @app.task(name="own", timeout=1.5)
    async def own() -> None: ...

    assert own.timeout == 1.5


def test_a_task_can_opt_out_entirely() -> None:
    app = gylo.Gylo()

    @app.task(name="forever", timeout=None)
    async def forever() -> None: ...

    assert forever.timeout is None, (
        "an explicit None must survive, or a task that legitimately runs long "
        "would silently inherit a deadline it was written to avoid"
    )


def test_an_app_can_change_the_default() -> None:
    app = gylo.Gylo(default_timeout=7.0)

    @app.task(name="from_app")
    async def from_app() -> None: ...

    assert from_app.timeout == 7.0


def test_a_durable_task_must_be_async() -> None:
    app = gylo.Gylo()

    with pytest.raises(TypeError, match="must be async"):

        @app.task(name="sync_durable", durable=True)
        def sync_durable(ctx) -> None: ...


def test_whether_a_body_is_a_coroutine_is_recorded() -> None:
    app = gylo.Gylo()

    @app.task(name="is_async")
    async def coroutine() -> None: ...

    @app.task(name="is_sync")
    def plain() -> None: ...

    assert coroutine.is_async
    assert not plain.is_async
