import pytest

from gylo import Gylo, NoRetryError


class Transient(Exception):
    pass


class Permanent(Exception):
    pass


class Subclass(Permanent):
    pass


@pytest.fixture
def app():
    return Gylo()


def test_ordinary_exceptions_are_retried_by_default(app):
    @app.task(name="default")
    def task():
        pass

    assert task.should_retry(Transient())
    assert task.should_retry(RuntimeError())


def test_no_retry_error_is_never_retried(app):
    @app.task(name="anything")
    def task():
        pass

    assert not task.should_retry(NoRetryError())


def test_excluded_types_are_not_retried(app):
    @app.task(name="excluding", no_retry_on=(Permanent,))
    def task():
        pass

    assert task.should_retry(Transient())
    assert not task.should_retry(Permanent())


def test_exclusion_covers_subclasses(app):
    @app.task(name="subclasses", no_retry_on=(Permanent,))
    def task():
        pass

    assert not task.should_retry(Subclass())


def test_retry_on_narrows_what_is_retried(app):
    @app.task(name="narrow", retry_on=(Transient,))
    def task():
        pass

    assert task.should_retry(Transient())
    assert not task.should_retry(Permanent())


def test_exclusion_wins_over_inclusion(app):
    @app.task(name="both", retry_on=(Permanent,), no_retry_on=(Subclass,))
    def task():
        pass

    assert task.should_retry(Permanent())
    assert not task.should_retry(Subclass())


def test_a_task_stays_callable(app):
    @app.task(name="callable")
    def double(value):
        return value * 2

    assert double(21) == 42


def test_registering_the_same_name_twice_is_rejected(app):
    @app.task(name="taken")
    def first():
        pass

    with pytest.raises(ValueError, match="already registered"):

        @app.task(name="taken")
        def second():
            pass
