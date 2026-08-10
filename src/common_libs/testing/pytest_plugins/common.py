"""Misc pytest hooks shared across tests in downstream projects.

To enable these hooks from a downstream project, add following line in the top-level `conftest.py`:

pytest_plugins = ["common_libs.testing.pytest_plugins.common"]
"""

from __future__ import annotations

import logging
import os
import re
import sys
import uuid
from collections.abc import Callable, Generator
from contextlib import contextmanager
from functools import wraps
from typing import Any

import pytest
from pytest import Config, Item, MonkeyPatch, Session, Subtests
from xdist import is_xdist_worker

from common_libs.ansi_colors import ColorCodes, color
from common_libs.logging import get_logger
from common_libs.utils import list_items, log_section

_PYTEST_IS_RUNNING = "PYTEST_IS_RUNNING"
_PYTEST_IS_VERBOSE = "PYTEST_IS_VERBOSE"
_CURRENT_TEST_UUID = "CURRENT_TEST_UUID"
_CURRENT_TEST_SESSION_UUID = "CURRENT_TEST_SESSION_UUID"

# Pytest marker expressions are intentionally handled conservatively here.
# Marker names may contain letters, numbers, underscores, and hyphens.
_MARK_EXPR_OPERATORS = {"and", "or", "not"}
_MARK_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_-]*")

logger = get_logger(__name__)


@pytest.fixture(autouse=True)
def _enable_tty(monkeypatch: MonkeyPatch) -> None:
    """Enable ANSI colors under pytest's output capture.

    Pytest replaces stdout and stderr with non-TTY streams during test execution,
    which disables ANSI color output from code that checks ``isatty()``. This fixture makes sure ANSI colors are
    enabled for all tests, even when output is captured.
    """
    monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
    monkeypatch.setattr(sys.stderr, "isatty", lambda: True)


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config: Config) -> None:
    os.environ[_PYTEST_IS_RUNNING] = "true"
    os.environ[_PYTEST_IS_VERBOSE] = str(bool(config.option.verbose)).lower()

    # Prevent pytest's logging plugin from producing its own output.
    config.option.log_level = "99"

    _validate_mark_expression(config)


def pytest_make_parametrize_id(val: Any, argname: str) -> str:
    """Render parametrized test IDs as `argname=repr(val)`.

    :param val: The parametrized value
    :param argname: The parameter name
    """
    return f"{argname}={val!r}"


def pytest_sessionstart(session: Session) -> None:
    """Assign a shared UUID to the whole test session.

    Set once by the controller (not xdist workers) and skipped during collection-only runs.

    :param session: The pytest session object
    """
    if not is_xdist_worker(session) and not session.config.option.collectonly:
        test_session_id = str(uuid.uuid4())
        os.environ[_CURRENT_TEST_SESSION_UUID] = test_session_id
        if _is_verbose():
            logger.info(
                f'Starting a test session - test_session_id: "{test_session_id}"', color_code=ColorCodes.LIGHT_BLUE
            )


def pytest_runtest_logstart() -> None:
    # Set CURRENT_TEST_UUID env var for the current test
    os.environ[_CURRENT_TEST_UUID] = str(uuid.uuid4())


def pytest_runtest_setup(item: Item) -> None:
    """Print a blank line before each test when output capturing is disabled.

    :param item: The test item about to run
    """
    if item.config.option.capture == "no":
        sys.stdout.write("\n")

    if _is_verbose():
        log_section("SETUP", sub_section=True, color_code=ColorCodes.DEFAULT)
        test_uuid = os.environ[_CURRENT_TEST_UUID]
        logger.info(f'Starting a test: - test_id: "{test_uuid}"', color_code=ColorCodes.LIGHT_BLUE)


def pytest_runtest_call() -> None:
    if _is_verbose():
        log_section("TEST", sub_section=True, color_code=ColorCodes.DEFAULT)


def pytest_runtest_teardown(item: Item) -> None:
    if item.config.option.capture == "no":
        sys.stdout.write("\n")
    if _is_verbose():
        log_section("TEARDOWN", sub_section=True, color_code=ColorCodes.DEFAULT)


@pytest.hookimpl(tryfirst=True)
def pytest_sessionfinish() -> None:
    """Detach all logging handlers at session end (pytest issue #5502 workaround)."""
    _patch_pytest_logging_issue()


@pytest.fixture
def subtests(subtests: Subtests) -> Generator[Subtests]:
    """Add section logging to Pytest's subtests fixture"""

    def monkey_patch_subtest(f: Callable[..., Any]) -> Callable[..., Any]:
        @contextmanager
        @wraps(f)
        def wrapper(msg: str | None = None, **kwargs: Any) -> Generator[None]:
            with f(**kwargs):
                if msg is not None:
                    log_section(f"[subtest] {msg}", sub_section=True, color_code=ColorCodes.LIGHT_BLUE)
                yield

        return wrapper

    if _is_verbose():
        subtests.test = monkey_patch_subtest(subtests.test)
    yield subtests


def _patch_pytest_logging_issue() -> None:
    """Work around pytest issue #5502 (https://github.com/pytest-dev/pytest/issues/5502).

    Pytest hijacks `sys.stdout` and replaces it with a buffer (FileIO) when `--capture=no`/`-s`
    is not used, then closes it at the end. The stdout used by logging is replaced too, so an
    "I/O operation on closed file" error occurs when a record is emitted after the replaced stdout
    is closed. Removing all handlers at session end avoids the late emit.
    """
    loggers = [logging.getLogger(), *logging.Logger.manager.loggerDict.values()]
    for l in loggers:
        if not isinstance(l, logging.Logger):
            continue
        for handler in l.handlers[:]:
            l.removeHandler(handler)


def _is_verbose() -> bool:
    """Return whether pytest is running in verbose mode."""
    return os.environ.get(_PYTEST_IS_VERBOSE) == "true"


def _validate_mark_expression(config: Config) -> None:
    """Abort collection when ``-m`` contains undefined markers."""
    mark_expression = config.option.markexpr
    if not mark_expression:
        return

    specified_marks = _extract_marks(mark_expression)
    defined_marks = set(_get_defined_markers(config))
    undefined_marks = specified_marks - defined_marks
    if not undefined_marks:
        return

    pytest.exit(
        color(
            f"ERROR: One or more unknown pytest marks were given:\n{list_items(undefined_marks)}",
            color_code=ColorCodes.RED,
        )
    )


def _extract_marks(mark_expression: str) -> set[str]:
    """Extract marker names from a pytest ``-m`` expression. This intentionally handles the common marker-expression
    syntax rather than trying to implement a full parser."""
    return {token for token in _MARK_NAME_RE.findall(mark_expression) if token not in _MARK_EXPR_OPERATORS}


def _get_defined_markers(config: Config) -> list[str]:
    """Return marker names configured in pytest's ``markers`` setting."""
    return [marker.split(":", 1)[0].strip() for marker in config.getini("markers")]
