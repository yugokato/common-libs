"""Misc pytest hooks shared across tests in downstream projects.

Enable these hooks from a downstream top-level `conftest.py`:

pytest_plugins = ["common_libs.testing.pytest_plugins.common"]
"""

import logging
import os
import sys
import uuid
from typing import Any

import pytest
from pytest import Config, Item, Session
from xdist import is_xdist_worker


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config: Config) -> None:
    """Force an unreachable log level so captured logs stay hidden.

    Our logging writes to stdout, so pytest's "Captured log" section is redundant noise.

    :param config: The pytest config object
    """
    config.option.log_level = "99"


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
        os.environ["CURRENT_TEST_SESSION_UUID"] = str(uuid.uuid4())


def pytest_runtest_setup(item: Item) -> None:
    """Print a blank line before each test when output capturing is disabled.

    :param item: The test item about to run
    """
    if item.config.option.capture == "no":
        sys.stdout.write("\n")


@pytest.hookimpl(tryfirst=True)
def pytest_sessionfinish() -> None:
    """Detach all logging handlers at session end (pytest issue #5502 workaround)."""
    _patch_pytest_logging_issue()


def _patch_pytest_logging_issue() -> None:
    """Work around pytest issue #5502 (https://github.com/pytest-dev/pytest/issues/5502).

    Pytest hijacks `sys.stdout` and replaces it with a buffer (FileIO) when `--capture=no`/`-s`
    is not used, then closes it at the end. The stdout used by logging is replaced too, so an
    "I/O operation on closed file" error occurs when a record is emitted after the replaced stdout
    is closed. Removing all handlers at session end avoids the late emit.
    """
    loggers = [logging.getLogger(), *list(logging.Logger.manager.loggerDict.values())]
    for logger in loggers:
        if not isinstance(logger, logging.Logger):
            continue
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
