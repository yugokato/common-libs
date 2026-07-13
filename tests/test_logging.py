"""Tests for common_libs.logging module"""

import io
import logging
import re
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import IO

import pytest

import common_libs
from common_libs.ansi_colors import ColorCodes
from common_libs.logging import (
    ColoredStreamHandler,
    CustomLoggingArgs,
    LogFormatter,
    LoggerAdapter,
    get_logger,
    setup_logging,
)


@pytest.fixture(autouse=True)
def _restore_logging_state() -> Iterator[None]:
    """Snapshot and restore logging state so `setup_logging`/`dictConfig` calls don't leak between tests"""
    loggers = [logging.getLogger(), logging.getLogger(common_libs.__name__), logging.getLogger(__name__)]
    snapshot = {
        logger: (logger.level, logger.propagate, list(logger.handlers), list(logger.filters)) for logger in loggers
    }
    yield
    for logger, (level, propagate, handlers, filters) in snapshot.items():
        logger.setLevel(level)
        logger.propagate = propagate
        logger.handlers = handlers
        logger.filters = filters


class TestSetupLogging:
    """Tests for setup_logging function"""

    def test_setup_logging_basic(self, logging_config_file: Path) -> None:
        """Test basic logging setup"""
        setup_logging(logging_config_file)
        logger = logging.getLogger(__name__)
        assert logger.level == logging.DEBUG

    def test_setup_logging_with_delta(self, logging_config_file: Path, delta_config_file: Path) -> None:
        """Test logging setup with delta config"""
        setup_logging(logging_config_file, delta_config_file)
        logger = logging.getLogger(__name__)
        assert logger.level == logging.INFO

    def test_setup_logging_default_config(self) -> None:
        """Test that setup_logging with no arguments applies the package's built-in config"""
        setup_logging()
        logger = logging.getLogger(common_libs.__name__)
        assert logger.level == logging.INFO
        assert logger.propagate is False
        assert any(isinstance(handler, ColoredStreamHandler) for handler in logger.handlers)


class TestGetLogger:
    """Tests for get_logger function"""

    def test_get_logger_returns_adapter(self) -> None:
        """Test that get_logger returns LoggerAdapter"""
        logger = get_logger("test")
        assert isinstance(logger, LoggerAdapter)

    def test_get_logger_name(self) -> None:
        """Test that logger has correct name"""
        name = "test.module"
        logger = get_logger(name)
        assert logger.logger.name == name


class TestCustomLoggingArgs:
    """Tests for CustomLoggingArgs enum"""

    def test_color_code_value(self) -> None:
        """Test COLOR_CODE enum value"""
        assert CustomLoggingArgs.COLOR_CODE.value == "color_code"


class TestLoggerAdapter:
    """Tests for LoggerAdapter class"""

    def test_logger_adapter_process_custom_args(self) -> None:
        """Test that process handles custom arguments"""
        logger = get_logger("test")
        _, kwargs = logger.process("test message", {"color_code": ColorCodes.RED})
        assert "extra" in kwargs
        assert kwargs["extra"]["color_code"] == ColorCodes.RED

    def test_logger_adapter_process_merges_extra(self) -> None:
        """Test that process merges extra dict"""
        logger = get_logger("test")
        _, kwargs = logger.process("test", {"extra": {"key": "value"}})
        assert kwargs["extra"]["key"] == "value"


class TestColoredStreamHandler:
    """Tests for ColoredStreamHandler class"""

    @pytest.fixture(autouse=True)
    def _force_tty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Simulate a TTY stdout so color-formatting tests are deterministic under test output capture"""
        monkeypatch.setattr("sys.stdout.isatty", lambda: True)

    @staticmethod
    def _build_handler(stream: IO[str]) -> ColoredStreamHandler:
        """Build a ColoredStreamHandler on the given stream with a plain message-only formatter"""
        handler = ColoredStreamHandler(stream)
        handler.setFormatter(logging.Formatter("%(message)s"))
        return handler

    @staticmethod
    def _make_record(level: int, color_code: str | None = None) -> logging.LogRecord:
        """Build a `LogRecord`, optionally with an explicit `color_code` attribute"""
        record = logging.LogRecord(
            name="test", level=level, pathname="", lineno=0, msg="test message", args=(), exc_info=None
        )
        if color_code is not None:
            record.color_code = color_code
        return record

    @pytest.mark.parametrize(
        ("level", "expected_color"),
        [
            (logging.CRITICAL, ColorCodes.RED),
            (logging.ERROR, ColorCodes.RED),
            (logging.WARNING, ColorCodes.YELLOW),
            (logging.INFO, None),
            (logging.DEBUG, ColorCodes.DARK_GREY),
        ],
        ids=["critical", "error", "warning", "info", "debug"],
    )
    def test_format_applies_color_by_level(self, level: int, expected_color: str | None) -> None:
        """Test that format applies the correct color for each log level"""
        handler = self._build_handler(sys.stdout)
        result = handler.format(self._make_record(level))
        if expected_color is not None:
            assert expected_color in result
        else:
            assert "test message" in result

    def test_format_uses_explicit_color_code(self) -> None:
        """Test that format uses the explicitly specified color_code over the level-based one"""
        handler = self._build_handler(sys.stdout)
        result = handler.format(self._make_record(logging.INFO, color_code=ColorCodes.MAGENTA))
        assert ColorCodes.MAGENTA in result

    def test_format_reusable_across_multiple_handlers(self) -> None:
        """Test that formatting the same record for multiple handlers preserves the explicit color_code"""
        record = self._make_record(logging.INFO, color_code=ColorCodes.MAGENTA)
        first_result = self._build_handler(sys.stdout).format(record)
        second_result = self._build_handler(sys.stdout).format(record)
        assert ColorCodes.MAGENTA in first_result
        assert ColorCodes.MAGENTA in second_result

    def test_format_no_color_when_no_color_env_var_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that format omits ANSI color codes when the `NO_COLOR` environment variable is set"""
        monkeypatch.setenv("NO_COLOR", "1")
        handler = self._build_handler(sys.stdout)
        result = handler.format(self._make_record(logging.ERROR))
        assert result == "test message"

    def test_format_no_color_when_no_color_env_var_is_empty_string(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that format omits ANSI color codes when `NO_COLOR` is set to an empty string

        Per the NO_COLOR spec, presence of the variable disables color regardless of its value.
        """
        monkeypatch.setenv("NO_COLOR", "")
        handler = self._build_handler(sys.stdout)
        result = handler.format(self._make_record(logging.ERROR))
        assert result == "test message"

    @pytest.mark.parametrize("value", ["0", "false", "False"], ids=["zero", "false", "false-capitalized"])
    def test_format_no_color_when_force_color_env_var_is_falsy(
        self, monkeypatch: pytest.MonkeyPatch, value: str
    ) -> None:
        """Test that format omits ANSI color codes when `FORCE_COLOR` is set to a falsy value, even on a tty"""
        monkeypatch.setenv("FORCE_COLOR", value)
        handler = self._build_handler(sys.stdout)
        result = handler.format(self._make_record(logging.ERROR))
        assert result == "test message"

    def test_format_force_color_overrides_no_color(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that a truthy `FORCE_COLOR` takes precedence over `NO_COLOR`"""
        monkeypatch.setenv("NO_COLOR", "1")
        monkeypatch.setenv("FORCE_COLOR", "1")
        handler = self._build_handler(io.StringIO())
        result = handler.format(self._make_record(logging.ERROR))
        assert ColorCodes.RED in result

    def test_format_no_color_for_non_tty_stream_even_when_stdout_is_a_tty(self) -> None:
        """Test that format omits ANSI color codes for a handler on a non-tty stream (eg. a file), even though
        `sys.stdout` (mocked by `_force_tty`) is a tty

        A formatter cannot know which stream its handler writes to, so the color decision must be based on the
        handler's own stream rather than `sys.stdout`.
        """
        handler = self._build_handler(io.StringIO())
        result = handler.format(self._make_record(logging.ERROR))
        assert result == "test message"

    def test_format_applies_color_when_force_color_env_var_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that format applies ANSI color codes when `FORCE_COLOR` is set, even when the stream is not a tty"""
        monkeypatch.setenv("FORCE_COLOR", "1")
        handler = self._build_handler(io.StringIO())
        result = handler.format(self._make_record(logging.ERROR))
        assert ColorCodes.RED in result


class TestLogFormatter:
    """Tests for LogFormatter class"""

    def test_default_format_and_datefmt(self) -> None:
        """Test that fmt and datefmt default to the standard values when not explicitly specified"""
        formatter = LogFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0, msg="test message", args=(), exc_info=None
        )
        result = formatter.format(record)
        timestamp, _, message = result.partition(" - ")
        assert message == "test message"
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}[+-]\d{4}", timestamp)

    def test_explicit_format_and_datefmt_override_defaults(self) -> None:
        """Test that explicitly specified fmt and datefmt override the defaults"""
        formatter = LogFormatter(fmt="%(message)s", datefmt="%Y")
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0, msg="test message", args=(), exc_info=None
        )
        result = formatter.format(record)
        assert result == "test message"

    def test_format_time_with_milliseconds(self) -> None:
        """Test formatTime with %f for milliseconds"""
        formatter = LogFormatter(datefmt="%Y-%m-%d %H:%M:%S.%f")
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0, msg="test", args=(), exc_info=None
        )
        result = formatter.formatTime(record, datefmt="%Y-%m-%d %H:%M:%S.%f")
        assert re.search(r"\.\d{3}$", result)

    def test_format_time_with_timezone(self) -> None:
        """Test formatTime with %z for timezone"""
        formatter = LogFormatter(datefmt="%Y-%m-%d %H:%M:%S%z")
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0, msg="test", args=(), exc_info=None
        )
        result = formatter.formatTime(record, datefmt="%Y-%m-%d %H:%M:%S%z")
        ct = formatter.converter(record.created)
        offset = ct.tm_gmtoff
        assert offset is not None
        sign = "-" if offset < 0 else "+"
        offset = abs(offset)
        expected_utc_offset = f"{sign}{offset // 3600:02d}{offset % 3600 // 60:02d}"
        assert result.endswith(expected_utc_offset)

    def test_format_time_utc_offset_with_gmtime_converter(self) -> None:
        """Test that formatTime renders a %z offset of +0000 when using a UTC (`gmtime`) converter"""
        formatter = LogFormatter(datefmt="%Y-%m-%d %H:%M:%S%z")
        formatter.converter = time.gmtime
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0, msg="test", args=(), exc_info=None
        )
        result = formatter.formatTime(record, datefmt="%Y-%m-%d %H:%M:%S%z")
        assert result.endswith("+0000")

    def test_format_time_default_fallback(self) -> None:
        """Test formatTime falls back to the standard `logging.Formatter` format when no datefmt"""
        formatter = LogFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0, msg="test", args=(), exc_info=None
        )
        result = formatter.formatTime(record, datefmt=None)
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}", result)
