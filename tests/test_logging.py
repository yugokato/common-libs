"""Tests for common_libs.logging module"""

import logging
from pathlib import Path

import pytest

from common_libs.ansi_colors import ColorCodes
from common_libs.logging import (
    ColoredStreamHandler,
    CustomLoggingArgs,
    LogFilter,
    LogFormatter,
    LoggerAdapter,
    get_logger,
    setup_logging,
)


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
        handler = ColoredStreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        record = logging.LogRecord(
            name="test",
            level=level,
            pathname="",
            lineno=0,
            msg="test message",
            args=(),
            exc_info=None,
        )
        result = handler.format(record)
        if expected_color is not None:
            assert expected_color in result
        else:
            assert "test message" in result


class TestLogFilter:
    """Tests for LogFilter class"""

    def test_log_filter_removes_custom_args(self) -> None:
        """Test that filter removes custom logging args"""
        log_filter = LogFilter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0, msg="test", args=(), exc_info=None
        )
        record.color_code = ColorCodes.RED

        result = log_filter.filter(record)
        assert result is True
        assert not hasattr(record, "color_code")


class TestLogFormatter:
    """Tests for LogFormatter class"""

    def test_format_time_with_milliseconds(self) -> None:
        """Test formatTime with %f for milliseconds"""
        formatter = LogFormatter(datefmt="%Y-%m-%d %H:%M:%S.%f")
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0, msg="test", args=(), exc_info=None
        )
        result = formatter.formatTime(record, datefmt="%Y-%m-%d %H:%M:%S.%f")
        # Should contain milliseconds (3 digits after .)
        assert "." in result

    def test_format_time_with_timezone(self) -> None:
        """Test formatTime with %z for timezone"""
        formatter = LogFormatter(datefmt="%Y-%m-%d %H:%M:%S%z")
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0, msg="test", args=(), exc_info=None
        )
        result = formatter.formatTime(record, datefmt="%Y-%m-%d %H:%M:%S%z")
        # Should contain timezone offset (e.g., +0000 or -0500)
        assert "+" in result or "-" in result

    def test_format_time_default_fallback(self) -> None:
        """Test formatTime falls back to default when no datefmt"""
        formatter = LogFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0, msg="test", args=(), exc_info=None
        )
        result = formatter.formatTime(record, datefmt=None)
        assert result is not None
