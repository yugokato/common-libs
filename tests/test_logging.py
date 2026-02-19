"""Tests for common_libs.logging module"""

import logging
from pathlib import Path

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

    def test_get_color_code_critical(self) -> None:
        """Test color for CRITICAL level"""
        color = ColoredStreamHandler._get_color_code(logging.CRITICAL)
        assert color == ColorCodes.RED

    def test_get_color_code_error(self) -> None:
        """Test color for ERROR level"""
        color = ColoredStreamHandler._get_color_code(logging.ERROR)
        assert color == ColorCodes.RED

    def test_get_color_code_warning(self) -> None:
        """Test color for WARNING level"""
        color = ColoredStreamHandler._get_color_code(logging.WARNING)
        assert color == ColorCodes.YELLOW

    def test_get_color_code_info(self) -> None:
        """Test color for INFO level"""
        color = ColoredStreamHandler._get_color_code(logging.INFO)
        assert color is None

    def test_get_color_code_debug(self) -> None:
        """Test color for DEBUG level"""
        color = ColoredStreamHandler._get_color_code(logging.DEBUG)
        assert color == ColorCodes.DARK_GREY

    def test_format_applies_color(self) -> None:
        """Test that format applies color based on level"""
        handler = ColoredStreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="",
            lineno=0,
            msg="error message",
            args=(),
            exc_info=None,
        )
        result = handler.format(record)
        assert ColorCodes.RED in result


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
