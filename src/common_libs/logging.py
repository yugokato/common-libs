from __future__ import annotations

import logging
import os
import time
from collections.abc import Mapping, MutableMapping
from enum import StrEnum, auto
from importlib import resources
from logging import LogRecord
from logging.config import dictConfig
from typing import Any

import yaml

from common_libs.ansi_colors import ColorCodes, color


def setup_logging(config: Mapping[str, Any] | None = None, delta_config: Mapping[str, Any] | None = None) -> None:
    """Setup logging

    Calling this is optional. Until it is called, `common_libs` loggers are silent (a `NullHandler` is attached
    at import time), so downstream projects that never call this function won't see any output or warnings.

    When `config` is not specified, the package's built-in config is applied. That default only configures
    the `common_libs` logger itself (colored console output, `propagate: false`), and leaves the root logger and
    any other loggers untouched. This means a downstream app's own handlers (eg. a file or JSON handler on the
    root logger) will NOT automatically capture `common_libs` logs. Projects that want unified logging should
    either pass their own `config`, or use `delta_config` to layer overrides (eg. `propagate: true`,
    a different level) onto the default config.

    :param config: Base logging config, following the `logging.config.dictConfig` schema. Defaults to the
                    package's built-in config when not specified
    :param delta_config: Delta logging config to merge onto the base config
    """
    if config is not None:
        _validate_config(config, "config")
        log_cfg = dict(config)
    else:
        config_text = (resources.files("common_libs") / "cfg" / "logging.yaml").read_text(encoding="utf-8")
        log_cfg = yaml.safe_load(config_text)

    if delta_config is not None:
        _validate_config(delta_config, "delta_config")
        if delta_config:
            from common_libs.utils import merge_dicts

            log_cfg = merge_dicts(log_cfg, dict(delta_config))

    dictConfig(log_cfg)


def get_logger(name: str) -> LoggerAdapter:
    """Return a logger for the specified name

    :param name: Logger name
    """
    logger = logging.getLogger(name)
    return LoggerAdapter(logger)


class CustomLoggingArgs(StrEnum):
    """Custom logging arguments"""

    def _generate_next_value_(name: str, start: int, count: int, last_values: list[str]) -> str:  # type: ignore[override]
        return name.lower()

    COLOR_CODE = auto()
    # TODO: Add more if needed


class LoggerAdapter(logging.LoggerAdapter[logging.Logger]):
    """Custom LoggerAdapter"""

    def process(self, msg: Any, kwargs: MutableMapping[str, Any]) -> tuple[Any, MutableMapping[str, Any]]:
        """Support custom arguments to logging calls, and add various fields to log extra

        eg. logger.info("message", color_code=ColorCodes.GREEN)
        """
        # NOTE: LoggerAdapter.process() seems to ignore `extra` given to `kwargs` in a log call.
        # (https://github.com/python/cpython/issues/76913)
        # We will fix this behavior by explicitly merging it with the self.extra
        # TODO: Switch to use the new `merge_extra=True` init option after Python 3.13
        extra = (self.extra or {}) | (kwargs.get("extra") or {})  # type: ignore[operator]
        for custom_arg in CustomLoggingArgs:
            if custom_arg in kwargs:
                extra.update(**{custom_arg: kwargs.pop(custom_arg)})
        kwargs["extra"] = extra
        return msg, kwargs


class LogFormatter(logging.Formatter):
    """Formatter that defaults `fmt`/`datefmt` to the standard format, and supports both `%f` and `%z` in `datefmt`"""

    DEFAULT_FORMAT: str = "%(asctime)s - %(message)s"
    DEFAULT_DATEFMT: str = "%Y-%m-%dT%H:%M:%S.%f%z"

    def __init__(self, fmt: str | None = None, datefmt: str | None = None, *args: Any, **kwargs: Any) -> None:
        """Initialize the formatter

        :param fmt: Log format string. Defaults to `DEFAULT_FORMAT` when not specified
        :param datefmt: Date format string. Defaults to `DEFAULT_DATEFMT` when not specified
        :param args: Additional positional arguments passed to `logging.Formatter`
        :param kwargs: Additional keyword arguments passed to `logging.Formatter`
        """
        super().__init__(
            self.DEFAULT_FORMAT if fmt is None else fmt,
            self.DEFAULT_DATEFMT if datefmt is None else datefmt,
            *args,
            **kwargs,
        )

    def formatTime(self, record: LogRecord, datefmt: str | None = None) -> str:
        """Overrides the default behavior to support both %f and %z in datefmt

        eg. datefmt="%Y-%m-%dT%H:%M:%S.%f%z" will display the timestamp as 2022-01-01T11:22:33.444-0000
        """
        if datefmt:
            ct = self.converter(record.created)
            datefmt = datefmt.replace("%f", f"{int(record.msecs):03d}")
            datefmt = datefmt.replace("%z", self._format_utc_offset(ct))
            return time.strftime(datefmt, ct)
        else:
            return super().formatTime(record)

    @staticmethod
    def _format_utc_offset(ct: time.struct_time) -> str:
        """Format the UTC offset of a `struct_time` as `+HHMM`/`-HHMM`, honoring `tm_gmtoff`

        :param ct: The converted `struct_time` whose `tm_gmtoff` provides the offset
        """
        offset = getattr(ct, "tm_gmtoff", None)
        if offset is None:
            offset = -(time.altzone if ct.tm_isdst > 0 else time.timezone)
        sign = "-" if offset < 0 else "+"
        offset = abs(offset)
        return f"{sign}{offset // 3600:02d}{offset % 3600 // 60:02d}"


class ColoredStreamHandler(logging.StreamHandler):  # type: ignore[type-arg]
    """`StreamHandler` that adds ANSI color to the formatted message based on the log level"""

    def format(self, record: LogRecord) -> str:
        """Add ANSI color code to record based on the level number, or one that is explicitly specified

        No color is added when the `NO_COLOR` environment variable is present, or when this handler's stream is
        not a terminal (eg. output is piped, redirected, or a file). Set `FORCE_COLOR` to a truthy value (other
        than `0`/`false`) to colorize regardless, or to `0`/`false` to force it off even on a terminal.
        """
        msg = super().format(record)
        if not self._should_colorize():
            return msg
        color_code = getattr(record, CustomLoggingArgs.COLOR_CODE, None) or self._get_color_code(record.levelno)
        return color(msg, color_code=color_code)

    def _should_colorize(self) -> bool:
        """Determine whether ANSI color should be applied to the formatted output"""
        force_color = os.environ.get("FORCE_COLOR")
        if force_color is not None:
            return force_color.strip().lower() not in ("", "0", "false")
        if "NO_COLOR" in os.environ:
            return False
        isatty = getattr(self.stream, "isatty", None)
        return isatty() if isatty is not None else False

    @staticmethod
    def _get_color_code(level: int) -> str | None:
        if level >= logging.ERROR:
            return ColorCodes.RED
        elif level >= logging.WARNING:
            return ColorCodes.YELLOW
        elif level >= logging.INFO:
            return None
        elif level >= logging.DEBUG:
            return ColorCodes.DARK_GREY
        else:
            return ColorCodes.DEFAULT


def _validate_config(config: Any, name: str) -> None:
    """Validate that a logging config argument is a `Mapping`

    :param config: The value to validate
    :param name: The parameter name to reference in the error message
    """
    if not isinstance(config, Mapping):
        raise TypeError(f"`{name}` must be a Mapping, not {type(config).__name__}")
