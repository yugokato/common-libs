from __future__ import annotations

import logging
import time
from enum import StrEnum, auto
from logging import LogRecord, config
from pathlib import Path

import yaml

from common_libs.ansi_colors import ColorCodes, color


def setup_logging(config_path: str | Path, delta_config_path: str | Path = None) -> None:
    """Setup logging

    :param config_path: File path to a base logging config (.yaml)
    :param delta_config_path: File path to a delta logging config (.yaml) to merge onto the base config
    """
    with open(config_path) as f:
        log_cfg = yaml.safe_load(f)

    if delta_config_path:
        from common_libs.utils import merge_dicts

        with open(delta_config_path) as f:
            delta_log_cfg = yaml.safe_load(f)
        log_cfg = merge_dicts(delta_log_cfg, log_cfg)

    config.dictConfig(log_cfg)


def get_logger(name: str) -> LoggerAdapter:
    """Return a logger for the specified name

    :param name: Logger name
    """
    logger = logging.getLogger(name)
    return LoggerAdapter(logger)


class CustomLoggingArgs(StrEnum):
    """Custom logging arguments"""

    def _generate_next_value_(name, start, count, last_values):
        return name.lower()

    COLOR_CODE = auto()
    # TODO: Add more if needed


class LoggerAdapter(logging.LoggerAdapter):
    """Custom LoggerAdapter"""

    def process(self, msg, kwargs):
        """Support custom arguments to logging calls, and add various fields to log extra

        eg. logger.info("message", color_code=ColorCodes.GREEN)
        """
        # NOTE: LoggerAdapter.process() seems to ignore `extra` given to `kwargs` in a log call.
        # (https://github.com/python/cpython/issues/76913)
        # We will fix this behavior by explicitly merging it with the self.extra
        # TODO: Switch to use the new `merge_extra=True` init option after Python 3.13
        extra = (self.extra or {}) | (kwargs.get("extra") or {})
        for custom_arg in CustomLoggingArgs:
            if custom_arg in kwargs:
                extra.update(**{custom_arg: kwargs.pop(custom_arg)})
        kwargs["extra"] = extra
        return msg, kwargs


class ColoredStreamHandler(logging.StreamHandler):
    """Colored StreamHandler"""

    @classmethod
    def _get_color_code(cls, level: int) -> str | None:
        if level >= logging.CRITICAL:
            return ColorCodes.RED
        elif level >= logging.ERROR:
            return ColorCodes.RED
        elif level >= logging.WARNING:
            return ColorCodes.YELLOW
        elif level >= logging.INFO:
            return None
        elif level >= logging.DEBUG:
            return ColorCodes.DARK_GREY
        else:
            return ColorCodes.DEFAULT

    def format(self, record):
        """Add ANSI color code to record based on the level number, or one that is explicitly specified"""
        color_code = getattr(record, CustomLoggingArgs.COLOR_CODE, None) or self._get_color_code(record.levelno)
        msg = super().format(record)
        return color(msg, color_code=color_code)


class LogFilter(logging.Filter):
    def filter(self, record) -> bool:
        for custom_arg in CustomLoggingArgs:
            if hasattr(record, custom_arg):
                delattr(record, custom_arg)

        return True


class LogFormatter(logging.Formatter):
    def formatTime(self, record: LogRecord, datefmt: str = None) -> str:
        """Overrides the default behavior to support both %f and %z in datefmt

        eg. datefmt="%Y-%m-%dT%H:%M:%S.%f%z" will display the timestamp as 2022-01-01T11:22:33.444-0000
        """
        if datefmt:
            ct = self.converter(record.created)
            datefmt = datefmt.replace("%f", "%03d" % int(record.msecs))
            datefmt = datefmt.replace("%z", time.strftime("%z"))
            return time.strftime(datefmt, ct)
        else:
            return super().formatTime(record)
