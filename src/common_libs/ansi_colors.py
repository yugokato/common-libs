import os
import re
import sys
from typing import IO, Any

PATTERN_COLOR_CODE = r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])"


class ColorCodes:
    DEFAULT = "\x1b[0m"
    DEFAULT2 = "\x1b[m"
    BLACK = "\x1b[30m"
    WHITE = "\x1b[97m"
    RED = "\x1b[31m"
    GREEN = "\x1b[32m"
    YELLOW = "\x1b[33m"
    BLUE = "\x1b[34m"
    MAGENTA = "\x1b[35m"
    CYAN = "\x1b[36m"
    DARK_GREY = "\x1b[90m"
    LIGHT_RED = "\x1b[91m"
    LIGHT_GREEN = "\x1b[92m"
    LIGHT_YELLOW = "\x1b[93m"
    LIGHT_BLUE = "\x1b[94m"
    LIGHT_MAGENTA = "\x1b[95m"
    LIGHT_CYAN = "\x1b[96m"

    # text styles
    BOLD = "\x1b[1m"
    UNDERLINE = "\x1b[4m"
    BLINK = "\x1b[5m"
    NEGATIVE = "\x1b[7m"


def should_color(stream: IO[str] | None = None) -> bool:
    """Determine whether ANSI color codes should be applied for the given stream

    `FORCE_COLOR` wins when set (any value other than `0`/`false`, case-insensitive, enables color, a falsy
    value disables it even on a terminal). Otherwise, the presence of the `NO_COLOR` environment variable
    disables color. Otherwise, color is enabled only when the stream is a terminal.

    :param stream: The stream color output would be written to. Defaults to `sys.stdout`
    """
    if stream is None:
        stream = sys.stdout
    force_color = os.environ.get("FORCE_COLOR")
    if force_color is not None:
        return force_color.strip().lower() not in ("", "0", "false")
    if "NO_COLOR" in os.environ:
        return False
    isatty = getattr(stream, "isatty", None)
    return isatty() if isatty is not None else False


def color(
    text: Any,
    color_code: str | None = ColorCodes.GREEN,
    bold: bool = False,
    underline: bool = False,
    escape: bool = False,
) -> str:
    """Add ANSI color code to string

    No color is added when `should_color()` (checked against `sys.stdout`) is `False`, eg. when `NO_COLOR` is
    set or output isn't a terminal.

    :param text: The original text to color
    :param color_code: ANSI color code
    :param bold: Bold text
    :param underline: Underline text
    :param escape: Escape each ansi color code (need for terminal prompt)
    """
    if not isinstance(text, str):
        text = str(text)
    if not should_color():
        return text

    colored_str = text
    if bold:
        colored_str = ColorCodes.BOLD + colored_str
    if underline:
        colored_str = ColorCodes.UNDERLINE + colored_str
    if color_code:
        colored_str = color_code + colored_str
    if bold or color_code:
        colored_str += ColorCodes.DEFAULT
    if escape:
        colored_str = escape_color_code(colored_str)
    return colored_str


def remove_color_code(string: str) -> str:
    """Remove ANSI color code"""
    return re.sub(PATTERN_COLOR_CODE, "", string)


def escape_color_code(string: str) -> str:
    """Escape each ANSI color code with "\x01" and "\x02".

    This is needed for the terminal history with arrow keys to work properly
    https://github.com/python/cpython/issues/64558
    """
    return re.sub(f"({PATTERN_COLOR_CODE})", "\x01" + r"\1" + "\x02", string)
