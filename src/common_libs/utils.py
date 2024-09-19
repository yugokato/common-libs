import keyword
import os
import re
import sys
import time
from copy import deepcopy
from typing import Any, Callable, Iterable

from common_libs.ansi_colors import ColorCodes, color
from common_libs.logging import get_logger

logger = get_logger(__name__)


def prompt_confirmation(prompt: str, yes: str = "Y", no: str = "N"):
    """Show a confirmation prompt before moving forward"""
    try:
        while (v := input(color(prompt + f" ({yes}/{no}): ", color_code=ColorCodes.RED))) != yes:
            if v == no:
                raise KeyboardInterrupt
    except KeyboardInterrupt:
        sys.exit()


def merge_dicts(dict1: dict[str, Any], dict2: dict[str, Any]) -> dict[str, Any]:
    """Merge two dictionaries

    :param dict1: The base dictionary
    :param dict2: Another dictionary
    :merge_list: Merge list items of dict1 and dict2. Defaults to overwrite with dict1's value
    """

    def merge(a: Any, b: Any):
        if isinstance(b, dict):
            for k, v in b.items():
                if k in a:
                    merge(a[k], v)
                else:
                    a[k] = v
        return a

    return merge(deepcopy(dict2), deepcopy(dict1))


def list_items(obj: Iterable[Any], style: str = "-", indent: int = 0) -> str:
    """List items as string value

    :param obj: Objects to list
    :param style: Style of the bullet
    :param indent: indentation level
    """
    space = " "

    def handle_newlines(lines):
        if "\n" in lines:
            inner_style = (indent + len(style)) * space
            offset = indent + len(style) + len(space)
            return list_items(lines.splitlines(), style=inner_style)[offset:]
        else:
            return lines

    return "\n".join(f"{' ' * indent}{style}{space}{handle_newlines(str(x))}" for x in obj)


def log_section(string: str, color_code: str = ColorCodes.GREEN, sub_section: bool = False):
    """Log given string as a styled section

    A section/sub-section will look like this:
        - section:
            2021-01-01 11:22:33.444 -
            ##################################################
            # The string                                     #
            ##################################################

        - sub-section:
            2021-01-01 11:22:33.444 -
            ------------------- The string -------------------

    :param string: String to log as a section
    :param color_code: ANSI color code
    :param sub_section: Log as a sub-section
    """
    len_padding = 4
    try:
        terminal_size_col = os.get_terminal_size().columns
    except OSError:
        terminal_size_col = 130

    def format_line(string):
        filler = " " * (terminal_size_col - (len(string) + len_padding))
        return f"# {string}{filler} #\n"

    if sub_section:
        sub_section_filler = "-" * int((terminal_size_col - (len(string) + 2)) / 2)
        logger.info(f"\n{sub_section_filler} {string} {sub_section_filler}", color_code=color_code)
    else:
        len_max_line_str = terminal_size_col - len_padding
        section = "#" * terminal_size_col

        msg = ""
        for line in string.split("\n"):
            line_msg = ""
            for word in line.split(" "):
                if len(f"{line_msg}{word} ") <= len_max_line_str:
                    line_msg += word + " "
                else:
                    msg += format_line(line_msg)
                    line_msg = f"{word} "
            else:
                # the last line
                msg += format_line(line_msg)

        logger.info(f"\n{section}\n{msg}{section}", color_code=color_code)


def clean_obj_name(name: str) -> str:
    """Convert the name to a legal Python object name

    - Illegal values will be converted to "_" (multiple illegal values in a row will be converted to single "_")
    - If the name starts with a number, "_" will be added at the beginning

    :param name: The original value
    """
    pattern_illegal_chars = r"\W+|^(?=\d)"
    has_illegal_chars = re.search(pattern_illegal_chars, name)
    is_reserved_name = keyword.iskeyword(name)
    if has_illegal_chars:
        name = re.sub(pattern_illegal_chars, "_", name)
    elif is_reserved_name:
        name = f"_{name}"

    return name


def wait_until(
    func: Callable,
    func_args: tuple[Any] = None,
    func_kwargs: dict[str, Any] = None,
    interval: float = 2,
    stop_condition: Callable = None,
    timeout: float = 30,
) -> Any:
    """Wait until the return value of the given polling function matches the expected condition, or raises a
    TimeoutError when specified wait time exceeds.

    :param func: A function to periodically check returned value with
    :param func_args: Positional arguments for the function
    :param func_kwargs: Keyword arguments for the function
    :param interval: An interval in second to check the result
    :param stop_condition: A condition to stop waiting. Defaults to whether the returned value can be evaluated as True
    :param timeout: Max time in seconds to wait
    """
    if not func_args:
        func_args = tuple()
    if not func_kwargs:
        func_kwargs = {}
    if not stop_condition:
        # Stop when any result is returned
        stop_condition = lambda x: x is not None

    start_time = time.time()
    while time.time() - start_time < timeout:
        ret_val = func(*func_args, **func_kwargs)
        if stop_condition(ret_val):
            return ret_val
        else:
            time.sleep(interval)
    raise TimeoutError(f"Waited for {timeout} seconds but the polling result did not match the expected condition")
