import inspect
import keyword
import os
import re
import sys
import time
from collections.abc import Callable, Iterable
from copy import deepcopy
from typing import Any

from common_libs.ansi_colors import ColorCodes, color
from common_libs.logging import get_logger

logger = get_logger(__name__)


def prompt_confirmation(prompt: str, yes: str = "Y", no: str = "N") -> None:
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

    def merge(a: Any, b: Any) -> Any:
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

    def handle_newlines(lines: str) -> str:
        if "\n" in lines:
            inner_style = (indent + len(style)) * space
            offset = indent + len(style) + len(space)
            return list_items(lines.splitlines(), style=inner_style)[offset:]
        else:
            return lines

    return "\n".join(f"{' ' * indent}{style}{space}{handle_newlines(str(x))}" for x in obj)


def log_section(string: str, color_code: str = ColorCodes.GREEN, sub_section: bool = False) -> None:
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

    def format_line(string: str) -> str:
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
    func: Callable[..., Any],
    func_args: tuple[Any, ...] | None = None,
    func_kwargs: dict[str, Any] | None = None,
    interval: float = 2,
    stop_condition: Callable[..., Any] | None = None,
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


def is_decorator_with_args(decorator: Any) -> bool:
    """Check if the given decorator is a regular decorator or a decorator that takes arguments

    :param decorator: A decorator function

    eg.
    >>> from functools import wraps
    >>> def decorator(f):
    >>>     @wraps(f)
    >>>     def wrapper(*args, **kwargs):
    >>>         return f(*args, **kwargs)
    >>>     return wrapper
    >>>
    >>> def decorator_with_args(arg1, /, *, arg2, **kwargs):
    >>>     def decorator(f):
    >>>         @wraps(f)
    >>>         def wrapper(*args, **kwargs):
    >>>             return f(*args, **kwargs)
    >>>         return wrapper
    >>>     return decorator
    >>>
    >>> is_decorator_with_args(decorator)
    False
    >>> is_decorator_with_args(decorator_with_args)
    True
    """
    dummy_orig_func_result = object()
    dummy_orig_func = lambda *args, **kwargs: dummy_orig_func_result

    def generate_callable_args(f: Callable[..., Any]) -> tuple[tuple[Any, ...], dict[str, Any]]:
        """Generate callable args and kwargs that match with the given function's signature"""
        args = []
        kwargs = {}
        sig_params = inspect.signature(f).parameters.values()
        for i, p in enumerate(sig_params):
            # generate fake args/kwargs to match with the function signature. If the signature has only one parameter,
            # we always assume that it may be for the original function (if not, the value doesn't matter).
            v = dummy_orig_func if len(sig_params) == 1 else "dummy_value"
            match (p.name, p.kind):
                case (_, p.POSITIONAL_OR_KEYWORD | p.POSITIONAL_ONLY | p.VAR_POSITIONAL):
                    args.append(v)
                case (param_name, p.KEYWORD_ONLY):
                    kwargs[param_name] = v
                case (_, p.VAR_KEYWORD):
                    ...
                case _:
                    raise NotImplementedError(
                        f"Unsupported signature parameter kind: {p.kind}. Please update this code"
                    )
        return tuple(args), kwargs

    if callable(decorator):
        # Get the decorator's wrapper function
        deco_args, deco_kwargs = generate_callable_args(decorator)
        wrapper_func = decorator(*deco_args, **deco_kwargs)
        if not callable(wrapper_func):
            # This is not a decorator
            return False

        if getattr(wrapper_func, "__wrapped__", None) is dummy_orig_func:
            # This is a regular decorator with @wraps(f) on the wrapper function. No need to look further
            return False

        # Call the wrapper function to see if the returned value is from the dummy function. If the decorator takes
        # arugments, the returned value should be another wrapper function
        wrapper_args, wrapper_kwargs = generate_callable_args(wrapper_func)
        wrapper_func_result = wrapper_func(*wrapper_args, **wrapper_kwargs)
        return wrapper_func_result is not dummy_orig_func_result
    else:
        return False
