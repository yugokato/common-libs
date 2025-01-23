import json
import re
from collections import defaultdict
from collections.abc import Iterator
from typing import Any

from common_libs.ansi_colors import ColorCodes, color, remove_color_code
from common_libs.logging import get_logger

logger = get_logger(__name__)


def parse_streamed_logs(logs: Iterator[bytes]) -> str:
    """Parse streamed logs and apply color based on the log level

    :param logs: A generator for streaming logs.

    NOTE: Each chunk/log may contain color code(s)
    """

    for chunk in logs:
        if chunk.rstrip():
            decoded_chunk = chunk.decode("utf-8")
            for line in decoded_chunk.splitlines():
                if color_code := _get_log_color(line):
                    line = _apply_color(line, color_code)
                yield line


def parse_json_logs(logs: str, filters: dict[str, Any] = None, formatter: str = None) -> str:
    """Parse JSON string logs and apply color based on the log level

    :param logs: Original logs
    :param filters: Filter JSON string logs with specified key/value pairs
    :param formatter: A formatter to apply to each JSON object. We apply formatter.format_map(log) to return a
                      formatted log line

    NOTE: Each chunk/log may contain color code(s)
    """
    lines = []
    for line in logs.splitlines():
        try:
            log = json.loads(line, strict=False)
        except json.decoder.JSONDecodeError:
            lines.append(line)
        else:
            if isinstance(log, dict):
                if filters and not does_log_match_filters(log, filters):
                    continue
                color_code = None
                if log_level := log.get("levelname"):
                    # NOTE: The log_level value (or even "levelname" key) can be wrapped with ANSI color code
                    color_code = _get_log_color(log_level)
                formatted_log = _format_log(line, log, formatter=formatter)
                if color_code:
                    formatted_log = _apply_color(formatted_log, color_code)
                lines.append(formatted_log)
            else:
                logger.warning(f"Unable to parse as a dictionary: {line}")
                lines.append(log)
    return "\n".join(lines)


def parse_streamed_json_logs(logs: Iterator[bytes], filters: dict[str, Any] = None, formatter: str = None) -> str:
    """Parse streamed JSON logs. A color will be applied based on the log level

    A long output can be streamed as multiple lines in a chunk, or even in multiple chunks. We buffer incomplete
    lines until it can be parsed as a complete JSON object.

    :param logs: Generator to stream logs. Note that each chunk can contain color code(s)
    :param filters: Filter JSON string logs with specified key/value pairs
    :param formatter: A formatter to apply to each JSON object. We apply formatter.format_map(log) to return a
                      formatted log line

    NOTE: Each chunk/log may contain color code(s)
    """

    class LogUnmatched(Exception): ...

    def parse_json_string_line(line: str):
        parsed_log: dict[str, Any] = json.loads(line, strict=False)

        if isinstance(parsed_log, dict):
            if filters and not does_log_match_filters(parsed_log, filters):
                raise LogUnmatched
        else:
            # Apparently not all lines are guaranteed to be a JSON log. Sometimes this can be just an integer for some
            # reason
            logger.warning(f"Unable to parse as a dictionary: {line}")
            return line

        color_code = None
        if log_level := parsed_log.get("levelname"):
            # NOTE: The log_level value (or even "levelname" key) can be wrapped with ANSI color code
            color_code = _get_log_color(log_level)

        formatted_line = _format_log(line, parsed_log, formatter=formatter)

        if color_code:
            formatted_line = _apply_color(formatted_line, color_code)

        return formatted_line

    buffered_lines = ""
    buffered_chunks = b""
    for chunk in logs:
        if chunk.rstrip():
            try:
                decoded_chunk = (buffered_chunks + chunk).decode("utf-8")
            except UnicodeDecodeError:
                # The chunk is imcomplete
                # TODO: Check if this handling is right
                buffered_chunks += chunk
                continue

            buffered_chunks = b""
            for line in decoded_chunk.splitlines():
                try:
                    line = parse_json_string_line(line)
                except json.decoder.JSONDecodeError:
                    buffered_lines += line
                except LogUnmatched:
                    pass
                else:
                    # To avoid accidentally keep buffering, clear buffer after the first successful JSON parsing
                    buffered_lines = ""
                    yield line

        if buffered_lines:
            try:
                line = parse_json_string_line(buffered_lines)
            except json.decoder.JSONDecodeError:
                # The buffered line is still incomplete as JSON. Will carry this over to the next chunk
                pass
            except LogUnmatched:
                buffered_lines = ""
            else:
                buffered_lines = ""
                yield line


def does_log_match_filters(json_log: dict[str, Any], filters: dict[str, Any]) -> bool:
    """Check if a JSON log matches the given filters"""

    def apply_filter(k: str, v: Any, log: dict[str, Any], is_negation: bool = False) -> bool:
        if k not in log:
            return False

        log_value = log[k]
        matched = None
        if isinstance(v, re.Pattern):
            matched = bool(re.match(v, log_value))
        elif (
            isinstance(log_value, int)
            and isinstance(v, str)
            and any(c in filter_v and filter_v.split(c)[1].strip().isdigit() for c in ["<=", ">=", "<", ">"])
        ):
            matched = eval(f"{log_value}{v}") is True
        elif isinstance(v, str):
            if v.startswith("NOT "):
                matched = apply_filter(k, v.replace("NOT ", ""), log, is_negation=True)
            elif "*" in v:
                pattern = re.escape(filter_v).replace("\\*", ".*")
                matched = bool(re.match(f"^{pattern}$", log_value))
            elif v.isdigit():
                matched = any([log_value == int(v), log_value == v])
        elif isinstance(v, bool) and log_value in ["True", "False"]:
            matched = eval(f"{log} is {v}")

        if matched is None:
            matched = log_value == v

        if is_negation:
            return not matched
        else:
            return matched

    try:
        for filter_k, filter_v in filters.items():
            if not apply_filter(filter_k, filter_v, json_log):
                return False
    except Exception as e:
        logger.error(f"Encountered an error while applying a filter:\n{type(e).__name__}: {str(e)}", exc_info=e)
        return False
    return True


def _get_log_color(log_part: str) -> str:
    """Return a color code based on the log level in the log or partial log"""
    if any(level in log_part for level in ["ERROR", "FATAL", "CRITICAL"]):
        color_code = ColorCodes.RED
    elif "WARNING" in log_part:
        color_code = ColorCodes.YELLOW
    elif "DEBUG" in log_part:
        color_code = ColorCodes.DARK_GREY
    else:
        color_code = None
    return color_code


def _format_log(original_line: str, parsed_log: dict[str, Any], formatter: str = None):
    if formatter:
        try:
            formatted_line = formatter.format_map(parsed_log)
        except KeyError:
            if any("\x1b[" in key for key in parsed_log.keys()):
                logger.warning(
                    "One or more log keys referenced by the formatter contain color code. The color will be removed"
                )
                parsed_log_no_color = json.loads(remove_color_code(original_line), strict=False)
                formatted_line = formatter.format_map(parsed_log_no_color)
            else:
                # Ignore missing key(s)
                formatted_line = formatter.format_map(defaultdict(str, parsed_log))
    else:
        # color codes are escaped in the dumped string. Unescape these so that output will be properly colored
        formatted_line = json.dumps(parsed_log, indent=4).replace("\\u001b", "\u001b")

    return formatted_line


def _apply_color(line: str, color_code: str):
    """Apply a color to the line that may/may not contain text(s) with another color.

    Existing colored texts will be preserved as is on top of the colored line by replacing all existing "default"
    color code in the middle of the line with the new color code.
    """
    default_color_code_pattern = rf"{re.escape(ColorCodes.DEFAULT)}|{re.escape(ColorCodes.DEFAULT2)}"
    return color(re.sub(default_color_code_pattern, color_code, line), color_code=color_code)
