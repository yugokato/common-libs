"""Tests for common_libs.containers.utils.log_parser module"""

import json
import re
from typing import Any

from common_libs.ansi_colors import ColorCodes
from common_libs.containers.utils.log_parser import (
    _get_log_color,
    does_log_match_filters,
    parse_json_logs,
    parse_streamed_json_logs,
    parse_streamed_logs,
)


class TestDoesLogMatchFilters:
    """Tests for does_log_match_filters function"""

    def test_simple_string_match(self) -> None:
        """Test matching a simple string value"""
        log = {"levelname": "INFO", "message": "hello"}
        assert does_log_match_filters(log, {"levelname": "INFO"}) is True

    def test_simple_string_no_match(self) -> None:
        """Test that non-matching string returns False"""
        log = {"levelname": "INFO", "message": "hello"}
        assert does_log_match_filters(log, {"levelname": "ERROR"}) is False

    def test_missing_key_returns_false(self) -> None:
        """Test that a missing key in the log returns False"""
        log = {"message": "hello"}
        assert does_log_match_filters(log, {"levelname": "INFO"}) is False

    def test_string_not_prefix(self) -> None:
        """Test matching with NOT prefix"""
        log = {"levelname": "INFO"}
        assert does_log_match_filters(log, {"levelname": "NOT ERROR"}) is True
        assert does_log_match_filters(log, {"levelname": "NOT INFO"}) is False

    def test_string_wildcard_match(self) -> None:
        """Test matching with wildcard *"""
        log = {"message": "hello world"}
        assert does_log_match_filters(log, {"message": "hello*"}) is True
        assert does_log_match_filters(log, {"message": "*world"}) is True
        assert does_log_match_filters(log, {"message": "bye*"}) is False

    def test_string_not_with_wildcard(self) -> None:
        """Test matching with NOT prefix combined with wildcard *"""
        log = {"message": "hello world"}
        assert does_log_match_filters(log, {"message": "NOT bye*"}) is True
        assert does_log_match_filters(log, {"message": "NOT hello*"}) is False

    def test_numeric_comparison_lte(self) -> None:
        """Test numeric comparison with <="""
        log = {"status_code": 200}
        assert does_log_match_filters(log, {"status_code": "<= 200"}) is True
        assert does_log_match_filters(log, {"status_code": "<= 199"}) is False

    def test_numeric_comparison_gte(self) -> None:
        """Test numeric comparison with >="""
        log = {"status_code": 500}
        assert does_log_match_filters(log, {"status_code": ">= 400"}) is True
        assert does_log_match_filters(log, {"status_code": ">= 501"}) is False

    def test_digit_string_matches_int(self) -> None:
        """Test that digit string matches integer log value"""
        log = {"status_code": 200}
        assert does_log_match_filters(log, {"status_code": "200"}) is True
        assert does_log_match_filters(log, {"status_code": "404"}) is False

    def test_regex_pattern_match(self) -> None:
        """Test matching with compiled regex pattern"""
        log = {"message": "error: something failed"}
        assert does_log_match_filters(log, {"message": re.compile(r"error: .*")}) is True
        assert does_log_match_filters(log, {"message": re.compile(r"^success")}) is False

    def test_multiple_filters_all_must_match(self) -> None:
        """Test that all filters must match"""
        log = {"levelname": "ERROR", "status_code": 500}
        assert does_log_match_filters(log, {"levelname": "ERROR", "status_code": 500}) is True
        assert does_log_match_filters(log, {"levelname": "ERROR", "status_code": 200}) is False

    def test_exception_returns_false(self) -> None:
        """Test that exception during filter returns False"""
        log = {"status_code": 200}
        # Passing non-standard filter type that causes an exception inside apply_filter
        assert does_log_match_filters(log, {"status_code": object()}) is False


class TestGetLogColor:
    """Tests for _get_log_color private function"""

    def test_error_level_returns_red(self) -> None:
        """Test that ERROR level returns RED color"""
        assert _get_log_color("ERROR") == ColorCodes.RED

    def test_fatal_level_returns_red(self) -> None:
        """Test that FATAL level returns RED color"""
        assert _get_log_color("FATAL") == ColorCodes.RED

    def test_critical_level_returns_red(self) -> None:
        """Test that CRITICAL level returns RED color"""
        assert _get_log_color("CRITICAL") == ColorCodes.RED

    def test_warning_level_returns_yellow(self) -> None:
        """Test that WARNING level returns YELLOW color"""
        assert _get_log_color("WARNING") == ColorCodes.YELLOW

    def test_debug_level_returns_dark_grey(self) -> None:
        """Test that DEBUG level returns DARK_GREY color"""
        assert _get_log_color("DEBUG") == ColorCodes.DARK_GREY

    def test_info_level_returns_none(self) -> None:
        """Test that INFO level returns None"""
        assert _get_log_color("INFO") is None

    def test_unknown_level_returns_none(self) -> None:
        """Test that unknown level returns None"""
        assert _get_log_color("TRACE") is None


class TestParseJsonLogs:
    """Tests for parse_json_logs function"""

    def test_basic_json_log(self) -> None:
        """Test parsing a basic JSON log line"""
        log_line = json.dumps({"levelname": "INFO", "message": "hello"})
        result = parse_json_logs(log_line)
        assert '"message"' in result
        assert '"hello"' in result

    def test_non_json_line_passthrough(self) -> None:
        """Test that non-JSON lines are passed through unchanged"""
        text = "plain text log line"
        result = parse_json_logs(text)
        assert result == text

    def test_error_log_gets_color(self) -> None:
        """Test that ERROR level logs get color applied"""
        log_line = json.dumps({"levelname": "ERROR", "message": "something failed"})
        result = parse_json_logs(log_line)
        # RED color code should be in the result
        assert ColorCodes.RED in result

    def test_warning_log_gets_color(self) -> None:
        """Test that WARNING level logs get color applied"""
        log_line = json.dumps({"levelname": "WARNING", "message": "watch out"})
        result = parse_json_logs(log_line)
        assert ColorCodes.YELLOW in result

    def test_filter_excludes_non_matching_logs(self) -> None:
        """Test that filters exclude non-matching logs"""
        logs = "\n".join(
            [
                json.dumps({"levelname": "ERROR", "service": "api"}),
                json.dumps({"levelname": "INFO", "service": "api"}),
            ]
        )
        result = parse_json_logs(logs, filters={"levelname": "ERROR"})
        assert "ERROR" in result
        assert "INFO" not in result

    def test_formatter_applied(self) -> None:
        """Test that formatter is applied to each log"""
        log_line = json.dumps({"levelname": "INFO", "message": "hello"})
        result = parse_json_logs(log_line, formatter="{levelname}: {message}")
        assert result == "INFO: hello"

    def test_multiple_lines(self) -> None:
        """Test parsing multiple JSON log lines returns both messages"""
        logs = "\n".join(
            [
                json.dumps({"levelname": "INFO", "message": "first"}),
                json.dumps({"levelname": "ERROR", "message": "second"}),
            ]
        )
        result = parse_json_logs(logs)
        assert '"first"' in result
        assert '"second"' in result


class TestParseStreamedLogs:
    """Tests for parse_streamed_logs function"""

    def test_basic_streamed_logs(self) -> None:
        """Test that streamed byte chunks are decoded and yielded"""
        chunks = [b"line1\n", b"line2\n", b"line3\n"]
        result = list(parse_streamed_logs(iter(chunks)))
        assert "line1" in result
        assert "line2" in result
        assert "line3" in result

    def test_empty_chunks_skipped(self) -> None:
        """Test that empty chunks are skipped"""
        chunks = [b"line1\n", b"   \n", b"line2\n"]
        result = list(parse_streamed_logs(iter(chunks)))
        assert len(result) == 2

    def test_color_applied_for_error(self) -> None:
        """Test that ERROR level lines get color applied"""
        chunks = [b"ERROR: something went wrong\n"]
        result = list(parse_streamed_logs(iter(chunks)))
        assert len(result) == 1
        assert ColorCodes.RED in result[0]


class TestParseStreamedJsonLogs:
    """Tests for parse_streamed_json_logs function"""

    def test_basic_streamed_json_logs(self) -> None:
        """Test that streamed JSON log chunks are parsed"""
        log_dict: dict[str, Any] = {"levelname": "INFO", "message": "hello"}
        chunk = json.dumps(log_dict).encode("utf-8") + b"\n"
        result = list(parse_streamed_json_logs(iter([chunk])))
        assert len(result) == 1

    def test_streamed_filter_applied(self) -> None:
        """Test that filters are applied to streamed JSON logs"""
        error_log: dict[str, Any] = {"levelname": "ERROR", "service": "api"}
        info_log: dict[str, Any] = {"levelname": "INFO", "service": "api"}
        chunks = [
            json.dumps(error_log).encode("utf-8") + b"\n",
            json.dumps(info_log).encode("utf-8") + b"\n",
        ]
        result = list(parse_streamed_json_logs(iter(chunks), filters={"levelname": "ERROR"}))
        assert len(result) == 1
        assert "ERROR" in result[0]
