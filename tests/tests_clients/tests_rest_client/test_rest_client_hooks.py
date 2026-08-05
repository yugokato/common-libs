"""Tests for common_libs.clients.rest_client.hooks module"""

import logging
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
from pytest_mock import MockFixture

from common_libs.clients.rest_client.hooks import (
    _print_api_summary,
    get_hooks,
    inject_hooks,
    request_hooks,
    response_hooks,
)
from common_libs.clients.rest_client.rest_client import RestClient
from common_libs.clients.rest_client.types import Request
from common_libs.clients.rest_client.utils import TRUNCATE_LEN, format_request_failure

HOOKS_LOGGER_NAME = "common_libs.clients.rest_client.hooks"


def _not_found_handler(request: httpx.Request) -> httpx.Response:
    """Serve a canned 404 response"""
    return httpx.Response(
        404, stream=httpx.ByteStream(b'{"error": "not found"}'), headers={"Content-Type": "application/json"}
    )


class TestInjectHooks:
    """Tests for inject_hooks decorator"""

    def test_inject_hooks_adds_hooks_to_extensions(self) -> None:
        """Test that inject_hooks adds request and response hooks to kwargs"""
        injected_kwargs: dict[str, Any] = {}

        @inject_hooks
        def dummy(self: RestClient, **kwargs: Any) -> dict[str, Any]:
            injected_kwargs.update(kwargs)
            return kwargs

        client = RestClient("http://example.com")
        dummy(client)

        assert "extensions" in injected_kwargs
        hooks = injected_kwargs["extensions"]["hooks"]
        assert "request" in hooks
        assert "response" in hooks

    def test_inject_hooks_pops_quiet_kwarg(self) -> None:
        """Test that inject_hooks removes 'quiet' from kwargs before passing to function"""
        received_kwargs: dict[str, Any] = {}

        @inject_hooks
        def dummy(self: RestClient, **kwargs: Any) -> dict[str, Any]:
            received_kwargs.update(kwargs)
            return kwargs

        client = RestClient("http://example.com")
        dummy(client, quiet=True)

        assert "quiet" not in received_kwargs


class TestGetHooks:
    """Tests for get_hooks function"""

    def test_returns_dict_with_request_and_response(self) -> None:
        """Test that get_hooks returns a dict with request and response keys"""
        client = RestClient("http://example.com")
        hooks = get_hooks(client, quiet=False)
        assert "request" in hooks
        assert "response" in hooks
        assert isinstance(hooks["request"], list)
        assert isinstance(hooks["response"], list)
        assert len(hooks["request"]) == 1
        assert len(hooks["response"]) == 1

    def test_hooks_are_callable(self) -> None:
        """Test that returned hooks are callable"""
        client = RestClient("http://example.com")
        hooks = get_hooks(client, quiet=False)
        assert callable(hooks["request"][0])
        assert callable(hooks["response"][0])

    def test_cached_for_same_client_and_quiet(self) -> None:
        """Test that same client and quiet value return cached result"""
        client = RestClient("http://example.com")
        hooks1 = get_hooks(client, quiet=False)
        hooks2 = get_hooks(client, quiet=False)
        assert hooks1 is hooks2

    def test_different_quiet_values_different_result(self) -> None:
        """Test that different quiet values produce different hook sets"""
        client = RestClient("http://example.com")
        hooks_quiet = get_hooks(client, quiet=True)
        hooks_verbose = get_hooks(client, quiet=False)
        assert hooks_quiet is not hooks_verbose

    def test_returns_empty_dict_when_log_requests_disabled(self) -> None:
        """Test that get_hooks returns an empty dict when log_requests is False, unless quiet is explicitly
        False
        """
        client = RestClient("http://example.com", log_requests=False)
        assert get_hooks(client, quiet=None) == {}
        assert get_hooks(client, quiet=True) == {}

    def test_explicit_quiet_false_overrides_log_requests_disabled(self) -> None:
        """Test that an explicit quiet=False re-enables hooks on a log_requests=False client"""
        client = RestClient("http://example.com", log_requests=False)
        assert get_hooks(client, quiet=False) != {}


class TestRequestHooks:
    """Tests for request_hooks function"""

    def test_request_hooks_logs_when_not_quiet(self, mock_hooks_logger: MagicMock, mocker: MockFixture) -> None:
        """Test that request_hooks logs the request when not quiet"""
        mock_request = mocker.MagicMock(spec=Request)
        mock_request.request_id = "hook-req-id"
        mock_request.method = "GET"
        mock_request.url = "http://example.com/api"
        mock_request.headers = {"Content-Type": "application/json"}
        mock_request.extensions = {}
        mock_request.read.return_value = b""

        request_hooks(mock_request, quiet=False)
        mock_hooks_logger.info.assert_called()

    def test_request_hooks_skips_when_quiet(self, mock_hooks_logger: MagicMock, mocker: MockFixture) -> None:
        """Test that request_hooks does not log when quiet=True"""
        mock_request = mocker.MagicMock(spec=Request)
        mock_request.request_id = "hook-req-id-quiet"

        request_hooks(mock_request, quiet=True)
        mock_hooks_logger.info.assert_not_called()


class TestResponseHooks:
    """Tests for response_hooks function"""

    def test_response_hooks_logs_success(
        self, mock_hooks_logger: MagicMock, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture
    ) -> None:
        """Test that response_hooks logs successful responses"""
        mocker.patch("common_libs.clients.rest_client.hooks._print_api_summary")

        mock_response = mock_response_factory(200)
        mock_client = mocker.MagicMock()
        mock_client.prettify_response_log = False

        response_hooks(mock_response, quiet=False, rest_client=mock_client)
        mock_hooks_logger.info.assert_called()

    def test_response_hooks_logs_a_one_line_summary_and_no_console_summary_when_quiet(
        self, mock_hooks_logger: MagicMock, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture
    ) -> None:
        """Test that response_hooks still logs an error response when quiet=True, reduced to the one-line
        failure summary, and skips the console summary entirely
        """
        mock_print_summary = mocker.patch("common_libs.clients.rest_client.hooks._print_api_summary")

        mock_response = mock_response_factory(500)
        mock_client = mocker.MagicMock()
        mock_client.prettify_response_log = False

        response_hooks(mock_response, quiet=True, rest_client=mock_client)

        mock_hooks_logger.error.assert_called_once()
        assert mock_hooks_logger.error.call_args[0][0] == format_request_failure(mock_response)
        mock_print_summary.assert_not_called()

    def test_response_hooks_logs_the_verbose_message_and_console_summary_when_not_quiet(
        self, mock_hooks_logger: MagicMock, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture
    ) -> None:
        """Test that response_hooks logs the verbose `response: <code> (<reason>)` message and still prints the
        console summary for an error response when quiet=False
        """
        mock_print_summary = mocker.patch("common_libs.clients.rest_client.hooks._print_api_summary")

        mock_response = mock_response_factory(500)
        mock_client = mocker.MagicMock()
        mock_client.prettify_response_log = False

        response_hooks(mock_response, quiet=False, rest_client=mock_client)

        mock_hooks_logger.error.assert_called_once()
        assert mock_hooks_logger.error.call_args[0][0] == "response: 500 (Error)"
        mock_print_summary.assert_called_once()


class TestHeaderMasking:
    """Tests that sensitive headers are masked in structured log records"""

    def _make_request(self, mocker: MockFixture, headers: dict[str, Any]) -> MagicMock:
        mock_request: MagicMock = mocker.MagicMock(spec=Request)
        mock_request.request_id = "req-mask-test"
        mock_request.method = "GET"
        mock_request.url = "http://example.com/api"
        mock_request.headers = headers
        mock_request.extensions = {}
        mock_request.read.return_value = b""
        return mock_request

    def test_request_log_extra_masks_authorization_header(
        self, mock_hooks_logger: MagicMock, mocker: MockFixture
    ) -> None:
        """Test that Authorization header is masked in the structured log extra for request hooks"""
        mock_request = self._make_request(
            mocker, {"Authorization": "Bearer secret", "Content-Type": "application/json"}
        )

        request_hooks(mock_request, quiet=False)

        logged_extra = mock_hooks_logger.info.call_args[1]["extra"]
        assert logged_extra["request_headers"]["Authorization"] == "***"
        assert logged_extra["request_headers"]["Content-Type"] == "application/json"

    def test_response_log_extra_masks_set_cookie_header(
        self,
        mock_hooks_logger: MagicMock,
        mock_response_factory: Callable[..., MagicMock],
        mocker: MockFixture,
    ) -> None:
        """Test that Set-Cookie header is masked in the structured log extra for response hooks"""
        mocker.patch("common_libs.clients.rest_client.hooks._print_api_summary")

        mock_response = mock_response_factory(200)
        mock_response.headers = {"Set-Cookie": "session=abc; HttpOnly", "Content-Type": "application/json"}
        mock_client = mocker.MagicMock()
        mock_client.prettify_response_log = False

        response_hooks(mock_response, quiet=False, rest_client=mock_client)

        logged_extra = mock_hooks_logger.info.call_args[1]["extra"]
        assert logged_extra["response_headers"]["Set-Cookie"] == "***"
        assert logged_extra["response_headers"]["Content-Type"] == "application/json"


class TestPayloadTruncation:
    """Tests that oversized payloads are truncated in API summary logs"""

    def _make_request(self, mocker: MockFixture, body: bytes) -> MagicMock:
        mock_request: MagicMock = mocker.MagicMock(spec=Request)
        mock_request.request_id = "trunc-req-id"
        mock_request.method = "POST"
        mock_request.url = "http://example.com/api"
        mock_request.headers = {"Content-Type": "application/json"}
        mock_request.extensions = {}
        mock_request.read.return_value = body
        return mock_request

    def test_large_json_payload_is_truncated_in_summary(self, mocker: MockFixture) -> None:
        """Test that a large JSON payload is truncated in the console summary"""
        mocker.patch("common_libs.clients.rest_client.hooks.logger.isEnabledFor", return_value=True)

        large_value = "v" * (TRUNCATE_LEN * 2)
        body = f'{{"key": "{large_value}"}}'.encode()
        mock_request = self._make_request(mocker, body)

        mock_response = mocker.MagicMock()
        mock_response.request = mock_request
        mock_response.status_code = 200
        mock_response.is_success = True
        mock_response.is_stream = False
        mock_response.elapsed.total_seconds.return_value = 0.1
        mock_response.headers = {}
        mock_response.reason_phrase = "OK"

        mock_client = mocker.MagicMock()
        mock_client.log_headers = False
        mock_client.prettify_response_log = False

        written: list[str] = []
        mocker.patch("sys.stdout.write", side_effect=written.append)
        mocker.patch("sys.stdout.flush")

        _print_api_summary(mock_response, rest_client=mock_client, processed_resp=None)

        output = "".join(written)
        assert "TRUNCATED" in output
        assert large_value not in output

    def test_small_payload_not_truncated_in_summary(self, mocker: MockFixture) -> None:
        """Test that a small payload passes through the summary unchanged"""
        mocker.patch("common_libs.clients.rest_client.hooks.logger.isEnabledFor", return_value=True)

        body = b'{"key": "short"}'
        mock_request = self._make_request(mocker, body)

        mock_response = mocker.MagicMock()
        mock_response.request = mock_request
        mock_response.status_code = 200
        mock_response.is_success = True
        mock_response.is_stream = False
        mock_response.elapsed.total_seconds.return_value = 0.1
        mock_response.headers = {}
        mock_response.reason_phrase = "OK"

        mock_client = mocker.MagicMock()
        mock_client.log_headers = False
        mock_client.prettify_response_log = False

        written: list[str] = []
        mocker.patch("sys.stdout.write", side_effect=written.append)
        mocker.patch("sys.stdout.flush")

        _print_api_summary(mock_response, rest_client=mock_client, processed_resp=None)

        output = "".join(written)
        assert "TRUNCATED" not in output
        assert "short" in output


class TestApiSummaryOptIn:
    """Tests that the console API summary is gated behind the logging opt-in"""

    def test_summary_silent_on_success_when_info_not_enabled(
        self, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture
    ) -> None:
        """Test that no summary is printed for a successful response when INFO logging is not enabled"""
        mocker.patch("common_libs.clients.rest_client.hooks.logger.isEnabledFor", return_value=False)

        mock_response = mock_response_factory(200)
        mock_response.request.read.return_value = b""
        mock_client = mocker.MagicMock()
        mock_client.log_headers = False
        mock_client.prettify_response_log = False

        written: list[str] = []
        mocker.patch("sys.stdout.write", side_effect=written.append)
        mocker.patch("sys.stdout.flush")

        _print_api_summary(mock_response, rest_client=mock_client, processed_resp="ok body")

        assert written == []

    def test_summary_silent_on_error_when_info_not_enabled(
        self, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture
    ) -> None:
        """Test that no summary is printed for an error response when INFO logging is not enabled"""
        mocker.patch("common_libs.clients.rest_client.hooks.logger.isEnabledFor", return_value=False)

        mock_response = mock_response_factory(500)
        mock_response.request.read.return_value = b""
        mock_client = mocker.MagicMock()
        mock_client.log_headers = False
        mock_client.prettify_response_log = False

        written: list[str] = []
        mocker.patch("sys.stdout.write", side_effect=written.append)
        mocker.patch("sys.stdout.flush")

        _print_api_summary(mock_response, rest_client=mock_client, processed_resp="error body")

        assert written == []

    def test_summary_printed_when_info_enabled(
        self, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture
    ) -> None:
        """Test that the summary is printed as usual once INFO logging is enabled"""
        mocker.patch("common_libs.clients.rest_client.hooks.logger.isEnabledFor", return_value=True)

        mock_response = mock_response_factory(200)
        mock_response.request.read.return_value = b""
        mock_client = mocker.MagicMock()
        mock_client.log_headers = False
        mock_client.prettify_response_log = False

        written: list[str] = []
        mocker.patch("sys.stdout.write", side_effect=written.append)
        mocker.patch("sys.stdout.flush")

        _print_api_summary(mock_response, rest_client=mock_client, processed_resp="ok body")

        output = "".join(written)
        assert "status_code" in output
        assert "ok body" in output


class TestLogRequestsDisabled:
    """Tests that `log_requests=False` fully suppresses request/response hooks end-to-end, overridable only
    by an explicit `quiet=False` on an individual call
    """

    def test_failed_call_produces_no_log_record_or_summary_when_disabled(
        self, caplog: pytest.LogCaptureFixture, mocker: MockFixture
    ) -> None:
        """Test that a non-2xx response emits neither a log record nor a console summary when log_requests
        is False
        """
        caplog.set_level(logging.INFO, logger=HOOKS_LOGGER_NAME)
        written: list[str] = []
        mocker.patch("sys.stdout.write", side_effect=written.append)
        mocker.patch("sys.stdout.flush")

        with RestClient(
            "https://example.com",
            log_requests=False,
            retry_policy=None,
            transport=httpx.MockTransport(_not_found_handler),
        ) as client:
            client.get("/missing")

        assert caplog.records == []
        assert written == []

    def test_failed_call_stays_silent_when_explicitly_quiet(
        self, caplog: pytest.LogCaptureFixture, mocker: MockFixture
    ) -> None:
        """Test that an explicit quiet=True on a log_requests=False client still emits nothing"""
        caplog.set_level(logging.INFO, logger=HOOKS_LOGGER_NAME)
        written: list[str] = []
        mocker.patch("sys.stdout.write", side_effect=written.append)
        mocker.patch("sys.stdout.flush")

        with RestClient(
            "https://example.com",
            log_requests=False,
            retry_policy=None,
            transport=httpx.MockTransport(_not_found_handler),
        ) as client:
            client.get("/missing", quiet=True)

        assert caplog.records == []
        assert written == []

    def test_explicit_quiet_false_overrides_log_requests_disabled(
        self, caplog: pytest.LogCaptureFixture, mocker: MockFixture
    ) -> None:
        """Test that an explicit quiet=False on a log_requests=False client re-enables logging for that call"""
        caplog.set_level(logging.INFO, logger=HOOKS_LOGGER_NAME)
        written: list[str] = []
        mocker.patch("sys.stdout.write", side_effect=written.append)
        mocker.patch("sys.stdout.flush")

        with RestClient(
            "https://example.com",
            log_requests=False,
            retry_policy=None,
            transport=httpx.MockTransport(_not_found_handler),
        ) as client:
            client.get("/missing", quiet=False)

        assert any(record.levelno == logging.ERROR for record in caplog.records)
        assert "".join(written) != ""

    def test_failed_call_still_logs_and_prints_summary_when_enabled(
        self, caplog: pytest.LogCaptureFixture, mocker: MockFixture
    ) -> None:
        """Test that the same failed call logs an error and prints a console summary when log_requests is True"""
        caplog.set_level(logging.INFO, logger=HOOKS_LOGGER_NAME)
        written: list[str] = []
        mocker.patch("sys.stdout.write", side_effect=written.append)
        mocker.patch("sys.stdout.flush")

        with RestClient(
            "https://example.com", retry_policy=None, transport=httpx.MockTransport(_not_found_handler)
        ) as client:
            client.get("/missing")

        assert any(record.levelno == logging.ERROR for record in caplog.records)
        assert "".join(written) != ""
