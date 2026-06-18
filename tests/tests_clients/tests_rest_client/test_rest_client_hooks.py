"""Tests for common_libs.clients.rest_client.hooks module"""

from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

from pytest_mock import MockFixture

from common_libs.clients.rest_client.ext import RequestExt
from common_libs.clients.rest_client.hooks import _print_api_summary, get_hooks, request_hooks, response_hooks
from common_libs.clients.rest_client.rest_client import RestClient
from common_libs.clients.rest_client.utils import TRUNCATE_LEN


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


class TestRequestHooks:
    """Tests for request_hooks function"""

    def test_request_hooks_logs_when_not_quiet(self, mock_hooks_logger: MagicMock, mocker: MockFixture) -> None:
        """Test that request_hooks logs the request when not quiet"""
        mock_request = mocker.MagicMock(spec=RequestExt)
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
        mock_request = mocker.MagicMock(spec=RequestExt)
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

    def test_response_hooks_logs_error_regardless_of_quiet(
        self, mock_hooks_logger: MagicMock, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture
    ) -> None:
        """Test that response_hooks always logs error responses even when quiet=True"""
        mocker.patch("common_libs.clients.rest_client.hooks._print_api_summary")

        mock_response = mock_response_factory(500)
        mock_client = mocker.MagicMock()
        mock_client.prettify_response_log = False

        response_hooks(mock_response, quiet=True, rest_client=mock_client)
        mock_hooks_logger.error.assert_called()


class TestHeaderMasking:
    """Tests that sensitive headers are masked in structured log records"""

    def _make_request(self, mocker: MockFixture, headers: dict[str, Any]) -> MagicMock:
        mock_request: MagicMock = mocker.MagicMock(spec=RequestExt)
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
        mock_request: MagicMock = mocker.MagicMock(spec=RequestExt)
        mock_request.request_id = "trunc-req-id"
        mock_request.method = "POST"
        mock_request.url = "http://example.com/api"
        mock_request.headers = {"Content-Type": "application/json"}
        mock_request.extensions = {}
        mock_request.read.return_value = body
        return mock_request

    def test_large_json_payload_is_truncated_in_summary(self, mocker: MockFixture) -> None:
        """Test that a large JSON payload is truncated in the console summary"""
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

        _print_api_summary(mock_response, quiet=False, rest_client=mock_client, processed_resp=None)

        output = "".join(written)
        assert "TRUNCATED" in output
        assert large_value not in output

    def test_small_payload_not_truncated_in_summary(self, mocker: MockFixture) -> None:
        """Test that a small payload passes through the summary unchanged"""
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

        _print_api_summary(mock_response, quiet=False, rest_client=mock_client, processed_resp=None)

        output = "".join(written)
        assert "TRUNCATED" not in output
        assert "short" in output
