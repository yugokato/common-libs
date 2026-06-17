"""Tests for common_libs.clients.rest_client.ext module"""

from collections.abc import Callable
from unittest.mock import AsyncMock, MagicMock

import pytest
from pytest_mock import MockFixture

from common_libs.clients.rest_client.ext import (
    AsyncHTTPClient,
    BearerAuth,
    RequestExt,
    RestResponse,
    SyncHTTPClient,
)
from common_libs.clients.rest_client.utils import get_request_from_exception


class TestBearerAuth:
    """Tests for BearerAuth class"""

    def test_auth_flow_sets_authorization_header(self) -> None:
        """Test that auth_flow adds Bearer authorization header to request"""
        token = "my-secret-token"
        auth = BearerAuth(token)
        request = RequestExt("GET", "http://example.com")
        request.request_id = "req-001"

        gen = auth.auth_flow(request)
        next(gen)  # advance the generator to apply the header

        assert request.headers["Authorization"] == f"Bearer {token}"

    def test_token_stored(self) -> None:
        """Test that token is stored on the auth object"""
        token = "my-token"
        auth = BearerAuth(token)
        assert auth.token == token


class TestRequestExt:
    """Tests for RequestExt class"""

    def test_init_has_extra_attributes(self) -> None:
        """Test that RequestExt initializes with extra attributes"""
        request = RequestExt("GET", "http://example.com")
        assert hasattr(request, "retried")
        assert request.retried is None
        assert hasattr(request, "start_time")
        assert request.start_time is None
        assert hasattr(request, "end_time")
        assert request.end_time is None

    def test_standard_http_attributes(self) -> None:
        """Test that standard httpx Request attributes are accessible"""
        url = "http://example.com/api"
        request = RequestExt("POST", url)
        assert request.method == "POST"
        assert str(request.url) == url


class TestRestResponse:
    """Tests for RestResponse dataclass"""

    def test_init_success_response(self, mock_response_factory: Callable[..., MagicMock]) -> None:
        """Test RestResponse initialization with a successful response"""
        mock_resp = mock_response_factory(200)
        rest_response = RestResponse(mock_resp)

        assert rest_response.status_code == 200
        assert rest_response.ok is True
        assert rest_response.request_id == "test-request-id"
        assert rest_response.response_time == 0.1
        assert rest_response.is_stream is False

    def test_init_error_response(self, mock_response_factory: Callable[..., MagicMock]) -> None:
        """Test RestResponse initialization with an error response"""
        mock_resp = mock_response_factory(404)
        rest_response = RestResponse(mock_resp)

        assert rest_response.status_code == 404
        assert rest_response.ok is False

    def test_init_stream_response(self, mock_response_factory: Callable[..., MagicMock]) -> None:
        """Test RestResponse initialization with a streaming response"""
        mock_resp = mock_response_factory(200, is_stream=True)
        rest_response = RestResponse(mock_resp)

        assert rest_response.is_stream is True
        assert rest_response.response is None
        assert rest_response.response_time is None

    def test_failed_stream_response_exposes_error_body(self, mock_response_factory: Callable[..., MagicMock]) -> None:
        """Test that RestResponse.response contains the parsed error body for failed streaming responses"""
        expected_body = {"error": "not found"}
        mock_resp = mock_response_factory(404, is_stream=True)
        mock_resp.json.return_value = expected_body
        rest_response = RestResponse(mock_resp)
        assert rest_response.is_stream is True
        assert rest_response.ok is False
        assert rest_response.response == expected_body

    def test_successful_stream_response_has_no_body(self, mock_response_factory: Callable[..., MagicMock]) -> None:
        """Test that RestResponse.response is None for successful streaming responses"""
        mock_resp = mock_response_factory(200, is_stream=True)
        rest_response = RestResponse(mock_resp)
        assert rest_response.response is None

    def test_raise_for_status_delegates_to_response(self, mock_response_factory: Callable[..., MagicMock]) -> None:
        """Test that raise_for_status delegates to the inner response"""
        mock_resp = mock_response_factory(200)
        rest_response = RestResponse(mock_resp)
        rest_response.raise_for_status()
        mock_resp.raise_for_status.assert_called_once()

    def test_stream_raises_for_non_stream_response(self, mock_response_factory: Callable[..., MagicMock]) -> None:
        """Test that stream() raises ValueError when response is not a stream"""
        mock_resp = mock_response_factory(200)
        rest_response = RestResponse(mock_resp)

        with pytest.raises(ValueError, match="not a stream"):
            next(rest_response.stream())

    def test_stream_invalid_mode_raises_value_error(self, mock_response_factory: Callable[..., MagicMock]) -> None:
        """Test that stream() raises ValueError for invalid mode"""
        mock_resp = mock_response_factory(200, is_stream=True)
        rest_response = RestResponse(mock_resp)

        with pytest.raises(ValueError, match="Invalid mode"):
            next(rest_response.stream(mode="invalid"))

    def test_stream_text_yields_whole_chunks(self, mock_response_factory: Callable[..., MagicMock]) -> None:
        """Test that stream() in text mode yields whole string chunks, not individual characters"""
        chunks = ["hello", " world"]
        mock_resp = mock_response_factory(200, is_stream=True)
        mock_resp.iter_text.return_value = iter(chunks)

        rest_response = RestResponse(mock_resp)
        result = list(rest_response.stream(mode="text"))

        assert result == chunks

    def test_stream_bytes_yields_whole_chunks(self, mock_response_factory: Callable[..., MagicMock]) -> None:
        """Test that stream() in bytes mode yields whole bytes chunks, not individual ints"""
        chunks = [b"foo", b"bar"]
        mock_resp = mock_response_factory(200, is_stream=True)
        mock_resp.iter_bytes.return_value = iter(chunks)

        rest_response = RestResponse(mock_resp)
        result = list(rest_response.stream(mode="bytes"))

        assert result == chunks

    def test_stream_line_yields_whole_lines(self, mock_response_factory: Callable[..., MagicMock]) -> None:
        """Test that stream() in line mode yields whole lines, not individual characters"""
        lines = ["line one", "line two"]
        mock_resp = mock_response_factory(200, is_stream=True)
        mock_resp.iter_lines.return_value = iter(lines)

        rest_response = RestResponse(mock_resp)
        result = list(rest_response.stream(mode="line"))

        assert result == lines

    def test_stream_raw_yields_whole_chunks(self, mock_response_factory: Callable[..., MagicMock]) -> None:
        """Test that stream() in raw mode yields whole bytes chunks, not individual ints"""
        chunks = [b"\x00\x01", b"\x02\x03"]
        mock_resp = mock_response_factory(200, is_stream=True)
        mock_resp.iter_raw.return_value = iter(chunks)

        rest_response = RestResponse(mock_resp)
        result = list(rest_response.stream(mode="raw"))

        assert result == chunks


class TestHTTPClientMixin:
    """Tests for HTTPClientMixin class"""

    def test_call_request_hooks_invokes_hooks(self, mocker: MockFixture) -> None:
        """Test that call_request_hooks calls each registered hook"""
        client = SyncHTTPClient(base_url="http://example.com")
        mock_hook = mocker.MagicMock()

        mock_request = mocker.MagicMock(spec=RequestExt)
        mock_request.extensions = {"hooks": {"request": [mock_hook]}}

        client.call_request_hooks(mock_request)
        mock_hook.assert_called_once_with(mock_request)

    def test_call_request_hooks_no_hooks(self, mocker: MockFixture) -> None:
        """Test that call_request_hooks handles missing hooks gracefully"""
        client = SyncHTTPClient(base_url="http://example.com")
        mock_request = mocker.MagicMock(spec=RequestExt)
        mock_request.extensions = {}

        # Should not raise
        client.call_request_hooks(mock_request)

    def test_build_log_data(self) -> None:
        """Test that _build_log_data returns expected log fields"""
        request_id = "log-req-id"
        client = SyncHTTPClient(base_url="http://example.com")
        request = RequestExt("GET", "http://example.com/api/users")
        request.request_id = request_id

        log_data = client._build_log_data(request)
        assert log_data["request_id"] == request_id
        assert "GET" in log_data["request"]
        assert log_data["method"] == "GET"


class TestSyncHTTPClient:
    """Tests for SyncHTTPClient.send()"""

    def test_send_injects_original_request_on_exception(self, mocker: MockFixture) -> None:
        """Test that send() attaches the original request to a raised exception via set_original_request"""
        client = SyncHTTPClient(base_url="http://example.com")
        request = client.build_request("GET", "/users")
        mocker.patch.object(client, "_send", side_effect=RuntimeError("error"))
        mocker.patch("common_libs.clients.rest_client.ext.logger")

        with pytest.raises(RuntimeError, match="error") as exc_info:
            client.send(request)

        assert get_request_from_exception(exc_info.value) is request


class TestAsyncHTTPClient:
    """Tests for AsyncHTTPClient.send()"""

    async def test_send_injects_original_request_on_exception(self, mocker: MockFixture) -> None:
        """Test that send() attaches the original request to a raised exception via set_original_request"""
        async with AsyncHTTPClient(base_url="http://example.com") as client:
            request = client.build_request("GET", "/users")
            mocker.patch.object(client, "_send", new_callable=AsyncMock, side_effect=RuntimeError("error"))
            mocker.patch("common_libs.clients.rest_client.ext.logger")

            with pytest.raises(RuntimeError, match="error") as exc_info:
                await client.send(request)

        assert get_request_from_exception(exc_info.value) is request
