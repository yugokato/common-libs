"""Tests for common_libs.clients.rest_client.ext module"""

import errno
from collections.abc import Callable
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import TransportError
from pytest_mock import MockFixture

from common_libs.clients.rest_client import RetryPolicy
from common_libs.clients.rest_client.ext import AsyncHTTPClient, BearerAuth, SyncHTTPClient
from common_libs.clients.rest_client.retry import BackoffStrategy
from common_libs.clients.rest_client.types import Request, RestResponse
from common_libs.clients.rest_client.utils import get_request_from_exception


class TestBearerAuth:
    """Tests for BearerAuth class"""

    def test_auth_flow_sets_authorization_header(self) -> None:
        """Test that auth_flow adds Bearer authorization header to request"""
        token = "my-secret-token"
        auth = BearerAuth(token)
        request = Request("GET", "http://example.com")
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

    def test_build_request_has_extra_attributes(self) -> None:
        """Test that a built request has extra attributes set by _modify_request"""
        client = SyncHTTPClient(base_url="http://example.com")
        request = client.build_request("GET", "/")
        assert hasattr(request, "retried")
        assert request.retried is None
        assert hasattr(request, "start_time")
        assert request.start_time is None
        assert hasattr(request, "end_time")
        assert request.end_time is None

    def test_standard_http_attributes(self) -> None:
        """Test that standard httpx Request attributes are accessible"""
        url = "http://example.com/api"
        request = Request("POST", url)
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

        mock_request = mocker.MagicMock(spec=Request)
        mock_request.extensions = {"hooks": {"request": [mock_hook]}}

        client.call_request_hooks(mock_request)
        mock_hook.assert_called_once_with(mock_request)

    def test_call_request_hooks_no_hooks(self, mocker: MockFixture) -> None:
        """Test that call_request_hooks handles missing hooks gracefully"""
        client = SyncHTTPClient(base_url="http://example.com")
        mock_request = mocker.MagicMock(spec=Request)
        mock_request.extensions = {}

        # Should not raise
        client.call_request_hooks(mock_request)

    def test_build_log_data(self) -> None:
        """Test that _build_log_data returns expected log fields"""
        request_id = "log-req-id"
        client = SyncHTTPClient(base_url="http://example.com")
        request = Request("GET", "http://example.com/api/users")
        request.request_id = request_id

        log_data = client._build_log_data(request)
        assert log_data["request_id"] == request_id
        assert "GET" in log_data["request"]
        assert log_data["method"] == "GET"
        assert isinstance(log_data["path"], str)


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

    def test_retry_none_disables_503_retry(
        self, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture
    ) -> None:
        """Test that retry_policy=None disables the default 503 retry so the response is returned immediately"""
        client = SyncHTTPClient(base_url="http://example.com", retry_policy=None)
        mock_503 = mock_response_factory(503)
        send_mock = mocker.patch.object(client, "_send", return_value=mock_503)
        mocker.patch("common_libs.clients.rest_client.ext.logger")

        result = client.send(client.build_request("GET", "/"))

        assert result is mock_503
        assert send_mock.call_count == 1

    def test_custom_retry_policy_retries_on_configured_status(
        self, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture
    ) -> None:
        """Test that a custom RetryPolicy with a different status code retries on that code"""
        mocker.patch("time.sleep")
        client = SyncHTTPClient(base_url="http://example.com", retry_policy=RetryPolicy(condition=429, retry_after=0))
        mock_429 = mock_response_factory(429)
        mock_ok = mock_response_factory(200)
        call_count = 0

        def _send_side_effect(request: MagicMock, **kwargs: object) -> MagicMock:
            nonlocal call_count
            call_count += 1
            return mock_429 if call_count == 1 else mock_ok

        mocker.patch.object(client, "_send", side_effect=_send_side_effect)
        mocker.patch("common_libs.clients.rest_client.ext.logger")

        result = client.send(client.build_request("GET", "/"))

        assert result is mock_ok
        assert call_count == 2

    def test_retry_policy_with_backoff_strategy_uses_exponential_delays(
        self, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture
    ) -> None:
        """Test that a RetryPolicy with a BackoffStrategy produces correct exponential sleep delays"""
        sleep_mock = mocker.patch("time.sleep")
        strategy = BackoffStrategy(base=1.0, factor=2.0, jitter=False)
        client = SyncHTTPClient(
            base_url="http://example.com",
            retry_policy=RetryPolicy(condition=503, num_retries=2, retry_after=strategy, safe_methods_only=False),
        )
        mock_503 = mock_response_factory(503)
        mock_ok = mock_response_factory(200)
        call_count = 0

        def _send_side_effect(request: MagicMock, **kwargs: object) -> MagicMock:
            nonlocal call_count
            call_count += 1
            return mock_ok if call_count == 3 else mock_503

        mocker.patch.object(client, "_send", side_effect=_send_side_effect)
        mocker.patch("common_libs.clients.rest_client.ext.logger")

        result = client.send(client.build_request("GET", "/"))

        assert result is mock_ok
        assert sleep_mock.call_count == 2
        assert sleep_mock.call_args_list[0][0][0] == pytest.approx(1.0)  # attempt 0
        assert sleep_mock.call_args_list[1][0][0] == pytest.approx(2.0)  # attempt 1

    def test_retry_policy_with_backoff_strategy_respects_retry_after_header(
        self, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture
    ) -> None:
        """Test that a RetryPolicy with BackoffStrategy(honor_retry_after=True) honors the Retry-After header"""
        sleep_mock = mocker.patch("time.sleep")
        strategy = BackoffStrategy(base=1.0, factor=2.0, jitter=False, honor_retry_after=True)
        client = SyncHTTPClient(
            base_url="http://example.com",
            retry_policy=RetryPolicy(condition=503, num_retries=1, retry_after=strategy, safe_methods_only=False),
        )
        mock_503 = mock_response_factory(503)
        mock_503.headers = {"Retry-After": "20"}
        mock_ok = mock_response_factory(200)
        call_count = 0

        def _send_side_effect(request: MagicMock, **kwargs: object) -> MagicMock:
            nonlocal call_count
            call_count += 1
            return mock_ok if call_count == 2 else mock_503

        mocker.patch.object(client, "_send", side_effect=_send_side_effect)
        mocker.patch("common_libs.clients.rest_client.ext.logger")

        result = client.send(client.build_request("GET", "/"))

        assert result is mock_ok
        sleep_mock.assert_called_once_with(pytest.approx(20.0))


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

    async def test_retry_none_disables_503_retry(
        self, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture
    ) -> None:
        """Test that retry_policy=None disables the default 503 retry so the response is returned immediately"""
        async with AsyncHTTPClient(base_url="http://example.com", retry_policy=None) as client:
            mock_503 = mock_response_factory(503)
            send_mock = mocker.patch.object(client, "_send", new_callable=AsyncMock, return_value=mock_503)
            mocker.patch("common_libs.clients.rest_client.ext.logger")

            result = await client.send(client.build_request("GET", "/"))

        assert result is mock_503
        assert send_mock.call_count == 1

    async def test_retry_policy_with_backoff_strategy_uses_exponential_delays(
        self, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture
    ) -> None:
        """Test that an async RetryPolicy with a BackoffStrategy produces correct exponential sleep delays"""
        sleep_mock = mocker.patch("asyncio.sleep", new_callable=AsyncMock)
        strategy = BackoffStrategy(base=1.0, factor=2.0, jitter=False)
        async with AsyncHTTPClient(
            base_url="http://example.com",
            retry_policy=RetryPolicy(condition=503, num_retries=2, retry_after=strategy, safe_methods_only=False),
        ) as client:
            mock_503 = mock_response_factory(503)
            mock_ok = mock_response_factory(200)
            call_count = 0

            def _send_side_effect(request: MagicMock, **kwargs: object) -> MagicMock:
                nonlocal call_count
                call_count += 1
                return mock_ok if call_count == 3 else mock_503

            mocker.patch.object(client, "_send", new_callable=AsyncMock, side_effect=_send_side_effect)
            mocker.patch("common_libs.clients.rest_client.ext.logger")

            result = await client.send(client.build_request("GET", "/"))

        assert result is mock_ok
        assert sleep_mock.call_count == 2
        assert sleep_mock.call_args_list[0][0][0] == pytest.approx(1.0)  # attempt 0
        assert sleep_mock.call_args_list[1][0][0] == pytest.approx(2.0)  # attempt 1

    async def test_retry_policy_with_backoff_strategy_respects_retry_after_header(
        self, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture
    ) -> None:
        """
        Test that an async RetryPolicy with BackoffStrategy(honor_retry_after=True) honors the Retry-After header
        """
        sleep_mock = mocker.patch("asyncio.sleep", new_callable=AsyncMock)
        strategy = BackoffStrategy(base=1.0, factor=2.0, jitter=False, honor_retry_after=True)
        async with AsyncHTTPClient(
            base_url="http://example.com",
            retry_policy=RetryPolicy(condition=503, num_retries=1, retry_after=strategy, safe_methods_only=False),
        ) as client:
            mock_503 = mock_response_factory(503)
            mock_503.headers = {"Retry-After": "20"}
            mock_ok = mock_response_factory(200)
            call_count = 0

            def _send_side_effect(request: MagicMock, **kwargs: object) -> MagicMock:
                nonlocal call_count
                call_count += 1
                return mock_ok if call_count == 2 else mock_503

            mocker.patch.object(client, "_send", new_callable=AsyncMock, side_effect=_send_side_effect)
            mocker.patch("common_libs.clients.rest_client.ext.logger")

            result = await client.send(client.build_request("GET", "/"))

        assert result is mock_ok
        sleep_mock.assert_called_once_with(pytest.approx(20.0))


class TestConnectionResetReconnect:
    """Tests for connection-reset reconnect behavior in both sync and async clients"""

    def _make_reset_error(self) -> TransportError:
        cause = OSError(errno.ECONNRESET, "socket error")
        err = TransportError("transport failed")
        err.__cause__ = cause
        return err

    def test_sync_safe_method_reconnects_on_reset(self, mocker: MockFixture) -> None:
        """Test that a safe-method (GET) request is automatically retried after a connection reset"""
        client = SyncHTTPClient(base_url="http://example.com")
        request = client.build_request("GET", "/data")
        mock_response = mocker.MagicMock()
        send_mock = mocker.patch.object(client, "_send", side_effect=[self._make_reset_error(), mock_response])
        mocker.patch("common_libs.clients.rest_client.ext.logger")

        result = client.send(request)

        assert result is mock_response
        assert send_mock.call_count == 2

    def test_sync_non_safe_method_raises_on_reset(self, mocker: MockFixture) -> None:
        """Test that a non-safe method (POST) is NOT retried on connection reset to avoid double-submit"""
        client = SyncHTTPClient(base_url="http://example.com")
        request = client.build_request("POST", "/submit")
        send_mock = mocker.patch.object(client, "_send", side_effect=self._make_reset_error())
        mocker.patch("common_libs.clients.rest_client.ext.logger")

        with pytest.raises(TransportError):
            client.send(request)

        assert send_mock.call_count == 1

    def test_sync_safe_method_does_not_reconnect_on_non_reset(self, mocker: MockFixture) -> None:
        """Test that a non-reset TransportError on a safe method propagates without reconnecting"""
        client = SyncHTTPClient(base_url="http://example.com")
        request = client.build_request("GET", "/data")
        send_mock = mocker.patch.object(client, "_send", side_effect=TransportError("boom"))
        mocker.patch("common_libs.clients.rest_client.ext.logger")

        with pytest.raises(TransportError):
            client.send(request)

        assert send_mock.call_count == 1

    async def test_async_safe_method_reconnects_on_reset(self, mocker: MockFixture) -> None:
        """Test that an async safe-method (GET) request is automatically retried after a connection reset"""
        async with AsyncHTTPClient(base_url="http://example.com") as client:
            request = client.build_request("GET", "/data")
            mock_response = mocker.MagicMock()
            send_mock = mocker.patch.object(
                client,
                "_send",
                new_callable=AsyncMock,
                side_effect=[self._make_reset_error(), mock_response],
            )
            mocker.patch("common_libs.clients.rest_client.ext.logger")

            result = await client.send(request)

        assert result is mock_response
        assert send_mock.call_count == 2

    async def test_async_non_safe_method_raises_on_reset(self, mocker: MockFixture) -> None:
        """Test that an async non-safe method (POST) is NOT retried on connection reset"""
        async with AsyncHTTPClient(base_url="http://example.com") as client:
            request = client.build_request("POST", "/submit")
            send_mock = mocker.patch.object(
                client, "_send", new_callable=AsyncMock, side_effect=self._make_reset_error()
            )
            mocker.patch("common_libs.clients.rest_client.ext.logger")

            with pytest.raises(TransportError):
                await client.send(request)

        assert send_mock.call_count == 1

    async def test_async_safe_method_does_not_reconnect_on_non_reset(self, mocker: MockFixture) -> None:
        """Test that a non-reset TransportError on an async safe method propagates without reconnecting"""
        async with AsyncHTTPClient(base_url="http://example.com") as client:
            request = client.build_request("GET", "/data")
            send_mock = mocker.patch.object(client, "_send", new_callable=AsyncMock, side_effect=TransportError("boom"))
            mocker.patch("common_libs.clients.rest_client.ext.logger")

            with pytest.raises(TransportError):
                await client.send(request)

        assert send_mock.call_count == 1
