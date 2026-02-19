"""Tests for common_libs.clients.rest_client.hooks module"""

import asyncio
from collections.abc import Callable
from unittest.mock import MagicMock

from httpx import Request, Response
from pytest_mock import MockFixture

from common_libs.clients.rest_client.ext import RequestExt
from common_libs.clients.rest_client.hooks import (
    _hook_factory,
    get_hooks,
    request_hooks,
    response_hooks,
)
from common_libs.clients.rest_client.rest_client import RestClient


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


class TestHookFactory:
    """Tests for _hook_factory function"""

    def test_sync_hook_skips_request_when_quiet(self, mocker: MockFixture) -> None:
        """Test that sync request hook skips execution when quiet=True"""
        mock_func = mocker.MagicMock()
        hook = _hook_factory(mock_func, async_mode=False, quiet=True)
        mock_request = mocker.MagicMock(spec=Request)
        hook(mock_request)
        # Request should be skipped when quiet=True
        mock_func.assert_not_called()

    def test_sync_hook_runs_request_when_not_quiet(self, mocker: MockFixture) -> None:
        """Test that sync request hook runs when quiet=False"""
        mock_func = mocker.MagicMock()
        hook = _hook_factory(mock_func, async_mode=False, quiet=False)
        mock_request = mocker.MagicMock(spec=Request)
        hook(mock_request)
        mock_func.assert_called_once()

    def test_sync_hook_skips_successful_response_when_quiet(self, mocker: MockFixture) -> None:
        """Test that sync response hook skips successful response when quiet=True"""
        mock_func = mocker.MagicMock()
        hook = _hook_factory(mock_func, async_mode=False, quiet=True)
        mock_response = mocker.MagicMock(spec=Response)
        mock_response.is_success = True
        hook(mock_response)
        mock_func.assert_not_called()

    def test_sync_hook_runs_failed_response_even_when_quiet(self, mocker: MockFixture) -> None:
        """Test that sync response hook runs for failed responses even when quiet=True"""
        mock_func = mocker.MagicMock()
        hook = _hook_factory(mock_func, async_mode=False, quiet=True)
        mock_response = mocker.MagicMock(spec=Response)
        mock_response.is_success = False
        hook(mock_response)
        mock_func.assert_called_once()

    def test_async_hook_returns_coroutine(self, mocker: MockFixture) -> None:
        """Test that async mode hook returns a coroutine when called"""
        mock_func = mocker.MagicMock()
        hook = _hook_factory(mock_func, async_mode=True, quiet=False)
        mock_request = mocker.MagicMock(spec=Request)

        result = hook(mock_request)
        assert asyncio.iscoroutine(result)
        # Clean up the coroutine to avoid warnings
        result.close()


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
