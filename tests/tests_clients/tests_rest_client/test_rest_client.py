"""Tests for common_libs.clients.rest_client.rest_client module"""

import asyncio
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockFixture

from common_libs.clients.rest_client.ext import AsyncHTTPClient, RestResponse, SyncHTTPClient
from common_libs.clients.rest_client.rest_client import AsyncRestClient, RestClient, inject_hooks


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


class TestRestClient:
    """Tests for RestClient class"""

    def test_init_sync_mode(self) -> None:
        """Test that RestClient creates a sync HTTP client"""
        client = RestClient("http://example.com")
        assert isinstance(client.client, SyncHTTPClient)
        assert client.async_mode is False

    def test_init_raises_in_async_context(self) -> None:
        """Test that RestClient raises RuntimeError when created in async context"""

        async def _create() -> None:
            RestClient("http://example.com")

        with pytest.raises(RuntimeError, match="cannot be used inside async context"):
            asyncio.run(_create())

    def test_get_call(self, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture) -> None:
        """Test that get() calls the underlying HTTP client and returns response as a RestResponse"""
        client = RestClient("http://example.com")
        mock_response = mock_response_factory()
        mock_httpx_get = mocker.patch.object(client.client, "get", return_value=mock_response)

        r = client.get("/users", quiet=True)
        mock_httpx_get.assert_called_once()
        assert isinstance(r, RestResponse)
        assert r.status_code == 200

    def test_post_call(self, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture) -> None:
        """Test that post() calls the underlying HTTP client and returns response as a RestResponse"""
        client = RestClient("http://example.com")
        mock_response = mock_response_factory(201)
        mock_httpx_post = mocker.patch.object(client.client, "post", return_value=mock_response)

        r = client.post("/users", quiet=True, name="alice")
        mock_httpx_post.assert_called_once()
        assert isinstance(r, RestResponse)
        assert r.status_code == 201

    def test_delete_call(self, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture) -> None:
        """Test that delete() calls the underlying HTTP client via request() and returns response as a RestResponse"""
        client = RestClient("http://example.com")
        mock_response = mock_response_factory()
        mock_httpx_request = mocker.patch.object(client.client, "request", return_value=mock_response)

        r = client.delete("/users/1", quiet=True)
        mock_httpx_request.assert_called_once()
        assert isinstance(r, RestResponse)
        assert r.status_code == 200

    def test_put_call(self, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture) -> None:
        """Test that put() calls the underlying HTTP client and returns response as a RestResponse"""
        client = RestClient("http://example.com")
        mock_response = mock_response_factory()
        mock_httpx_put = mocker.patch.object(client.client, "put", return_value=mock_response)

        r = client.put("/users/1", quiet=True, name="bob")
        mock_httpx_put.assert_called_once()
        assert isinstance(r, RestResponse)
        assert r.status_code == 200

    def test_patch_call(self, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture) -> None:
        """Test that patch() calls the underlying HTTP client and returns response as a RestResponse"""
        client = RestClient("http://example.com")
        mock_response = mock_response_factory()
        mock_httpx_patch = mocker.patch.object(client.client, "patch", return_value=mock_response)

        r = client.patch("/users/1", quiet=True, name="carol")
        mock_httpx_patch.assert_called_once()
        assert isinstance(r, RestResponse)
        assert r.status_code == 200

    def test_options_call(self, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture) -> None:
        """Test that options() calls the underlying HTTP client and returns response as a RestResponse"""
        client = RestClient("http://example.com")
        mock_response = mock_response_factory()
        mock_httpx_options = mocker.patch.object(client.client, "options", return_value=mock_response)

        r = client.options("/users", quiet=True)
        mock_httpx_options.assert_called_once()
        assert isinstance(r, RestResponse)
        assert r.status_code == 200

    def test_stream_call(self, mock_stream_response_factory: Callable[..., MagicMock], mocker: MockFixture) -> None:
        """Test that stream() calls the underlying client.stream() context manager and yields a RestResponse"""
        client = RestClient("http://example.com")
        mock_response = mock_stream_response_factory()
        mock_ctx = mocker.MagicMock()
        mock_ctx.__enter__ = mocker.MagicMock(return_value=mock_response)
        mock_ctx.__exit__ = mocker.MagicMock(return_value=False)
        mock_client_stream = mocker.patch.object(client.client, "stream", return_value=mock_ctx)

        with client.stream("GET", "/events", quiet=True) as r:
            assert isinstance(r, RestResponse)
            assert r.is_stream is True
            assert r.status_code == 200

        mock_client_stream.assert_called_once()

    def test_context_manager_closes_client(self, mocker: MockFixture) -> None:
        """Test that context manager calls close() on exit"""
        client = RestClient("http://example.com")
        mock_close = mocker.patch.object(client, "close")

        with client:
            pass

        mock_close.assert_called_once()


class TestAsyncRestClient:
    """Tests for AsyncRestClient class"""

    def test_init_async_mode(self) -> None:
        """Test that AsyncRestClient creates an async HTTP client"""
        client = AsyncRestClient("http://example.com")
        assert isinstance(client.client, AsyncHTTPClient)
        assert client.async_mode is True

    async def test_get_call_async(self, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture) -> None:
        """Test that get() in async mode calls the underlying HTTP client and returns response as a RestResponse"""
        client = AsyncRestClient("http://example.com")
        mock_response = mock_response_factory()
        mock_httpx_get = mocker.patch.object(client.client, "get", return_value=mock_response)

        r = await client.get("/users", quiet=True)
        mock_httpx_get.assert_called_once()
        assert isinstance(r, RestResponse)
        assert r.status_code == 200

    async def test_post_call_async(self, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture) -> None:
        """Test that post() in async mode calls the underlying HTTP client and returns response as a RestResponse"""
        client = AsyncRestClient("http://example.com")
        mock_response = mock_response_factory(201)
        mock_httpx_post = mocker.patch.object(client.client, "post", return_value=mock_response)

        r = await client.post("/users", quiet=True, name="alice")
        mock_httpx_post.assert_called_once()
        assert isinstance(r, RestResponse)
        assert r.status_code == 201

    async def test_delete_call_async(
        self, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture
    ) -> None:
        """Test that delete() in async mode calls the underlying HTTP client via request() and returns response as a
        RestResponse
        """
        client = AsyncRestClient("http://example.com")
        mock_response = mock_response_factory()
        mock_httpx_request = mocker.patch.object(client.client, "request", return_value=mock_response)

        r = await client.delete("/users/1", quiet=True)
        mock_httpx_request.assert_called_once()
        assert isinstance(r, RestResponse)
        assert r.status_code == 200

    async def test_put_call_async(self, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture) -> None:
        """Test that put() in async mode calls the underlying HTTP client and returns response as a RestResponse"""
        client = AsyncRestClient("http://example.com")
        mock_response = mock_response_factory()
        mock_httpx_put = mocker.patch.object(client.client, "put", return_value=mock_response)

        r = await client.put("/users/1", quiet=True, name="bob")
        mock_httpx_put.assert_called_once()
        assert isinstance(r, RestResponse)
        assert r.status_code == 200

    async def test_patch_call_async(self, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture) -> None:
        """Test that patch() in async mode calls the underlying HTTP client and returns response as a RestResponse"""
        client = AsyncRestClient("http://example.com")
        mock_response = mock_response_factory()
        mock_httpx_patch = mocker.patch.object(client.client, "patch", return_value=mock_response)

        r = await client.patch("/users/1", quiet=True, name="carol")
        mock_httpx_patch.assert_called_once()
        assert isinstance(r, RestResponse)
        assert r.status_code == 200

    async def test_options_call_async(
        self, mock_response_factory: Callable[..., MagicMock], mocker: MockFixture
    ) -> None:
        """Test that options() in async mode calls the underlying HTTP client and returns response as a RestResponse"""
        client = AsyncRestClient("http://example.com")
        mock_response = mock_response_factory()
        mock_httpx_options = mocker.patch.object(client.client, "options", return_value=mock_response)

        r = await client.options("/users", quiet=True)
        mock_httpx_options.assert_called_once()
        assert isinstance(r, RestResponse)
        assert r.status_code == 200

    async def test_stream_call_async(
        self, mock_stream_response_factory: Callable[..., MagicMock], mocker: MockFixture
    ) -> None:
        """Test that stream() in async mode calls the underlying HTTP client and yields a RestResponse"""
        client = AsyncRestClient("http://example.com")
        mock_response = mock_stream_response_factory()
        mock_ctx = mocker.MagicMock()
        mock_ctx.__aenter__ = mocker.AsyncMock(return_value=mock_response)
        mock_ctx.__aexit__ = mocker.AsyncMock(return_value=False)
        mock_client_stream = mocker.patch.object(client.client, "stream", return_value=mock_ctx)

        async with client.stream("GET", "/events", quiet=True) as r:
            assert isinstance(r, RestResponse)
            assert r.is_stream is True
            assert r.status_code == 200

        mock_client_stream.assert_called_once()

    async def test_context_manager_closes_client_async(self, mocker: MockFixture) -> None:
        """Test that context manager calls close() on exit"""
        client = AsyncRestClient("http://example.com")
        mock_close = mocker.patch.object(client, "close")

        async with client:
            pass

        mock_close.assert_called_once()
