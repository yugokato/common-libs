from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Self, TypeVar
from urllib.parse import urlparse

from httpx2 import USE_CLIENT_DEFAULT

from common_libs.logging import get_logger

from .auth import ClientAuth, RequestAuth
from .base import RestClientBase
from .hooks import inject_hooks
from .rate_limit import RateLimit
from .retry import DEFAULT_RETRY_POLICY, RetryPolicy
from .types import RestResponse
from .utils import manage_content_type

ClientType = TypeVar("ClientType", "RestClient", "AsyncRestClient")

logger = get_logger(__name__)


class RestClient(RestClientBase):
    """Sync Rest API client.

    `auth`, `files`, and `quiet` are reserved keywords on `get()`/`post()`/`put()`/`patch()`/`delete()`/
    `options()`/`stream()`. A JSON payload or query parameter that happens to use one of those names cannot be
    passed through `**payload`/`**query_params` on these methods.
    """

    def __init__(
        self,
        base_url: str,
        *,
        log_requests: bool = True,
        log_headers: bool = False,
        prettify_response_log: bool = True,
        retry_policy: RetryPolicy | None = DEFAULT_RETRY_POLICY,
        rate_limit: RateLimit | None = None,
        auth: ClientAuth = None,
        **client_opts: Any,
    ) -> None:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            raise RuntimeError(
                f"{RestClient.__name__} cannot be used inside async context. Use {AsyncRestClient.__name__} instead."
            )
        super().__init__(
            base_url,
            log_requests=log_requests,
            log_headers=log_headers,
            prettify_response_log=prettify_response_log,
            retry_policy=retry_policy,
            rate_limit=rate_limit,
            auth=auth,
            **client_opts,
        )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying httpx2 client"""
        self.client.close()

    def get(
        self, path: str, /, *, auth: RequestAuth = USE_CLIENT_DEFAULT, quiet: bool | None = None, **query_params: Any
    ) -> RestResponse:
        """Make a GET API request

        :param path: Endpoint path
        :param auth: Auth to use for this call only. Defaults to the client's own auth. Pass `None` to send the
                     request unauthenticated
        :param quiet: Suppress request/response logs for this call (Failed calls are still logged with reduced context).
                      Overrides the client's own `log_requests` default when given explicitly
        :param query_params: Query parameters
        """
        return self._request("GET", path, params=query_params, auth=auth, quiet=quiet)

    def post(
        self,
        path: str,
        /,
        *,
        files: dict[str, Any] | None = None,
        auth: RequestAuth = USE_CLIENT_DEFAULT,
        quiet: bool | None = None,
        **payload: Any,
    ) -> RestResponse:
        """Make a POST API request

        :param path: Endpoint path
        :param files: File to upload
        :param auth: Auth to use for this call only. Defaults to the client's own auth. Pass `None` to send the
                     request unauthenticated
        :param quiet: Suppress request/response logs for this call (Failed calls are still logged with reduced context).
                      Overrides the client's own `log_requests` default when given explicitly
        :param payload: JSON payload. When `files` is provided, these are sent as multipart form fields instead.
        """
        if files:
            return self._request("POST", path, data=payload or None, files=files, auth=auth, quiet=quiet)
        return self._request("POST", path, json=payload, auth=auth, quiet=quiet)

    def delete(
        self, path: str, /, *, auth: RequestAuth = USE_CLIENT_DEFAULT, quiet: bool | None = None, **payload: Any
    ) -> RestResponse:
        """Make a DELETE API request

        :param path: Endpoint path
        :param auth: Auth to use for this call only. Defaults to the client's own auth. Pass `None` to send the
                     request unauthenticated
        :param quiet: Suppress request/response logs for this call (Failed calls are still logged with reduced context).
                      Overrides the client's own `log_requests` default when given explicitly
        :param payload: JSON payload
        """
        return self._request("DELETE", path, json=payload, auth=auth, quiet=quiet)

    def put(
        self, path: str, /, *, auth: RequestAuth = USE_CLIENT_DEFAULT, quiet: bool | None = None, **payload: Any
    ) -> RestResponse:
        """Make a PUT API request

        :param path: Endpoint path
        :param auth: Auth to use for this call only. Defaults to the client's own auth. Pass `None` to send the
                     request unauthenticated
        :param quiet: Suppress request/response logs for this call (Failed calls are still logged with reduced context).
                      Overrides the client's own `log_requests` default when given explicitly
        :param payload: JSON payload
        """
        return self._request("PUT", path, json=payload, auth=auth, quiet=quiet)

    def patch(
        self, path: str, /, *, auth: RequestAuth = USE_CLIENT_DEFAULT, quiet: bool | None = None, **payload: Any
    ) -> RestResponse:
        """Make a PATCH API request

        :param path: Endpoint path
        :param auth: Auth to use for this call only. Defaults to the client's own auth. Pass `None` to send the
                     request unauthenticated
        :param quiet: Suppress request/response logs for this call (Failed calls are still logged with reduced context).
                      Overrides the client's own `log_requests` default when given explicitly
        :param payload: JSON payload
        """
        return self._request("PATCH", path, json=payload, auth=auth, quiet=quiet)

    def options(
        self, path: str, /, *, auth: RequestAuth = USE_CLIENT_DEFAULT, quiet: bool | None = None, **query_params: Any
    ) -> RestResponse:
        """Make an OPTIONS API request

        :param path: Endpoint path
        :param auth: Auth to use for this call only. Defaults to the client's own auth. Pass `None` to send the
                     request unauthenticated
        :param quiet: Suppress request/response logs for this call (Failed calls are still logged with reduced context).
                      Overrides the client's own `log_requests` default when given explicitly
        :param query_params: Query parameters
        """
        return self._request("OPTIONS", path, params=query_params, auth=auth, quiet=quiet)

    @contextmanager
    @inject_hooks
    @manage_content_type
    def stream(
        self,
        method: str,
        path: str,
        /,
        *,
        auth: RequestAuth = USE_CLIENT_DEFAULT,
        quiet: bool | None = None,
        **raw_options: Any,
    ) -> Generator[RestResponse]:
        """Stream an HTTP API request

        :param method: Endpoint method
        :param path: Endpoint path
        :param auth: Auth to use for this call only. Defaults to the client's own auth. Pass `None` to send the
                     request unauthenticated
        :param quiet: Suppress request/response logs for this call (Failed calls are still logged with reduced context).
                      Overrides the client's own `log_requests` default when given explicitly
        :param raw_options: Any other parameters passed directly to the httpx2 library
        """
        with self.client.stream(method.upper(), path, auth=auth, **raw_options) as r:
            yield RestResponse(r)

    @inject_hooks
    @manage_content_type
    def _request(
        self,
        method: str,
        path: str,
        /,
        *,
        auth: RequestAuth = USE_CLIENT_DEFAULT,
        quiet: bool | None = None,
        **raw_options: Any,
    ) -> RestResponse:
        """Low-level function for all HTTP verb methods

        :param method: HTTP method
        :param path: Endpoint path
        :param auth: Auth to use for this call only. Defaults to the client's own auth. Pass `None` to send the
                     request unauthenticated
        :param quiet: Suppress request/response logs for this call (Failed calls are still logged with reduced context).
                      Overrides the client's own `log_requests` default when given explicitly
        :param raw_options: Any other parameters passed directly to the httpx2 library
        """
        r = self.client.request(method.upper(), path, auth=auth, **raw_options)
        return RestResponse(r)


class AsyncRestClient(RestClientBase):
    """Async Rest API client.

    `auth`, `files`, and `quiet` are reserved keywords on `get()`/`post()`/`put()`/`patch()`/`delete()`/
    `options()`/`stream()`. A JSON payload or query parameter that happens to use one of those names cannot be
    passed through `**payload`/`**query_params` on these methods.
    """

    def __init__(
        self,
        base_url: str,
        *,
        log_requests: bool = True,
        log_headers: bool = False,
        prettify_response_log: bool = True,
        retry_policy: RetryPolicy | None = DEFAULT_RETRY_POLICY,
        rate_limit: RateLimit | None = None,
        auth: ClientAuth = None,
        **client_opts: Any,
    ) -> None:
        super().__init__(
            base_url,
            log_requests=log_requests,
            log_headers=log_headers,
            prettify_response_log=prettify_response_log,
            async_mode=True,
            retry_policy=retry_policy,
            rate_limit=rate_limit,
            auth=auth,
            **client_opts,
        )

    @classmethod
    @asynccontextmanager
    async def http3(cls, base_url: str, **kwargs: Any) -> AsyncGenerator[AsyncRestClient]:
        """Async context manager that yields an AsyncRestClient connected over HTTP/3.

        This is experimental/temporary until the official HTTP/3 support is added to httpx2

        :param base_url: Base URL of the API (must use the https scheme).
        :param kwargs: Additional keyword arguments forwarded to AsyncRestClient
        """
        try:
            from aioquic.asyncio.client import connect
            from aioquic.h3.connection import H3_ALPN
            from aioquic.quic.configuration import QuicConfiguration

            from .http3 import H3Transport
        except ImportError:
            raise RuntimeError(
                "HTTP/3 client requires optional http3 dependency: Install with 'pip install common_libs[http3]'"
            )

        parsed = urlparse(base_url)
        if parsed.scheme != "https":
            raise ValueError(f"The URL schema must be https, not {parsed.scheme}")

        async with connect(
            parsed.hostname,
            parsed.port or 443,
            configuration=QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN),
            create_protocol=H3Transport,
        ) as transport:
            async with cls(base_url, transport=transport, **kwargs) as client:
                yield client

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the underlying httpx2 client"""
        await self.client.aclose()

    async def get(
        self, path: str, /, *, auth: RequestAuth = USE_CLIENT_DEFAULT, quiet: bool | None = None, **query_params: Any
    ) -> RestResponse:
        """Make a GET API request

        :param path: Endpoint path
        :param auth: Auth to use for this call only. Defaults to the client's own auth. Pass `None` to send the
                     request unauthenticated
        :param quiet: Suppress request/response logs for this call (Failed calls are still logged with reduced context).
                      Overrides the client's own `log_requests` default when given explicitly
        :param query_params: Query parameters
        """
        return await self._request("GET", path, params=query_params, auth=auth, quiet=quiet)

    async def post(
        self,
        path: str,
        /,
        *,
        files: dict[str, Any] | None = None,
        auth: RequestAuth = USE_CLIENT_DEFAULT,
        quiet: bool | None = None,
        **payload: Any,
    ) -> RestResponse:
        """Make a POST API request

        :param path: Endpoint path
        :param files: File to upload
        :param auth: Auth to use for this call only. Defaults to the client's own auth. Pass `None` to send the
                     request unauthenticated
        :param quiet: Suppress request/response logs for this call (Failed calls are still logged with reduced context).
                      Overrides the client's own `log_requests` default when given explicitly
        :param payload: JSON payload. When `files` is provided, these are sent as multipart form fields instead.
        """
        if files:
            return await self._request("POST", path, data=payload or None, files=files, auth=auth, quiet=quiet)
        return await self._request("POST", path, json=payload, auth=auth, quiet=quiet)

    async def delete(
        self, path: str, /, *, auth: RequestAuth = USE_CLIENT_DEFAULT, quiet: bool | None = None, **payload: Any
    ) -> RestResponse:
        """Make a DELETE API request

        :param path: Endpoint path
        :param auth: Auth to use for this call only. Defaults to the client's own auth. Pass `None` to send the
                     request unauthenticated
        :param quiet: Suppress request/response logs for this call (Failed calls are still logged with reduced context).
                      Overrides the client's own `log_requests` default when given explicitly
        :param payload: JSON payload
        """
        return await self._request("DELETE", path, json=payload, auth=auth, quiet=quiet)

    async def put(
        self, path: str, /, *, auth: RequestAuth = USE_CLIENT_DEFAULT, quiet: bool | None = None, **payload: Any
    ) -> RestResponse:
        """Make a PUT API request

        :param path: Endpoint path
        :param auth: Auth to use for this call only. Defaults to the client's own auth. Pass `None` to send the
                     request unauthenticated
        :param quiet: Suppress request/response logs for this call (Failed calls are still logged with reduced context).
                      Overrides the client's own `log_requests` default when given explicitly
        :param payload: JSON payload
        """
        return await self._request("PUT", path, json=payload, auth=auth, quiet=quiet)

    async def patch(
        self, path: str, /, *, auth: RequestAuth = USE_CLIENT_DEFAULT, quiet: bool | None = None, **payload: Any
    ) -> RestResponse:
        """Make a PATCH API request

        :param path: Endpoint path
        :param auth: Auth to use for this call only. Defaults to the client's own auth. Pass `None` to send the
                     request unauthenticated
        :param quiet: Suppress request/response logs for this call (Failed calls are still logged with reduced context).
                      Overrides the client's own `log_requests` default when given explicitly
        :param payload: JSON payload
        """
        return await self._request("PATCH", path, json=payload, auth=auth, quiet=quiet)

    async def options(
        self, path: str, /, *, auth: RequestAuth = USE_CLIENT_DEFAULT, quiet: bool | None = None, **query_params: Any
    ) -> RestResponse:
        """Make an OPTIONS API request

        :param path: Endpoint path
        :param auth: Auth to use for this call only. Defaults to the client's own auth. Pass `None` to send the
                     request unauthenticated
        :param quiet: Suppress request/response logs for this call (Failed calls are still logged with reduced context).
                      Overrides the client's own `log_requests` default when given explicitly
        :param query_params: Query parameters
        """
        return await self._request("OPTIONS", path, params=query_params, auth=auth, quiet=quiet)

    @asynccontextmanager
    @inject_hooks
    @manage_content_type
    async def stream(
        self,
        method: str,
        path: str,
        /,
        *,
        auth: RequestAuth = USE_CLIENT_DEFAULT,
        quiet: bool | None = None,
        **raw_options: Any,
    ) -> AsyncGenerator[RestResponse]:
        """Stream an HTTP API request

        :param method: Endpoint method
        :param path: Endpoint path
        :param auth: Auth to use for this call only. Defaults to the client's own auth. Pass `None` to send the
                     request unauthenticated
        :param quiet: Suppress request/response logs for this call (Failed calls are still logged with reduced context).
                      Overrides the client's own `log_requests` default when given explicitly
        :param raw_options: Any other parameters passed directly to the httpx2 library
        """
        async with self.client.stream(method.upper(), path, auth=auth, **raw_options) as r:
            yield RestResponse(r)

    @inject_hooks
    @manage_content_type
    async def _request(
        self,
        method: str,
        path: str,
        /,
        *,
        auth: RequestAuth = USE_CLIENT_DEFAULT,
        quiet: bool | None = None,
        **raw_options: Any,
    ) -> RestResponse:
        """Low-level function for all HTTP verb methods

        :param method: HTTP method
        :param path: Endpoint path
        :param auth: Auth to use for this call only. Defaults to the client's own auth. Pass `None` to send the
                     request unauthenticated
        :param quiet: Suppress request/response logs for this call (Failed calls are still logged with reduced context).
                      Overrides the client's own `log_requests` default when given explicitly
        :param raw_options: Any other parameters passed directly to the httpx2 library
        """
        r = await self.client.request(method.upper(), path, auth=auth, **raw_options)
        return RestResponse(r)
