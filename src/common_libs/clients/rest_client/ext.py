from __future__ import annotations

import traceback
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

from httpx import AsyncClient, TimeoutException, TransportError
from httpx import Client as SyncClient
from httpx._auth import Auth

from common_libs.logging import get_logger

from .retry import DEFAULT_RETRY_POLICY, RetryPolicy, retry_on
from .types import Request, Response
from .utils import SAFE_HTTP_METHODS, is_connection_reset, set_request_to_exception

logger = get_logger(__name__)


class BearerAuth(Auth):
    def __init__(self, token: str) -> None:
        self.token = token

    def auth_flow(self, request: Request) -> Generator[Request]:
        request.headers["Authorization"] = f"Bearer {self.token}"
        yield request


class HTTPClientMixin:
    """Shared mixin for sync and async httpx clients"""

    _request_id_header = "X-Request-ID"

    def __init__(self, *args: Any, retry: RetryPolicy | None = DEFAULT_RETRY_POLICY, **kwargs: Any) -> None:
        """Initialize the mixin and build the retry decorator from the given policy.

        :param retry: Retry policy controlling automatic retry behavior, or `None` to disable retries.
        :param args: Positional arguments forwarded to the underlying httpx client.
        :param kwargs: Keyword arguments forwarded to the underlying httpx client.
        """
        self._retry_decorator: Any = (
            retry_on(
                retry.condition,
                num_retries=retry.num_retries,
                retry_after=retry.retry_after,
                safe_methods_only=retry.safe_methods_only,
            )
            if retry is not None
            else None
        )
        super().__init__(*args, **kwargs)

    def build_request(self, *args: Any, **kwargs: Any) -> Request:
        request = super().build_request(*args, **kwargs)  # type: ignore[misc]
        return self._modify_request(request)

    def _build_redirect_request(self, *args: Any, **kwargs: Any) -> Request:
        request = super()._build_redirect_request(*args, **kwargs)  # type: ignore[misc]
        return self._modify_request(request)

    def call_request_hooks(self, request: Request) -> None:
        """Call request hooks"""
        hooks = request.extensions.get("hooks", {})
        for request_hook in hooks.get("request", []):
            request_hook(request)

    async def acall_request_hooks(self, request: Request) -> None:
        """Call request hooks (for async mode)"""
        hooks = request.extensions.get("hooks", {})
        for request_hook in hooks.get("request", []):
            await request_hook(request)

    def call_response_hooks(self, response: Response) -> None:
        """Call response hooks"""
        if response.is_stream and not response.is_success:
            response.read()
        hooks = response.request.extensions.get("hooks", {})
        for response_hook in hooks.get("response", []):
            response_hook(response)

    async def acall_response_hooks(self, response: Response) -> None:
        """Call response hooks (for async mode)"""
        if response.is_stream and not response.is_success:
            await response.aread()
        hooks = response.request.extensions.get("hooks", {})
        for response_hook in hooks.get("response", []):
            await response_hook(response)

    @contextmanager
    def set_timestamp(self, request: Request) -> Generator[None]:
        """Set request start/end time

        :param request: Request
        """
        request.start_time = datetime.now(tz=UTC)
        try:
            yield
        finally:
            request.end_time = datetime.now(tz=UTC)

    def _modify_request(self, request: Request) -> Request:
        request_id = request.headers.get(self._request_id_header)
        if not request_id:
            request_id = str(uuid.uuid4())
            request.headers[self._request_id_header] = request_id
        request.request_id = request_id
        request.start_time = None
        request.end_time = None
        request.retried = None
        return request

    def _modify_response(self, response: Response) -> Response:
        response.is_stream = not response.is_closed
        return response

    def _build_log_data(self, request: Request) -> dict[str, str]:
        return {
            "request_id": request.request_id,
            "request": f"{request.method.upper()} {request.url}",
            "method": request.method,
            "path": str(request.url),
        }

    def _handle_error(self, e: Exception, request: Request, log_data: dict[str, str]) -> None:
        log_data["traceback"] = traceback.format_exc()
        if isinstance(e, TimeoutException):
            logger.error(
                f"Request timed out: {request.method.upper()} {request.url}\n (request_id: {request.request_id})",
                extra=log_data,
            )
        else:
            logger.error(
                f"An unexpected error occurred while processing the API request (request_id: {request.request_id})\n"
                f"request: {request.method.upper()} {request.url}\n"
                f"error: {type(e).__name__}: {e}",
                extra=log_data,
            )

    def _should_reconnect(self, exc: TransportError, request: Request) -> bool:
        """Return True if the request should be transparently reconnected after a connection reset.

        :param exc: The transport error that was raised.
        :param request: The request that triggered the error.
        """
        return is_connection_reset(exc) and request.method.upper() in SAFE_HTTP_METHODS


class SyncHTTPClient(HTTPClientMixin, SyncClient):
    """Sync HTTP client that extends httpx.Client"""

    def send(self, request: Request, **kwargs: Any) -> Response:
        """Add following behaviors to httpx's client.send()

        - Set X-Request-ID header
        - Dispatch request hooks
        - Reconnect in case a connection is reset by peer (safe methods only)
        - Retry on the configured policy (default: 503)
        - Log exceptions
        """
        log_data = self._build_log_data(request)
        send_fn = self._retry_decorator(self._send) if self._retry_decorator is not None else self._send
        try:
            try:
                return cast(Response, send_fn(request, **kwargs))
            except TransportError as e:
                if self._should_reconnect(e, request):
                    logger.warning("The connection was already reset by peer. Reconnecting...", extra=log_data)
                    return cast(Response, send_fn(request, **kwargs))
                else:
                    raise
        except Exception as e:
            set_request_to_exception(e, request)
            self._handle_error(e, request, log_data)
            raise

    def _send(self, request: Request, **kwargs: Any) -> Response:
        """Send a request"""
        self.call_request_hooks(request)
        try:
            with self.set_timestamp(request):
                resp = cast(Response, super().send(request, **kwargs))
        except Exception as e:
            set_request_to_exception(e, request)
            raise
        self._modify_response(resp)
        self.call_response_hooks(resp)
        return resp


class AsyncHTTPClient(HTTPClientMixin, AsyncClient):
    """Async HTTP client that extends httpx.AsyncClient"""

    async def send(self, request: Request, **kwargs: Any) -> Response:
        """Add following behaviors to httpx's async client.send()

        - Set X-Request-ID header
        - Dispatch request hooks
        - Reconnect in case a connection is reset by peer (safe methods only)
        - Retry on the configured policy (default: 503)
        - Log exceptions
        """
        log_data = self._build_log_data(request)
        send_fn = self._retry_decorator(self._send) if self._retry_decorator is not None else self._send
        try:
            try:
                return cast(Response, await send_fn(request, **kwargs))
            except TransportError as e:
                if self._should_reconnect(e, request):
                    logger.warning("The connection was already reset by peer. Reconnecting...", extra=log_data)
                    return cast(Response, await send_fn(request, **kwargs))
                else:
                    raise
        except Exception as e:
            set_request_to_exception(e, request)
            self._handle_error(e, request, log_data)
            raise

    async def _send(self, request: Request, **kwargs: Any) -> Response:
        """Send a request"""
        await self.acall_request_hooks(request)
        try:
            with self.set_timestamp(request):
                resp = cast(Response, await super().send(request, **kwargs))
        except Exception as e:
            set_request_to_exception(e, request)
            raise
        self._modify_response(resp)
        await self.acall_response_hooks(resp)
        return resp
