from __future__ import annotations

import traceback
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, cast

from httpx2 import AsyncClient, TimeoutException, TransportError
from httpx2 import Client as SyncClient
from httpx2._client import _is_https_redirect

from common_libs.logging import get_logger

from .rate_limit import RateLimit, RateLimiter
from .retry import DEFAULT_RETRY_POLICY, RetryPolicy, retry_on
from .types import Request, Response
from .utils import (
    REQUEST_ID_HEADER,
    SAFE_HTTP_METHODS,
    build_log_data,
    get_sensitive_names,
    is_connection_reset,
    set_request_to_exception,
)

logger = get_logger(__name__)


class HTTPClientMixin:
    """Shared mixin for sync and async httpx2 clients"""

    _request_id_header = REQUEST_ID_HEADER

    def __init__(
        self,
        *args: Any,
        retry_policy: RetryPolicy | None = DEFAULT_RETRY_POLICY,
        rate_limit: RateLimit | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the mixin and build the retry decorator and rate limiter from the given configs.

        :param retry_policy: Retry policy controlling automatic retry behavior, or `None` to disable retries.
        :param rate_limit: Client-side rate limit enforced on every request attempt, or `None` to disable.
        :param args: Positional arguments forwarded to the underlying httpx2 client.
        :param kwargs: Keyword arguments forwarded to the underlying httpx2 client.
        """
        self._retry_decorator: Any = (
            retry_on(
                retry_policy.condition,
                num_retries=retry_policy.num_retries,
                retry_after=retry_policy.retry_after,
                safe_methods_only=retry_policy.safe_methods_only,
            )
            if retry_policy is not None
            else None
        )
        self._rate_limiter: RateLimiter | None = RateLimiter(rate_limit) if rate_limit is not None else None
        super().__init__(*args, **kwargs)

    def build_request(self, *args: Any, **kwargs: Any) -> Request:
        request = super().build_request(*args, **kwargs)  # type: ignore[misc]
        return self._modify_request(request)

    def _build_redirect_request(self, request: Request, response: Response) -> Request:
        redirected = super()._build_redirect_request(request, response)  # type: ignore[misc]
        self._strip_sensitive_headers(request, redirected)
        return self._modify_request(redirected)

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
        """Stamp the client's own attributes on a request that does not already carry them

        Called both when the client builds a request and again right before one is dispatched, so a
        request that skipped the client's request building (handed straight to `send()`, or built from
        scratch by an auth flow) is still guaranteed a `request_id`.

        :param request: The request to stamp.
        """
        if not hasattr(request, "request_id"):
            request_id = request.headers.get(self._request_id_header)
            if not request_id:
                request_id = str(uuid.uuid4())
                request.headers[self._request_id_header] = request_id
            request.request_id = request_id
        for attr in ("start_time", "end_time", "retried"):
            if not hasattr(request, attr):
                setattr(request, attr, None)
        return request

    def _modify_response(self, response: Response) -> Response:
        response.is_stream = not response.is_closed
        return response

    def _strip_sensitive_headers(self, original: Request, redirected: Request) -> None:
        """Drop a header an auth declared sensitive from a cross-origin redirect

        httpx2 already strips `Authorization` on a redirect that changes origin, except for a same-host http-to-https
        upgrade. A custom header name a `TokenProviderAuth`/`APIKeyAuth` was configured with is not `httpx2`'s to know
        about, so it would otherwise survive that same redirect.

        :param original: The request that received the redirect response.
        :param redirected: The follow-up request httpx2 already built for the redirect.
        """
        names = {n.lower() for n in get_sensitive_names(original)} - {"authorization"}
        same_origin = original.url.origin == redirected.url.origin
        if not names or same_origin or _is_https_redirect(original.url, redirected.url):
            return
        for name in [n for n in redirected.headers if n.lower() in names]:
            del redirected.headers[name]

    def _handle_error(self, e: Exception, request: Request) -> None:
        log_data = build_log_data(request)
        log_data["traceback"] = traceback.format_exc()
        url = log_data["path"]
        request_id = log_data["request_id"]
        if isinstance(e, TimeoutException):
            logger.error(
                f"Request timed out: {request.method.upper()} {url}\n (request_id: {request_id})", extra=log_data
            )
        else:
            logger.error(
                f"An unexpected error occurred while processing the API request\n"
                f"- request: {request.method.upper()} {url}\n"
                f"- error: {type(e).__name__}: {e}\n"
                f"- request_id: {request_id}",
                extra=log_data,
            )

    def _should_reconnect(self, exc: TransportError, request: Request) -> bool:
        """Return True if the request should be transparently reconnected after a connection reset.

        :param exc: The transport error that was raised.
        :param request: The request that triggered the error.
        """
        return is_connection_reset(exc) and request.method.upper() in SAFE_HTTP_METHODS


class SyncHTTPClient(HTTPClientMixin, SyncClient):
    """Sync HTTP client that extends httpx2.Client"""

    def send(self, request: Request, **kwargs: Any) -> Response:
        """Add following behaviors to httpx2's client.send()

        - Set X-Request-ID header
        - Apply the client-side rate limit per attempt (when configured)
        - Dispatch request hooks
        - Reconnect in case a connection is reset by peer (safe methods only)
        - Retry on the configured policy (default: 503)
        - Log exceptions
        """
        self._modify_request(request)
        send_fn = self._retry_decorator(self._send) if self._retry_decorator is not None else self._send
        try:
            try:
                return cast(Response, send_fn(request, **kwargs))
            except TransportError as e:
                if self._should_reconnect(e, request):
                    logger.warning(
                        "The connection was already reset by peer. Reconnecting...", extra=build_log_data(request)
                    )
                    return cast(Response, send_fn(request, **kwargs))
                else:
                    raise
        except Exception as e:
            set_request_to_exception(e, request)
            self._handle_error(e, request)
            raise

    def _send(self, request: Request, **kwargs: Any) -> Response:
        """Send a request"""
        if self._rate_limiter is not None:
            self._rate_limiter.acquire()
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

    def _send_single_request(self, request: Request) -> Response:
        """Stamp `request` before it goes on the wire

        The one dispatch point every request reaches, including one an auth flow built from scratch rather
        than passing through `send()`.

        :param request: The request about to be sent.
        """
        return cast(Response, super()._send_single_request(self._modify_request(request)))


class AsyncHTTPClient(HTTPClientMixin, AsyncClient):
    """Async HTTP client that extends httpx2.AsyncClient"""

    async def send(self, request: Request, **kwargs: Any) -> Response:
        """Add following behaviors to httpx2's async client.send()

        - Set X-Request-ID header
        - Apply the client-side rate limit per attempt (when configured)
        - Dispatch request hooks
        - Reconnect in case a connection is reset by peer (safe methods only)
        - Retry on the configured policy (default: 503)
        - Log exceptions
        """
        self._modify_request(request)
        send_fn = self._retry_decorator(self._send) if self._retry_decorator is not None else self._send
        try:
            try:
                return cast(Response, await send_fn(request, **kwargs))
            except TransportError as e:
                if self._should_reconnect(e, request):
                    logger.warning(
                        "The connection was already reset by peer. Reconnecting...", extra=build_log_data(request)
                    )
                    return cast(Response, await send_fn(request, **kwargs))
                else:
                    raise
        except Exception as e:
            set_request_to_exception(e, request)
            self._handle_error(e, request)
            raise

    async def _send(self, request: Request, **kwargs: Any) -> Response:
        """Send a request"""
        if self._rate_limiter is not None:
            await self._rate_limiter.aacquire()
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

    async def _send_single_request(self, request: Request) -> Response:
        """Stamp `request` before it goes on the wire

        The one dispatch point every request reaches, including one an auth flow built from scratch rather
        than passing through `send()`.

        :param request: The request about to be sent.
        """
        return cast(Response, await super()._send_single_request(self._modify_request(request)))
