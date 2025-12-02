from __future__ import annotations

import traceback
import uuid
from collections.abc import AsyncGenerator, Awaitable, Generator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import partial
from typing import Any, Literal, TypeAlias, Union, cast

from httpx import AsyncClient, Request, Response, TimeoutException, TransportError
from httpx import Client as SyncClient
from httpx._auth import Auth

from common_libs.logging import get_logger

from .utils import process_response, retry_on

JSONType: TypeAlias = str | int | float | bool | None | list["JSONType"] | dict[str, "JSONType"]
APIResponse: TypeAlias = Union["RestResponse", Awaitable["RestResponse"]]

logger = get_logger(__name__)


class BearerAuth(Auth):
    def __init__(self, token: str) -> None:
        self.token = token

    def auth_flow(self, request: RequestExt) -> Generator[RequestExt]:
        request.headers["Authorization"] = f"Bearer {self.token}"
        yield request


class RequestExt(Request):
    """Extended Request class to add the following capabilities:

    - generates a request UUID for each request
    - add request start_time and end_time
    - store retried request
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.request_id = str(uuid.uuid4())
        self.start_time: datetime | None = None
        self.end_time: datetime | None = None
        self.retried: RequestExt | None = None


@dataclass
class ResponseExt(Response):
    """Extended Response class"""

    request: RequestExt
    is_stream: bool = False


@dataclass(frozen=True)
class RestResponse:
    """Response class that wraps the httpx Response object"""

    # raw response returned from httpx lib
    _response: ResponseExt = field(init=True)

    request_id: str = field(init=False)
    status_code: int = field(init=False)
    response: JSONType = field(init=False)
    response_time: float = field(init=False)
    request: RequestExt = field(init=False)
    ok: bool = field(init=False)
    is_stream: bool = field(init=False)

    def __post_init__(self) -> None:
        is_stream = self._response.is_stream
        object.__setattr__(self, "request_id", self._response.request.request_id)
        object.__setattr__(self, "status_code", self._response.status_code)
        object.__setattr__(self, "response_time", None if is_stream else self._response.elapsed.total_seconds())
        if is_stream:
            object.__setattr__(self, "response", None)
        else:
            object.__setattr__(self, "response", self._process_response(self._response))
        object.__setattr__(self, "request", self._response.request)
        object.__setattr__(self, "ok", self._response.is_success)
        object.__setattr__(self, "is_stream", is_stream)

    def raise_for_status(self) -> None:
        self._response.raise_for_status()

    def stream(
        self, mode: Literal["text", "bytes", "line", "raw"] = "text", chunk_size: int | None = None
    ) -> Generator[str | bytes]:
        """Shortcut to various httpx's response iteration functions"""
        if not self.is_stream:
            raise ValueError("This response is not a stream")

        if mode == "text":
            iter_func = partial(self._response.iter_text, chunk_size=chunk_size)
        elif mode == "bytes":
            iter_func = partial(self._response.iter_bytes, chunk_size=chunk_size)
        elif mode == "line":
            if chunk_size:
                raise ValueError("chunk size is not supported for line-by-line streaming")
            iter_func = self._response.iter_lines
        elif mode == "raw":
            iter_func = partial(self._response.iter_raw, chunk_size=chunk_size)
        else:
            raise ValueError(f"Invalid mode: {mode}")
        for d in iter_func():
            yield from d

    async def astream(
        self, mode: Literal["text", "bytes", "line", "raw"] = "text", chunk_size: int | None = None
    ) -> AsyncGenerator[str | bytes]:
        """Shortcut to various httpx's response iteration functions (for async)"""
        if not self.is_stream:
            raise ValueError("This response is not a stream")

        if mode == "text":
            iter_func = partial(self._response.aiter_text, chunk_size=chunk_size)
        elif mode == "bytes":
            iter_func = partial(self._response.aiter_bytes, chunk_size=chunk_size)
        elif mode == "line":
            if chunk_size:
                raise ValueError("chunk size is not supported for line-by-line streaming")
            iter_func = self._response.aiter_lines
        elif mode == "raw":
            iter_func = partial(self._response.aiter_raw, chunk_size=chunk_size)
        else:
            raise ValueError(f"Invalid mode: {mode}")
        async for d in iter_func():
            yield d

    def _process_response(self, response: ResponseExt) -> JSONType:
        """Get json-encoded content of a response if possible, otherwise return content of the response."""
        return process_response(response)


class HTTPClientMixin:
    """Shared mixin for sync and async httpx clients"""

    def build_request(self, *args: Any, **kwargs: Any) -> RequestExt:
        request = super().build_request(*args, **kwargs)  # type: ignore[misc]
        return self._modify_request(request)

    def _build_redirect_request(self, *args: Any, **kwargs: Any) -> RequestExt:
        request = super()._build_redirect_request(*args, **kwargs)  # type: ignore[misc]
        return self._modify_request(request)

    def call_request_hooks(self, request: RequestExt) -> None:
        """Call request hooks"""
        hooks = request.extensions.get("hooks", {})
        for request_hook in hooks.get("request", []):
            request_hook(request)

    async def acall_request_hooks(self, request: RequestExt) -> None:
        """Call request hooks (for async mode)"""
        hooks = request.extensions.get("hooks", {})
        for request_hook in hooks.get("request", []):
            await request_hook(request)

    def call_response_hooks(self, response: ResponseExt) -> None:
        """Call response hooks"""
        response.is_stream = not response.is_closed
        hooks = response.request.extensions.get("hooks", {})
        for response_hook in hooks.get("response", []):
            response_hook(response)

    async def acall_response_hooks(self, response: ResponseExt) -> None:
        """Call response hooks (for async mode)"""
        response.is_stream = not response.is_closed
        hooks = response.request.extensions.get("hooks", {})
        for response_hook in hooks.get("response", []):
            await response_hook(response)

    def _modify_request(self, request: Request) -> RequestExt:
        request.request_id = str(uuid.uuid4())
        request.start_time = None
        request.end_time = None
        request.retried = None
        return cast(RequestExt, request)

    def _build_log_data(self, request: RequestExt) -> dict[str, str]:
        return {
            "request_id": getattr(request, "request_id", None),
            "request": f"{request.method.upper()} {request.url}",
            "method": request.method,
            "path": str(request.url),
        }

    def _handle_error(self, e: Exception, request: RequestExt, log_data: dict[str, str]) -> None:
        if isinstance(e, TimeoutException):
            log_data["traceback"] = traceback.format_exc()
            logger.error(
                f"Request timed out: {request.method.upper()} {request.url}\n (request_id: {request.request_id})",
                extra=log_data,
            )
        else:
            log_data["traceback"] = traceback.format_exc()
            logger.error(
                f"An unexpected error occurred while processing the API request (request_id: {request.request_id})\n"
                f"request: {request.method.upper()} {request.url}\n"
                f"error: {type(e).__name__}: {e}",
                extra=log_data,
            )


class SyncHTTPClient(HTTPClientMixin, SyncClient):
    """Sync HTTP client that extends httpx.Client"""

    def send(self, request: RequestExt, **kwargs: Any) -> ResponseExt:
        """Add following behaviors to httpx's client.send()

        - Set X-Request-ID header
        - Dispatch request hooks
        - Reconnect in case a connection is reset by peer
        - Log exceptions
        """
        log_data = self._build_log_data(request)
        try:
            try:
                return cast(ResponseExt, self._send(request, **kwargs))
            except TransportError as e:
                if "Connection reset by peer" in str(e):
                    logger.warning("The connection was already reset by peer. Reconnecting...", extra=log_data)
                    return cast(ResponseExt, self._send(request, **kwargs))
                else:
                    raise
        except Exception as e:
            self._handle_error(e, request, log_data)
            raise

    @retry_on(503, retry_after=15, safe_methods_only=True)
    def _send(self, request: RequestExt, **kwargs: Any) -> ResponseExt:
        """Send a request"""
        self.call_request_hooks(request)
        request.start_time = datetime.now(tz=UTC)
        try:
            resp = cast(ResponseExt, super().send(request, **kwargs))
        finally:
            request.end_time = datetime.now(tz=UTC)
        self.call_response_hooks(resp)
        return resp


class AsyncHTTPClient(HTTPClientMixin, AsyncClient):
    async def send(self, request: RequestExt, **kwargs: Any) -> ResponseExt:
        """Async HTTP client that extends httpx.AsyncClient"""
        log_data = self._build_log_data(request)
        try:
            try:
                return cast(ResponseExt, await self._send(request, **kwargs))
            except TransportError as e:
                if "Connection reset by peer" in str(e):
                    logger.warning("The connection was already reset by peer. Reconnecting...", extra=log_data)
                    return cast(ResponseExt, await self._send(request, **kwargs))
                else:
                    raise
        except Exception as e:
            self._handle_error(e, request, log_data)
            raise

    @retry_on(503, retry_after=15, safe_methods_only=True)
    async def _send(self, request: RequestExt, **kwargs: Any) -> ResponseExt:
        """Send a request"""
        await self.acall_request_hooks(request)
        request.start_time = datetime.now(tz=UTC)
        try:
            resp = cast(ResponseExt, await super().send(request, **kwargs))
        finally:
            request.end_time = datetime.now(tz=UTC)
        await self.acall_response_hooks(resp)
        return resp
