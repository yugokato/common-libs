from __future__ import annotations

from collections.abc import AsyncGenerator, Callable, Generator, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from typing import Any, Literal, TypeAlias

from httpx import Request as _Request
from httpx import Response as _Response

JSONType: TypeAlias = str | int | float | bool | None | list["JSONType"] | dict[str, "JSONType"]
RetryCondition: TypeAlias = int | type[Exception] | Sequence[int | type[Exception]] | Callable[..., bool]


class Request(_Request):
    """Extended httpx Request for type checking only.

    Attributes are monkey-patched onto httpx.Request object when building a request.
    """

    request_id: str
    start_time: datetime | None
    end_time: datetime | None
    retried: Request | None


class Response(_Response):
    """Extended httpx Response for type checking only.

    Attributes are monkey-patched onto httpx.Response object when building a response.
    """

    request: Request
    is_stream: bool


@dataclass(frozen=True)
class RestResponse:
    """Response class that wraps the httpx Response object"""

    # raw response returned from httpx lib
    _response: Response = field(init=True)

    request_id: str = field(init=False)
    status_code: int = field(init=False)
    response: Any = field(init=False)
    response_time: float | None = field(init=False)
    request: Request = field(init=False)
    ok: bool = field(init=False)
    is_stream: bool = field(init=False)

    def __post_init__(self) -> None:
        is_stream = self._response.is_stream
        object.__setattr__(self, "request_id", self._response.request.request_id)
        object.__setattr__(self, "status_code", self._response.status_code)
        object.__setattr__(self, "response_time", None if is_stream else self._response.elapsed.total_seconds())
        if is_stream and self._response.is_success:
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
        yield from iter_func()

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

    def _process_response(self, response: Response) -> JSONType:
        """Get json-encoded content of a response if possible, otherwise return content of the response."""
        from .utils import process_response

        return process_response(response)


@dataclass(frozen=True)
class RetryPolicy:
    """Policy controlling automatic HTTP request retry behavior.

    Pass an instance to `RestClient` or `AsyncRestClient` via the `retry` parameter.
    Use `retry=None` to disable automatic retries entirely.

    :param condition: Status code(s), exception class(es), or a callable matching the retry trigger.
                      Accepts the same forms as `retry_on`.
    :param num_retries: Maximum number of retry attempts.
    :param retry_after: Seconds to wait before retrying, or a callable that receives the response or
                        exception and returns the wait time.
    :param safe_methods_only: When `True`, only retries requests with safe HTTP methods
                              (GET, HEAD, OPTIONS).
    """

    condition: RetryCondition = 503
    num_retries: int = 1
    retry_after: float | int | Callable[..., float | int] = 15
    safe_methods_only: bool = True
