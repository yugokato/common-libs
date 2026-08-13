from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from typing import Any, Literal, TypeAlias

from httpx2 import Request as _Request
from httpx2 import Response as _Response

JSONType: TypeAlias = str | int | float | bool | list["JSONType"] | dict[str, "JSONType"] | None

StreamMode: TypeAlias = Literal["text", "bytes", "line", "raw"]
_STREAM_MODES: tuple[StreamMode, ...] = ("text", "bytes", "line", "raw")


class Request(_Request):
    """Extended httpx2 Request for type checking only.

    Attributes are monkey-patched onto httpx2.Request object when building a request. `isinstance()` checks
    against this class are always `False` for objects the client produces, since those remain plain
    `httpx2.Request` instances.
    """

    request_id: str
    start_time: datetime | None
    end_time: datetime | None
    retried: Request | None


class Response(_Response):
    """Extended httpx2 Response for type checking only.

    Attributes are monkey-patched onto httpx2.Response object when building a response. `isinstance()` checks
    against this class are always `False` for objects the client produces, since those remain plain
    `httpx2.Response` instances.
    """

    request: Request
    is_stream: bool


@dataclass(frozen=True)
class RestResponse:
    """Response class that wraps the httpx2 Response object"""

    # raw response returned from httpx2 lib
    _response: Response

    request_id: str = field(init=False)
    status_code: int = field(init=False)
    response: Any = field(init=False)
    response_time: float | None = field(init=False)
    request: Request = field(init=False)
    ok: bool = field(init=False)
    is_stream: bool = field(init=False)

    def __post_init__(self) -> None:
        from .utils import get_response_time, process_response

        resp = self._response
        is_stream = resp.is_stream
        for name, value in {
            "request_id": resp.request.request_id,
            "status_code": resp.status_code,
            "response_time": get_response_time(resp),
            "response": None if (is_stream and resp.is_success) else process_response(resp),
            "request": resp.request,
            "ok": resp.is_success,
            "is_stream": is_stream,
        }.items():
            object.__setattr__(self, name, value)

    def raise_for_status(self) -> None:
        """Raise an exception if the response has an error status code."""
        self._response.raise_for_status()

    def stream(self, mode: StreamMode = "text", chunk_size: int | None = None) -> Iterator[str | bytes]:
        """Shortcut to various httpx2's response iteration functions

        :param mode: The streaming mode: `text`, `bytes`, `line`, or `raw`.
        :param chunk_size: The size of each chunk to read. Not supported for `line` mode.
        """
        self._validate_stream(mode, chunk_size)
        funcs: dict[StreamMode, Callable[[], Iterator[str | bytes]]] = {
            "text": partial(self._response.iter_text, chunk_size=chunk_size),
            "bytes": partial(self._response.iter_bytes, chunk_size=chunk_size),
            "line": self._response.iter_lines,
            "raw": partial(self._response.iter_raw, chunk_size=chunk_size),
        }
        return funcs[mode]()

    def astream(self, mode: StreamMode = "text", chunk_size: int | None = None) -> AsyncIterator[str | bytes]:
        """Shortcut to various httpx2's response iteration functions (for async)

        :param mode: The streaming mode: `text`, `bytes`, `line`, or `raw`.
        :param chunk_size: The size of each chunk to read. Not supported for `line` mode.
        """
        self._validate_stream(mode, chunk_size)
        funcs: dict[StreamMode, Callable[[], AsyncIterator[str | bytes]]] = {
            "text": partial(self._response.aiter_text, chunk_size=chunk_size),
            "bytes": partial(self._response.aiter_bytes, chunk_size=chunk_size),
            "line": self._response.aiter_lines,
            "raw": partial(self._response.aiter_raw, chunk_size=chunk_size),
        }
        return funcs[mode]()

    def _validate_stream(self, mode: StreamMode, chunk_size: int | None) -> None:
        """Validate stream mode/chunk_size before an iterator is built

        :param mode: The streaming mode to validate.
        :param chunk_size: The chunk size to validate.
        """
        if not self.is_stream:
            raise ValueError("This response is not a stream")
        if mode not in _STREAM_MODES:
            raise ValueError(f"Invalid mode: {mode}")
        if mode == "line" and chunk_size:
            raise ValueError("chunk size is not supported for line-by-line streaming")
