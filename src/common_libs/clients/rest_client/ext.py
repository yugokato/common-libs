import traceback
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TypeAlias

from requests import ConnectionError, PreparedRequest, ReadTimeout, Response, Session
from requests.auth import AuthBase
from requests.hooks import dispatch_hook

from common_libs.logging import get_logger

from .utils import process_response, retry_on

JSONType: TypeAlias = str | int | float | bool | None | list["JSONType"] | dict[str, "JSONType"]


logger = get_logger(__name__)


class BearerAuth(AuthBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["Authorization"] = f"Bearer {self.token}"
        return r


class PreparedRequestExt(PreparedRequest):
    """Extended PreparedRequest class that generates a request UUID for each request"""

    def __init__(self):
        super().__init__()
        self.request_id = str(uuid.uuid4())
        self.start_time: datetime | None = None
        self.end_time: datetime | None = None
        self.retried: PreparedRequestExt | None = None


@dataclass
class ResponseExt(Response):
    """Extended Response class"""

    request: PreparedRequestExt


@dataclass(frozen=True)
class RestResponse:
    """Response class that wraps the requests Response object"""

    # raw response returned from requests lib
    _response: ResponseExt | Response = field(init=True)

    request_id: str = field(init=False)
    status_code: int = field(init=False)
    response: JSONType = field(init=False)
    response_time: float = field(init=False)
    request: PreparedRequestExt = field(init=False)
    ok: bool = field(init=False)
    is_stream: bool = False

    def __post_init__(self):
        object.__setattr__(self, "request_id", self._response.request.request_id)
        object.__setattr__(self, "status_code", self._response.status_code)
        object.__setattr__(self, "response_time", self._response.elapsed.total_seconds())
        if self.is_stream and self._response.ok:
            object.__setattr__(self, "response", None)
        else:
            object.__setattr__(self, "response", self._process_response(self._response))
        object.__setattr__(self, "request", self._response.request)
        object.__setattr__(self, "ok", self._response.ok)

    @property
    def response_as_generator(self) -> Iterator[JSONType]:
        """Return response as a generator. Use this when iterating a large response"""
        yield from self.response

    def raise_for_status(self):
        self._response.raise_for_status()

    def _process_response(self, response: ResponseExt):
        """Get json-encoded content of a response if possible, otherwise return content of the response."""
        return process_response(response)


class SessionExt(Session):
    def __init__(self):
        super().__init__()

    def send(self, request: PreparedRequestExt, **kwargs) -> ResponseExt:
        """Add following behaviors to requests.Session.send()

        - Set X-Request-ID header
        - Dispatch request hooks
        - Reconnect in case a connection is reset by peer
        - Log exceptions
        """
        log_data = {
            "request_id": request.request_id,
            "request": f"{request.method.upper()} {request.url}",
            "method": request.method,
            "path": request.path_url,
        }
        request.headers.update({"X-Request-ID": request.request_id})
        try:
            try:
                return self._send(request, **kwargs)
            except ConnectionError as e:
                if "Connection reset by peer" in str(e):
                    logger.warning("The connection was already reset by peer. Reconnecting...", extra=log_data)
                    return self._send(request, **kwargs)
                else:
                    raise
        except ReadTimeout as e:
            log_data["traceback"] = traceback.format_exc()
            logger.error(
                f"Request timed out: {request.method.upper()} {request.url}\n (request_id: {request.request_id})",
                extra=log_data,
            )
            raise e from None
        except Exception as e:
            log_data["traceback"] = traceback.format_exc()
            logger.error(
                f"An unexpected error occurred while processing the API request (request_id: {request.request_id})\n"
                f"request: {request.method.upper()} {request.url}\n"
                f"error: {type(e).__name__}: {e}",
                extra=log_data,
            )
            raise

    @retry_on(503, retry_after=15, safe_methods_only=True)
    def _send(self, request: PreparedRequestExt, **kwargs) -> Response | ResponseExt:
        """Send a request"""
        request.start_time = datetime.now(tz=UTC)
        dispatch_hook("request", request.hooks, request, **kwargs)
        try:
            return super().send(request, **kwargs)
        finally:
            request.end_time = datetime.now(tz=UTC)
