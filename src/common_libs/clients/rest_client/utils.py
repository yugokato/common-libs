from __future__ import annotations

import errno
import inspect
import json
from collections.abc import Callable
from functools import lru_cache, wraps
from http import HTTPStatus
from json import JSONDecodeError
from typing import TYPE_CHECKING, Any, Concatenate, ParamSpec, TypeVar
from urllib.parse import parse_qs, urlparse

from httpx import Client

from common_libs.logging import get_logger

from .types import JSONType, Request, Response, RestResponse

if TYPE_CHECKING:
    from .rest_client import ClientType


P = ParamSpec("P")
T = TypeVar("T")

logger = get_logger(__name__)

TRUNCATE_LEN = 512
ORIGINAL_REQUEST_ATTR = "_original_request"
SAFE_HTTP_METHODS = ("GET", "HEAD", "OPTIONS")
_SENSITIVE_FIELD_NAMES = frozenset({"password", "token", "secret", "api_key", "apikey", "api-key"})
_SENSITIVE_HEADER_NAMES = frozenset(
    {"authorization", "proxy-authorization", "cookie", "set-cookie", "x-api-key", "api-key"}
)


def process_request_body(
    request: Request, hide_sensitive_values: bool = True, truncate_bytes: bool = False
) -> str | bytes:
    """Process request body"""
    body = request.read()
    if body:
        body = _decode_utf8(body)
        if isinstance(body, bytes):
            if truncate_bytes and len(body) > TRUNCATE_LEN:
                body = _truncate(body)
        else:
            try:
                body = json.loads(body)
            except (
                JSONDecodeError,
                UnicodeDecodeError,
            ):
                if not isinstance(body, str):
                    return body
            if hide_sensitive_values:
                body = mask_sensitive_value(body, request.headers.get("Content-Type", ""))
    return body


def mask_sensitive_value(body: Any, content_type: str) -> Any:
    """Mask a field value when a field name of the request body contains specific word"""
    if isinstance(body, dict):
        for k, v in body.items():
            if any(part in k.lower() for part in _SENSITIVE_FIELD_NAMES):
                if isinstance(v, list):
                    body[k] = ["*" * len(item) if isinstance(item, str) else "***" for item in v]
                elif isinstance(v, str):
                    body[k] = "*" * len(v)
                else:
                    body[k] = "***"
            elif isinstance(v, dict):
                mask_sensitive_value(v, content_type)
            elif isinstance(v, list):
                for nested_obj in v:
                    mask_sensitive_value(nested_obj, content_type)
    elif isinstance(body, str) and content_type == "application/x-www-form-urlencoded" and "=" in body:
        # Convert application/x-www-form-urlencoded data to a dictionary and mask sensitive values
        parsed_body = {k: v for p in body.split("&") if p and "=" in p for k, v in [p.split("=", 1)]}
        masked_body = mask_sensitive_value(parsed_body, content_type)
        return "&".join(f"{k}={v}" for k, v in masked_body.items())

    return body


def mask_sensitive_headers(headers: dict[str, str]) -> dict[str, str]:
    """Return a copy of `headers` with sensitive values replaced by asterisks.

    Header names are matched case-insensitively against a built-in blocklist
    (`Authorization`, `Proxy-Authorization`, `Cookie`, `Set-Cookie`, `X-Api-Key`, `Api-Key`).
    All other headers are passed through unchanged.

    :param headers: A mapping of header name to header value.
    """
    return {k: ("***" if k.lower() in _SENSITIVE_HEADER_NAMES else v) for k, v in headers.items()}


def process_response(response: Response | RestResponse, prettify: bool = False) -> JSONType:
    """Get json-encoded content of a response if possible, otherwise return content of the response"""
    if isinstance(response, RestResponse):
        response = response._response

    try:
        if response.is_stream:
            if response.is_success:
                raise NotImplementedError("Should not be used for a successful stream response")
            # NOTE: We assume response.read() / response.aread() was already called for failed stream requests.
            #       Especially for async mode, don't call asyncio.run(response.aread()) in here as it can cause
            #       RuntimeError: "<asyncio.locks.Event object at xxx [unset]>is bound to a different event loop" error
        resp = response.json()
        if prettify:
            resp = json.dumps(resp, indent=2)
    except JSONDecodeError:
        resp = _decode_utf8(response.content)

    return resp


def parse_query_strings(url: str) -> dict[str, Any] | None:
    """Parse query strings in the URL and return as a dictionary, if any"""
    q = urlparse(url)
    if q.query:
        query_params = parse_qs(q.query)
        return {k: v[0] if len(v) == 1 else v for k, v in query_params.items()}
    return None


def get_response_reason(response: Response) -> str:
    """Get response reason from the response. If the response doesn't have the value, we resolve it using HTTPStatus"""
    if response.reason_phrase:
        return response.reason_phrase
    else:
        try:
            return HTTPStatus(response.status_code).phrase
        except ValueError:
            return ""


def is_connection_reset(exc: BaseException) -> bool:
    """Return True if `exc` or any chained exception represents a TCP connection-reset by peer.

    :param exc: The exception to inspect, including its `__cause__` / `__context__` chain.
    """
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, ConnectionResetError) or (
            isinstance(current, OSError) and current.errno == errno.ECONNRESET
        ):
            return True
        current = current.__cause__ if current.__cause__ is not None else current.__context__
    return "Connection reset by peer" in str(exc)


def manage_content_type(f: Callable[Concatenate[ClientType, P], T]) -> Callable[Concatenate[ClientType, P], T]:
    """Set Content-Type: application/json header by default to a request whenever appropriate"""

    @wraps(f)
    def wrapper(self: ClientType, *args: P.args, **kwargs: P.kwargs) -> T:
        if kwargs.get("json") == {}:
            kwargs["json"] = None
        session_headers = self.client.headers
        raw_headers: Any = kwargs.get("headers")
        request_headers: dict[str, Any] = dict(raw_headers or {})
        merged = {**session_headers, **request_headers}
        has_content_type_header = "Content-Type" in [h.title() for h in merged]
        if (
            not has_content_type_header
            and kwargs.get("json") is not None
            and not any([kwargs.get("data"), kwargs.get("files")])
        ):
            request_headers["Content-Type"] = "application/json"
            kwargs["headers"] = request_headers
        return f(self, *args, **kwargs)

    return wrapper


@lru_cache
def get_supported_request_parameters() -> list[str]:
    """Return a list of supported request parameters"""
    custom_parameters = ["quiet"]
    requests_lib_params = inspect.signature(Client.request).parameters
    return [k for k, v in requests_lib_params.items() if v.default is not v.empty] + custom_parameters


def set_request_to_exception(exc: BaseException, request: Request) -> None:
    """Attach the original request to an exception so retry_on can chain it via request.retried

    :param exc: Exception to attach the request to
    :param request: Original request being sent when the exception was raised
    """
    setattr(exc, ORIGINAL_REQUEST_ATTR, request)


def get_request_from_exception(exc: BaseException) -> Request | None:
    """Return the original request attached to an exception by set_original_request, if any

    :param exc: Exception possibly carrying an attached request
    """
    return getattr(exc, ORIGINAL_REQUEST_ATTR, None)


def truncate_body(value: str | bytes) -> str | bytes:
    """Truncate a request/response body string or bytes when it exceeds the log threshold.

    :param value: The body string or bytes to truncate.
    """
    if len(value) > TRUNCATE_LEN:
        return _truncate(value)
    return value


def _decode_utf8(obj: Any) -> Any:
    """Decode bytes object with UTF-8, if possible"""
    if obj and isinstance(obj, bytes):
        try:
            obj = obj.decode("utf-8")
        except UnicodeDecodeError:
            # Binary file
            pass
    return obj


def _truncate(v: str | bytes) -> str | bytes:
    """Truncate value"""
    assert isinstance(v, str | bytes)
    trunc_pos = int(TRUNCATE_LEN / 2)
    trunc_mark = "   ...TRUNCATED...   "
    if isinstance(v, bytes):
        trunc_mark = trunc_mark.encode("utf-8")  # type: ignore[assignment]
    else:
        trunc_mark = "\n\n" + trunc_mark + "\n\n"
    return v[:trunc_pos] + trunc_mark + v[-trunc_pos:]  # type: ignore[operator]
