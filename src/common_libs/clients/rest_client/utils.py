from __future__ import annotations

import errno
import inspect
import json
from collections.abc import Callable, Iterable
from functools import lru_cache, wraps
from http import HTTPStatus
from json import JSONDecodeError
from typing import TYPE_CHECKING, Any, Concatenate, ParamSpec, TypeVar
from urllib.parse import parse_qs, urlparse

from httpx2 import URL, Client, RequestNotRead

from common_libs.logging import get_logger

from .types import JSONType, Request, Response, RestResponse

if TYPE_CHECKING:
    from .rest_client import ClientType


P = ParamSpec("P")
T = TypeVar("T")

logger = get_logger(__name__)

TRUNCATE_LEN = 512
ORIGINAL_REQUEST_ATTR = "_original_request"
REQUEST_ID_HEADER = "X-Request-ID"
SENSITIVE_NAMES_EXTENSION = "sensitive_names"
SAFE_HTTP_METHODS = ("GET", "HEAD", "OPTIONS")
_SENSITIVE_FIELD_NAMES = frozenset({"password", "token", "secret", "api_key", "apikey", "api-key"})
_SENSITIVE_QUERY_NAMES = frozenset(
    {
        "access_token",
        "api_key",
        "apikey",
        "api-key",
        "auth_token",
        "client_secret",
        "id_token",
        "password",
        "refresh_token",
        "secret",
        "signature",
        "token",
    }
)
_SENSITIVE_HEADER_NAMES = frozenset(
    {"authorization", "proxy-authorization", "cookie", "set-cookie", "x-api-key", "api-key"}
)


def process_request_body(
    request: Request, hide_sensitive_values: bool = True, truncate_bytes: bool = False
) -> str | bytes:
    """Process request body

    Reads the body only when it is already buffered, e.g. a `json=`/`data=` payload, which httpx2 encodes
    eagerly. A `files=` upload or a streamed `content=` body that has not been read yet is reported as
    `<streaming>` instead of being forced into memory here: forcing it would defeat the point of streaming it,
    and would make an auth `401` replay decision depend on whether request logging happened to run first.
    """
    try:
        body = request.content
    except RequestNotRead:
        return "<streaming>"
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
            if _is_sensitive_field_name(k):
                body[k] = _mask_field_value(v)
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


def mask_sensitive_headers(headers: dict[str, str], extra_names: Iterable[str] = ()) -> dict[str, str]:
    """Return a copy of `headers` with sensitive values replaced by asterisks.

    Header names are matched case-insensitively against a built-in blocklist
    (`Authorization`, `Proxy-Authorization`, `Cookie`, `Set-Cookie`, `X-Api-Key`, `Api-Key`), plus any
    name in `extra_names`. All other headers are passed through unchanged.

    :param headers: A mapping of header name to header value.
    :param extra_names: Additional header names to mask, matched case-insensitively.
    """
    names = _SENSITIVE_HEADER_NAMES | {n.lower() for n in extra_names}
    return {k: ("***" if k.lower() in names else v) for k, v in headers.items()}


def mask_sensitive_url(url: str, extra_names: Iterable[str] = ()) -> str:
    """Return `url` with sensitive query parameter values and any userinfo password replaced by asterisks.

    Query parameter names are matched exactly and case-insensitively against a built-in list (`token`,
    `api_key`, `password`, `secret`, ...), plus any name in `extra_names`. Matching is exact rather than
    substring-based, unlike a request body field, so a pagination cursor like `page_token` is not masked just
    because it contains the word "token". A password embedded in the URL's userinfo is masked too, since
    httpx2 accepts it as an implicit `BasicAuth`. A URL with no `?` or `@` is returned as-is without being
    parsed at all.

    :param url: The URL string to mask.
    :param extra_names: Additional query parameter names to mask, matched exactly and
                        case-insensitively. Used to mask a name an auth scheme was configured with,
                        e.g. a custom `APIKeyAuth` parameter name that falls outside the built-in list.
    """
    if "?" not in url and "@" not in url:
        return url
    parsed = URL(url)
    masked_query = _mask_query(parsed.query, _SENSITIVE_QUERY_NAMES | {n.lower() for n in extra_names})
    if not parsed.password and masked_query == parsed.query:
        return url
    kwargs: dict[str, Any] = {}
    if masked_query != parsed.query:
        kwargs["query"] = masked_query
    if parsed.password:
        kwargs["username"] = parsed.username
        kwargs["password"] = "*" * len(parsed.password)
    return str(parsed.copy_with(**kwargs))


def _mask_query(query: bytes, names: frozenset[str]) -> bytes:
    """Mask sensitive parameter values in a URL's raw, still percent-encoded query string

    :param query: The query bytes, as returned by `httpx2.URL.query`.
    :param names: Lowercased parameter names whose values should be masked.
    """
    parts = []
    for part in query.split(b"&"):
        name, eq, value = part.partition(b"=")
        if eq and name.decode("ascii", "replace").lower() in names:
            part = name + b"=" + b"*" * len(value)
        parts.append(part)
    return b"&".join(parts)


def get_sensitive_names(request: Request) -> frozenset[str]:
    """Return the extra header/query parameter names an auth declared as sensitive on `request`

    Populated by an auth's flow (see `APIKeyAuth`, `TokenProviderAuth`) via the `sensitive_names` request
    extension, so a custom header or query parameter name is masked out of logs the same as a built-in
    one.

    :param request: The request to read the declared names from.
    """
    return request.extensions.get(SENSITIVE_NAMES_EXTENSION, frozenset())


def mask_request_headers(request: Request, headers: dict[str, str]) -> dict[str, str]:
    """Return a copy of `headers` masked using the built-in blocklist plus any names an auth declared sensitive
    on `request`

    :param request: The request to read declared sensitive names from. Pass `response.request` when masking a
                    response's headers, since that is the same object the auth flow mutated.
    :param headers: The header mapping to mask.
    """
    return mask_sensitive_headers(headers, extra_names=get_sensitive_names(request))


def build_log_data(request: Request) -> dict[str, Any]:
    """Build the `request_id`/`request`/`method`/`path` fields shared by every request/response logging path

    :param request: The request to summarize.
    """
    url = mask_sensitive_url(str(request.url), extra_names=get_sensitive_names(request))
    return {
        "request_id": request.request_id,
        "request": f"{request.method.upper()} {url}",
        "method": request.method,
        "path": url,
    }


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


def get_response_time(response: Response) -> float | None:
    """Return the response's elapsed time in seconds, or `None` for a streaming response.

    :param response: The response to read the elapsed time from.
    """
    return None if response.is_stream else response.elapsed.total_seconds()


def get_response_reason(response: Response) -> str:
    """Get response reason from the response. If the response doesn't have the value, we resolve it using HTTPStatus"""
    if response.reason_phrase:
        return response.reason_phrase
    else:
        try:
            return HTTPStatus(response.status_code).phrase
        except ValueError:
            return ""


def format_request_failure(response: Response | RestResponse) -> str:
    """Return a one-line summary of a failed request.

    :param response: The failed response
    """
    if isinstance(response, RestResponse):
        response = response._response

    log_data = build_log_data(response.request)
    status = str(response.status_code)
    if reason := get_response_reason(response):
        status += f" {reason}"
    return f"{log_data['request']} - {status} (request_id: {log_data['request_id']})"


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
        is_json_request = "json" in kwargs
        if kwargs.get("json") == {}:
            kwargs["json"] = None
        session_headers = self.client.headers
        raw_headers: Any = kwargs.get("headers")
        request_headers: dict[str, Any] = dict(raw_headers or {})
        merged = {**session_headers, **request_headers}
        has_content_type_header = "Content-Type" in [h.title() for h in merged]
        if not has_content_type_header and is_json_request and not any([kwargs.get("data"), kwargs.get("files")]):
            request_headers["Content-Type"] = "application/json"
            kwargs["headers"] = request_headers
        return f(self, *args, **kwargs)

    return wrapper


@lru_cache
def get_supported_request_parameters() -> list[str]:
    """Return a list of supported request parameters"""
    custom_params = ["quiet"]
    client_params = inspect.signature(Client.request).parameters
    return [k for k, v in client_params.items() if v.default is not v.empty] + custom_params


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


def _is_sensitive_field_name(name: str) -> bool:
    """Return whether a field or query parameter name matches a known sensitive word"""
    return any(part in name.lower() for part in _SENSITIVE_FIELD_NAMES)


def _mask_field_value(value: Any) -> Any:
    """Mask a single sensitive field value, preserving its shape"""
    if isinstance(value, list):
        return ["*" * len(item) if isinstance(item, str) else "***" for item in value]
    elif isinstance(value, str):
        return "*" * len(value)
    else:
        return "***"


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
