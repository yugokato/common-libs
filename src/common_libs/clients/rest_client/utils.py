from __future__ import annotations

import asyncio
import errno
import inspect
import json
import time
from collections.abc import Awaitable, Callable, Sequence
from functools import lru_cache, wraps
from http import HTTPStatus
from json import JSONDecodeError
from typing import TYPE_CHECKING, Any, Concatenate, ParamSpec, TypeVar, cast
from urllib.parse import parse_qs, urlparse

from httpx import Client

from common_libs.logging import get_logger

from .types import JSONType, Request, Response, RestResponse, RetryPolicy

if TYPE_CHECKING:
    from .rest_client import ClientType


P = ParamSpec("P")
T = TypeVar("T")
R = TypeVar("R", bound="RestResponse")

logger = get_logger(__name__)


DEFAULT_RETRY_POLICY = RetryPolicy()
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


def retry_on(
    condition: int | type[Exception] | Sequence[int | type[Exception]] | Callable[[R | Exception], bool],
    num_retries: int = 1,
    retry_after: float | int | Callable[[R | Exception], float | int] = 5,
    safe_methods_only: bool = False,
    _async_mode: bool = False,
) -> Callable[[Callable[P, R | Awaitable[R]]], Callable[P, R | Awaitable[R]]]:
    """Retry the request if the given condition matches

    :param condition: Status code(s), exception class(es), or a callable taking the response or exception to retry on.
                      A sequence may contain status codes, exception classes, or a mix of both.
                      When a callable is passed, it receives the response on HTTP-level retries, or the raised
                      exception when an exception-based retry occurs; the callable must handle both cases gracefully.
                      When evaluated against a response, the callable must not raise — errors are not caught.
    :param num_retries: Max number of retries
    :param retry_after: Wait time before retrying in seconds; when the condition matched on an exception, a callable
                        `retry_after` receives the raised exception instead of a response
    :param safe_methods_only: Retry only for safe HTTP methods (GET/HEAD/OPTIONS). For exception-based retries,
                              when the method cannot be determined (no request attached), the retry is skipped.
    :param _async_mode: Explicitly signal that the wrapped function executes async code
    """
    status_codes: frozenset[int] | None = None
    exc_types: tuple[type[Exception], ...] | None = None
    predicate: Callable[[R | Exception], bool] | None = None

    if isinstance(condition, bool):
        raise ValueError(f"Invalid condition: {condition!r}")
    elif isinstance(condition, int):
        status_codes = frozenset({condition})
    elif isinstance(condition, type) and issubclass(condition, Exception):
        exc_types = (condition,)
    elif callable(condition) and not isinstance(condition, type):
        predicate = condition
    elif isinstance(condition, Sequence) and not isinstance(condition, (str, bytes, bytearray)):
        items = tuple(condition)
        if not items:
            raise ValueError("condition sequence must not be empty")
        codes = [x for x in items if isinstance(x, int) and not isinstance(x, bool)]
        excs = [x for x in items if isinstance(x, type) and issubclass(x, Exception)]
        if len(codes) + len(excs) != len(items):
            raise ValueError(
                f"condition sequence must contain only status codes and/or exception classes: {condition!r}"
            )
        if codes:
            status_codes = frozenset(codes)
        if excs:
            exc_types = cast("tuple[type[Exception], ...]", tuple(excs))
    else:
        raise ValueError(f"Invalid condition: {condition!r}")

    is_callable_condition = predicate is not None

    def matches_condition(resp: R | None, exc: Exception | None) -> bool:
        if exc is not None:
            if exc_types is not None:
                return True  # already type-filtered in call()
            if predicate is not None:
                try:
                    return bool(predicate(exc))
                except Exception:
                    return False
            return False
        # exc is None — evaluate against the response
        assert resp is not None
        if status_codes is not None:
            return resp.status_code in status_codes
        if predicate is not None:
            return bool(predicate(resp))
        return False  # exception-based condition but no exception raised (e.g. after a successful retry)

    def _get_retry_context(
        request: Request | None, resp: R | None, exc: Exception | None
    ) -> tuple[float | int, str, dict[str, Any]]:
        """Compute the wait duration, log message, and log extras for a pending retry attempt."""
        if exc is not None:
            wait_secs = retry_after(exc) if callable(retry_after) else retry_after
            msg = "Retry condition matched." if is_callable_condition else f"Exception {type(exc).__name__} raised."
            log_extra: dict[str, Any] = {"exception": f"{type(exc)}: {exc!s}"}
        else:
            assert resp is not None
            assert request is not None
            wait_secs = retry_after(resp) if callable(retry_after) else retry_after
            msg = "Retry condition matched." if is_callable_condition else f"Received status code {resp.status_code}."
            log_extra = {
                "status_code": resp.status_code,
                "response": process_response(resp, prettify=True),
                "request_id": request.request_id,
            }
        return wait_secs, msg, log_extra

    def _log_retry(wait_secs: float | int, msg: str, log_extra: dict[str, Any]) -> None:
        logger.warning(f"{msg} Retrying in {wait_secs} seconds...", extra=log_extra)

    def _log_exhausted(num_retried: int, condition_matched: bool, resp: R | None, exc: Exception | None) -> None:
        if num_retried > 0 and condition_matched:
            text = f"{num_retried} times" if num_retried > 1 else "once"
            if exc is not None:
                reason = f"raised {type(exc).__name__}"
            else:
                assert resp is not None
                reason = (
                    "matches the condition" if is_callable_condition else f"received status code {resp.status_code}"
                )
            logger.warning(f"Retried {text} but request still {reason}")

    def _should_swallow(e: Exception) -> bool:
        """Return True when `e` matches the retry condition and should be captured rather than propagated."""
        if exc_types is not None:
            return isinstance(e, exc_types)
        return is_callable_condition

    def _prepare_retry(
        resp: R | None, exc: Exception | None, num_retried: int
    ) -> tuple[Request | None, float | int] | None:
        """Decide whether to retry, logging the outcome.

        Return `(request, wait_secs)` when a retry should happen, or `None` to stop.
        """
        matched = matches_condition(resp, exc)
        if num_retried >= num_retries or not matched:
            _log_exhausted(num_retried, matched, resp, exc)
            return None
        if exc is not None:
            request = get_request_from_exception(exc)
        else:
            assert resp is not None
            request = resp.request
        if safe_methods_only and (request is None or request.method.upper() not in SAFE_HTTP_METHODS):
            logger.warning("Retry condition matched but skipped (safe_methods_only=True).")
            return None
        wait_secs, msg, log_extra = _get_retry_context(request, resp, exc)
        _log_retry(wait_secs, msg, log_extra)
        return request, wait_secs

    def _finalize(resp: R | None, exc: Exception | None) -> R:
        """Raise the final exception if one is set, otherwise return the final response."""
        if exc is not None:
            raise exc
        assert resp is not None
        return resp

    def _retry_sync(f: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        def call() -> tuple[R | None, Exception | None]:
            try:
                return f(*args, **kwargs), None
            except Exception as e:
                if not _should_swallow(e):
                    raise
                return None, e

        resp, exc = call()
        num_retried = 0
        while (plan := _prepare_retry(resp, exc, num_retried)) is not None:
            request, wait_secs = plan
            time.sleep(wait_secs)
            resp, exc = call()
            if resp is not None and request is not None:
                resp.request.retried = request
            num_retried += 1
        return _finalize(resp, exc)

    async def _retry_async(f: Callable[P, R | Awaitable[R]], *args: P.args, **kwargs: P.kwargs) -> R:
        async def call() -> tuple[R | None, Exception | None]:
            try:
                r = f(*args, **kwargs)
                if inspect.isawaitable(r):
                    r = await r
                return r, None
            except Exception as e:
                if not _should_swallow(e):
                    raise
                return None, e

        resp, exc = await call()
        num_retried = 0
        while (plan := _prepare_retry(resp, exc, num_retried)) is not None:
            request, wait_secs = plan
            await asyncio.sleep(wait_secs)
            resp, exc = await call()
            if resp is not None and request is not None:
                resp.request.retried = request
            num_retried += 1
        return _finalize(resp, exc)

    def decorator(f: Callable[P, R | Awaitable[R]]) -> Callable[P, R | Awaitable[R]]:
        @wraps(f)
        async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return await _retry_async(f, *args, **kwargs)

        @wraps(f)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return _retry_sync(f, *args, **kwargs)  # type: ignore[arg-type]

        if _async_mode or inspect.iscoroutinefunction(f):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


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
