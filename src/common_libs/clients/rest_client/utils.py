from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import Awaitable, Callable, Sequence
from functools import lru_cache, wraps
from http import HTTPStatus
from json import JSONDecodeError
from typing import TYPE_CHECKING, Any, Concatenate, ParamSpec, TypeVar, cast
from urllib.parse import parse_qs, urlparse

from httpx import Client

from common_libs.logging import get_logger

if TYPE_CHECKING:
    from .ext import JSONType, RequestExt, ResponseExt, RestResponse
    from .rest_client import ClientType


P = ParamSpec("P")
T = TypeVar("T")
R = TypeVar("R", bound="RestResponse")

logger = get_logger(__name__)


TRUNCATE_LEN = 512
ORIGINAL_REQUEST_ATTR = "_original_request"
SAFE_HTTP_METHODS = ("GET", "HEAD", "OPTIONS")


def process_request_body(
    request: RequestExt, hide_sensitive_values: bool = True, truncate_bytes: bool = False
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
                body = mask_sensitive_value(body, request.headers["Content-Type"])
    return body


def mask_sensitive_value(body: Any, content_type: str) -> Any:
    """Mask a field value when a field name of the request body contains specific word"""
    if isinstance(body, dict):
        part_field_names_to_mask_value = [
            "password"
            # TODO: Add more if needed
        ]
        for k, v in body.items():
            if isinstance(v, dict):
                mask_sensitive_value(v, content_type)
            elif isinstance(v, list):
                for nested_obj in v:
                    mask_sensitive_value(nested_obj, content_type)
            elif isinstance(v, str) and any(part in k for part in part_field_names_to_mask_value):
                body[k] = "*" * len(v)
    elif isinstance(body, str) and content_type == "application/x-www-form-urlencoded" and "=" in body:
        # Convert application/x-www-form-urlencoded data to a dictionary and mask sensitive values
        parsed_body = {k: v for k, v in [p.split("=") for p in body.split("&") if p]}
        masked_body = mask_sensitive_value(parsed_body, content_type)
        return "&".join(f"{k}={v}" for k, v in masked_body.items())

    return body


def process_response(response: ResponseExt | RestResponse, prettify: bool = False) -> JSONType:
    """Get json-encoded content of a response if possible, otherwise return content of the response"""
    from .ext import RestResponse

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


def get_response_reason(response: ResponseExt) -> str:
    """Get response reason from the response. If the response doesn't have the value, we resolve it using HTTPStatus"""
    if response.reason_phrase:
        return response.reason_phrase
    else:
        try:
            return HTTPStatus(response.status_code).phrase
        except ValueError:
            return ""


def manage_content_type(f: Callable[Concatenate[ClientType, P], T]) -> Callable[Concatenate[ClientType, P], T]:
    """Set Content-Type: application/json header by default to a request whenever appropriate"""

    @wraps(f)
    def wrapper(self: ClientType, *args: P.args, **kwargs: P.kwargs) -> T:
        session_headers = self.client.headers
        request_headers = cast(dict[str, Any], kwargs.get("headers", {}))
        headers = {**session_headers, **request_headers}
        has_content_type_header = "Content-Type" in [h.title() for h in list(headers.keys())]
        content_type_set = False
        if not has_content_type_header and (kwargs.get("json") or not any([kwargs.get("data"), kwargs.get("files")])):
            self.client.headers.update({"Content-Type": "application/json"})
            content_type_set = True
        try:
            return f(self, *args, **kwargs)
        finally:
            if content_type_set:
                self.client.headers.pop("Content-Type", None)

    return wrapper


def retry_on(
    condition: int | type[Exception] | Sequence[int] | Sequence[type[Exception]] | Callable[[R | Exception], bool],
    num_retries: int = 1,
    retry_after: float | int | Callable[[R | Exception], float | int] = 5,
    safe_methods_only: bool = False,
    _async_mode: bool = False,
) -> Callable[[Callable[P, R | Awaitable[R]]], Callable[P, R | Awaitable[R]]]:
    """Retry the request if the given condition matches

    :param condition: Status code(s), exception class(es), or a callable taking the response or exception to retry on.
                      When a callable is passed, it receives the response on HTTP-level retries, or the raised
                      exception when an exception-based retry occurs; the callable must handle both cases gracefully.
    :param num_retries: Max number of retries
    :param retry_after: Wait time before retrying in seconds; when the condition matched on an exception, a callable
                        retry_after receives the raised exception instead of a response
    :param safe_methods_only: Retry only for safe HTTP methods (GET/HEAD/OPTIONS). For exception-based retries,
                              when the method cannot be determined (no request attached), the retry is skipped.
    :param _async_mode: Explicitly signal that the wrapped function executes async code
    """
    exc_types: tuple[type[Exception], ...] | None = None
    if isinstance(condition, type) and issubclass(condition, Exception):
        condition = (condition,)

    if isinstance(condition, (tuple, list)):
        if not condition:
            raise ValueError("condition sequence must not be empty")
        if all(isinstance(x, type) and issubclass(x, Exception) for x in condition):
            exc_types = tuple(condition)

    # True when condition is a plain callable (not an exception class or sequence of exception classes)
    is_callable_condition = callable(condition) and not isinstance(condition, type)

    def matches_condition(resp: R | None, exc: Exception | None) -> bool:
        if exc is not None:
            if exc_types is not None:
                return True  # already type-filtered in call()
            # callable condition — try to evaluate against the exception
            if is_callable_condition:
                try:
                    return bool(condition(exc))  # type: ignore
                except Exception:
                    return False
            return False  # non-callable condition cannot match an exception; unreachable in practice
        # exc is None — evaluate against the response
        if exc_types is not None:
            return False  # exception-based condition but no exception raised (e.g. after a successful retry)
        assert resp is not None
        if isinstance(condition, int):
            return resp.status_code == condition
        elif isinstance(condition, tuple | list) and all(isinstance(x, int) for x in condition):
            return resp.status_code in condition
        elif is_callable_condition:
            return condition(resp)  # type: ignore
        else:
            raise ValueError(f"Invalid condition: {condition}")

    async def _retry_on(f: Callable[P, R | Awaitable[R]], *args: P.args, **kwargs: P.kwargs) -> R:
        async def call() -> tuple[R | None, Exception | None]:
            """Run f once. Return (response, None), or (None, exc) when a matching exception is caught."""
            try:
                r = f(*args, **kwargs)
                if inspect.isawaitable(r):
                    r = await r
                return r, None
            except Exception as e:
                if exc_types is not None:
                    if not isinstance(e, exc_types):
                        raise
                elif not is_callable_condition:
                    raise
                return None, e

        resp, exc = await call()
        num_retried = 0
        log_extra: dict[str, Any]
        while num_retried < num_retries and matches_condition(resp, exc):
            if exc is not None:
                request = get_request_from_exception(exc)  # may be None when no request was injected
                if safe_methods_only and (request is None or request.method.upper() not in SAFE_HTTP_METHODS):
                    logger.warning("Retry condition matched but skipped (safe_methods_only=True).")
                    raise exc
                wait_secs = retry_after(exc) if callable(retry_after) else retry_after
                msg = "Retry condition matched." if callable(condition) else f"Exception {type(exc).__name__} raised."
                log_extra = {"exception": f"{type(exc)}: {exc!s}"}
            else:
                assert resp is not None
                request = resp.request
                if safe_methods_only and request.method.upper() not in SAFE_HTTP_METHODS:
                    logger.warning("Retry condition matched but skipped (safe_methods_only=True).")
                    return resp
                wait_secs = retry_after(resp) if callable(retry_after) else retry_after
                msg = "Retry condition matched." if callable(condition) else f"Received status code {resp.status_code}."
                log_extra = {
                    "status_code": resp.status_code,
                    "response": process_response(resp, prettify=True),
                    "request_id": request.request_id,
                }
            msg += f" Retrying in {wait_secs} seconds..."
            logger.warning(msg, extra=log_extra)

            await asyncio.sleep(wait_secs)

            resp, exc = await call()
            if exc is None and request is not None:
                assert resp is not None
                resp.request.retried = request
            num_retried += 1

        if num_retried > 0 and matches_condition(resp, exc):
            text = f"{num_retries} times" if num_retries > 1 else "once"
            if exc is not None:
                reason = f"raised {type(exc).__name__}"
            else:
                assert resp is not None
                reason = "matches the condition" if callable(condition) else f"received status code {resp.status_code}"
            logger.warning(f"Retried {text} but request still {reason}")

        if exc is not None:
            raise exc
        assert resp is not None
        return resp

    def decorator(f: Callable[P, R | Awaitable[R]]) -> Callable[P, R | Awaitable[R]]:
        @wraps(f)
        async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return await _retry_on(f, *args, **kwargs)

        @wraps(f)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return asyncio.run(_retry_on(f, *args, **kwargs))

        if _async_mode or inspect.iscoroutinefunction(f):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


@lru_cache
def get_supported_request_parameters() -> list[str]:
    """Return a list of supported request parameters"""
    custom_parameters = ["quiet", "query"]
    requests_lib_params = inspect.signature(Client.request).parameters
    return [k for k, v in requests_lib_params.items() if v.default is not v.empty] + custom_parameters


def set_request_to_exception(exc: BaseException, request: RequestExt) -> None:
    """Attach the original request to an exception so retry_on can chain it via request.retried

    :param exc: Exception to attach the request to
    :param request: Original request being sent when the exception was raised
    """
    setattr(exc, ORIGINAL_REQUEST_ATTR, request)


def get_request_from_exception(exc: BaseException) -> RequestExt | None:
    """Return the original request attached to an exception by set_original_request, if any

    :param exc: Exception possibly carrying an attached request
    """
    return getattr(exc, ORIGINAL_REQUEST_ATTR, None)


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
