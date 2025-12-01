from __future__ import annotations

import asyncio
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

if TYPE_CHECKING:
    from .ext import JSONType, RequestExt, ResponseExt, RestResponse
    from .rest_client import ClientType


P = ParamSpec("P")
R = TypeVar("R")

logger = get_logger(__name__)


TRUNCATE_LEN = 512


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
            response.read()
        resp = response.json()
        if prettify:
            resp = json.dumps(resp, indent=4)
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


def manage_content_type(f: Callable[Concatenate[ClientType, P], R]) -> Callable[Concatenate[ClientType, P], R]:
    """Set Content-Type: application/json header by default to a request whenever appropriate"""

    @wraps(f)
    def wrapper(self: ClientType, *args: P.args, **kwargs: P.kwargs) -> R:
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
    condition: int | Sequence[int] | Callable[[R], bool],
    num_retry: int = 1,
    retry_after: float = 5,
    safe_methods_only: bool = False,
    _async_mode: bool = False,
) -> Callable[[Callable[P, R | Awaitable[R]]], Callable[P, R | Awaitable[R]]]:
    """Retry the request if the given condition matches

    :param condition: Either status code(s) or a function that takes response object as the argument
    :param num_retry: Max number of retries
    :param retry_after: Wait time before retrying in seconds
    :param safe_methods_only: Retry will happen only for safe methods
    :param _async_mode: Explicitly signal that the wrapped function executes async code
    """

    def matches_condition(r: ResponseExt) -> bool:
        if isinstance(condition, int):
            return r.status_code == condition
        elif isinstance(condition, tuple | list) and all(isinstance(x, int) for x in condition):
            return r.status_code in condition
        elif callable(condition):
            return condition(r)
        else:
            raise ValueError(f"Invalid condition: {condition}")

    def decorator_with_args(
        f: Callable[P, R | Awaitable[R]],
    ) -> Callable[P, R | Awaitable[R]]:
        from .ext import ResponseExt

        if _async_mode or inspect.iscoroutinefunction(f):

            @wraps(f)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                resp = cast(ResponseExt, await cast(Awaitable[R], f(*args, **kwargs)))
                num_retried = 0

                while num_retried < num_retry and matches_condition(resp):
                    if safe_methods_only and resp.request.method.upper() not in ["GET", "HEAD", "OPTIONS"]:
                        logger.warning("Retry condition matched but skipped (safe_methods_only=True).")
                        return resp

                    original_request: RequestExt = resp.request
                    msg = (
                        "Retry condition matched."
                        if callable(condition)
                        else f"Received status code {resp.status_code}."
                    )
                    msg += f" Retrying in {retry_after} seconds..."
                    logger.warning(
                        msg,
                        extra={
                            "status_code": resp.status_code,
                            "response": process_response(resp, prettify=True),
                            "request_id": original_request.request_id,
                        },
                    )

                    await asyncio.sleep(retry_after)
                    resp = cast(ResponseExt, await cast(Awaitable[R], f(*args, **kwargs)))
                    resp.request.retried = original_request
                    num_retried += 1

                if matches_condition(resp):
                    text = f"{num_retry} times" if num_retry > 1 else "once"
                    msg = f"Retried {text} but request still"
                    msg += (
                        " matches the condition" if callable(condition) else f" received status code {resp.status_code}"
                    )
                    logger.warning(msg)
                return resp

            return async_wrapper
        else:

            @wraps(f)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                resp = cast(ResponseExt, f(*args, **kwargs))
                num_retried = 0
                while num_retried < num_retry:
                    if matches_condition(resp):
                        if safe_methods_only and resp.request.method.upper() not in ["GET", "HEAD", "OPTIONS"]:
                            logger.warning(
                                "Retry condition matched but will be skipped (safe_methods_only=True was given)"
                            )
                            return resp

                        original_request: RequestExt = resp.request
                        if callable(condition):
                            msg = "Retry condition matched."
                        else:
                            msg = f"Received status code {resp.status_code}."
                        msg += f" Retrying in {retry_after} seconds..."
                        logger.warning(
                            msg,
                            extra={
                                "status_code": resp.status_code,
                                "response": process_response(resp, prettify=True),
                                "request_id": original_request.request_id,
                            },
                        )
                        time.sleep(retry_after)
                        resp = cast(ResponseExt, f(*args, **kwargs))
                        resp.request.retried = original_request
                        num_retried += 1
                    else:
                        break

                if matches_condition(resp):
                    text = f"{num_retry} times" if num_retry > 1 else "once"
                    msg = f"Retried {text} but the request still"
                    if callable(condition):
                        msg += " matches the condition"
                    else:
                        msg += f" received status code {resp.status_code}"
                    logger.warning(msg)
                return resp

        return wrapper

    return decorator_with_args


@lru_cache
def get_supported_request_parameters() -> list[str]:
    """Return a list of supported request parameters"""
    custom_parameters = ["quiet", "query"]
    requests_lib_params = inspect.signature(Client.request).parameters
    return [k for k, v in requests_lib_params.items() if v.default is not v.empty] + custom_parameters


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
