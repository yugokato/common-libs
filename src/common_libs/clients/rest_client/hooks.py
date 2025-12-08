from __future__ import annotations

import asyncio
import json
import sys
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from httpx import Request, Response

from common_libs.ansi_colors import ColorCodes, color
from common_libs.logging import get_logger

from .utils import get_response_reason, parse_query_strings, process_request_body, process_response

if TYPE_CHECKING:
    from .ext import RequestExt, ResponseExt
    from .rest_client import ClientType


logger = get_logger(__name__)
_hook_executor = ThreadPoolExecutor(max_workers=32)


@lru_cache
def get_hooks(rest_client: ClientType, quiet: bool) -> dict[str, list[Callable[..., Any]]]:
    """Get request/response hooks"""
    async_mode = rest_client.async_mode
    return {
        "request": [_hook_factory(request_hooks, async_mode, quiet)],
        "response": [_hook_factory(response_hooks, async_mode, quiet, rest_client)],
    }


def request_hooks(request: RequestExt, quiet: bool) -> None:
    """Request hooks"""
    _log_request(request, quiet)


def response_hooks(response: ResponseExt, quiet: bool, rest_client: ClientType) -> None:
    """Response hooks

    Note: Combining these hooks under one hook function makes sure that the response log and the API summary are
          displayed as one message on the console for async mode.
    """
    _log_response(response, quiet, rest_client)
    _print_api_summary(response, quiet, rest_client)


def _log_request(request: RequestExt, quiet: bool) -> None:
    """Log API request"""
    if not quiet:
        log_data = {
            "request_id": request.request_id,
            "request": f"{request.method.upper()} {request.url}",
            "method": request.method,
            "path": request.url,
            "payload": process_request_body(request),
            "request_headers": request.headers,
        }
        logger.info(f"request: {request.method} {request.url}", extra=log_data)


def _log_response(response: ResponseExt, quiet: bool, rest_client: ClientType) -> None:
    """Log API response"""
    request: RequestExt = response.request
    log_data = {
        "request_id": request.request_id,
        "request": f"{request.method.upper()} {request.url}",
        "method": request.method,
        "path": request.url,
        "status_code": response.status_code,
        "response_headers": response.headers,
        "response_time": None if response.stream else response.elapsed.total_seconds(),
    }
    if response.is_stream and response.is_success:
        log_data.update(response="N/A (streaming)")
    else:
        log_data.update(response=process_response(response, prettify=rest_client.prettify_response_log))

    msg = f"response: {response.status_code}"
    if reason := get_response_reason(response):
        msg += f" ({reason})"

    if response.is_success:
        if not quiet:
            logger.info(msg, extra=log_data)
    else:
        # Log response regardless of the "quiet" value
        logger.error(msg, extra=log_data)


def _print_api_summary(response: ResponseExt, quiet: bool, rest_client: ClientType) -> None:
    """Print API request/response summary to the console"""
    prettify = rest_client.prettify_response_log
    log_headers = rest_client.log_headers
    request: RequestExt = response.request
    if quiet:
        if not response.is_success:
            # Print to the console regardless of the "quiet" value
            processed_resp = process_response(response, prettify=prettify)
            err = (
                f"request_id: {request.request_id}\n"
                f"request: {request.method} {request.url}\n"
                f"status_code: {response.status_code}\n"
                f"response:{processed_resp}\n"
            )
            sys.stdout.write(color(err, color_code=ColorCodes.RED))
            sys.stdout.flush()
    else:
        bullet = "-"
        summary = ""

        # request_id
        summary += color(f"{bullet} request_id: {request.request_id}\n", color_code=ColorCodes.CYAN)

        # method and url
        summary += color(f"{bullet} request: {request.method} {response.url}\n", color_code=ColorCodes.CYAN)

        # request headers
        if log_headers:
            summary += color(f"{bullet} request_headers: {request.headers}\n", color_code=ColorCodes.CYAN)

        # request payload and query parameters
        if query_strings := parse_query_strings(str(request.url)):
            summary += color(f"{bullet} query params: {query_strings}\n", color_code=ColorCodes.CYAN)
        request_body = process_request_body(request, truncate_bytes=True)
        if request_body:
            payload: str | bytes
            try:
                payload = json.dumps(request_body)
            except TypeError:
                payload = request_body
            summary += color(f"{bullet} payload: {payload}\n", color_code=ColorCodes.CYAN)  # type: ignore

        # status_code and reason
        status_color_code = ColorCodes.GREEN if response.is_success else ColorCodes.RED
        summary += color(f"{bullet} status_code: ", color_code=ColorCodes.CYAN) + color(
            response.status_code, color_code=status_color_code
        )
        if reason := get_response_reason(response):
            summary += f" ({reason})"
        summary += "\n"

        # response
        formatted_response: Any
        if response.is_stream and response.is_success:
            formatted_response = "N/A (streaming)"
        else:
            formatted_response = process_response(response, prettify=prettify)
        if not response.is_success:
            formatted_response = color(formatted_response, color_code=ColorCodes.RED)
        if formatted_response is not None:
            summary += color(f"{bullet} response: ", color_code=ColorCodes.CYAN)
            summary += f"{formatted_response}\n"

        # response headers
        if log_headers:
            summary += color(f"{bullet} response_headers: {response.headers}\n", color_code=ColorCodes.CYAN)

        # response time
        if not response.is_stream:
            summary += color(
                f"{bullet} response_time: {response.elapsed.total_seconds()}s\n", color_code=ColorCodes.CYAN
            )

        sys.stdout.write(summary)
        sys.stdout.flush()


def _hook_factory(
    hook_func: Callable[..., Any], async_mode: bool, quiet: bool, *hook_args: Any, **hook_kwargs: Any
) -> Callable[..., Any]:
    """Dynamically create a hook with arguments"""

    def should_skip(request_or_response: Request | Response) -> bool:
        assert isinstance(request_or_response, Request | Response)
        if isinstance(request_or_response, Request):
            return quiet is True
        else:
            return quiet and request_or_response.is_success

    def sync_hook(request_or_response: RequestExt | ResponseExt) -> Any:
        if not should_skip(request_or_response):
            return hook_func(request_or_response, quiet, *hook_args, **hook_kwargs)

    async def async_hook(request_or_response: RequestExt | ResponseExt) -> Any:
        # Run the sync hook in a threadpool so it doesn't block. Note that instead of asyncio.to_thread(), we use
        # run_in_executor() with the custom hook_executor configured with larger max workers than the default
        if not should_skip(request_or_response):
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(_hook_executor, sync_hook, request_or_response)

    return async_hook if async_mode else sync_hook
