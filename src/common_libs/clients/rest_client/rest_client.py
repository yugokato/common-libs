from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Callable, Generator
from contextlib import asynccontextmanager, contextmanager
from functools import wraps
from typing import Any, Concatenate, ParamSpec, Self, TypeVar, cast

from common_libs.logging import get_logger

from .base import RestClientBase
from .ext import ResponseExt, RestResponse
from .hooks import get_hooks
from .utils import manage_content_type

P = ParamSpec("P")
R = TypeVar("R")
ClientType = TypeVar("ClientType", "RestClient", "AsyncRestClient")

logger = get_logger(__name__)


def inject_hooks(f: Callable[Concatenate[ClientType, P], R]) -> Callable[Concatenate[ClientType, P], R]:
    """Inject request/response hooks as extensions option to a request"""

    @wraps(f)
    def wrapper(self: ClientType, *args: P.args, **kwargs: P.kwargs) -> R:
        assert isinstance(kwargs, dict)  # for making mypy happy
        quiet = kwargs.pop("quiet", False)
        kwargs.setdefault("extensions", {}).update(hooks=get_hooks(self, quiet))
        return f(self, *args, **kwargs)

    return wrapper


class RestClient(RestClientBase):
    """Sync Rest API client"""

    def __init__(
        self, base_url: str, *, log_headers: bool = False, prettify_response_log: bool = True, **kwargs: Any
    ) -> None:
        super().__init__(base_url, log_headers=log_headers, prettify_response_log=prettify_response_log, **kwargs)
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            raise RuntimeError(
                f"{RestClient.__name__} cannot be used inside async context. Use {AsyncRestClient.__name__} instead."
            )

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying httpx client"""
        self.client.close()

    def get(self, path: str, /, *, quiet: bool = False, **query_params: Any) -> RestResponse:
        """Make a GET API request

        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param query_params: Query parameters
        """
        return self._get(path, params=query_params, quiet=quiet)

    def post(
        self, path: str, /, *, files: dict[str, Any] | None = None, quiet: bool = False, **payload: Any
    ) -> RestResponse:
        """Make a POST API request

        :param path: Endpoint path
        :param files: File to upload
        :param quiet: A flag to suppress API request/response log
        :param payload: JSON payload
        """
        return self._post(path, json=payload, files=files, quiet=quiet)

    def delete(self, path: str, /, *, quiet: bool = False, **payload: Any) -> RestResponse:
        """Make a DELETE API request

        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param payload: JSON payload
        """
        return self._delete(path, json=payload, quiet=quiet)

    def put(self, path: str, /, *, quiet: bool = False, **payload: Any) -> RestResponse:
        """Make a PUT API request

        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param payload: JSON payload
        """
        return self._put(path, json=payload, quiet=quiet)

    def patch(self, path: str, /, *, quiet: bool = False, **payload: Any) -> RestResponse:
        """Make a PATCH API request

        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param payload: JSON payload
        """
        return self._patch(path, json=payload, quiet=quiet)

    def options(self, path: str, /, *, quiet: bool = False, **query_params: Any) -> RestResponse:
        """Make an OPTIONS API request

        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param query_params: Query parameters
        """
        return self._options(path, params=query_params, quiet=quiet)

    @contextmanager
    @inject_hooks
    @manage_content_type
    def stream(self, method: str, path: str, /, *, quiet: bool = False, **raw_options: Any) -> Generator[RestResponse]:
        """Stream an HTTP API request

        :param method: Endpoint method
        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        with self.client.stream(method.upper(), path, **raw_options) as r:
            yield RestResponse(r)

    @inject_hooks
    @manage_content_type
    def _get(self, path: str, /, *, quiet: bool = False, **raw_options: Any) -> RestResponse:
        """Low-level function of get()

        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        r = self.client.get(path, **raw_options)
        return RestResponse(cast(ResponseExt, r))

    @inject_hooks
    @manage_content_type
    def _post(self, path: str, /, *, quiet: bool = False, **raw_options: Any) -> RestResponse:
        """Low-level function of post()

        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        r = self.client.post(path, **raw_options)
        return RestResponse(cast(ResponseExt, r))

    @inject_hooks
    @manage_content_type
    def _delete(self, path: str, /, *, quiet: bool = False, **raw_options: Any) -> RestResponse:
        """Low-level function of delete()

        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        # Use client.request() as client.delete() doesn't support json parameter
        r = self.client.request("DELETE", path, **raw_options)
        return RestResponse(cast(ResponseExt, r))

    @inject_hooks
    @manage_content_type
    def _put(self, path: str, /, *, quiet: bool = False, **raw_options: Any) -> RestResponse:
        """Low-level function of put()

        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        r = self.client.put(path, **raw_options)
        return RestResponse(cast(ResponseExt, r))

    @inject_hooks
    @manage_content_type
    def _patch(self, path: str, /, *, quiet: bool = False, **raw_options: Any) -> RestResponse:
        """Low-level function of patch()

        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        r = self.client.patch(path, **raw_options)
        return RestResponse(cast(ResponseExt, r))

    @inject_hooks
    @manage_content_type
    def _options(self, path: str, /, *, quiet: bool = False, **raw_options: Any) -> RestResponse:
        """Low-level function of options()

        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        r = self.client.options(path, **raw_options)
        return RestResponse(cast(ResponseExt, r))


class AsyncRestClient(RestClientBase):
    """Async Rest API client"""

    def __init__(
        self, base_url: str, *, log_headers: bool = False, prettify_response_log: bool = True, **kwargs: Any
    ) -> None:
        super().__init__(
            base_url, log_headers=log_headers, prettify_response_log=prettify_response_log, async_mode=True, **kwargs
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the underlying httpx client"""
        await self.client.aclose()

    async def get(self, path: str, /, *, quiet: bool = False, **query_params: Any) -> RestResponse:
        """Make a GET API request

        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param query_params: Query parameters
        """
        return await self._get(path, params=query_params, quiet=quiet)

    async def post(
        self, path: str, /, *, files: dict[str, Any] | None = None, quiet: bool = False, **payload: Any
    ) -> RestResponse:
        """Make a POST API request

        :param path: Endpoint path
        :param files: File to upload
        :param quiet: A flag to suppress API request/response log
        :param payload: JSON payload
        """
        return await self._post(path, json=payload, files=files, quiet=quiet)

    async def delete(self, path: str, /, *, quiet: bool = False, **payload: Any) -> RestResponse:
        """Make a DELETE API request

        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param payload: JSON payload
        """
        return await self._delete(path, json=payload, quiet=quiet)

    async def put(self, path: str, /, *, quiet: bool = False, **payload: Any) -> RestResponse:
        """Make a PUT API request

        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param payload: JSON payload
        """
        return await self._put(path, json=payload, quiet=quiet)

    async def patch(self, path: str, /, *, quiet: bool = False, **payload: Any) -> RestResponse:
        """Make a PATCH API request

        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param payload: JSON payload
        """
        return await self._patch(path, json=payload, quiet=quiet)

    async def options(self, path: str, /, *, quiet: bool = False, **query_params: Any) -> RestResponse:
        """Make an OPTIONS API request

        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param query_params: Query parameters
        """
        return await self._options(path, params=query_params, quiet=quiet)

    @asynccontextmanager
    @inject_hooks
    @manage_content_type
    async def stream(
        self, method: str, path: str, /, *, quiet: bool = False, **raw_options: Any
    ) -> AsyncGenerator[RestResponse]:
        """Stream an HTTP API request

        :param method: Endpoint method
        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        async with self.client.stream(method.upper(), path, **raw_options) as r:
            yield RestResponse(r)

    @inject_hooks
    @manage_content_type
    async def _get(self, path: str, /, *, quiet: bool = False, **raw_options: Any) -> RestResponse:
        """Low-level function of get()

        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        r = await self.client.get(path, **raw_options)
        return RestResponse(cast(ResponseExt, r))

    @inject_hooks
    @manage_content_type
    async def _post(self, path: str, /, *, quiet: bool = False, **raw_options: Any) -> RestResponse:
        """Low-level function of post()

        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        r = await self.client.post(path, **raw_options)
        return RestResponse(cast(ResponseExt, r))

    @inject_hooks
    @manage_content_type
    async def _delete(self, path: str, /, *, quiet: bool = False, **raw_options: Any) -> RestResponse:
        """Low-level function of delete()

        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        # Use client.request() as client.delete() doesn't support json parameter
        r = await self.client.request("DELETE", path, **raw_options)
        return RestResponse(cast(ResponseExt, r))

    @inject_hooks
    @manage_content_type
    async def _put(self, path: str, /, *, quiet: bool = False, **raw_options: Any) -> RestResponse:
        """Low-level function of put()

        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        r = await self.client.put(path, **raw_options)
        return RestResponse(cast(ResponseExt, r))

    @inject_hooks
    @manage_content_type
    async def _patch(self, path: str, /, *, quiet: bool = False, **raw_options: Any) -> RestResponse:
        """Low-level function of patch()

        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        r = await self.client.patch(path, **raw_options)
        return RestResponse(cast(ResponseExt, r))

    @inject_hooks
    @manage_content_type
    async def _options(self, path: str, /, *, quiet: bool = False, **raw_options: Any) -> RestResponse:
        """Low-level function of options()

        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        r = await self.client.options(path, **raw_options)
        return RestResponse(cast(ResponseExt, r))
