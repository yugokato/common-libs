from __future__ import annotations

from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import Any, cast

from common_libs.logging import get_logger

from .base import RestClientBase
from .ext import ResponseExt, RestResponse
from .hooks import get_hooks
from .utils import manage_content_type

logger = get_logger(__name__)


class RestClient(RestClientBase):
    """Sync Rest API client"""

    def __init__(
        self,
        base_url: str,
        *,
        log_headers: bool = False,
        prettify_response_log: bool = True,
        timeout: int | float = 30,
    ) -> None:
        super().__init__(
            base_url, log_headers=log_headers, prettify_response_log=prettify_response_log, timeout=timeout
        )

    def get(self, path: str, /, *, quiet: bool = False, **query_params: Any) -> RestResponse:
        """Make a GET API request

        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param query_params: Query parameters
        """
        return self._get(path, query=query_params, quiet=quiet)

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
        return self._options(path, query=query_params, quiet=quiet)

    @contextmanager
    @manage_content_type
    def stream(
        self,
        method: str,
        path: str,
        /,
        *,
        json: dict[str, Any] | list[Any] | None = None,
        query: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
        quiet: bool = False,
        **raw_options: Any,
    ) -> Generator[RestResponse]:
        """Stream an HTTP API request"""
        with self.client.stream(
            method.upper(),
            self._generate_url(path, query=query),
            json=json,
            files=files,
            extensions={"hooks": get_hooks(self, quiet)},
            **raw_options,
        ) as r:
            yield RestResponse(r)

    @manage_content_type
    def _get(
        self, path: str, /, *, query: dict[str, Any] | None = None, quiet: bool = False, **raw_options: Any
    ) -> RestResponse:
        """Low-level function of get()

        :param path: Endpoint path
        :param query: Query parameters
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        r = self.client.get(
            self._generate_url(path),
            params=query,
            timeout=(raw_options.pop("timeout", self.timeout)),
            extensions={"hooks": get_hooks(self, quiet)},
            **raw_options,
        )
        return RestResponse(cast(ResponseExt, r))

    @manage_content_type
    def _post(
        self,
        path: str,
        /,
        *,
        json: dict[str, Any] | list[Any] | None = None,
        query: dict[str, Any] | None = None,
        quiet: bool = False,
        **raw_options: Any,
    ) -> RestResponse:
        """Low-level function of post()

        :param path: Endpoint path
        :param json: JSON payload
        :param query: Query parameters
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        r = self.client.post(
            self._generate_url(path),
            params=query,
            json=json,
            timeout=(raw_options.pop("timeout", self.timeout)),
            extensions={"hooks": get_hooks(self, quiet)},
            **raw_options,
        )
        return RestResponse(cast(ResponseExt, r))

    @manage_content_type
    def _delete(
        self,
        path: str,
        /,
        *,
        json: dict[str, Any] | list[Any] | None = None,
        query: dict[str, Any] | None = None,
        quiet: bool = False,
        **raw_options: Any,
    ) -> RestResponse:
        """Low-level function of delete()

        :param path: Endpoint path
        :param json: JSON payload
        :param query: Query parameters
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        # Use client.request() as client.delete() doesn't support json parameter
        r = self.client.request(
            "DELETE",
            self._generate_url(path),
            json=json,
            params=query,
            timeout=(raw_options.pop("timeout", self.timeout)),
            extensions={"hooks": get_hooks(self, quiet)},
            **raw_options,
        )
        return RestResponse(cast(ResponseExt, r))

    @manage_content_type
    def _put(
        self,
        path: str,
        /,
        *,
        json: dict[str, Any] | list[Any] | None = None,
        query: dict[str, Any] | None = None,
        quiet: bool = False,
        **raw_options: Any,
    ) -> RestResponse:
        """Low-level function of put()

        :param path: Endpoint path
        :param json: JSON payload
        :param query: Query parameters
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        r = self.client.put(
            self._generate_url(path, query=query),
            json=json,
            timeout=(raw_options.pop("timeout", self.timeout)),
            extensions={"hooks": get_hooks(self, quiet)},
            **raw_options,
        )
        return RestResponse(cast(ResponseExt, r))

    @manage_content_type
    def _patch(
        self,
        path: str,
        /,
        *,
        json: dict[str, Any] | list[Any] | None = None,
        query: dict[str, Any] | None = None,
        quiet: bool = False,
        **raw_options: Any,
    ) -> RestResponse:
        """Low-level function of patch()

        :param path: Endpoint path
        :param json: JSON payload
        :param query: Query parameters
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        r = self.client.patch(
            self._generate_url(path, query=query),
            json=json,
            timeout=(raw_options.pop("timeout", self.timeout)),
            extensions={"hooks": get_hooks(self, quiet)},
            **raw_options,
        )
        return RestResponse(cast(ResponseExt, r))

    @manage_content_type
    def _options(
        self, path: str, /, *, query: dict[str, Any] | None = None, quiet: bool = False, **raw_options: Any
    ) -> RestResponse:
        """Low-level function of options()

        :param path: Endpoint path
        :param query: Query parameters
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        r = self.client.options(
            self._generate_url(path, query=query),
            timeout=(raw_options.pop("timeout", self.timeout)),
            extensions={"hooks": get_hooks(self, quiet)},
            **raw_options,
        )
        return RestResponse(cast(ResponseExt, r))


class AsyncRestClient(RestClientBase):
    """Async Rest API client"""

    def __init__(
        self,
        base_url: str,
        *,
        log_headers: bool = False,
        prettify_response_log: bool = True,
        timeout: int | float = 30,
    ) -> None:
        super().__init__(
            base_url,
            log_headers=log_headers,
            prettify_response_log=prettify_response_log,
            timeout=timeout,
            async_mode=True,
        )

    async def get(self, path: str, /, *, quiet: bool = False, **query_params: Any) -> RestResponse:
        """Make a GET API request

        :param path: Endpoint path
        :param quiet: A flag to suppress API request/response log
        :param query_params: Query parameters
        """
        return await self._get(path, query=query_params, quiet=quiet)

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
        return await self._options(path, query=query_params, quiet=quiet)

    @asynccontextmanager
    @manage_content_type
    async def stream(
        self,
        method: str,
        path: str,
        /,
        *,
        json: dict[str, Any] | list[Any] | None = None,
        query: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
        quiet: bool = False,
        **raw_options: Any,
    ) -> AsyncGenerator[RestResponse]:
        """Stream an HTTP API request"""
        async with self.client.stream(
            method.upper(),
            self._generate_url(path, query=query),
            json=json,
            files=files,
            extensions={"hooks": get_hooks(self, quiet)},
            **raw_options,
        ) as r:
            yield RestResponse(r)

    @manage_content_type
    async def _get(
        self, path: str, /, *, query: dict[str, Any] | None = None, quiet: bool = False, **raw_options: Any
    ) -> RestResponse:
        """Low-level function of get()

        :param path: Endpoint path
        :param query: Query parameters
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        r = await self.client.get(
            self._generate_url(path),
            params=query,
            timeout=(raw_options.pop("timeout", self.timeout)),
            extensions={"hooks": get_hooks(self, quiet)},
            **raw_options,
        )
        return RestResponse(cast(ResponseExt, r))

    @manage_content_type
    async def _post(
        self,
        path: str,
        /,
        *,
        json: dict[str, Any] | list[Any] | None = None,
        query: dict[str, Any] | None = None,
        quiet: bool = False,
        **raw_options: Any,
    ) -> RestResponse:
        """Low-level function of post()

        :param path: Endpoint path
        :param json: JSON payload
        :param query: Query parameters
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        r = await self.client.post(
            self._generate_url(path),
            params=query,
            json=json,
            timeout=(raw_options.pop("timeout", self.timeout)),
            extensions={"hooks": get_hooks(self, quiet)},
            **raw_options,
        )
        return RestResponse(cast(ResponseExt, r))

    @manage_content_type
    async def _delete(
        self,
        path: str,
        /,
        *,
        json: dict[str, Any] | list[Any] | None = None,
        query: dict[str, Any] | None = None,
        quiet: bool = False,
        **raw_options: Any,
    ) -> RestResponse:
        """Low-level function of delete()

        :param path: Endpoint path
        :param json: JSON payload
        :param query: Query parameters
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        # Use client.request() as client.delete() doesn't support json parameter
        r = await self.client.request(
            "DELETE",
            self._generate_url(path),
            json=json,
            params=query,
            timeout=(raw_options.pop("timeout", self.timeout)),
            extensions={"hooks": get_hooks(self, quiet)},
            **raw_options,
        )
        return RestResponse(cast(ResponseExt, r))

    @manage_content_type
    async def _put(
        self,
        path: str,
        /,
        *,
        json: dict[str, Any] | list[Any] | None = None,
        query: dict[str, Any] | None = None,
        quiet: bool = False,
        **raw_options: Any,
    ) -> RestResponse:
        """Low-level function of put()

        :param path: Endpoint path
        :param json: JSON payload
        :param query: Query parameters
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        r = await self.client.put(
            self._generate_url(path, query=query),
            json=json,
            timeout=(raw_options.pop("timeout", self.timeout)),
            extensions={"hooks": get_hooks(self, quiet)},
            **raw_options,
        )
        return RestResponse(cast(ResponseExt, r))

    @manage_content_type
    async def _patch(
        self,
        path: str,
        /,
        *,
        json: dict[str, Any] | list[Any] | None = None,
        query: dict[str, Any] | None = None,
        quiet: bool = False,
        **raw_options: Any,
    ) -> RestResponse:
        """Low-level function of patch()

        :param path: Endpoint path
        :param json: JSON payload
        :param query: Query parameters
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        r = await self.client.patch(
            self._generate_url(path, query=query),
            json=json,
            timeout=(raw_options.pop("timeout", self.timeout)),
            extensions={"hooks": get_hooks(self, quiet)},
            **raw_options,
        )
        return RestResponse(cast(ResponseExt, r))

    @manage_content_type
    async def _options(
        self, path: str, /, *, query: dict[str, Any] | None = None, quiet: bool = False, **raw_options: Any
    ) -> RestResponse:
        """Low-level function of options()

        :param path: Endpoint path
        :param query: Query parameters
        :param quiet: A flag to suppress API request/response log
        :param raw_options: Any other parameters passed directly to the httpx library
        """
        r = await self.client.options(
            self._generate_url(path, query=query),
            timeout=(raw_options.pop("timeout", self.timeout)),
            extensions={"hooks": get_hooks(self, quiet)},
            **raw_options,
        )
        return RestResponse(cast(ResponseExt, r))
