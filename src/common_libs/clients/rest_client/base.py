from collections.abc import Callable
from typing import Any

from httpx import Timeout
from httpx._types import TimeoutTypes

from common_libs.logging import get_logger

from .ext import AsyncHTTPClient, BearerAuth, SyncHTTPClient
from .retry import DEFAULT_RETRY_POLICY, RetryPolicy

logger = get_logger(__name__)


class RestClientBase:
    """Base class for sync and async rest client"""

    def __init__(
        self,
        base_url: str,
        /,
        *,
        log_headers: bool = False,
        prettify_response_log: bool = True,
        async_mode: bool = False,
        timeout: TimeoutTypes = Timeout(5.0, read=30),
        retry: RetryPolicy | None = DEFAULT_RETRY_POLICY,
        **kwargs: Any,
    ) -> None:
        """
        :param base_url: API base url
        :param log_headers: Include request/response headers to the API summary logs
        :param prettify_response_log: Prettify response in the API summary logs
        :param async_mode: Use async mode
        :param timeout: The client-level timeout settings. This can be overridden in each request
        :param retry: Retry policy for automatic request retries, or `None` to disable.
                      Defaults to retrying once on HTTP 503 after 5 s for safe methods only.
        :param kwargs: Any other parameters to pass to the httpx client
        """
        self.log_headers = log_headers
        self.prettify_response_log = prettify_response_log
        self.async_mode = async_mode
        self._hooks_cache: dict[bool, dict[str, list[Callable[..., Any]]]] = {}
        kwargs.setdefault("http2", True)
        init_opts = dict(base_url=base_url, timeout=timeout, retry=retry, **kwargs)
        if self.async_mode:
            self.client = AsyncHTTPClient(**init_opts)
        else:
            self.client = SyncHTTPClient(**init_opts)

    @property
    def base_url(self) -> str:
        return str(self.client.base_url)

    @base_url.setter
    def base_url(self, url: str) -> None:
        self.client.base_url = url

    def get_bearer_token(self) -> str | None:
        """Get bearer token in the current session"""
        if isinstance(self.client.auth, BearerAuth):
            return self.client.auth.token
        elif (
            authorization_header := self.client.headers.get("Authorization")
        ) and authorization_header.lower().startswith("bearer "):
            return authorization_header.split(maxsplit=1)[-1]
        return None

    def set_bearer_token(self, token: str) -> None:
        """Set bearer token to the current session"""
        self.client.auth = BearerAuth(token)

    def unset_bearer_token(self) -> None:
        """Unset bearer token from the current session"""
        self.client.auth = None
