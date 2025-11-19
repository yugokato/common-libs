from typing import Any

from httpx import Timeout
from httpx._types import TimeoutTypes

from common_libs.logging import get_logger

from .ext import AsyncHTTPClient, BearerAuth, SyncHTTPClient

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
        **kwargs: Any,
    ) -> None:
        """
        :param base_url: API base url
        :param log_headers: Include request/response headers to the API summary logs
        :param prettify_response_log: Prettify response in the API summary logs
        :param async_mode: Use async mode
        :param timeout: The client-level timeout settings. This can be overridden in each request
        :param kwargs: Any other parameters to pass to the httpx client
        """
        self.base_url = base_url
        self.log_headers = log_headers
        self.prettify_response_log = prettify_response_log
        self.async_mode = async_mode
        if self.async_mode:
            self.client = AsyncHTTPClient(base_url=self.base_url, timeout=timeout, **kwargs)
        else:
            self.client = SyncHTTPClient(base_url=self.base_url, timeout=timeout, **kwargs)

    def get_bearer_token(self) -> str | None:
        """Get bear token in the current session"""
        if isinstance(self.client.auth, BearerAuth):
            return self.client.auth.token
        elif (
            authorization_header := self.client.headers.get("Authorization")
        ) and authorization_header.lower().startswith("bear "):
            return authorization_header.split(" ")[1]
        return None

    def set_bearer_token(self, token: str) -> None:
        """Set bear token to the current session"""
        self.client.auth = BearerAuth(token)

    def unset_bear_token(self) -> None:
        """Unset bear token from the current session"""
        self.client.auth = None
