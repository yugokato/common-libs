from typing import Any

from common_libs.logging import get_logger

from .ext import AsyncHTTPClient, BearerAuth, SyncHTTPClient
from .utils import generate_query_string

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
        timeout: int | float = 30,
        async_mode: bool = False,
    ) -> None:
        """
        :param base_url: API base url
        :param log_headers: Include request/response headers to the API summary logs
        :param prettify_response_log: Prettify response in the API summary logs
        :param timeout: Session timeout in seconds
        :param async_mode: Use async mode
        """
        self.base_url = base_url
        self.timeout = timeout
        self.log_headers = log_headers
        self.prettify_response_log = prettify_response_log
        self.client: SyncHTTPClient | AsyncHTTPClient
        self.async_mode = async_mode
        if self.async_mode:
            self.client = AsyncHTTPClient(base_url=self.base_url)
        else:
            self.client = SyncHTTPClient(base_url=self.base_url)

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

    def _generate_url(self, path: str, query: dict[str, Any] | None = None) -> str:
        if not path.startswith("/"):
            path = "/" + path
        url = f"{self.base_url}{path}"
        if query:
            url += f"?{generate_query_string(query)}"
        return url
