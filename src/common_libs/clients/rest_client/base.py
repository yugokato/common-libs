from __future__ import annotations

from collections.abc import Callable
from typing import Any

from httpx2 import Auth, Timeout
from httpx2._types import TimeoutTypes

from common_libs.logging import get_logger

from .auth import BearerAuth, ClientAuth, TokenProviderAuth
from .ext import AsyncHTTPClient, SyncHTTPClient
from .rate_limit import RateLimit
from .retry import DEFAULT_RETRY_POLICY, RetryPolicy

logger = get_logger(__name__)


class RestClientBase:
    """Base class for sync and async rest client"""

    def __init__(
        self,
        base_url: str,
        /,
        *,
        log_requests: bool = True,
        log_headers: bool = False,
        prettify_response_log: bool = True,
        async_mode: bool = False,
        timeout: TimeoutTypes = Timeout(5.0, read=30),
        retry_policy: RetryPolicy | None = DEFAULT_RETRY_POLICY,
        rate_limit: RateLimit | None = None,
        auth: ClientAuth = None,
        **client_opts: Any,
    ) -> None:
        """
        :param base_url: API base url
        :param log_requests: Log each request/response. Set to `False` to disable them entirely, including failed call
                             logs. Per-call `quiet` parameter can override this for the call, but `quiet=True` still
                             logs failed calls.
        :param log_headers: Include request/response headers to the API summary logs
        :param prettify_response_log: Prettify response in the API summary logs
        :param async_mode: Use async mode
        :param timeout: The client-level timeout settings. This can be overridden in each request
        :param retry_policy: Retry policy for automatic request retries, or `None` to disable.
                             Defaults to retrying once on HTTP 503 after 5 s for safe methods only.
        :param rate_limit: Client-side rate limit applied to all requests made through this client, or `None` to
                           disable (default). Automatic retries and reconnects also count against the budget.
                           A `TokenProviderAuth` 401 replay does not: it is dispatched inside httpx2's own auth
                           handling, below where the limiter and request/response logging run
        :param auth: Auth applied to every request that does not override it via the per-call `auth` parameter.
                     Accepts an `httpx2.Auth` instance (e.g. from `common_libs.clients.rest_client.auth`), a
                     `(username, password)` tuple, or a callable that takes and returns a request
        :param client_opts: Any other parameters to pass to the underlying `httpx2` client
        """
        self.log_requests = log_requests
        self.log_headers = log_headers
        self.prettify_response_log = prettify_response_log
        self.async_mode = async_mode
        self._hooks_cache: dict[bool, dict[str, list[Callable[..., Any]]]] = {}
        client_opts.setdefault("http2", True)
        init_opts = dict(
            base_url=base_url,
            timeout=timeout,
            retry_policy=retry_policy,
            rate_limit=rate_limit,
            auth=auth,
            **client_opts,
        )
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

    @property
    def auth(self) -> Auth | None:
        """The auth applied to every request that does not override it"""
        return self.client.auth

    @auth.setter
    def auth(self, value: ClientAuth) -> None:
        self.client.auth = value

    @property
    def token(self) -> str | None:
        """The bearer token in the current session, or `None` when there is none.

        Reads from the current auth when it is a `BearerAuth` or a `TokenProviderAuth`, `None` otherwise. For a
        `TokenProviderAuth`, which fetches its token lazily from a provider, this is `None` until the first request has
        actually fetched one.

        Assigning to this property writes through to whatever bearer-style auth is installed (installing a
        `BearerAuth` when there is none), and raises `TypeError` for any other auth scheme. With a `TokenProviderAuth`
        installed, assigning `None` clears its cache rather than logging out: the next request re-fetches from
        the provider instead of sending no `Authorization` header at all. Assign `client.auth = None` to log
        out unconditionally.
        """
        auth = self.client.auth
        return auth.token if isinstance(auth, BearerAuth | TokenProviderAuth) else None

    @token.setter
    def token(self, value: str | None) -> None:
        auth = self.client.auth
        if isinstance(auth, BearerAuth | TokenProviderAuth):
            auth.token = value
        elif auth is not None:
            raise TypeError(
                f"client.token manages a bearer-style auth, but {type(auth).__name__} is installed. Assign "
                f"client.auth instead."
            )
        elif value is not None:
            self.client.auth = BearerAuth(value)
