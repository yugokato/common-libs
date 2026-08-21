from __future__ import annotations

import asyncio
import inspect
import threading
import time
import typing
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Literal, TypeAlias

from httpx2 import Auth, BasicAuth, DigestAuth, NetRCAuth, RequestNotRead
from httpx2._client import UseClientDefault
from httpx2._types import AuthTypes

from common_libs.logging import get_logger

from .types import Request, Response
from .utils import SENSITIVE_NAMES_EXTENSION

logger = get_logger(__name__)

__all__ = [
    "APIKeyAuth",
    "BasicAuth",
    "BearerAuth",
    "ClientAuth",
    "DigestAuth",
    "NetRCAuth",
    "RequestAuth",
    "Token",
    "TokenError",
    "TokenProviderAuth",
]

ApiKeyLocation: TypeAlias = Literal["header", "query"]
ClientAuth: TypeAlias = AuthTypes | None
RequestAuth: TypeAlias = AuthTypes | UseClientDefault | None

DEFAULT_EXPIRY_LEEWAY = 30.0
_API_KEY_LOCATIONS: tuple[ApiKeyLocation, ...] = typing.get_args(ApiKeyLocation)


class TokenError(RuntimeError):
    """Raised when a token provider fails, returns nothing usable, or is async on a sync client"""


@dataclass(frozen=True, slots=True)
class Token:
    """A bearer token with an optional lifetime.

    Return an instance from a `TokenProviderAuth` provider, or assign one to a `TokenProviderAuth`'s `token` property,
    when the token's lifetime is known. A bare `str` is treated the same as `Token(value)` with no known expiry, which
    is used until the server rejects it with `401`.

    A `TokenProviderAuth` fetches a new token once `expires_in` has elapsed, measured from the moment the auth stores
    the token rather than from when this `Token` was constructed.
    """

    value: str = field(repr=False)
    expires_in: float | None = None

    def __post_init__(self) -> None:
        if not self.value:
            raise ValueError("value must not be empty")
        if self.expires_in is not None and self.expires_in <= 0:
            raise ValueError("expires_in must be a positive number")


TokenProvider: TypeAlias = Callable[[], "str | Token"] | Callable[[], Awaitable["str | Token"]]


class _TokenHeaderAuth(Auth):
    """Shared base for an auth that sends a token value in a request header"""

    def __init__(self, *, scheme: str = "Bearer", header_name: str = "Authorization") -> None:
        self.scheme = scheme
        self.header_name = header_name
        self._token: Token | None = None

    @property
    def token(self) -> str | None:
        """The token value currently held by this auth, or `None` when there is none"""
        return self._get_token_value()

    @token.setter
    def token(self, value: str | Token | None) -> None:
        if value == "":
            raise ValueError("token must not be empty")
        self._set_token(value if value is None or isinstance(value, Token) else Token(value))

    def _get_token_value(self) -> str | None:
        """Return the value of the token currently held, or `None` when there is none"""
        return None if self._token is None else self._token.value

    def _set_token(self, token: Token | None) -> None:
        """Store `token` as the token currently held

        :param token: The token to store, or `None` to clear it.
        """
        self._token = token

    def _apply_token(self, request: Request, value: str | None) -> None:
        """Attach the token value under the configured header, when there is one

        :param request: The outgoing request to authenticate.
        :param value: The token value to send, or `None` to leave the request unauthenticated.
        """
        if value is not None:
            request.headers[self.header_name] = f"{self.scheme} {value}".strip()
            _mark_sensitive(request, self.header_name)


class TokenProviderAuth(_TokenHeaderAuth):
    """Auth that attaches a bearer token obtained from a caller-supplied sync or async callable, caching and
    refreshing it as needed.

    The provider is called on the first request, and again whenever the cached token expires or, when `retry_on_401`,
    the server answers `401`. It must return a non-empty `str`, or a `Token` when the lifetime is known, and a `Token`
    lets the cache expire it proactively instead of waiting for a `401`. `TokenError` is reserved for problems this
    auth itself detects (see below); an exception the provider raises propagates to the caller unchanged.

    A provider that dispatches a request through this same client, e.g. a login call, must pass `auth=None` on that
    call. Without it, the nested call blocks waiting for the very fetch it is nested inside and this auth raises
    `TokenError` instead of deadlocking. A provider that offloads its request to a bare `threading.Thread` or
    `loop.run_in_executor` worker still deadlocks rather than raising, since this auth has no way to detect it
    dispatched the nested request.

    A synchronous provider blocks the event loop for the duration of the fetch when used with `AsyncRestClient`.
    Use an async provider there when the fetch does any real I/O.

    Example, fetching a token by logging in through the same client:

        client = RestClient(base_url)

        def fetch_token() -> Token:
            r = client.post("/login", auth=None, username=USER, password=PASSWORD, quiet=True)
            return Token(r.response["access_token"], expires_in=r.response.get("expires_in"))

        client.auth = TokenProviderAuth(fetch_token)

    `AsyncRestClient` takes the same shape with an async provider, using `await client.post(...)`.
    """

    def __init__(
        self,
        provider: TokenProvider,
        *,
        scheme: str = "Bearer",
        header_name: str = "Authorization",
        retry_on_401: bool = True,
        leeway: float = DEFAULT_EXPIRY_LEEWAY,
    ) -> None:
        """
        :param provider: A zero argument sync or async callable returning the token value, or a `Token` when the
                         lifetime is known.
        :param scheme: The `Authorization` scheme to send, e.g. `Token` or `SSWS` instead of the default `Bearer`.
                       Pass `""` to send the bare token value with no scheme prefix.
        :param header_name: The header to send the token under, e.g. `X-Auth-Token` instead of the default
                            `Authorization`.
        :param retry_on_401: Refresh the token and replay the request once when the server returns `401`, if the
                             request body is still available to replay.
        :param leeway: Seconds before a token's recorded expiry to treat it as already expired and fetch a new one.
                      Clamped to at most half the token's own `expires_in`, so a short-lived token is never
                      treated as already expired the moment it is cached.
        """
        if not callable(provider):
            raise ValueError("provider must be callable")
        if leeway < 0:
            raise ValueError("leeway must not be negative")
        super().__init__(scheme=scheme, header_name=header_name)
        self._provider = provider
        self.retry_on_401 = retry_on_401
        self.leeway = leeway
        self._deadline: float | None = None
        self._state_lock = threading.Lock()
        self._fetch_lock = threading.Lock()
        self._async_fetch_lock: asyncio.Lock | None = None
        self._async_fetch_lock_loop: asyncio.AbstractEventLoop | None = None
        self._fetching_owners: set[object] = set()

    def sync_auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        """Attach the cached token to `request`, fetching or refreshing it as needed

        Sends at most one replay: only when the server answers `401` and the request body can still be
        replayed is the request sent a second time, with a freshly fetched token. A second `401` on the
        replay ends the flow.

        :param request: The outgoing request to authenticate.
        """
        token = self._acquire_token()
        self._apply_token(request, token.value)
        response = yield request
        if self._handle_401(request, response, token):
            logger.warning("The request received a 401. Replaying it once with a refreshed token.")
            self._apply_token(request, self._acquire_token().value)
            yield request

    async def async_auth_flow(self, request: Request) -> AsyncGenerator[Request, Response]:
        """Attach the cached token to `request`, fetching or refreshing it as needed (async)

        Mirrors `sync_auth_flow`.

        :param request: The outgoing request to authenticate.
        """
        token = await self._acquire_token_async()
        self._apply_token(request, token.value)
        response = yield request
        if self._handle_401(request, response, token):
            logger.warning("The request received a 401. Replaying it once with a refreshed token.")
            self._apply_token(request, (await self._acquire_token_async()).value)
            yield request

    def _get_token_value(self) -> str | None:
        """Return the value of the cached token, or `None` when there is none"""
        with self._state_lock:
            return None if self._token is None else self._token.value

    def _set_token(self, token: Token | None) -> None:
        """Cache `token` via `_store_token`, so an explicit assignment also seeds its expiry deadline

        :param token: The token to cache, or `None` to clear it.
        """
        self._store_token(token)

    def _acquire_token(self) -> Token:
        """Return the token to attach, fetching one under the single-flight lock when none is cached"""
        token = self._cached_token()
        if token is None:
            self._check_not_reentrant()
            with self._fetch_lock:
                token = self._cached_token()
                if token is None:
                    with self._fetching():
                        token = self._fetch()
                    self._store_token(token)
        return token

    async def _acquire_token_async(self) -> Token:
        """Return the token to attach, fetching one under the single-flight lock when none is cached (async)"""
        token = self._cached_token()
        if token is None:
            self._check_not_reentrant()
            async with self._async_lock():
                token = self._cached_token()
                if token is None:
                    with self._fetching():
                        token = await self._afetch()
                    self._store_token(token)
        return token

    def _check_not_reentrant(self) -> None:
        """Raise `TokenError` when the current caller is already fetching a token on this auth

        Guards against a provider that dispatches a request through the same client it authenticates, which
        would otherwise deadlock on the fetch lock.
        """
        with self._state_lock:
            reentrant = _current_owner() in self._fetching_owners
        if reentrant:
            raise TokenError(
                f"{type(self).__name__}'s provider dispatched a request through the client it authenticates, "
                f"which would deadlock. Pass auth=None on that call, or use a separate client."
            )

    @contextmanager
    def _fetching(self) -> Generator[None]:
        """Record the current caller as fetching a token for the duration of the `with` block

        Consulted by `_check_not_reentrant`. A `set` rather than a single slot, since the sync and async fetch
        paths guard with independent locks and can therefore have concurrent fetchers, e.g. one `TokenProviderAuth`
        shared between a sync and an async client.
        """
        owner = _current_owner()
        with self._state_lock:
            self._fetching_owners.add(owner)
        try:
            yield
        finally:
            with self._state_lock:
                self._fetching_owners.discard(owner)

    def _cached_token(self) -> Token | None:
        """Return the cached token, or `None` when there is none or it has expired"""
        with self._state_lock:
            token, deadline = self._token, self._deadline
        if token is not None and deadline is not None and time.monotonic() >= deadline:
            logger.debug("The cached token has expired or is about to expire. A new one will be requested.")
            return None
        return token

    def _store_token(self, token: Token | None) -> None:
        """Cache `token` and record the deadline at which it is treated as expired

        :param token: The token to cache, or `None` to clear it.
        """
        deadline = None
        if token is not None and token.expires_in is not None:
            deadline = time.monotonic() + token.expires_in - min(self.leeway, token.expires_in / 2)
        with self._state_lock:
            self._token, self._deadline = token, deadline

    def _handle_401(self, request: Request, response: Response, token: Token) -> bool:
        """Return whether `request` should be replayed with a freshly fetched token

        A `401` invalidates the cached token so a later request does not reuse one the server just rejected, even
        when `retry_on_401` is `False` or this request's body cannot be replayed right now.

        `request` is the flow's own request, not `response.request`: under a followed redirect the two are
        different objects, since httpx2 builds a fresh request per hop, and only `request` is what the flow
        would actually re-yield to replay.

        :param request: The request the flow attached `token` to, which would be replayed.
        :param response: The response received for that request.
        :param token: The token that was applied to the request whose response triggered this call.
        """
        if response.status_code != 401:
            return False
        self._invalidate(token)
        if not self.retry_on_401:
            return False
        if not self._can_replay(request):
            logger.warning(
                "The request received a 401, but its body cannot be replayed (e.g. a streamed or "
                "multipart upload that was never fully buffered). Returning the 401 response as-is."
            )
            return False
        return True

    def _invalidate(self, token: Token) -> None:
        """Clear the cached token if it is still the one just used

        :param token: The token that was applied to the request whose response triggered this call.
        """
        with self._state_lock:
            if self._token is token:
                self._token = None
                self._deadline = None

    def _fetch(self) -> Token:
        """Call the provider synchronously and return the resulting token, rejecting an async provider"""
        logger.debug("Requesting a token from the configured provider")
        result: Any = self._provider()
        if inspect.isawaitable(result):
            if inspect.iscoroutine(result):
                result.close()
            raise TokenError(
                f"{type(self).__name__}'s provider returned an awaitable, but this is a sync client that cannot "
                f"await an async provider. Use AsyncRestClient, or pass a sync provider."
            )
        return self._require_token(result)

    async def _afetch(self) -> Token:
        """Call the provider, awaiting the result when the provider is async, and return the resulting token"""
        logger.debug("Requesting a token from the configured provider")
        result: Any = self._provider()
        if inspect.isawaitable(result):
            result = await result
        return self._require_token(result)

    @staticmethod
    def _require_token(result: Any) -> Token:
        """Normalize a provider's return value into a `Token`, raising when it returned nothing usable

        :param result: The raw value returned by the token provider.
        """
        if not result:
            raise TokenError("The token provider returned no token")
        return result if isinstance(result, Token) else Token(result)

    @staticmethod
    def _can_replay(request: Request) -> bool:
        """Return whether `request`'s body is already buffered in memory and can be sent again

        :param request: The request that was just sent.
        """
        try:
            request.content
        except RequestNotRead:
            return False
        return True

    def _async_lock(self) -> asyncio.Lock:
        """Return the single-flight lock for the running loop, rebuilding it when the loop changed

        An `asyncio.Lock` binds to the loop that first awaits it, so one auth reused across separate
        `asyncio.run()` calls needs a fresh lock each time. Two loops running concurrently in separate threads
        still get only degraded single-flight with each other, since each rebinds the lock out from under the
        other.
        """
        loop = asyncio.get_running_loop()
        with self._state_lock:
            if self._async_fetch_lock is None or self._async_fetch_lock_loop is not loop:
                self._async_fetch_lock = asyncio.Lock()
                self._async_fetch_lock_loop = loop
            return self._async_fetch_lock


class BearerAuth(_TokenHeaderAuth):
    """Static bearer token auth.

    Sends `Authorization: Bearer <token>` on every request. Assign `auth.token = ...` to change the token later,
    `auth.token = None` to stop sending it, or `client.auth = BearerAuth(...)` to replace the auth outright. Use
    `TokenProviderAuth` instead for a token that must be fetched or refreshed.
    """

    def __init__(self, token: str | None, *, scheme: str = "Bearer", header_name: str = "Authorization") -> None:
        """
        :param token: The bearer token to send, or `None` to send no `Authorization` header until one is assigned.
        :param scheme: The `Authorization` scheme to send, e.g. `Token` or `SSWS` instead of the default `Bearer`.
                       Pass `""` to send the bare token value with no scheme prefix.
        :param header_name: The header to send the token under, e.g. `X-Auth-Token` instead of the default
                            `Authorization`.
        """
        super().__init__(scheme=scheme, header_name=header_name)
        self.token = token

    def auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        """Attach the static token to `request`, when one is set

        :param request: The outgoing request to authenticate.
        """
        self._apply_token(request, self.token)
        yield request


class APIKeyAuth(Auth):
    """Static API key auth.

    Sends the key as a header (default) or a query parameter. `RestClient`/`AsyncRestClient` mask the key's
    value out of request/response logs and the console summary regardless of `name`: this auth records the
    header or query parameter name it wrote so the logging path knows to mask it, the same mechanism `BearerAuth`
    and `TokenProviderAuth` use for their own header. Placing the key in a query parameter still embeds it in the
    request URL, so any other code that reads `response.request.url` directly would see it. Prefer `location="header"`
    when that matters. For a key sent as a cookie, set it directly on the underlying client's `cookies` instead.
    """

    def __init__(self, key: str, *, name: str = "X-API-Key", location: ApiKeyLocation = "header") -> None:
        """
        :param key: The API key value.
        :param name: The header or query parameter name to send the key under.
        :param location: Where to place the key: `header` or `query`.
        """
        if not key:
            raise ValueError("key must not be empty")
        if not name:
            raise ValueError("name must not be empty")
        if location not in _API_KEY_LOCATIONS:
            raise ValueError(f"location must be one of {_API_KEY_LOCATIONS}")
        self.key = key
        self.name = name
        self.location = location

    def auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        """Attach the API key to `request`

        :param request: The outgoing request to authenticate.
        """
        if self.location == "header":
            request.headers[self.name] = self.key
        else:
            request.url = request.url.copy_merge_params({self.name: self.key})
        _mark_sensitive(request, self.name)
        yield request


def _current_owner() -> object:
    """Return an object identifying the current caller: the running asyncio task, or the current thread

    Used to detect a `TokenProviderAuth` provider that dispatches a request through the same client it authenticates,
    which would otherwise deadlock on the fetch lock.
    """
    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None
    return task or threading.current_thread()


def _mark_sensitive(request: Request, name: str) -> None:
    """Record `name` as a header or query parameter this auth just wrote onto `request`, so logging masks it

    :param request: The request the auth just attached a credential to.
    :param name: The header or query parameter name to mask out of logs.
    """
    names: frozenset[str] = request.extensions.get(SENSITIVE_NAMES_EXTENSION, frozenset())
    request.extensions[SENSITIVE_NAMES_EXTENSION] = names | {name}
