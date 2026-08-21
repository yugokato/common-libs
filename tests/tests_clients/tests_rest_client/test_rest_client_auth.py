"""Tests for common_libs.clients.rest_client.auth module"""

import asyncio
import functools
import json
import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

import httpx2
import pytest
from pytest_mock import MockFixture

from common_libs.clients.rest_client.auth import APIKeyAuth, BearerAuth, Token, TokenError, TokenProviderAuth
from common_libs.clients.rest_client.rest_client import AsyncRestClient, RestClient
from common_libs.clients.rest_client.retry import RetryPolicy
from common_libs.clients.rest_client.types import Request

BASE_URL = "https://example.com/v1"
API_PATH = "/things"
AUTH_LOGGER_NAME = "common_libs.clients.rest_client.auth"
HOOKS_LOGGER_NAME = "common_libs.clients.rest_client.hooks"


def make_json_response(status_code: int, data: dict[str, object]) -> httpx2.Response:
    """Build a stream-backed JSON response for a MockTransport handler

    Unlike passing `json=` (preloaded content), a stream-backed response is only readable once the client
    explicitly reads it, which is what makes this a meaningful check for auth flows that must read a response
    themselves before parsing it.

    :param status_code: HTTP status code
    :param data: JSON body
    """
    return httpx2.Response(
        status_code, stream=httpx2.ByteStream(json.dumps(data).encode()), headers={"Content-Type": "application/json"}
    )


class TestToken:
    """Tests for the Token dataclass"""

    def test_rejects_empty_value(self) -> None:
        """Test that an empty token value raises ValueError"""
        with pytest.raises(ValueError, match="value must not be empty"):
            Token("")

    def test_rejects_non_positive_expires_in(self) -> None:
        """Test that a non-positive expires_in raises ValueError"""
        with pytest.raises(ValueError, match="expires_in must be a positive number"):
            Token("abc", expires_in=0)

    def test_repr_excludes_value(self) -> None:
        """Test that repr never includes the token value"""
        assert "secret-value" not in repr(Token("secret-value", expires_in=60))


class TestTokenProviderAuthExpiry:
    """Tests for how TokenProviderAuth tracks and reacts to a cached token's expiry"""

    def test_no_expiry_never_treated_as_expired(self) -> None:
        """Test that a token with no expires_in is never refetched"""
        calls = {"n": 0}

        def provider() -> str:
            calls["n"] += 1
            return "tok"

        auth = TokenProviderAuth(provider)
        for _ in range(3):
            request = Request("GET", "http://example.com")
            next(auth.sync_auth_flow(request))
        assert calls["n"] == 1

    def test_leeway_triggers_refetch_before_the_deadline(self, mocker: MockFixture) -> None:
        """Test that leeway causes a refetch before the token's actual deadline is reached"""
        clock = {"now": 0.0}
        mocker.patch("time.monotonic", lambda: clock["now"])
        calls = {"n": 0}

        def provider() -> Token:
            calls["n"] += 1
            return Token("tok", expires_in=10)

        auth = TokenProviderAuth(provider, leeway=6.0)
        next(auth.sync_auth_flow(Request("GET", "http://example.com")))
        clock["now"] = 5.0
        next(auth.sync_auth_flow(Request("GET", "http://example.com")))
        assert calls["n"] == 2

    def test_short_lived_token_is_not_born_expired(self) -> None:
        """Test that a token whose expires_in is shorter than the leeway is still cached across calls instead of
        being treated as already expired the moment it is stored
        """
        calls = {"n": 0}

        def provider() -> Token:
            calls["n"] += 1
            return Token("tok", expires_in=10)

        auth = TokenProviderAuth(provider)
        for _ in range(5):
            request = Request("GET", "http://example.com")
            next(auth.sync_auth_flow(request))
        assert calls["n"] == 1


class TestBearerAuth:
    """Tests for BearerAuth"""

    def test_sync_auth_flow_sets_authorization_header(self) -> None:
        """Test that the sync auth flow adds the Bearer authorization header"""
        auth = BearerAuth("my-secret-token")
        request = Request("GET", "http://example.com")
        gen = auth.sync_auth_flow(request)
        next(gen)
        assert request.headers["Authorization"] == "Bearer my-secret-token"

    async def test_async_auth_flow_sets_authorization_header(self) -> None:
        """Test that the async auth flow adds the Bearer authorization header"""
        auth = BearerAuth("my-secret-token")
        request = Request("GET", "http://example.com")
        gen = auth.async_auth_flow(request)
        await anext(gen)
        assert request.headers["Authorization"] == "Bearer my-secret-token"

    def test_token_property_round_trip(self) -> None:
        """Test that the token property reflects the constructor value and can be reassigned"""
        auth = BearerAuth("initial")
        assert auth.token == "initial"
        auth.token = "updated"
        assert auth.token == "updated"

    def test_assigning_a_token_object_sends_its_value_not_its_repr(self) -> None:
        """Test that assigning a `Token` instance sends the token's value, the same as `TokenProviderAuth` accepts,
        rather than the dataclass's own repr
        """
        auth = BearerAuth("initial")
        auth.token = Token("secret-value", expires_in=60)
        assert auth.token == "secret-value"
        request = Request("GET", "http://example.com")
        next(auth.sync_auth_flow(request))
        assert request.headers["Authorization"] == "Bearer secret-value"

    def test_token_none_sends_no_header(self) -> None:
        """Test that a cleared token results in no Authorization header being sent"""
        auth = BearerAuth("initial")
        auth.token = None
        request = Request("GET", "http://example.com")
        gen = auth.sync_auth_flow(request)
        next(gen)
        assert "Authorization" not in request.headers

    def test_custom_scheme(self) -> None:
        """Test that a custom scheme replaces the default Bearer scheme in the Authorization header"""
        auth = BearerAuth("my-token", scheme="Token")
        request = Request("GET", "http://example.com")
        next(auth.sync_auth_flow(request))
        assert request.headers["Authorization"] == "Token my-token"

    def test_empty_scheme_sends_bare_token(self) -> None:
        """Test that an empty scheme sends the bare token value with no prefix"""
        auth = BearerAuth("my-token", scheme="")
        request = Request("GET", "http://example.com")
        next(auth.sync_auth_flow(request))
        assert request.headers["Authorization"] == "my-token"

    def test_custom_header_name(self) -> None:
        """Test that a custom header_name sends the token under that header instead of Authorization"""
        auth = BearerAuth("my-token", header_name="X-Auth-Token")
        request = Request("GET", "http://example.com")
        next(auth.sync_auth_flow(request))
        assert request.headers["X-Auth-Token"] == "Bearer my-token"
        assert "Authorization" not in request.headers

    def test_401_does_not_erase_the_static_token(self) -> None:
        """Test that a 401 response leaves the token in place instead of refreshing (and clearing) it

        `BearerAuth` is a standalone `httpx2.Auth` with no fetch/refresh machinery at all, so a 401 simply passes
        through unchanged rather than reaching a refresh path.
        """

        def handler(request: httpx2.Request) -> httpx2.Response:
            return httpx2.Response(401, stream=httpx2.ByteStream(b"{}"))

        auth = BearerAuth("my-secret-token")
        with RestClient(BASE_URL, auth=auth, retry_policy=None, transport=httpx2.MockTransport(handler)) as client:
            r = client.get(API_PATH, quiet=True)

        assert r.status_code == 401
        assert auth.token == "my-secret-token"


class TestAPIKeyAuth:
    """Tests for APIKeyAuth"""

    def test_header_placement(self) -> None:
        """Test that the key is sent as a header by default"""
        auth = APIKeyAuth("secret-key")
        request = Request("GET", "http://example.com")
        next(auth.auth_flow(request))
        assert request.headers["X-API-Key"] == "secret-key"

    def test_custom_header_name(self) -> None:
        """Test that a custom header name is honored"""
        auth = APIKeyAuth("secret-key", name="X-Custom-Key", location="header")
        request = Request("GET", "http://example.com")
        next(auth.auth_flow(request))
        assert request.headers["X-Custom-Key"] == "secret-key"

    def test_query_placement(self) -> None:
        """Test that the key is sent as a query parameter"""
        auth = APIKeyAuth("secret-key", name="api_key", location="query")
        request = Request("GET", "http://example.com/things")
        next(auth.auth_flow(request))
        assert dict(request.url.params)["api_key"] == "secret-key"

    def test_query_placement_is_idempotent(self) -> None:
        """Test that re-running the flow on the same request does not duplicate the query parameter"""
        auth = APIKeyAuth("secret-key", name="api_key", location="query")
        request = Request("GET", "http://example.com")
        next(auth.auth_flow(request))
        next(auth.auth_flow(request))
        assert dict(request.url.params) == {"api_key": "secret-key"}

    def test_rejects_empty_key(self) -> None:
        """Test that an empty key raises ValueError"""
        with pytest.raises(ValueError, match="key must not be empty"):
            APIKeyAuth("")

    def test_rejects_empty_name(self) -> None:
        """Test that an empty name raises ValueError"""
        with pytest.raises(ValueError, match="name must not be empty"):
            APIKeyAuth("key", name="")

    def test_rejects_invalid_location(self) -> None:
        """Test that an unsupported location raises ValueError"""
        with pytest.raises(ValueError, match="location must be one of"):
            APIKeyAuth("key", location="body")


class TestTokenProviderAuthProvider:
    """Tests for how TokenProviderAuth calls a sync or async provider"""

    def test_rejects_non_callable_provider(self) -> None:
        """Test that a non-callable provider raises ValueError"""
        with pytest.raises(ValueError, match="provider must be callable"):
            TokenProviderAuth("not-a-callable")

    def test_rejects_negative_leeway(self) -> None:
        """Test that a negative leeway raises ValueError instead of pushing a token's deadline past its actual
        expiry
        """
        with pytest.raises(ValueError, match="leeway must not be negative"):
            TokenProviderAuth(lambda: "tok", leeway=-1)

    def test_token_setter_empty_string_raises(self) -> None:
        """Test that assigning an empty string to auth.token raises instead of silently caching an empty token"""
        auth = TokenProviderAuth(lambda: "tok")
        auth.token = "some-token"
        with pytest.raises(ValueError, match="token must not be empty"):
            auth.token = ""
        assert auth.token == "some-token"

    def test_sync_provider_returning_none_raises(self) -> None:
        """Test that a sync provider returning None raises TokenError instead of sending unauthenticated"""

        def provider() -> None:
            return None

        with (
            RestClient(BASE_URL, auth=TokenProviderAuth(provider), retry_policy=None) as client,
            pytest.raises(TokenError, match="no token"),
        ):
            client.get(API_PATH, quiet=True)

    async def test_async_provider_returning_none_raises(self) -> None:
        """Test that an async provider returning None raises TokenError instead of sending unauthenticated"""

        async def provider() -> None:
            return None

        with pytest.raises(TokenError, match="no token"):
            async with AsyncRestClient(BASE_URL, auth=TokenProviderAuth(provider), retry_policy=None) as client:
                await client.get(API_PATH, quiet=True)

    def test_sync_provider_raising_propagates_unchanged(self) -> None:
        """Test that a sync provider's own exception propagates to the caller unchanged, not wrapped in
        TokenError
        """
        original = ConnectionError("dns lookup failed")

        def provider() -> str:
            raise original

        with (
            RestClient(BASE_URL, auth=TokenProviderAuth(provider), retry_policy=None) as client,
            pytest.raises(ConnectionError) as exc_info,
        ):
            client.get(API_PATH, quiet=True)
        assert exc_info.value is original

    async def test_async_provider_raising_propagates_unchanged(self) -> None:
        """Test that an async provider's own exception propagates to the caller unchanged, not wrapped in
        TokenError
        """

        async def provider() -> str:
            raise ConnectionError("dns lookup failed")

        with pytest.raises(ConnectionError, match="dns lookup failed"):
            async with AsyncRestClient(BASE_URL, auth=TokenProviderAuth(provider), retry_policy=None) as client:
                await client.get(API_PATH, quiet=True)

    def test_sync_provider_with_sync_client(self) -> None:
        """Test that a sync provider authenticates requests made through RestClient"""
        calls = {"n": 0}

        def provider() -> str:
            calls["n"] += 1
            return "provider-token"

        def handler(request: httpx2.Request) -> httpx2.Response:
            assert request.headers.get("Authorization") == "Bearer provider-token"
            return make_json_response(200, {"ok": True})

        with RestClient(
            BASE_URL, auth=TokenProviderAuth(provider), retry_policy=None, transport=httpx2.MockTransport(handler)
        ) as client:
            r1 = client.get(API_PATH, quiet=True)
            r2 = client.get(API_PATH, quiet=True)

        assert r1.status_code == 200
        assert r2.status_code == 200
        assert calls["n"] == 1

    async def test_async_provider_with_async_client(self) -> None:
        """Test that an async provider authenticates requests made through AsyncRestClient"""
        calls = {"n": 0}

        async def provider() -> str:
            calls["n"] += 1
            return "async-provider-token"

        def handler(request: httpx2.Request) -> httpx2.Response:
            assert request.headers.get("Authorization") == "Bearer async-provider-token"
            return make_json_response(200, {"ok": True})

        async with AsyncRestClient(
            BASE_URL, auth=TokenProviderAuth(provider), retry_policy=None, transport=httpx2.MockTransport(handler)
        ) as client:
            r = await client.get(API_PATH, quiet=True)

        assert r.status_code == 200
        assert calls["n"] == 1

    def test_async_provider_on_sync_client_raises(self, recwarn: pytest.WarningsRecorder) -> None:
        """Test that an async provider used from a sync client raises TokenError with no unraised-coroutine warning"""

        async def provider() -> str:
            return "unused"

        def handler(request: httpx2.Request) -> httpx2.Response:
            return make_json_response(200, {"ok": True})

        with (
            RestClient(
                BASE_URL, auth=TokenProviderAuth(provider), retry_policy=None, transport=httpx2.MockTransport(handler)
            ) as client,
            pytest.raises(TokenError, match="async provider"),
        ):
            client.get(API_PATH, quiet=True)

        assert not any("was never awaited" in str(w.message) for w in recwarn.list)

    async def test_partial_around_async_function_detected(self) -> None:
        """Test that a functools.partial wrapping an async function is awaited correctly"""

        async def provider(prefix: str) -> str:
            return f"{prefix}-token"

        def handler(request: httpx2.Request) -> httpx2.Response:
            assert request.headers.get("Authorization") == "Bearer partial-token"
            return make_json_response(200, {"ok": True})

        async with AsyncRestClient(
            BASE_URL,
            auth=TokenProviderAuth(functools.partial(provider, "partial")),
            retry_policy=None,
            transport=httpx2.MockTransport(handler),
        ) as client:
            r = await client.get(API_PATH, quiet=True)

        assert r.status_code == 200

    async def test_object_with_async_call_detected(self) -> None:
        """Test that a callable object whose __call__ is async is awaited correctly"""

        class AsyncCallable:
            async def __call__(self) -> str:
                return "object-token"

        def handler(request: httpx2.Request) -> httpx2.Response:
            assert request.headers.get("Authorization") == "Bearer object-token"
            return make_json_response(200, {"ok": True})

        async with AsyncRestClient(
            BASE_URL,
            auth=TokenProviderAuth(AsyncCallable()),
            retry_policy=None,
            transport=httpx2.MockTransport(handler),
        ) as client:
            r = await client.get(API_PATH, quiet=True)

        assert r.status_code == 200

    def test_provider_called_again_after_expiry(self, mocker: MockFixture) -> None:
        """Test that the provider is re-invoked once the previously returned token has expired"""
        clock = {"now": 0.0}
        mocker.patch("time.monotonic", lambda: clock["now"])
        calls = {"n": 0}

        def provider() -> Token:
            calls["n"] += 1
            return Token(f"token-{calls['n']}", expires_in=10)

        def handler(request: httpx2.Request) -> httpx2.Response:
            return make_json_response(200, {"used": request.headers.get("Authorization")})

        with RestClient(
            BASE_URL, auth=TokenProviderAuth(provider), retry_policy=None, transport=httpx2.MockTransport(handler)
        ) as client:
            r1 = client.get(API_PATH, quiet=True)
            clock["now"] = 11.0
            r2 = client.get(API_PATH, quiet=True)

        assert r1.response["used"] == "Bearer token-1"
        assert r2.response["used"] == "Bearer token-2"
        assert calls["n"] == 2


class TestTokenProviderAuthReentrancy:
    """Tests for how a token source that dispatches a request through the same client it authenticates is
    handled: raising `TokenError` instead of deadlocking on the fetch lock when it forgets `auth=None`, and
    working normally when it remembers it

    Neither hang-check test stubs anything about reentrancy detection, so a regression that fails to detect
    the nested call would hang on the fetch lock here instead of raising.
    """

    def test_sync_provider_reentering_without_auth_none_raises(self) -> None:
        """Test that a sync provider dispatching through the same client without auth=None raises TokenError
        naming the fix, and does not hang
        """

        def handler(request: httpx2.Request) -> httpx2.Response:
            return make_json_response(200, {"access_token": "tok"})

        client = RestClient(BASE_URL, retry_policy=None, transport=httpx2.MockTransport(handler))

        def provider() -> str:
            return client.get("/login", quiet=True).response["access_token"]

        client.auth = TokenProviderAuth(provider)
        outcome: dict[str, BaseException] = {}

        def run() -> None:
            try:
                client.get(API_PATH, quiet=True)
            except BaseException as e:
                outcome["error"] = e

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        thread.join(timeout=5)
        client.close()

        assert not thread.is_alive(), "the reentrant call hung"
        assert isinstance(outcome["error"], TokenError)
        assert "auth=None" in str(outcome["error"])

    async def test_async_provider_reentering_without_auth_none_raises(self) -> None:
        """Test that an async provider dispatching through the same client without auth=None raises
        TokenError naming the fix, rather than hanging
        """

        async def handler(request: httpx2.Request) -> httpx2.Response:
            return make_json_response(200, {"access_token": "tok"})

        async with AsyncRestClient(BASE_URL, retry_policy=None, transport=httpx2.MockTransport(handler)) as client:

            async def provider() -> str:
                r = await client.get("/login", quiet=True)
                return r.response["access_token"]

            client.auth = TokenProviderAuth(provider)

            with pytest.raises(TokenError) as exc_info:
                await asyncio.wait_for(client.get(API_PATH, quiet=True), timeout=5)

        assert "auth=None" in str(exc_info.value)

    def test_sync_provider_reentering_with_auth_none_succeeds(self) -> None:
        """Test that a sync provider dispatching through the same client with auth=None sends that nested
        request with no Authorization header, while the outer request still carries the fetched token
        """
        seen_headers: dict[str, str | None] = {}

        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.path.endswith("/login"):
                seen_headers["login"] = request.headers.get("Authorization")
                return make_json_response(200, {"access_token": "tok"})
            seen_headers["outer"] = request.headers.get("Authorization")
            return make_json_response(200, {"ok": True})

        client = RestClient(BASE_URL, retry_policy=None, transport=httpx2.MockTransport(handler))

        def provider() -> str:
            return client.get("/login", auth=None, quiet=True).response["access_token"]

        client.auth = TokenProviderAuth(provider)
        outcome: dict[str, httpx2.Response] = {}

        def run() -> None:
            outcome["response"] = client.get(API_PATH, quiet=True)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        thread.join(timeout=5)
        client.close()

        assert not thread.is_alive(), "the reentrant call hung"
        assert outcome["response"].status_code == 200
        assert seen_headers["login"] is None
        assert seen_headers["outer"] == "Bearer tok"

    async def test_async_provider_reentering_with_auth_none_succeeds(self) -> None:
        """Test that an async provider dispatching through the same client with auth=None sends that nested
        request with no Authorization header, while the outer request still carries the fetched token
        """
        seen_headers: dict[str, str | None] = {}

        async def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.path.endswith("/login"):
                seen_headers["login"] = request.headers.get("Authorization")
                return make_json_response(200, {"access_token": "tok"})
            seen_headers["outer"] = request.headers.get("Authorization")
            return make_json_response(200, {"ok": True})

        async with AsyncRestClient(BASE_URL, retry_policy=None, transport=httpx2.MockTransport(handler)) as client:

            async def provider() -> str:
                r = await client.get("/login", auth=None, quiet=True)
                return r.response["access_token"]

            client.auth = TokenProviderAuth(provider)

            r = await asyncio.wait_for(client.get(API_PATH, quiet=True), timeout=5)

        assert r.status_code == 200
        assert seen_headers["login"] is None
        assert seen_headers["outer"] == "Bearer tok"


class TestTokenProviderAuth401Retry:
    """Tests for the 401 refresh-and-replay behavior in TokenProviderAuth.sync_auth_flow/async_auth_flow"""

    def _make_provider_and_handler(
        self,
    ) -> tuple[Callable[[], str], Callable[[httpx2.Request], httpx2.Response], dict[str, int]]:
        counts = {"token_fetches": 0, "api_calls": 0}
        issued_tokens: list[str] = []
        rejected_once = {"done": False}

        def provider() -> str:
            counts["token_fetches"] += 1
            token = f"tok-{counts['token_fetches']}"
            issued_tokens.append(token)
            return token

        def handler(request: httpx2.Request) -> httpx2.Response:
            counts["api_calls"] += 1
            auth_header = request.headers.get("Authorization")
            if auth_header == f"Bearer {issued_tokens[0]}" and not rejected_once["done"]:
                rejected_once["done"] = True
                return httpx2.Response(401, stream=httpx2.ByteStream(b'{"error": "expired"}'))
            return make_json_response(200, {"used": auth_header})

        return provider, handler, counts

    def test_401_triggers_one_refresh_and_one_replay(self) -> None:
        """Test that a 401 response refreshes the token once and replays the request once, ending in success"""
        provider, handler, counts = self._make_provider_and_handler()
        with RestClient(
            BASE_URL, auth=TokenProviderAuth(provider), retry_policy=None, transport=httpx2.MockTransport(handler)
        ) as client:
            r = client.get(API_PATH, quiet=True)

        assert r.status_code == 200
        assert r.response["used"] == "Bearer tok-2"
        assert counts["token_fetches"] == 2
        assert counts["api_calls"] == 2

    def test_second_401_on_replay_is_returned_as_is(self) -> None:
        """Test that a second consecutive 401 ends the flow instead of looping"""

        def handler(request: httpx2.Request) -> httpx2.Response:
            return httpx2.Response(401, stream=httpx2.ByteStream(b'{"error": "expired"}'))

        with RestClient(
            BASE_URL,
            auth=TokenProviderAuth(lambda: "always-rejected"),
            retry_policy=None,
            transport=httpx2.MockTransport(handler),
        ) as client:
            r = client.get(API_PATH, quiet=True)

        assert r.status_code == 401

    def test_second_401_does_not_trigger_a_third_fetch(self) -> None:
        """Test that the second 401 on the replay does not invalidate the token and fetch yet again

        Regression: the replay used to re-invalidate a token it had just proven good, so an endpoint that keeps
        401ing for a reason unrelated to the token (e.g. a permissions error) would double the fetch rate on
        every request instead of fetching once per attempt.
        """
        counts = {"n": 0}

        def provider() -> str:
            counts["n"] += 1
            return f"tok-{counts['n']}"

        def handler(request: httpx2.Request) -> httpx2.Response:
            return httpx2.Response(401, stream=httpx2.ByteStream(b'{"error": "expired"}'))

        with RestClient(
            BASE_URL, auth=TokenProviderAuth(provider), retry_policy=None, transport=httpx2.MockTransport(handler)
        ) as client:
            r = client.get(API_PATH, quiet=True)

        assert r.status_code == 401
        assert counts["n"] == 2

    def test_identical_refreshed_token_still_replays_the_original_request(self) -> None:
        """Test that a refresh returning the same token value still replays the request rather than returning the
        401 response body as if it were the answer to the original request

        The replay is not skipped just because the value is unchanged: since the flow never loops more than
        once, replaying is already bounded to a single extra request.
        """
        counts = {"token_fetches": 0, "api_calls": 0}

        def provider() -> str:
            counts["token_fetches"] += 1
            return "same-token"

        def handler(request: httpx2.Request) -> httpx2.Response:
            counts["api_calls"] += 1
            return httpx2.Response(401, stream=httpx2.ByteStream(b'{"error": "still expired"}'))

        auth = TokenProviderAuth(provider)
        with RestClient(BASE_URL, auth=auth, retry_policy=None, transport=httpx2.MockTransport(handler)) as client:
            r = client.get(API_PATH, quiet=True)

        assert r.status_code == 401
        assert r.response == {"error": "still expired"}
        assert counts["token_fetches"] == 2
        assert counts["api_calls"] == 2

    def test_retry_on_401_disabled_returns_401_with_no_refetch(self) -> None:
        """Test that retry_on_401=False sends the request once and does not attempt a refresh on 401"""
        counts = {"token_fetches": 0}

        def provider() -> str:
            counts["token_fetches"] += 1
            return "tok"

        def handler(request: httpx2.Request) -> httpx2.Response:
            return httpx2.Response(401, stream=httpx2.ByteStream(b'{"error": "expired"}'))

        with RestClient(
            BASE_URL,
            auth=TokenProviderAuth(provider, retry_on_401=False),
            retry_policy=None,
            transport=httpx2.MockTransport(handler),
        ) as client:
            r = client.get(API_PATH, quiet=True)

        assert r.status_code == 401
        assert counts["token_fetches"] == 1

    def test_retry_on_401_disabled_still_invalidates_the_cached_token(self) -> None:
        """Test that retry_on_401=False still invalidates a token the server just rejected, so a later
        request fetches a fresh one instead of reusing the one that was already rejected

        Regression: invalidation used to be nested inside the retry_on_401 check, so a client configured
        with retry_on_401=False against an endpoint that always answers 401 never recovered, since nothing
        else clears a token that carries no expires_in.
        """
        counts = {"n": 0}

        def provider() -> str:
            counts["n"] += 1
            return f"tok-{counts['n']}"

        seen_headers: list[str | None] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen_headers.append(request.headers.get("Authorization"))
            return httpx2.Response(401, stream=httpx2.ByteStream(b'{"error": "expired"}'))

        with RestClient(
            BASE_URL,
            auth=TokenProviderAuth(provider, retry_on_401=False),
            retry_policy=None,
            transport=httpx2.MockTransport(handler),
        ) as client:
            for _ in range(3):
                r = client.get(API_PATH, quiet=True)
                assert r.status_code == 401

        assert counts["n"] == 3
        assert seen_headers == ["Bearer tok-1", "Bearer tok-2", "Bearer tok-3"]

    async def test_401_replay_composes_with_outer_retry_policy(self) -> None:
        """Test that combining an outer retry policy for 401 with the in-flow refresh does not loop unboundedly

        The in-flow replay resolves the 401 on its own, so the outer retry policy never has a reason to
        fire: exactly one extra fetch and one extra request, not the up-to-3 the condition previously only
        capped.
        """
        provider, handler, counts = self._make_provider_and_handler()
        async with AsyncRestClient(
            BASE_URL,
            auth=TokenProviderAuth(provider),
            retry_policy=RetryPolicy(condition=401, num_retries=1, retry_after=0),
            transport=httpx2.MockTransport(handler),
        ) as client:
            r = await client.get(API_PATH, quiet=True)

        assert r.status_code == 200
        assert counts["token_fetches"] == 2
        assert counts["api_calls"] == 2

    def test_401_replay_works_under_stream(self) -> None:
        """Test that a 401 on client.stream() still refreshes the token and replays, the same as a regular
        request, and the replayed response streams correctly
        """
        counts = {"token_fetches": 0, "api_calls": 0}

        def provider() -> str:
            counts["token_fetches"] += 1
            return f"tok-{counts['token_fetches']}"

        def handler(request: httpx2.Request) -> httpx2.Response:
            counts["api_calls"] += 1
            if counts["api_calls"] == 1:
                return httpx2.Response(401, stream=httpx2.ByteStream(b'{"error": "expired"}'))
            return httpx2.Response(200, stream=httpx2.ByteStream(b"chunk1chunk2"))

        with RestClient(
            BASE_URL, auth=TokenProviderAuth(provider), retry_policy=None, transport=httpx2.MockTransport(handler)
        ) as client:
            with client.stream("GET", API_PATH, quiet=True) as r:
                assert r.status_code == 200
                assert list(r.stream("bytes")) == [b"chunk1chunk2"]

        assert counts["token_fetches"] == 2
        assert counts["api_calls"] == 2

    def test_401_after_followed_redirect_still_replays(self) -> None:
        """Test that a 401 arriving after a followed same-origin redirect is still replayed with a refreshed
        token, instead of being returned as-is

        `response.request` at the point the 401 arrives is the request `httpx2` built for the redirect hop, not
        the original request the auth flow would actually replay. Uses a bare `BaseTransport`, not
        `MockTransport`, since `MockTransport` pre-reads the body of every request it is handed, including the
        redirect hop, which would mask the distinction this test exercises.
        """
        counts = {"token_fetches": 0, "dispatches": 0}

        def provider() -> str:
            counts["token_fetches"] += 1
            return f"tok-{counts['token_fetches']}"

        class _RedirectingTransport(httpx2.BaseTransport):
            def handle_request(self, request: httpx2.Request) -> httpx2.Response:
                counts["dispatches"] += 1
                if request.url.path.endswith("/things"):
                    return httpx2.Response(302, headers={"Location": "/v1/things2"})
                if request.headers.get("Authorization") == "Bearer tok-1":
                    return httpx2.Response(401, stream=httpx2.ByteStream(b'{"error": "expired"}'))
                return make_json_response(200, {"used": request.headers.get("Authorization")})

        with RestClient(
            BASE_URL,
            auth=TokenProviderAuth(provider),
            retry_policy=None,
            follow_redirects=True,
            transport=_RedirectingTransport(),
        ) as client:
            r = client.get(API_PATH, quiet=True)

        assert r.status_code == 200
        assert r.response["used"] == "Bearer tok-2"
        assert counts["token_fetches"] == 2


class TestRequestBodyReplay:
    """Tests for how a refreshable auth's 401 replay interacts with a request body it cannot cheaply buffer"""

    def test_multipart_upload_is_not_eagerly_buffered(self) -> None:
        """Test that a files= upload is not read into memory before being sent, isolated from request logging
        (which independently buffers the body for its own payload log line when not quiet)
        """
        buffered_before_send: dict[str, bool | None] = {"value": None}

        class _NoPreReadTransport(httpx2.BaseTransport):
            def handle_request(self, request: httpx2.Request) -> httpx2.Response:
                buffered_before_send["value"] = hasattr(request, "_content")
                return make_json_response(200, {"ok": True})

        with RestClient(
            BASE_URL, auth=TokenProviderAuth(lambda: "tok"), retry_policy=None, transport=_NoPreReadTransport()
        ) as client:
            client.post(API_PATH, files={"f": ("n.txt", b"x" * 1000)}, quiet=True)

        assert buffered_before_send["value"] is False

    @pytest.mark.parametrize("quiet", [True, False])
    def test_401_on_unbuffered_upload_is_returned_without_a_replay(
        self, quiet: bool, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that a 401 on a files= upload whose body was never buffered is returned as-is, without an attempt
        to replay it, regardless of whether request logging is on for the call

        Regression: request logging used to force-read the body for its own payload log line, which made it
        replayable as a side effect. That made the replay decision depend on `quiet`: buffered (and replayed)
        with `quiet=False`, left unbuffered (and not replayed) with `quiet=True`.
        """
        caplog.set_level(logging.WARNING, logger=AUTH_LOGGER_NAME)
        counts = {"api_calls": 0}

        class _StreamingTransport(httpx2.BaseTransport):
            def handle_request(self, request: httpx2.Request) -> httpx2.Response:
                counts["api_calls"] += 1
                return httpx2.Response(401, stream=httpx2.ByteStream(b'{"error": "expired"}'))

        with RestClient(
            BASE_URL, auth=TokenProviderAuth(lambda: "tok"), retry_policy=None, transport=_StreamingTransport()
        ) as client:
            r = client.post(API_PATH, files={"f": ("n.txt", b"x" * 1000)}, quiet=quiet)

        assert r.status_code == 401
        assert counts["api_calls"] == 1
        assert any("cannot be replayed" in record.getMessage() for record in caplog.records)

    def test_401_on_unreplayable_body_invalidates_the_cached_token(self) -> None:
        """Test that a 401 on a request whose body cannot be replayed still invalidates the cached token, so the
        next request fetches a fresh one instead of reusing the one the server just rejected

        Uses a bare `BaseTransport`, not `MockTransport`, since `MockTransport` itself pre-reads the request
        body before invoking the handler, which would buffer the multipart body and defeat the point of this
        test.
        """
        counts = {"n": 0}

        def provider() -> str:
            counts["n"] += 1
            return f"tok-{counts['n']}"

        class _Transport(httpx2.BaseTransport):
            calls = 0

            def handle_request(self, request: httpx2.Request) -> httpx2.Response:
                self.calls += 1
                if self.calls == 1:
                    return httpx2.Response(401, stream=httpx2.ByteStream(b'{"error": "expired"}'))
                return make_json_response(200, {"used": request.headers.get("Authorization")})

        with RestClient(
            BASE_URL, auth=TokenProviderAuth(provider), retry_policy=None, transport=_Transport()
        ) as client:
            r1 = client.post(API_PATH, files={"f": ("n.txt", b"x" * 10)}, quiet=True)
            r2 = client.get(API_PATH, quiet=True)

        assert r1.status_code == 401
        assert r2.response["used"] == "Bearer tok-2"
        assert counts["n"] == 2

    def test_json_payload_still_replays_on_401(self) -> None:
        """Test that a json= request, whose body is already buffered when built, still replays normally on 401"""
        counts = {"api_calls": 0}

        def handler(request: httpx2.Request) -> httpx2.Response:
            counts["api_calls"] += 1
            if counts["api_calls"] == 1:
                return httpx2.Response(401, stream=httpx2.ByteStream(b'{"error": "expired"}'))
            return make_json_response(200, {"ok": True})

        with RestClient(
            BASE_URL,
            auth=TokenProviderAuth(lambda: "tok"),
            retry_policy=None,
            transport=httpx2.MockTransport(handler),
        ) as client:
            r = client.post(API_PATH, quiet=True, name="value")

        assert r.status_code == 200
        assert counts["api_calls"] == 2


class TestRedirectSensitiveHeaders:
    """Tests for stripping an auth-declared sensitive header from a cross-origin redirect"""

    def test_custom_header_stripped_on_cross_origin_redirect(self) -> None:
        """Test that a custom auth header is dropped when a redirect changes host, the same as httpx2 already
        does for Authorization
        """
        seen_headers = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.host == "example.com":
                return httpx2.Response(302, headers={"Location": "https://other.example.com/things"})
            seen_headers.append(dict(request.headers))
            return make_json_response(200, {"ok": True})

        with RestClient(
            BASE_URL,
            auth=TokenProviderAuth(lambda: "tok", header_name="X-Auth-Token"),
            retry_policy=None,
            follow_redirects=True,
            transport=httpx2.MockTransport(handler),
        ) as client:
            client.get(API_PATH, quiet=True)

        # dict(httpx2.Headers) normalizes names to lowercase
        assert "x-auth-token" not in seen_headers[0]

    def test_custom_header_survives_same_host_https_upgrade(self) -> None:
        """Test that a custom auth header is preserved across a same-host http-to-https upgrade, mirroring
        httpx2's own exception for Authorization
        """
        seen_headers = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.scheme == "http":
                return httpx2.Response(302, headers={"Location": "https://example.com/things"})
            seen_headers.append(dict(request.headers))
            return make_json_response(200, {"ok": True})

        with RestClient(
            "http://example.com/v1",
            auth=TokenProviderAuth(lambda: "tok", header_name="X-Auth-Token"),
            retry_policy=None,
            follow_redirects=True,
            transport=httpx2.MockTransport(handler),
        ) as client:
            client.get(API_PATH, quiet=True)

        assert seen_headers[0]["x-auth-token"] == "Bearer tok"

    def test_custom_header_stripped_on_same_host_different_port_redirect(self) -> None:
        """Test that a custom auth header is dropped when a redirect changes port on the same host, mirroring
        httpx2's own origin check for Authorization
        """
        seen_headers = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.port is None:
                return httpx2.Response(302, headers={"Location": "https://example.com:8443/things"})
            seen_headers.append(dict(request.headers))
            return make_json_response(200, {"ok": True})

        with RestClient(
            BASE_URL,
            auth=TokenProviderAuth(lambda: "tok", header_name="X-Auth-Token"),
            retry_policy=None,
            follow_redirects=True,
            transport=httpx2.MockTransport(handler),
        ) as client:
            client.get(API_PATH, quiet=True)

        assert "x-auth-token" not in seen_headers[0]

    def test_custom_header_stripped_on_non_default_port_https_redirect(self) -> None:
        """Test that a custom auth header is dropped on an http-to-https redirect from a non-default port,
        since that is not the same-port upgrade httpx2 exempts from stripping Authorization
        """
        seen_headers = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            if request.url.scheme == "http":
                return httpx2.Response(302, headers={"Location": "https://example.com/things"})
            seen_headers.append(dict(request.headers))
            return make_json_response(200, {"ok": True})

        with RestClient(
            "http://example.com:8080/v1",
            auth=TokenProviderAuth(lambda: "tok", header_name="X-Auth-Token"),
            retry_policy=None,
            follow_redirects=True,
            transport=httpx2.MockTransport(handler),
        ) as client:
            client.get(API_PATH, quiet=True)

        assert "x-auth-token" not in seen_headers[0]


class TestTokenProviderAuthObservability:
    """Tests that a token fetch never leaks into logs or the console summary"""

    def test_token_value_never_logged(self, caplog: pytest.LogCaptureFixture, mocker: MockFixture) -> None:
        """Test that the raw token value never appears in a log record or the console summary"""
        caplog.set_level(logging.DEBUG)
        written: list[str] = []
        mocker.patch("sys.stdout.write", side_effect=written.append)
        mocker.patch("sys.stdout.flush")

        secret_token = "super-secret-access-token-value"

        def handler(request: httpx2.Request) -> httpx2.Response:
            return make_json_response(200, {"ok": True})

        with RestClient(
            BASE_URL,
            auth=TokenProviderAuth(lambda: secret_token),
            retry_policy=None,
            transport=httpx2.MockTransport(handler),
        ) as client:
            r = client.get(API_PATH, quiet=False)

        assert r.status_code == 200
        for record in caplog.records:
            assert secret_token not in record.getMessage()
            assert secret_token not in str(getattr(record, "response", ""))
        assert not any(secret_token in line for line in written)

    def test_custom_header_masked_on_retried_request_log(
        self, caplog: pytest.LogCaptureFixture, mocker: MockFixture
    ) -> None:
        """Test that a custom auth header is masked in the request log and console summary of a retried
        request, not just the first attempt

        A retried request reuses the same `Request` object the auth flow already wrote the header onto,
        unlike the first attempt, where request hooks run before the auth flow.
        """
        caplog.set_level(logging.DEBUG)
        written: list[str] = []
        mocker.patch("sys.stdout.write", side_effect=written.append)
        mocker.patch("sys.stdout.flush")

        secret_token = "super-secret-access-token-value"
        calls = {"n": 0}

        def handler(request: httpx2.Request) -> httpx2.Response:
            calls["n"] += 1
            return make_json_response(503 if calls["n"] == 1 else 200, {"ok": True})

        with RestClient(
            BASE_URL,
            auth=TokenProviderAuth(lambda: secret_token, header_name="X-Auth-Token"),
            retry_policy=RetryPolicy(retry_after=0),
            transport=httpx2.MockTransport(handler),
        ) as client:
            r = client.get(API_PATH, quiet=False)

        assert r.status_code == 200
        assert calls["n"] == 2
        for record in caplog.records:
            assert secret_token not in record.getMessage()
            assert secret_token not in str(getattr(record, "request_headers", ""))
        assert not any(secret_token in line for line in written)

    def test_api_key_query_param_masked_in_logs(self, caplog: pytest.LogCaptureFixture, mocker: MockFixture) -> None:
        """Test that an API key placed in a query parameter, under a name outside the built-in blocklist, is
        masked in request/response logs
        """
        caplog.set_level(logging.INFO, logger=HOOKS_LOGGER_NAME)
        written: list[str] = []
        mocker.patch("sys.stdout.write", side_effect=written.append)
        mocker.patch("sys.stdout.flush")
        secret_key = "leaky-api-key-value"

        def handler(request: httpx2.Request) -> httpx2.Response:
            return make_json_response(200, {"ok": True})

        with RestClient(
            BASE_URL,
            auth=APIKeyAuth(secret_key, name="my_custom_key", location="query"),
            retry_policy=None,
            transport=httpx2.MockTransport(handler),
        ) as client:
            client.get(API_PATH, quiet=False)

        for record in caplog.records:
            assert secret_key not in record.getMessage()
            assert secret_key not in str(getattr(record, "path", ""))
        assert not any(secret_key in line for line in written)


class TestTokenProviderAuthConcurrency:
    """Tests for single-flight token fetching under concurrent use"""

    def test_sync_concurrent_requests_fetch_once(self) -> None:
        """Test that 8 concurrent sync requests trigger exactly one token fetch"""
        counts = {"token_fetches": 0}
        lock = threading.Lock()

        def provider() -> str:
            with lock:
                counts["token_fetches"] += 1
            time.sleep(0.05)
            return "tok"

        def handler(request: httpx2.Request) -> httpx2.Response:
            return make_json_response(200, {"ok": True})

        with RestClient(
            BASE_URL, auth=TokenProviderAuth(provider), retry_policy=None, transport=httpx2.MockTransport(handler)
        ) as client:
            with ThreadPoolExecutor(8) as executor:
                results = list(executor.map(lambda _: client.get(API_PATH, quiet=True), range(8)))

        assert all(r.status_code == 200 for r in results)
        assert counts["token_fetches"] == 1

    async def test_async_concurrent_requests_fetch_once(self) -> None:
        """Test that 8 concurrently gathered async requests trigger exactly one token fetch"""
        counts = {"token_fetches": 0}

        async def provider() -> str:
            counts["token_fetches"] += 1
            await asyncio.sleep(0.05)
            return "tok"

        def handler(request: httpx2.Request) -> httpx2.Response:
            return make_json_response(200, {"ok": True})

        async with AsyncRestClient(
            BASE_URL, auth=TokenProviderAuth(provider), retry_policy=None, transport=httpx2.MockTransport(handler)
        ) as client:
            results = await asyncio.gather(*[client.get(API_PATH, quiet=True) for _ in range(8)])

        assert all(r.status_code == 200 for r in results)
        assert counts["token_fetches"] == 1

    def test_fetch_lock_released_when_provider_fails(self) -> None:
        """Test that a failed token fetch releases the single-flight lock, so a later call can still succeed"""
        state = {"fail": True}

        def provider() -> str:
            if state["fail"]:
                raise ConnectionError("connection refused")
            return "tok"

        def handler(request: httpx2.Request) -> httpx2.Response:
            return make_json_response(200, {"ok": True})

        auth = TokenProviderAuth(provider)
        with RestClient(BASE_URL, auth=auth, retry_policy=None, transport=httpx2.MockTransport(handler)) as client:
            with pytest.raises(ConnectionError):
                client.get(API_PATH, quiet=True)
            assert auth._fetch_lock.locked() is False

            state["fail"] = False
            r = client.get(API_PATH, quiet=True)

        assert r.status_code == 200

    def test_async_lock_rebinds_across_separate_event_loops(self) -> None:
        """Test that reusing one auth object across two separate asyncio.run() calls does not raise, even when
        several concurrent requests within each loop contend the single-flight lock and bind it to that loop

        `asyncio.Lock.acquire()` has an uncontended fast path that never binds the lock to a loop, so a single
        request per loop would pass even without the rebinding this test is for. The provider awaits a real
        suspension point so a second gathered request actually contends the lock while the first is fetching.
        """

        async def handler(request: httpx2.Request) -> httpx2.Response:
            return make_json_response(200, {"ok": True})

        async def provider() -> str:
            await asyncio.sleep(0.02)
            return "tok"

        auth = TokenProviderAuth(provider)

        async def make_calls() -> list[int]:
            async with AsyncRestClient(
                BASE_URL, auth=auth, retry_policy=None, transport=httpx2.MockTransport(handler)
            ) as client:
                results = await asyncio.gather(*[client.get(API_PATH, quiet=True) for _ in range(4)])
                return [r.status_code for r in results]

        statuses_1 = asyncio.run(make_calls())
        auth.token = None
        statuses_2 = asyncio.run(make_calls())

        assert statuses_1 == [200, 200, 200, 200]
        assert statuses_2 == [200, 200, 200, 200]


class TestRestClientAuthSurface:
    """Tests for the auth surface exposed on RestClient/AsyncRestClient"""

    def test_ctor_auth_reaches_underlying_client(self) -> None:
        """Test that the auth constructor parameter is applied to the underlying httpx2 client"""
        auth = BearerAuth("ctor-token")
        with RestClient(BASE_URL, auth=auth) as client:
            assert client.client.auth is auth

    def test_per_request_auth_overrides_client_auth(self) -> None:
        """Test that a per-request auth overrides the client's own auth for a single call only"""
        seen_headers = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen_headers.append(request.headers.get("Authorization"))
            return make_json_response(200, {"ok": True})

        with RestClient(
            BASE_URL, auth=BearerAuth("client-token"), retry_policy=None, transport=httpx2.MockTransport(handler)
        ) as client:
            client.get(API_PATH, auth=BearerAuth("override-token"), quiet=True)
            client.get(API_PATH, quiet=True)

        assert seen_headers == ["Bearer override-token", "Bearer client-token"]

    def test_per_request_auth_none_is_unauthenticated_for_that_call_only(self) -> None:
        """Test that auth=None on a single call sends no Authorization header, without disturbing the client auth"""
        seen_headers = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen_headers.append(request.headers.get("Authorization"))
            return make_json_response(200, {"ok": True})

        with RestClient(
            BASE_URL, auth=BearerAuth("client-token"), retry_policy=None, transport=httpx2.MockTransport(handler)
        ) as client:
            client.get(API_PATH, auth=None, quiet=True)
            assert client.client.auth is not None
            client.get(API_PATH, quiet=True)

        assert seen_headers == [None, "Bearer client-token"]

    def test_auth_not_smuggled_into_query_params(self) -> None:
        """Test that the auth keyword is never sent as a literal query parameter"""
        seen_query = {}

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen_query.update(dict(request.url.params))
            return make_json_response(200, {"ok": True})

        with RestClient(BASE_URL, retry_policy=None, transport=httpx2.MockTransport(handler)) as client:
            client.get(API_PATH, auth=None, quiet=True)

        assert "auth" not in seen_query
