"""Tests for common_libs.clients.rest_client.base module"""

import pytest

from common_libs.clients.rest_client.auth import BasicAuth, BearerAuth, TokenProviderAuth
from common_libs.clients.rest_client.base import RestClientBase
from common_libs.clients.rest_client.ext import AsyncHTTPClient, SyncHTTPClient


class TestRestClientBase:
    """Tests for RestClientBase class"""

    def test_init_sync_mode(self) -> None:
        """Test initialization in sync mode creates SyncHTTPClient"""
        client = RestClientBase("http://example.com")
        assert client.async_mode is False
        assert isinstance(client.client, SyncHTTPClient)
        assert client.log_requests is True
        assert client.log_headers is False
        assert client.prettify_response_log is True

    def test_init_async_mode(self) -> None:
        """Test initialization in async mode creates AsyncHTTPClient"""
        client = RestClientBase("http://example.com", async_mode=True)
        assert client.async_mode is True
        assert isinstance(client.client, AsyncHTTPClient)

    def test_init_custom_options(self) -> None:
        """Test initialization with custom log_requests, log_headers, and prettify options"""
        client = RestClientBase("http://example.com", log_requests=False, log_headers=True, prettify_response_log=False)
        assert client.log_requests is False
        assert client.log_headers is True
        assert client.prettify_response_log is False

    def test_base_url_property(self) -> None:
        """Test that base_url property returns the configured base URL"""
        url = "http://example.com/api/"
        client = RestClientBase(url)
        assert client.base_url == url

    def test_base_url_setter(self) -> None:
        """Test that base_url setter updates the underlying client"""
        new_url = "http://new-host.com"
        client = RestClientBase("http://example.com")
        client.base_url = new_url
        assert client.base_url == new_url

    def test_auth_ctor_param_reaches_underlying_client(self) -> None:
        """Test that the auth constructor parameter is passed through to the underlying httpx2 client"""
        auth = BearerAuth("ctor-token")
        client = RestClientBase("http://example.com", auth=auth)
        assert client.client.auth is auth

    def test_auth_property_getter(self) -> None:
        """Test that the auth property reads the underlying client's auth"""
        auth = BearerAuth("some-token")
        client = RestClientBase("http://example.com")
        client.client.auth = auth
        assert client.auth is auth

    def test_auth_property_setter(self) -> None:
        """Test that the auth property setter updates the underlying client's auth"""
        auth = BearerAuth("some-token")
        client = RestClientBase("http://example.com")
        client.auth = auth
        assert client.client.auth is auth

    def test_get_token_from_token_auth(self) -> None:
        """Test getting the token when a TokenProviderAuth is set"""
        token = "my-token-123"
        client = RestClientBase("http://example.com")
        client.client.auth = BearerAuth(token)
        assert client.token == token

    def test_get_token_from_provider_backed_token_auth(self) -> None:
        """Test getting the token when a provider-backed TokenProviderAuth already has a cached token"""
        auth = TokenProviderAuth(lambda: "provider-token")
        auth.token = "cached-token"
        client = RestClientBase("http://example.com")
        client.client.auth = auth
        assert client.token == "cached-token"

    def test_get_token_none_when_no_auth(self) -> None:
        """Test that the token property returns None when no auth is set"""
        client = RestClientBase("http://example.com")
        client.client.auth = None
        assert client.token is None

    def test_token_setter_logs_in_with_a_bearer_auth(self) -> None:
        """Test that assigning client.token installs a BearerAuth carrying that token"""
        client = RestClientBase("http://example.com")
        client.token = "new-token"
        assert isinstance(client.client.auth, BearerAuth)
        assert client.token == "new-token"

    def test_token_setter_none_clears_the_bearer_auth_in_place(self) -> None:
        """Test that assigning None to client.token clears the installed BearerAuth's token rather than
        discarding the auth itself
        """
        auth = BearerAuth("some-token")
        client = RestClientBase("http://example.com", auth=auth)
        client.token = None
        assert client.client.auth is auth
        assert client.token is None

    def test_token_setter_none_with_no_auth_is_a_noop(self) -> None:
        """Test that assigning None to client.token when no auth is installed stays a no-op instead of
        installing a BearerAuth that holds no token
        """
        client = RestClientBase("http://example.com")
        client.token = None
        assert client.client.auth is None

    def test_token_setter_empty_string_raises(self) -> None:
        """Test that assigning an empty string to client.token raises instead of silently clearing the auth"""
        client = RestClientBase("http://example.com", auth=BearerAuth("some-token"))
        with pytest.raises(ValueError, match="token must not be empty"):
            client.token = ""
        assert client.token == "some-token"

    def test_token_setter_forwards_to_an_installed_token_auth(self) -> None:
        """Test that assigning client.token seeds an installed TokenProviderAuth's cache instead of replacing the
        auth with a BearerAuth
        """
        auth = TokenProviderAuth(lambda: "provider-token")
        client = RestClientBase("http://example.com", auth=auth)
        client.token = "seeded-token"
        assert client.client.auth is auth
        assert client.token == "seeded-token"

    def test_token_setter_raises_for_a_non_bearer_style_auth(self) -> None:
        """Test that assigning client.token when a non-bearer-style auth is installed raises TypeError
        instead of silently discarding it
        """
        auth = BasicAuth("user", "pw")
        client = RestClientBase("http://example.com", auth=auth)
        with pytest.raises(TypeError, match="BasicAuth"):
            client.token = "new-token"
        assert client.client.auth is auth

    def test_token_getter_returns_none_for_a_non_bearer_style_auth(self) -> None:
        """Test that the token getter returns None when a non-bearer-style auth is installed, rather than
        raising, since only the setter is restricted to bearer-style auths
        """
        client = RestClientBase("http://example.com", auth=BasicAuth("user", "pw"))
        assert client.token is None

    def test_auth_assignment_logs_in_and_token_reflects_it(self) -> None:
        """Test that assigning a BearerAuth to client.auth is how you log in, and client.token reads it back"""
        client = RestClientBase("http://example.com")
        client.auth = BearerAuth("new-token")
        assert client.token == "new-token"

    def test_auth_assignment_of_none_logs_out(self) -> None:
        """Test that assigning None to client.auth is how you log out, and client.token reflects that"""
        client = RestClientBase("http://example.com", auth=BearerAuth("some-token"))
        client.auth = None
        assert client.token is None

    def test_http2_enabled_by_default(self) -> None:
        """Test that HTTP/2 is enabled by default when no http2 kwarg is supplied"""
        client = RestClientBase("http://example.com")
        assert isinstance(client.client, SyncHTTPClient)

    def test_http2_can_be_disabled(self) -> None:
        """Test that passing `http2=False` does not raise and creates the client successfully"""
        client = RestClientBase("http://example.com", http2=False)
        assert isinstance(client.client, SyncHTTPClient)

    def test_http2_can_be_explicitly_enabled(self) -> None:
        """Test that passing `http2=True` explicitly does not raise"""
        client = RestClientBase("http://example.com", http2=True)
        assert isinstance(client.client, SyncHTTPClient)
