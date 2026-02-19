"""Tests for common_libs.clients.rest_client.base module"""

from common_libs.clients.rest_client.base import RestClientBase
from common_libs.clients.rest_client.ext import AsyncHTTPClient, BearerAuth, SyncHTTPClient


class TestRestClientBase:
    """Tests for RestClientBase class"""

    def test_init_sync_mode(self) -> None:
        """Test initialization in sync mode creates SyncHTTPClient"""
        client = RestClientBase("http://example.com")
        assert client.async_mode is False
        assert isinstance(client.client, SyncHTTPClient)
        assert client.log_headers is False
        assert client.prettify_response_log is True

    def test_init_async_mode(self) -> None:
        """Test initialization in async mode creates AsyncHTTPClient"""
        client = RestClientBase("http://example.com", async_mode=True)
        assert client.async_mode is True
        assert isinstance(client.client, AsyncHTTPClient)

    def test_init_custom_options(self) -> None:
        """Test initialization with custom log_headers and prettify options"""
        client = RestClientBase("http://example.com", log_headers=True, prettify_response_log=False)
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

    def test_get_bearer_token_from_bearer_auth(self) -> None:
        """Test getting bearer token when BearerAuth is set"""
        token = "my-token-123"
        client = RestClientBase("http://example.com")
        client.client.auth = BearerAuth(token)
        result = client.get_bearer_token()
        assert result == token

    def test_get_bearer_token_none_when_no_auth(self) -> None:
        """Test that get_bearer_token returns None when no auth is set"""
        client = RestClientBase("http://example.com")
        client.client.auth = None
        token = client.get_bearer_token()
        assert token is None

    def test_set_bearer_token(self) -> None:
        """Test that set_bearer_token sets BearerAuth on the client"""
        token = "new-token"
        client = RestClientBase("http://example.com")
        client.set_bearer_token(token)
        assert isinstance(client.client.auth, BearerAuth)
        assert client.client.auth.token == token

    def test_unset_bear_token(self) -> None:
        """Test that unset_bear_token removes auth from the client"""
        client = RestClientBase("http://example.com")
        client.set_bearer_token("some-token")
        client.unset_bear_token()
        assert client.client.auth is None
