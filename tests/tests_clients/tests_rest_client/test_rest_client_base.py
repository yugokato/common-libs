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

    def test_unset_bearer_token(self) -> None:
        """Test that unset_bearer_token removes auth from the client"""
        client = RestClientBase("http://example.com")
        client.set_bearer_token("some-token")
        client.unset_bearer_token()
        assert client.client.auth is None

    def test_get_bearer_token_from_header_with_extra_whitespace(self) -> None:
        """Test that get_bearer_token correctly extracts the token when the Authorization header has extra whitespace"""
        token = "my-token-123"
        client = RestClientBase("http://example.com")
        client.client.headers["Authorization"] = f"Bearer  {token}"  # two spaces
        result = client.get_bearer_token()
        assert result == token

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
