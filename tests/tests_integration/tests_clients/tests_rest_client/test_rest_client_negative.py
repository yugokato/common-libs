"""Negative integration tests for common_libs.clients.rest_client"""

import asyncio
import ssl
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Literal

import httpx2
import pytest

from common_libs.clients.rest_client import AsyncRestClient, RestClient, RetryPolicy
from common_libs.clients.rest_client.auth import TokenProviderAuth
from tests.libs.containers.base import HOST
from tests.libs.containers.http3_proxy import Http3ProxyContainer
from tests.libs.containers.mock_server import XDIST_GROUP, MockResponse, MockServerContainer

pytestmark = [pytest.mark.integrationtest, pytest.mark.xdist_group(XDIST_GROUP)]

# A privileged port that nothing listens on, which the random host ports of containers never take
UNUSED_PORT = 1
HTTP3_TIMEOUT = 15.0
RETRY_DELAY = 0.05


class TestErrorResponses:
    """Tests for how RestClient reports the error responses of a real server"""

    @pytest.mark.parametrize("status_code", [400, 401, 404, 500, 503])
    def test_error_status_is_not_ok_and_can_be_raised(
        self,
        client_factory: Callable[..., RestClient],
        mock_server: MockServerContainer,
        path: str,
        status_code: int,
    ) -> None:
        """Test that a 4xx or 5xx response is returned as not ok, and raise_for_status() raises for it"""
        mock_server.expect(path, MockResponse(status=status_code, body={"error": "failed"}))

        response = client_factory().get(path)

        assert (response.status_code, response.ok, response.response) == (status_code, False, {"error": "failed"})
        with pytest.raises(httpx2.HTTPStatusError, match=str(status_code)):
            response.raise_for_status()

    def test_last_response_is_returned_when_the_retries_run_out(
        self, client_factory: Callable[..., RestClient], mock_server: MockServerContainer, path: str
    ) -> None:
        """Test that the last 503 is returned after the allowed number of retries"""
        mock_server.expect(path, MockResponse(status=503))
        client = client_factory(retry_policy=RetryPolicy(num_retries=2, retry_after=RETRY_DELAY))

        response = client.get(path)

        assert (response.status_code, len(mock_server.received(path))) == (503, 3)

    def test_repeated_401_is_returned_after_one_token_refresh(
        self, client_factory: Callable[..., RestClient], mock_server: MockServerContainer, path: str
    ) -> None:
        """Test that a 401 to the refreshed token is returned instead of being replayed again"""
        mock_server.expect(path, MockResponse(status=401))
        provided: list[str] = []

        def provide_token() -> str:
            provided.append(f"token-{len(provided) + 1}")
            return provided[-1]

        response = client_factory(auth=TokenProviderAuth(provide_token)).get(path)

        sent = [request.header("authorization") for request in mock_server.received(path)]
        assert (response.status_code, provided, sent) == (
            401,
            ["token-1", "token-2"],
            ["Bearer token-1", "Bearer token-2"],
        )


class TestConnectionFailures:
    """Tests for how RestClient reports the failures of the connection to a real server"""

    def test_slow_response_raises_a_timeout_and_logs_it(
        self,
        client_factory: Callable[..., RestClient],
        mock_server: MockServerContainer,
        path: str,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test that a response that takes longer than the read timeout raises, and the timeout is logged"""
        mock_server.expect(path, MockResponse(delay=2.0))
        client = client_factory(timeout=httpx2.Timeout(5.0, read=0.3))

        with pytest.raises(httpx2.ReadTimeout):
            client.get(path)

        assert "Request timed out" in caplog.text

    def test_unreachable_server_raises_a_connect_error(self, client_factory: Callable[..., RestClient]) -> None:
        """Test that a connection to a port nobody listens on raises a ConnectError"""
        client = client_factory(base_url=f"http://{HOST}:{UNUSED_PORT}")

        with pytest.raises(httpx2.ConnectError):
            client.get("/anything")

    @pytest.mark.parametrize(
        ("scheme", "error"),
        [
            ("http", "Server disconnected without sending a response"),
            ("https", "StreamReset"),
        ],
        ids=["http1", "http2"],
    )
    def test_dropped_connection_raises_a_protocol_error(
        self,
        client_factory: Callable[..., RestClient],
        mock_server: MockServerContainer,
        path: str,
        scheme: Literal["http", "https"],
        error: str,
    ) -> None:
        """Test that a connection that the server closes without a response raises a RemoteProtocolError"""
        mock_server.expect_dropped_connection(path)

        with pytest.raises(httpx2.RemoteProtocolError, match=error):
            client_factory(base_url=mock_server.url(scheme)).get(path)

    def test_certificate_of_an_untrusted_authority_is_rejected(
        self, client_factory: Callable[..., RestClient], mock_server: MockServerContainer, path: str
    ) -> None:
        """Test that the default verification rejects the certificate the mock server's own CA signed"""
        mock_server.expect(path)

        with pytest.raises(httpx2.ConnectError) as exc_info:
            client_factory(verify=True).get(path)

        # The wording of the error depends on the trust store of the platform, so the type of the cause is checked
        assert any(isinstance(error, ssl.SSLCertVerificationError) for error in _exception_chain(exc_info.value))

    def test_redirect_loop_raises_too_many_redirects(
        self, client_factory: Callable[..., RestClient], mock_server: MockServerContainer, path: str
    ) -> None:
        """Test that a server that keeps redirecting to itself stops the client after the maximum redirects"""
        mock_server.expect(path, MockResponse(status=302, headers={"Location": path}))

        with pytest.raises(httpx2.TooManyRedirects):
            client_factory(follow_redirects=True, max_redirects=2).get(path)

        assert len(mock_server.received(path)) == 3


class TestHttp3Negative:
    """Tests for AsyncRestClient.http3() when the connection can not be made"""

    async def test_certificate_of_an_untrusted_authority_is_rejected(self, http3_proxy: Http3ProxyContainer) -> None:
        """Test that the default verification, which trusts public CAs only, fails the handshake with the proxy"""
        with pytest.raises(ConnectionError):
            async with asyncio.timeout(HTTP3_TIMEOUT):
                async with AsyncRestClient.http3(http3_proxy.url):
                    pass

    async def test_missing_ca_path_is_reported_before_connecting(self, tmp_path: Path) -> None:
        """Test that a CA path that does not exist fails at once instead of during the handshake"""
        with pytest.raises(FileNotFoundError, match="does not exist"):
            async with AsyncRestClient.http3(f"https://{HOST}:{UNUSED_PORT}", verify=tmp_path / "missing-ca.pem"):
                pass


def _exception_chain(error: BaseException | None) -> Iterator[BaseException]:
    """Yield an exception followed by the exceptions that caused it"""
    while error is not None:
        yield error
        error = error.__cause__ or error.__context__
