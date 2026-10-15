"""Integration tests for common_libs.clients.rest_client.auth module"""

import base64
import logging
import re
from collections.abc import Callable
from pathlib import Path
from typing import Literal

import pytest

from common_libs.clients.rest_client import AsyncRestClient, RestClient
from common_libs.clients.rest_client.auth import (
    APIKeyAuth,
    BasicAuth,
    BearerAuth,
    ClientAuth,
    DigestAuth,
    NetRCAuth,
    TokenProviderAuth,
)
from tests.libs.containers.mock_server import HOST, XDIST_GROUP, MockResponse, MockServerContainer

pytestmark = [pytest.mark.integrationtest, pytest.mark.xdist_group(XDIST_GROUP)]

USERNAME = "alice"
PASSWORD = "s3cret"
SECRET = "super-secret-credential"


class TestStaticCredentials:
    """Tests for the auth classes that send the same credentials with every request"""

    def test_basic_auth_sends_the_encoded_credentials(
        self, client_factory: Callable[..., RestClient], mock_server: MockServerContainer, path: str
    ) -> None:
        """Test that BasicAuth sends the username and the password as an Authorization header"""
        mock_server.expect(path)

        client_factory(auth=BasicAuth(USERNAME, PASSWORD)).get(path)

        assert mock_server.received(path)[0].header("authorization") == _basic_header(USERNAME, PASSWORD)

    def test_netrc_auth_sends_the_credentials_of_the_host(
        self, client_factory: Callable[..., RestClient], mock_server: MockServerContainer, path: str, tmp_path: Path
    ) -> None:
        """Test that NetRCAuth looks up the credentials of the server in a netrc file"""
        netrc = tmp_path / "netrc"
        netrc.write_text(f"machine {HOST} login {USERNAME} password {PASSWORD}\n")
        mock_server.expect(path)

        client_factory(auth=NetRCAuth(file=str(netrc))).get(path)

        assert mock_server.received(path)[0].header("authorization") == _basic_header(USERNAME, PASSWORD)

    def test_digest_auth_answers_the_challenge_of_the_server(
        self, client_factory: Callable[..., RestClient], mock_server: MockServerContainer, path: str
    ) -> None:
        """Test that DigestAuth sends the request again with the credentials after the server challenges it"""
        challenge = 'Digest realm="test-realm", nonce="abc123", qop="auth", algorithm=MD5'
        mock_server.expect(path, MockResponse(body={"ok": True}), headers={"Authorization": "Digest .*"}, priority=10)
        mock_server.expect(path, MockResponse(status=401, headers={"WWW-Authenticate": challenge}))

        response = client_factory(auth=DigestAuth(USERNAME, PASSWORD)).get(path)

        first, second = mock_server.received(path)
        assert (response.status_code, first.header("authorization")) == (200, None)
        expected = (
            rf'Digest username="{USERNAME}", realm="test-realm", nonce="abc123", uri="{re.escape(path)}", '
            r'response="[0-9a-f]{32}", algorithm=MD5, qop=auth, nc=00000001, cnonce="[0-9a-f]+"'
        )
        assert re.fullmatch(expected, second.header("authorization") or "")

    def test_token_property_sends_a_bearer_token(
        self, client_factory: Callable[..., RestClient], mock_server: MockServerContainer, path: str
    ) -> None:
        """Test that setting the token of a client sends it as a bearer token"""
        mock_server.expect(path)
        client = client_factory()
        client.token = "token-1"

        client.get(path)

        assert mock_server.received(path)[0].header("authorization") == "Bearer token-1"

    @pytest.mark.parametrize(
        ("auth", "sent"),
        [(BearerAuth("token-2"), "Bearer token-2"), (None, None)],
        ids=["another-auth", "no-auth"],
    )
    def test_auth_of_a_request_overrides_the_auth_of_the_client(
        self,
        client_factory: Callable[..., RestClient],
        mock_server: MockServerContainer,
        path: str,
        auth: ClientAuth,
        sent: str | None,
    ) -> None:
        """Test that the auth passed to a request replaces the one of the client, and None sends no credentials"""
        mock_server.expect(path)

        client_factory(auth=BearerAuth("token-1")).get(path, auth=auth)

        assert mock_server.received(path)[0].header("authorization") == sent

    @pytest.mark.parametrize(
        ("location", "expected"),
        [("header", ("key-1", None)), ("query", (None, ["key-1"]))],
    )
    def test_api_key_is_sent_in_the_given_location(
        self,
        client_factory: Callable[..., RestClient],
        mock_server: MockServerContainer,
        path: str,
        location: Literal["header", "query"],
        expected: tuple[str | None, list[str] | None],
    ) -> None:
        """Test that APIKeyAuth puts the key in a header or in the query string"""
        mock_server.expect(path)

        client_factory(auth=APIKeyAuth("key-1", name="X-Api-Token", location=location)).get(path)

        request = mock_server.received(path)[0]
        assert (request.header("x-api-token"), request.query.get("X-Api-Token")) == expected


class TestTokenProviderAuth:
    """Tests for TokenProviderAuth against a server that rejects a stale token"""

    @pytest.fixture(autouse=True)
    def server_that_accepts_the_second_token(self, mock_server: MockServerContainer, path: str) -> None:
        """Make the server answer 401 to every token except token-2"""
        mock_server.expect(
            path, MockResponse(body={"ok": True}), headers={"Authorization": "Bearer token-2"}, priority=10
        )
        mock_server.expect(path, MockResponse(status=401))

    def test_stale_token_is_replaced_and_the_request_is_sent_again(
        self, client_factory: Callable[..., RestClient], mock_server: MockServerContainer, path: str
    ) -> None:
        """Test that a 401 makes the client fetch a new token and send the same request again, with its body"""
        tokens = iter(["token-1", "token-2"])
        client = client_factory(auth=TokenProviderAuth(lambda: next(tokens)))

        response = client.post(path, name=USERNAME)

        sent = [(request.header("authorization"), request.body) for request in mock_server.received(path)]
        assert (response.status_code, sent) == (
            200,
            [("Bearer token-1", {"name": USERNAME}), ("Bearer token-2", {"name": USERNAME})],
        )

    async def test_async_provider_is_awaited_for_the_new_token(
        self, async_client_factory: Callable[..., AsyncRestClient], mock_server: MockServerContainer, path: str
    ) -> None:
        """Test that an async client awaits an async provider, and replays the request with the new token"""
        tokens = iter(["token-1", "token-2"])

        async def provide_token() -> str:
            return next(tokens)

        client = async_client_factory(auth=TokenProviderAuth(provide_token))

        response = await client.post(path, name=USERNAME)

        sent = [request.header("authorization") for request in mock_server.received(path)]
        assert (response.status_code, sent) == (200, ["Bearer token-1", "Bearer token-2"])


class TestCredentialMasking:
    """Tests for the masking of credentials in the logs of RestClient"""

    @pytest.mark.parametrize(
        ("auth", "masked"),
        [
            (BearerAuth(SECRET), "'authorization': '***'"),
            (APIKeyAuth(SECRET), "'x-api-key': '***'"),
            (APIKeyAuth(SECRET, name="api_key", location="query"), f"api_key={'*' * len(SECRET)}"),
        ],
        ids=["bearer-header", "api-key-header", "api-key-query"],
    )
    def test_credentials_are_masked_in_the_logs_but_sent_to_the_server(
        self,
        client_factory: Callable[..., RestClient],
        mock_server: MockServerContainer,
        path: str,
        caplog: pytest.LogCaptureFixture,
        capsys: pytest.CaptureFixture,
        auth: ClientAuth,
        masked: str,
    ) -> None:
        """Test that the console summary and the log records show a credential masked while the server gets it"""
        caplog.set_level(logging.INFO, logger="common_libs")
        mock_server.expect(path)

        client_factory(auth=auth, log_headers=True).get(path)

        request = mock_server.received(path)[0]
        shown = capsys.readouterr().out + "".join(str(record.__dict__) for record in caplog.records)
        assert masked in shown
        assert SECRET not in shown
        assert SECRET in str((request.headers, request.query))


class TestRedirectsWithCredentials:
    """Tests for the credentials RestClient sends when a redirect leads to another origin"""

    @pytest.mark.parametrize(
        ("auth", "header"),
        [(BearerAuth(SECRET), "authorization"), (APIKeyAuth(SECRET), "x-api-key")],
        ids=["bearer", "api-key"],
    )
    def test_credentials_are_not_sent_to_another_origin(
        self,
        client_factory: Callable[..., RestClient],
        mock_server: MockServerContainer,
        path: str,
        auth: ClientAuth,
        header: str,
    ) -> None:
        """Test that a redirect from HTTPS to plain HTTP, which is another origin, does not carry the credentials"""
        landed = f"{path}/landed"
        mock_server.expect(path, MockResponse(status=302, headers={"Location": mock_server.url("http") + landed}))
        mock_server.expect(landed, MockResponse(body={"landed": True}))

        response = client_factory(auth=auth, follow_redirects=True).get(path)

        sent = [mock_server.received(p)[0].header(header) for p in (path, landed)]
        assert (response.response, [value is not None for value in sent]) == ({"landed": True}, [True, False])


def _basic_header(username: str, password: str) -> str:
    return "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()
