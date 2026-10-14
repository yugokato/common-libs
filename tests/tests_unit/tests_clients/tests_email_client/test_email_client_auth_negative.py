"""Negative tests for common_libs.clients.email_client.auth module"""

from collections.abc import Callable
from typing import Any

import pytest
from pytest_mock import MockerFixture

from common_libs.clients.email_client import EmailClient, EmailClientError, OAuth2Auth
from tests.libs.email_client.fake_imap import ACCESS_TOKEN, HOST, USERNAME, FakeIMAPServer

pytestmark = [pytest.mark.unittest]

TOKEN_ERROR = "The OAuth2 token must be a string that is not blank and has no control characters"
INVALID_TOKENS = [
    pytest.param("", id="empty"),
    pytest.param("  ", id="blank"),
    pytest.param("ya29.token\x01", id="separator"),
    pytest.param("ya29.token\r\nA2 LOGOUT", id="line-break"),
    pytest.param("ya29.\x00token", id="null"),
]


class TestOAuth2AuthNegative:
    """Tests for `OAuth2Auth` with invalid input and failing servers"""

    @pytest.mark.parametrize("token", INVALID_TOKENS)
    def test_rejects_invalid_token(self, token: str) -> None:
        """Test that a token that is blank or would break the protocol is rejected when the object is created"""
        with pytest.raises(ValueError, match=TOKEN_ERROR):
            OAuth2Auth(token)

    @pytest.mark.parametrize("token", [*INVALID_TOKENS, pytest.param(None, id="none")])
    def test_rejects_invalid_token_from_function(
        self, client_factory: Callable[..., EmailClient], imap_server: FakeIMAPServer, token: Any
    ) -> None:
        """Test that a token returned by the function is checked the same way, before connecting to the server"""
        client = client_factory(auth=OAuth2Auth(lambda: token))

        with pytest.raises(ValueError, match=TOKEN_ERROR):
            client.find_emails(USERNAME)

        assert imap_server.connection_attempts == 0

    def test_rejects_username_with_control_characters(
        self, client_factory: Callable[..., EmailClient], imap_server: FakeIMAPServer
    ) -> None:
        """Test that a login name that would break the XOAUTH2 message is rejected before connecting to the server"""
        client = client_factory(username=f"{USERNAME}\x01auth=Bearer other", auth=OAuth2Auth(ACCESS_TOKEN))

        with pytest.raises(ValueError, match="username must not contain control characters"):
            client.find_emails(USERNAME)

        assert imap_server.connection_attempts == 0

    def test_acknowledges_the_error_challenge_and_reports_the_failure(
        self, client_factory: Callable[..., EmailClient], imap_server: FakeIMAPServer, email_address: str
    ) -> None:
        """Test that a rejected token is reported right away, after the server's error challenge got an empty reply"""
        client = client_factory(auth=OAuth2Auth("expired-token"))

        with pytest.raises(EmailClientError, match=f"Unable to log in to {HOST} as {USERNAME!r}.*AUTHENTICATIONFAILED"):
            client.wait_for_email(email_address)

        connection = imap_server.connections[0]
        assert (imap_server.connection_attempts, connection.challenge_replies, connection.logged_out) == (
            1,
            [b""],
            True,
        )

    @pytest.mark.parametrize(
        "error",
        [ConnectionError("Token service unavailable"), RuntimeError("Credentials revoked")],
        ids=["os-error", "other-error"],
    )
    def test_does_not_mistake_errors_of_the_function_for_a_failed_login(
        self,
        client_factory: Callable[..., EmailClient],
        imap_server: FakeIMAPServer,
        mocker: MockerFixture,
        email_address: str,
        error: Exception,
    ) -> None:
        """Test that an error of the token function reaches the caller as is, even if it is an `OSError`"""
        client = client_factory(auth=OAuth2Auth(mocker.Mock(side_effect=error)))

        with pytest.raises(type(error), match=str(error)):
            client.wait_for_email(email_address)

        assert imap_server.connection_attempts == 0

    def test_does_not_retry_errors_of_the_function_after_the_mailbox_was_reached(
        self,
        client_factory: Callable[..., EmailClient],
        imap_server: FakeIMAPServer,
        mocker: MockerFixture,
        email_address: str,
    ) -> None:
        """Test that a wait ends right away when the token function fails, instead of retrying like a server error"""
        provider = mocker.Mock(side_effect=[ACCESS_TOKEN, ConnectionError("Token service unavailable")])
        client = client_factory(auth=OAuth2Auth(provider))
        client.find_emails(email_address)

        with pytest.raises(ConnectionError, match="Token service unavailable"):
            client.wait_for_email(email_address)

        assert (provider.call_count, imap_server.connection_attempts) == (2, 1)
