"""Tests for common_libs.clients.email_client.auth module"""

from collections.abc import Callable

import pytest
from pytest_mock import MockerFixture

from common_libs.clients.email_client import EmailClient, OAuth2Auth, PasswordAuth
from tests.libs.email_client.fake_imap import ACCESS_TOKEN, USERNAME, FakeIMAPServer

pytestmark = [pytest.mark.unittest]

SECRET = "s3cret-value"


class TestPasswordAuth:
    """Tests for `PasswordAuth`"""

    def test_logs_in_with_the_password(self, client: EmailClient, imap_server: FakeIMAPServer) -> None:
        """Test that the login name and the password are sent with the `LOGIN` command"""
        client.find_emails(USERNAME)

        assert imap_server.connections[0].credentials == (USERNAME, "secret")
        assert [c for c in imap_server.commands if c.startswith(("LOGIN", "AUTHENTICATE"))] == ["LOGIN"]


class TestOAuth2Auth:
    """Tests for `OAuth2Auth`"""

    @pytest.mark.parametrize("token", [ACCESS_TOKEN, lambda: ACCESS_TOKEN], ids=["string", "function"])
    def test_logs_in_with_the_token(
        self,
        client_factory: Callable[..., EmailClient],
        imap_server: FakeIMAPServer,
        token: str | Callable[[], str],
    ) -> None:
        """Test that the token, given directly or by a function, is sent with the `XOAUTH2` mechanism"""
        client_factory(auth=OAuth2Auth(token)).find_emails(USERNAME)

        assert imap_server.connections[0].credentials == (USERNAME, ACCESS_TOKEN)
        assert [c for c in imap_server.commands if c.startswith(("LOGIN", "AUTHENTICATE"))] == ["AUTHENTICATE XOAUTH2"]

    def test_asks_the_function_for_a_token_before_every_connection(
        self,
        client_factory: Callable[..., EmailClient],
        imap_server: FakeIMAPServer,
        mocker: MockerFixture,
        email_address: str,
        send_email_during_wait: Callable[..., None],
    ) -> None:
        """Test that a token that expires while waiting is replaced, because every check opens a new connection"""
        provider = mocker.Mock(return_value=ACCESS_TOKEN)
        send_email_during_wait(3, email_address)

        client_factory(auth=OAuth2Auth(provider)).wait_for_email(email_address)

        assert (provider.call_count, imap_server.connection_attempts) == (3, 3)


class TestAuthRepr:
    """Tests for the string representation of the auth objects"""

    @pytest.mark.parametrize(
        "auth",
        [PasswordAuth(SECRET), OAuth2Auth(SECRET), OAuth2Auth(lambda: SECRET)],
        ids=["password", "token", "token-function"],
    )
    def test_hides_the_secret(self, auth: PasswordAuth | OAuth2Auth) -> None:
        """Test that the password and the token do not show up in logs and failure reports"""
        assert SECRET not in repr(auth)
