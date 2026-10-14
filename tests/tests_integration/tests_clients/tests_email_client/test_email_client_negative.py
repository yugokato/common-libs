"""Negative integration tests for common_libs.clients.email_client.client module"""

import re
import ssl
from collections.abc import Callable
from datetime import date, timedelta

import pytest

from common_libs.clients.email_client import EmailClient, EmailClientError, EmailNotFoundError, OAuth2Auth, PasswordAuth
from tests.libs.email_client.emails import EmailSpec
from tests.libs.email_client.fake_app import FakeApp
from tests.libs.email_client.mail_server import HOST, USERNAME

pytestmark = [pytest.mark.integrationtest, pytest.mark.xdist_group(__name__)]

# A privileged port that nothing listens on, which the random host ports of containers never take
UNUSED_PORT = 1
SHORT_TIMEOUT = 0.3


class TestWaitForEmailNegative:
    """Tests for `EmailClient.wait_for_email()` against a real IMAP server when nothing matches"""

    def test_raises_when_the_subject_does_not_match(
        self,
        client: EmailClient,
        app: FakeApp,
        email_address: str,
        password_reset: EmailSpec,
        workspace_invitation: EmailSpec,
    ) -> None:
        """Test that the wait ends with an error that lists the subject of the email that did arrive"""
        app.send_email(password_reset.name, email_address)

        with pytest.raises(EmailNotFoundError) as exc_info:
            client.wait_for_email(email_address, subject=workspace_invitation.subject, timeout=SHORT_TIMEOUT)

        assert repr(password_reset.subject) in str(exc_info.value)

    def test_raises_when_no_email_arrives_after_the_given_one(
        self, client: EmailClient, app: FakeApp, email_address: str, password_reset: EmailSpec
    ) -> None:
        """Test that the email that was already returned is not returned again when waiting for a later one"""
        app.send_email(password_reset.name, email_address)
        first = client.wait_for_email(email_address)

        with pytest.raises(EmailNotFoundError):
            client.wait_for_email(email_address, after=first, timeout=SHORT_TIMEOUT)


class TestFindEmailsNegative:
    """Tests for `EmailClient.find_emails()` against a real IMAP server when nothing matches"""

    @pytest.mark.parametrize("similar", ["x{email_address}", "{email_address}.au"], ids=["prefix", "suffix"])
    def test_ignores_email_to_a_similar_address(
        self, client: EmailClient, app: FakeApp, email_address: str, password_reset: EmailSpec, similar: str
    ) -> None:
        """Test that an email to an address that contains the searched one is delivered but not returned for it"""
        similar_address = similar.format(email_address=email_address)
        app.send_email(password_reset.name, similar_address)
        client.wait_for_email(similar_address)

        assert client.find_emails(email_address) == []

    def test_ignores_emails_received_before_the_day(
        self, client: EmailClient, app: FakeApp, email_address: str, password_reset: EmailSpec
    ) -> None:
        """Test that the server does not return an email it received before the day the search starts from"""
        app.send_email(password_reset.name, email_address)

        assert client.find_emails(email_address, since=date.today() + timedelta(days=2)) == []


class TestConnectionNegative:
    """Tests for how `EmailClient` reports connection problems of a real IMAP server"""

    def test_raises_when_the_password_is_wrong(
        self, client_factory: Callable[..., EmailClient], email_address: str
    ) -> None:
        """Test that the server rejecting the login is reported right away"""
        client = client_factory(auth=PasswordAuth("wrong-password"))

        with pytest.raises(EmailClientError, match="^" + re.escape(f"Unable to log in to {HOST} as {USERNAME!r}")):
            client.wait_for_email(email_address)

    def test_raises_when_the_access_token_is_rejected(
        self, client_factory: Callable[..., EmailClient], email_address: str
    ) -> None:
        """Test that an OAuth2 access token the server does not accept is reported right away"""
        client = client_factory(auth=OAuth2Auth("expired-token"))

        with pytest.raises(EmailClientError, match="^" + re.escape(f"Unable to log in to {HOST} as {USERNAME!r}")):
            client.wait_for_email(email_address)

    def test_raises_when_the_server_is_unreachable(
        self, client_factory: Callable[..., EmailClient], email_address: str
    ) -> None:
        """Test that a port nobody listens on is reported right away instead of waiting for the timeout"""
        client = client_factory(port=UNUSED_PORT)

        with pytest.raises(EmailClientError, match="^" + re.escape(f"Unable to connect to {HOST}:{UNUSED_PORT}")):
            client.wait_for_email(email_address)

    def test_rejects_untrusted_certificate_by_default(
        self, client_factory: Callable[..., EmailClient], email_address: str
    ) -> None:
        """Test that a server with a self-signed certificate is refused unless a custom SSL context allows it"""
        client = client_factory(ssl_context=None)

        with pytest.raises(EmailClientError, match="^" + re.escape(f"Unable to connect to {HOST}:")) as exc_info:
            client.wait_for_email(email_address)

        assert isinstance(exc_info.value.__cause__, ssl.SSLCertVerificationError)

    def test_raises_when_the_mailbox_does_not_exist(
        self, client_factory: Callable[..., EmailClient], email_address: str
    ) -> None:
        """Test that a mailbox the server does not have is reported right away with its name"""
        client = client_factory(mailbox="No such folder")

        with pytest.raises(EmailClientError, match=r"^Unable to open mailbox 'No such folder'"):
            client.wait_for_email(email_address)
