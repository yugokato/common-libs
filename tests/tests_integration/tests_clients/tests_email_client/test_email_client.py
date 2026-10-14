"""Integration tests for common_libs.clients.email_client.client module"""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

import pytest

from common_libs.clients.email_client import EmailClient, OAuth2Auth
from tests.libs.email_client.addresses import generate_email_address
from tests.libs.email_client.emails import EmailSpec, parametrize_email_specs
from tests.libs.email_client.fake_app import FakeApp
from tests.libs.email_client.mail_server import ACCESS_TOKEN, MailServerContainer

pytestmark = [pytest.mark.integrationtest, pytest.mark.xdist_group(__name__)]

SEND_DELAY = 0.2
ONE_DAY = timedelta(days=1)


class TestWaitForEmail:
    """Tests for `EmailClient.wait_for_email()` against a real IMAP server"""

    @pytest.mark.parametrize("delay", [0, SEND_DELAY], ids=["sent-before-waiting", "sent-while-waiting"])
    @parametrize_email_specs
    def test_returns_link_of_the_main_button(
        self, client: EmailClient, app: FakeApp, email_address: str, spec: EmailSpec, delay: float
    ) -> None:
        """Test that the main button link is returned whether the email was sent before or after waiting started"""
        app.send_email(spec.name, email_address, delay=delay)

        email = client.wait_for_email(email_address, subject=spec.subject)

        assert email.get_link(spec.button_text) == spec.url

    def test_ignores_emails_to_other_addresses(
        self, client: EmailClient, app: FakeApp, email_address: str, password_reset: EmailSpec
    ) -> None:
        """Test that a newer email to another address is not mistaken for the one waited for"""
        app.send_email(password_reset.name, email_address)
        app.send_email(password_reset.name, generate_email_address())

        email = client.wait_for_email(email_address)

        assert (email.to, email.get_link(password_reset.button_text)) == ((email_address,), password_reset.url)

    @pytest.mark.parametrize(
        ("sent_to", "wait_for"),
        [(str.upper, str.lower), (str.lower, str.upper)],
        ids=["upper-case-header", "upper-case-query"],
    )
    def test_matches_address_case_insensitively(
        self,
        client: EmailClient,
        app: FakeApp,
        email_address: str,
        password_reset: EmailSpec,
        sent_to: Callable[[str], str],
        wait_for: Callable[[str], str],
    ) -> None:
        """Test that the server finds the email whether the case differs in the header or in the query"""
        app.send_email(password_reset.name, sent_to(email_address))

        assert client.wait_for_email(wait_for(email_address)).to == (sent_to(email_address),)

    def test_returns_newest_email_when_several_match(
        self,
        client: EmailClient,
        app: FakeApp,
        email_address: str,
        password_reset: EmailSpec,
        workspace_invitation: EmailSpec,
    ) -> None:
        """Test that the most recent of several emails to the address is returned"""
        app.send_email(password_reset.name, email_address)
        app.send_email(workspace_invitation.name, email_address)

        assert client.wait_for_email(email_address).subject == workspace_invitation.subject

    def test_waits_for_resent_email(
        self, client: EmailClient, app: FakeApp, email_address: str, password_reset: EmailSpec
    ) -> None:
        """Test that an email sent again is returned when waiting for an email after the first one"""
        app.send_email(password_reset.name, email_address)
        first = client.wait_for_email(email_address)
        app.send_email(password_reset.name, email_address, delay=SEND_DELAY)

        second = client.wait_for_email(email_address, after=first)

        assert second.uid is not None and first.uid is not None
        assert second.uid > first.uid

    @pytest.mark.parametrize("client_per_machine", [False, True], ids=["one-client", "client-per-machine"])
    def test_serves_concurrent_waits(
        self,
        client: EmailClient,
        client_factory: Callable[..., EmailClient],
        app: FakeApp,
        password_reset: EmailSpec,
        workspace_invitation: EmailSpec,
        client_per_machine: bool,
    ) -> None:
        """Test that concurrent waits each get the email for their own address, whether they share one client or have
        one each (as the same test running on different machines does), even when the emails arrive in the reverse
        order of the waits
        """
        specs = [password_reset, workspace_invitation] * 4
        cases = [
            (generate_email_address("signup"), spec, 0.05 * (len(specs) - index)) for index, spec in enumerate(specs)
        ]

        def wait_for_email(email_address: str, spec: EmailSpec, delay: float) -> tuple[tuple[str, ...], str]:
            app.send_email(spec.name, email_address, delay=delay)
            email = (client_factory() if client_per_machine else client).wait_for_email(email_address)
            return email.to, email.get_link(spec.button_text)

        with ThreadPoolExecutor(max_workers=len(cases)) as pool:
            results = list(pool.map(lambda case: wait_for_email(*case), cases))

        assert results == [((address,), spec.url) for address, spec, _ in cases]

    def test_returns_email_received_since_a_day_ago(
        self, client: EmailClient, app: FakeApp, email_address: str, password_reset: EmailSpec
    ) -> None:
        """Test that the server finds an email it received today when the search starts from the day before"""
        app.send_email(password_reset.name, email_address)

        email = client.wait_for_email(email_address, since=date.today() - ONE_DAY)

        assert email.subject == password_reset.subject

    def test_leaves_emails_unread(
        self,
        client: EmailClient,
        app: FakeApp,
        mail_server: MailServerContainer,
        email_address: str,
        password_reset: EmailSpec,
    ) -> None:
        """Test that reading an email does not mark it as read for other users of the mailbox"""
        app.send_email(password_reset.name, email_address)
        email = client.wait_for_email(email_address)
        assert email.uid is not None

        assert b"\\Seen" not in mail_server.get_flags(email.uid)


class TestOAuth2:
    """Tests for logging in to a real IMAP server with an OAuth2 access token"""

    @pytest.mark.parametrize("token", [ACCESS_TOKEN, lambda: ACCESS_TOKEN], ids=["string", "function"])
    def test_logs_in_with_the_access_token(
        self,
        client_factory: Callable[..., EmailClient],
        app: FakeApp,
        email_address: str,
        password_reset: EmailSpec,
        token: str | Callable[[], str],
    ) -> None:
        """Test that the server accepts the access token, whether it is given directly or by a function"""
        app.send_email(password_reset.name, email_address)
        client = client_factory(auth=OAuth2Auth(token))

        assert client.wait_for_email(email_address).get_link(password_reset.button_text) == password_reset.url


class TestFindEmails:
    """Tests for `EmailClient.find_emails()` against a real IMAP server"""

    def test_returns_emails_of_the_address_oldest_first(
        self,
        client: EmailClient,
        app: FakeApp,
        email_address: str,
        password_reset: EmailSpec,
        workspace_invitation: EmailSpec,
    ) -> None:
        """Test that only the emails to the address are returned, in the order they were sent"""
        app.send_email(password_reset.name, email_address)
        app.send_email(password_reset.name, generate_email_address())
        app.send_email(workspace_invitation.name, email_address)

        subjects = [email.subject for email in client.find_emails(email_address)]

        assert subjects == [password_reset.subject, workspace_invitation.subject]

    def test_returns_nothing_for_unused_address(self, client: EmailClient, email_address: str) -> None:
        """Test that an address that received nothing has no emails, even though the mailbox has many"""
        assert client.find_emails(email_address) == []
