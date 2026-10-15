"""Tests for common_libs.clients.email_client.client module"""

import imaplib
import re
import ssl
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from typing import TypeAlias

import pytest

from common_libs.clients.email_client import EmailClient
from common_libs.clients.email_client.client import SOCKET_TIMEOUT
from tests.libs.common.fake_clock import FakeClock
from tests.libs.email_client.addresses import generate_email_address
from tests.libs.email_client.fake_imap import HOST, PASSWORD, USERNAME, FakeIMAPServer, Response

pytestmark = [pytest.mark.unittest]

POLL_INTERVAL = 0.5
ONE_DAY = timedelta(days=1)
Scripts: TypeAlias = list[tuple[str, BaseException | Response | None]]


class TestWaitForEmail:
    """Tests for `EmailClient.wait_for_email()`"""

    def test_returns_email_already_in_mailbox(
        self, client: EmailClient, email_address: str, send_email: Callable[..., int]
    ) -> None:
        """Test that an email sent before waiting started is returned immediately"""
        subject = "Activate your account"
        uid = send_email(email_address, subject=subject)

        email = client.wait_for_email(email_address)

        assert (email.uid, email.subject, email.to) == (uid, subject, (email_address,))

    def test_returns_email_that_arrives_while_waiting(
        self,
        client: EmailClient,
        imap_server: FakeIMAPServer,
        email_address: str,
        send_email_during_wait: Callable[..., None],
    ) -> None:
        """Test that the mailbox is polled until the email arrives"""
        send_email_during_wait(3, email_address)

        email = client.wait_for_email(email_address)

        assert email.to == (email_address,)
        assert imap_server.searches == 3

    def test_ignores_emails_to_other_addresses(
        self, client: EmailClient, imap_server: FakeIMAPServer, email_address: str, send_email: Callable[..., int]
    ) -> None:
        """Test that newer emails to other addresses, such as those of other tests, are neither returned nor fetched"""
        subject = "Mine"
        send_email(email_address, subject=subject)
        send_email(generate_email_address(), subject="Somebody else")

        assert client.wait_for_email(email_address).subject == subject
        assert imap_server.count("UID FETCH") == 1

    @pytest.mark.parametrize("subject", ["Reset", re.compile(r"your password$")], ids=["text", "pattern"])
    def test_selects_email_by_subject(
        self, client: EmailClient, email_address: str, send_email: Callable[..., int], subject: str | re.Pattern[str]
    ) -> None:
        """Test that only an email whose subject contains the text, or in which the pattern is found, is returned"""
        reset_subject = "Reset your password"
        send_email(email_address, subject="Activate your account")
        send_email(email_address, subject=reset_subject)
        send_email(email_address, subject="Welcome")

        assert client.wait_for_email(email_address, subject=subject).subject == reset_subject

    def test_returns_newest_of_several_matches(
        self, client: EmailClient, email_address: str, send_email: Callable[..., int]
    ) -> None:
        """Test that the most recent email wins when several match"""
        subject = "Activate account"
        send_email(email_address, subject=subject)
        uid = send_email(email_address, subject=subject)

        assert client.wait_for_email(email_address, subject="Activate").uid == uid

    def test_waits_for_email_after_the_given_one(
        self,
        client: EmailClient,
        email_address: str,
        send_email: Callable[..., int],
        send_email_during_wait: Callable[..., None],
    ) -> None:
        """Test that an email resent with the same subject is returned instead of the first one"""
        subject = "Activate account"
        send_email(email_address, subject=subject)
        first = client.wait_for_email(email_address)
        send_email_during_wait(3, email_address, subject=subject)

        second = client.wait_for_email(email_address, after=first)

        assert second.uid is not None and first.uid is not None
        assert second.uid > first.uid

    def test_fetches_each_email_only_once(
        self,
        client: EmailClient,
        imap_server: FakeIMAPServer,
        email_address: str,
        send_email: Callable[..., int],
        send_email_during_wait: Callable[..., None],
    ) -> None:
        """Test that an email that does not match is not downloaded again on every poll"""
        subject = "Reset"
        uid = send_email(email_address, subject="Activate account")
        send_email_during_wait(3, email_address, subject=subject)

        client.wait_for_email(email_address, subject=subject)

        assert imap_server.commands.count(f"UID FETCH {uid} (BODY.PEEK[])") == 1

    def test_opens_mailbox_read_only(
        self, client: EmailClient, imap_server: FakeIMAPServer, email_address: str, send_email: Callable[..., int]
    ) -> None:
        """Test that the mailbox is examined and emails are peeked at, so nothing is marked as read"""
        send_email(email_address)

        client.wait_for_email(email_address)

        assert [c for c in imap_server.commands if c.startswith(("SELECT", "EXAMINE"))] == ['EXAMINE "INBOX"']
        assert imap_server.marked_seen == set()

    def test_uses_a_new_connection_for_every_poll(
        self,
        client: EmailClient,
        imap_server: FakeIMAPServer,
        email_address: str,
        send_email_during_wait: Callable[..., None],
    ) -> None:
        """Test that every poll logs in and out on its own, so no connection stays open while waiting"""
        send_email_during_wait(3, email_address)

        client.wait_for_email(email_address)

        session = ["LOGIN", "LOGOUT"]
        assert [c for c in imap_server.commands if c in session] == session * 3
        assert imap_server.count("EXAMINE") == 3

    @pytest.mark.parametrize(
        ("mailbox", "quoted"),
        [
            ("[Gmail]/All Mail", '"[Gmail]/All Mail"'),
            ('Say "hi"', '"Say \\"hi\\""'),
            ("back\\slash", '"back\\\\slash"'),
        ],
        ids=["space", "double-quote", "backslash"],
    )
    def test_quotes_mailbox_name_and_address(
        self,
        client_factory: Callable[..., EmailClient],
        imap_server: FakeIMAPServer,
        email_address: str,
        send_email: Callable[..., int],
        mailbox: str,
        quoted: str,
    ) -> None:
        """Test that a mailbox name with spaces and special characters, and the address, are sent as quoted strings"""
        client = client_factory(mailbox=mailbox)
        send_email(email_address)

        client.wait_for_email(email_address)

        assert f"EXAMINE {quoted}" in imap_server.commands
        assert f'UID SEARCH TO "{email_address}"' in imap_server.commands

    @pytest.mark.parametrize(
        "to", ["Anna <{email_address}>", "other@example.com, {email_address}"], ids=["name", "several"]
    )
    def test_finds_address_among_recipients(
        self, client: EmailClient, email_address: str, send_email: Callable[..., int], to: str
    ) -> None:
        """Test that an email is found when the address comes with a display name or other recipients"""
        send_email(to.format(email_address=email_address))

        assert email_address in client.wait_for_email(email_address).to

    @pytest.mark.parametrize(
        ("sent_to", "wait_for"),
        [(str.upper, str.lower), (str.lower, str.upper)],
        ids=["upper-case-header", "upper-case-query"],
    )
    def test_matches_address_case_insensitively(
        self,
        client: EmailClient,
        email_address: str,
        send_email: Callable[..., int],
        sent_to: Callable[[str], str],
        wait_for: Callable[[str], str],
    ) -> None:
        """Test that the case of the address does not matter, whether it differs in the email or in the query"""
        send_email(sent_to(email_address))

        assert client.wait_for_email(wait_for(email_address)).to == (sent_to(email_address),)

    @pytest.mark.parametrize("to", ["QA+Tag@Example.COM", "first.last@sub.example.co.uk", "o'brien@example.com"])
    def test_finds_email_to_unusual_valid_address(
        self, client: EmailClient, send_email: Callable[..., int], to: str
    ) -> None:
        """Test that valid addresses with upper case letters, subdomains, or an apostrophe are accepted and found"""
        send_email(to)

        assert client.wait_for_email(to).to == (to,)

    def test_ignores_unsolicited_fetch_responses(
        self, client: EmailClient, imap_server: FakeIMAPServer, email_address: str, send_email: Callable[..., int]
    ) -> None:
        """Test that flag updates of other emails, sent by the server along with the response, are ignored"""
        subject = "Activate account"
        imap_server.unsolicited_fetch_items = [b"9 (FLAGS (\\Seen))"]
        send_email(email_address, subject=subject)

        assert client.wait_for_email(email_address).subject == subject

    @pytest.mark.parametrize("command", ["EXAMINE", "SEARCH", "FETCH"])
    @pytest.mark.parametrize(
        "error",
        [imaplib.IMAP4.abort("socket error"), ConnectionResetError(), TimeoutError()],
        ids=["abort", "reset", "timeout"],
    )
    def test_reconnects_once_when_connection_drops(
        self,
        client: EmailClient,
        imap_server: FakeIMAPServer,
        email_address: str,
        send_email: Callable[..., int],
        caplog: pytest.LogCaptureFixture,
        command: str,
        error: Exception,
    ) -> None:
        """Test that a connection that dropped while waiting (e.g. closed by the server) is reopened and logged"""
        send_email(email_address)
        imap_server.script(command, error)

        assert client.wait_for_email(email_address).to == (email_address,)
        assert len(imap_server.connections) == 2
        assert all(connection.logged_out for connection in imap_server.connections)
        assert "Lost the connection to the IMAP server" in caplog.text

    def test_reconnects_every_time_a_poll_drops_the_connection(
        self, client: EmailClient, imap_server: FakeIMAPServer, email_address: str, send_email: Callable[..., int]
    ) -> None:
        """Test that each poll gets its own new attempt, so a long wait survives several dropped connections"""
        send_email(email_address)
        imap_server.script("SEARCH", imaplib.IMAP4.abort("socket error"))
        imap_server.script("SEARCH", ("OK", [b""]))
        imap_server.script("SEARCH", imaplib.IMAP4.abort("socket error"))

        assert client.wait_for_email(email_address).to == (email_address,)
        assert len(imap_server.connections) == 4
        assert all(connection.logged_out for connection in imap_server.connections)

    @pytest.mark.parametrize(
        "error",
        [None, imaplib.IMAP4.abort("socket error: EOF"), ConnectionResetError(), TimeoutError()],
        ids=["answered", "abort", "reset", "timeout"],
    )
    def test_closes_connection_even_when_logout_fails(
        self,
        client: EmailClient,
        imap_server: FakeIMAPServer,
        email_address: str,
        send_email: Callable[..., int],
        error: Exception | None,
    ) -> None:
        """Test that a connection is closed after the poll, including when the server dropped it before the logout"""
        send_email(email_address)
        imap_server.script("LOGOUT", error)

        assert client.wait_for_email(email_address).to == (email_address,)
        assert len(imap_server.connections) == 1
        assert all(connection.closed for connection in imap_server.connections)

    def test_keeps_fetched_emails_across_reconnects(
        self,
        client: EmailClient,
        imap_server: FakeIMAPServer,
        email_address: str,
        send_email: Callable[..., int],
        send_email_during_wait: Callable[..., None],
    ) -> None:
        """Test that an email that does not match is not downloaded again after the connection was reopened"""
        subject = "Reset"
        uid = send_email(email_address, subject="Activate account")
        send_email_during_wait(3, email_address, subject=subject)
        imap_server.script("SEARCH", ("OK", [str(uid).encode()]))
        imap_server.script("SEARCH", imaplib.IMAP4.abort("socket error"))

        email = client.wait_for_email(email_address, subject=subject)

        assert email.subject == subject
        assert len(imap_server.connections) == 3
        assert imap_server.commands.count(f"UID FETCH {uid} (BODY.PEEK[])") == 1

    @pytest.mark.parametrize(
        ("scripts", "logged"),
        [
            ([("CONNECT", None), ("CONNECT", ConnectionRefusedError("Connection refused"))], "Connection refused"),
            (
                [("LOGIN", None), ("LOGIN", imaplib.IMAP4.error("[ALERT] Too many simultaneous connections"))],
                "Too many simultaneous connections",
            ),
            ([("EXAMINE", None), ("EXAMINE", ("NO", [b"Temporary failure"]))], "Temporary failure"),
            ([("SEARCH", None), ("SEARCH", ("NO", [b"Temporary failure"]))], "Temporary failure"),
            ([("SEARCH", None), ("SEARCH", imaplib.IMAP4.error("BAD Command unknown"))], "IMAP error: BAD Command"),
            (
                [
                    ("SEARCH", None),
                    ("SEARCH", imaplib.IMAP4.abort("socket error")),
                    ("SEARCH", imaplib.IMAP4.abort("socket error")),
                ],
                "Lost the connection to the IMAP server",
            ),
            ([("FETCH", ("NO", [b"Some messages could not be FETCHed (Failure)"]))], "could not be FETCHed"),
        ],
        ids=["connect", "login", "examine", "search", "search-bad", "connection-dropped-twice", "fetch"],
    )
    def test_keeps_waiting_when_a_later_poll_fails(
        self,
        client_factory: Callable[..., EmailClient],
        imap_server: FakeIMAPServer,
        fake_clock: FakeClock,
        email_address: str,
        send_email_during_wait: Callable[..., None],
        caplog: pytest.LogCaptureFixture,
        scripts: Scripts,
        logged: str,
    ) -> None:
        """Test that a poll that fails after the mailbox was reached is logged and repeated at the next interval"""
        send_email_during_wait(2, email_address)
        for command, outcome in scripts:
            imap_server.script(command, outcome)

        email = client_factory(poll_interval=POLL_INTERVAL).wait_for_email(email_address)

        assert email.to == (email_address,)
        assert fake_clock.sleeps == [POLL_INTERVAL, POLL_INTERVAL]
        assert logged in caplog.text

    @pytest.mark.parametrize("method", ["wait_for_email", "find_emails"])
    def test_retries_the_first_poll_of_a_later_wait(
        self,
        client_factory: Callable[..., EmailClient],
        imap_server: FakeIMAPServer,
        fake_clock: FakeClock,
        email_address: str,
        send_email: Callable[..., int],
        method: str,
    ) -> None:
        """Test that once a client has reached the mailbox, a failing first poll of another wait is repeated"""
        client = client_factory(poll_interval=POLL_INTERVAL)
        send_email(email_address)
        getattr(client, method)(email_address)
        other_address = generate_email_address()
        send_email(other_address)
        imap_server.script("CONNECT", ConnectionRefusedError("Connection refused"))

        assert client.wait_for_email(other_address).to == (other_address,)
        assert imap_server.connection_attempts == 3
        assert fake_clock.sleeps == [POLL_INTERVAL]

    def test_fetches_again_when_the_server_returned_no_body(
        self,
        client: EmailClient,
        imap_server: FakeIMAPServer,
        fake_clock: FakeClock,
        email_address: str,
        send_email: Callable[..., int],
    ) -> None:
        """Test that an email whose body the server did not return is fetched again instead of being forgotten"""
        subject = "Activate account"
        send_email(email_address, subject=subject)
        imap_server.script("FETCH", ("OK", [None]))

        assert client.wait_for_email(email_address).subject == subject
        assert imap_server.count("UID FETCH") == 2

    def test_can_be_shared_by_threads(self, client: EmailClient, send_email: Callable[..., int]) -> None:
        """Test that parallel waits in one client each get the email for their own address"""
        addresses = [generate_email_address() for _ in range(8)]
        for address in addresses:
            send_email(address)

        with ThreadPoolExecutor(max_workers=len(addresses)) as pool:
            emails = list(pool.map(client.wait_for_email, addresses))

        assert [email.to for email in emails] == [(address,) for address in addresses]


class TestFindEmails:
    """Tests for `EmailClient.find_emails()`"""

    def test_returns_all_emails_oldest_first(
        self, client: EmailClient, email_address: str, send_email: Callable[..., int]
    ) -> None:
        """Test that every email to the address is returned in the order of arrival"""
        subjects = ["First", "Second", "Third"]
        for subject in subjects:
            send_email(email_address, subject=subject)

        assert [email.subject for email in client.find_emails(email_address)] == subjects

    def test_returns_oldest_first_when_server_lists_uids_out_of_order(
        self, client: EmailClient, imap_server: FakeIMAPServer, email_address: str, send_email: Callable[..., int]
    ) -> None:
        """Test that the order of arrival does not depend on the order of the UIDs in the search response"""
        subjects = ["First", "Second"]
        uids = [send_email(email_address, subject=subject) for subject in subjects]
        imap_server.script("SEARCH", ("OK", [" ".join(map(str, reversed(uids))).encode()]))

        assert [email.subject for email in client.find_emails(email_address)] == subjects

    @pytest.mark.parametrize("subject", ["Activate", re.compile("^Activate")], ids=["text", "pattern"])
    def test_filters_by_subject(
        self, client: EmailClient, email_address: str, send_email: Callable[..., int], subject: str | re.Pattern[str]
    ) -> None:
        """Test that only emails whose subject contains the text, or in which the pattern is found, are returned"""
        first_match, second_match = "Activate account", "Activate again"
        for text in (first_match, "Welcome", second_match):
            send_email(email_address, subject=text)

        emails = client.find_emails(email_address, subject=subject)

        assert [email.subject for email in emails] == [first_match, second_match]

    def test_returns_only_emails_after_the_given_one(
        self, client: EmailClient, email_address: str, send_email: Callable[..., int]
    ) -> None:
        """Test that emails up to and including the given one are skipped"""
        subject = "Second"
        send_email(email_address, subject="First")
        first = client.find_emails(email_address)[0]
        send_email(email_address, subject=subject)

        assert [email.subject for email in client.find_emails(email_address, after=first)] == [subject]

    def test_returns_empty_list_without_waiting(
        self, client: EmailClient, imap_server: FakeIMAPServer, email_address: str
    ) -> None:
        """Test that the mailbox is checked just once when there is no email"""
        assert client.find_emails(email_address) == []
        assert imap_server.searches == 1

    def test_returns_only_emails_received_since_the_day(
        self, client: EmailClient, imap_server: FakeIMAPServer, email_address: str, send_email: Callable[..., int]
    ) -> None:
        """Test that emails the server received before the day are neither returned nor downloaded"""
        since = date(2026, 10, 14)
        for subject, day in [("Before", since - ONE_DAY), ("Same day", since), ("After", since + ONE_DAY)]:
            send_email(email_address, subject=subject, internal_date=day)

        emails = client.find_emails(email_address, since=since)

        assert [email.subject for email in emails] == ["Same day", "After"]
        assert imap_server.count("UID FETCH") == 2

    def test_combines_the_day_with_the_subject_and_the_previous_email(
        self, client: EmailClient, email_address: str, send_email: Callable[..., int]
    ) -> None:
        """Test that an email must satisfy the day, the subject, and the `after` email all at once"""
        since = date(2026, 10, 14)
        send_email(email_address, subject="Reset 1", internal_date=since)
        send_email(email_address, subject="Reset too old", internal_date=since - ONE_DAY)
        send_email(email_address, subject="Welcome", internal_date=since)
        send_email(email_address, subject="Reset 2", internal_date=since)
        first = client.find_emails(email_address, subject="Reset 1")[0]

        emails = client.find_emails(email_address, subject="Reset", after=first, since=since)

        assert [email.subject for email in emails] == ["Reset 2"]


@pytest.mark.parametrize("method", ["wait_for_email", "find_emails"])
class TestSince:
    """Tests for the `since` option of `EmailClient.wait_for_email()` and `EmailClient.find_emails()`"""

    @pytest.mark.parametrize(
        ("since", "expected"),
        [
            (date(2026, 10, 1), "1-Oct-2026"),
            (date(2026, 12, 25), "25-Dec-2026"),
            (datetime(2026, 3, 9, 23, 59, tzinfo=UTC), "9-Mar-2026"),
        ],
        ids=["single-digit-day", "double-digit-day", "datetime"],
    )
    def test_searches_with_the_day_in_the_format_imap_requires(
        self,
        client: EmailClient,
        imap_server: FakeIMAPServer,
        email_address: str,
        send_email: Callable[..., int],
        method: str,
        since: date,
        expected: str,
    ) -> None:
        """Test that the day is sent as day-month-year with an English month abbreviation and no zero padding"""
        send_email(email_address, internal_date=date(since.year, since.month, since.day))

        getattr(client, method)(email_address, since=since)

        assert f'UID SEARCH TO "{email_address}" SINCE {expected}' in imap_server.commands


class TestConnection:
    """Tests for how `EmailClient` connects to the server"""

    @pytest.mark.parametrize(
        ("use_ssl", "port", "expected"),
        [
            (True, None, 993),
            (False, None, 143),
            (True, 1993, 1993),
            (False, 1143, 1143),
            (True, 1, 1),
            (False, 65535, 65535),
        ],
        ids=["imaps", "imap", "custom-imaps", "custom-imap", "lowest-port", "highest-port"],
    )
    def test_connects_to_the_port(
        self,
        client_factory: Callable[..., EmailClient],
        imap_server: FakeIMAPServer,
        use_ssl: bool,
        port: int | None,
        expected: int,
    ) -> None:
        """Test that the standard port of the protocol is used unless a port is given"""
        client_factory(use_ssl=use_ssl, port=port).find_emails(USERNAME)

        connection = imap_server.connections[0]
        assert (connection.host, connection.port, connection.secure) == (HOST, expected, use_ssl)

    def test_logs_in_with_the_credentials(self, client: EmailClient, imap_server: FakeIMAPServer) -> None:
        """Test that the username and password are sent to the server"""
        client.find_emails(USERNAME)

        assert imap_server.connections[0].credentials == (USERNAME, PASSWORD)

    def test_verifies_the_server_certificate_by_default(self, client: EmailClient, imap_server: FakeIMAPServer) -> None:
        """Test that SSL connections verify the certificate and host name unless told otherwise"""
        client.find_emails(USERNAME)

        context = imap_server.connections[0].ssl_context
        assert context is not None
        assert (context.verify_mode, context.check_hostname) == (ssl.CERT_REQUIRED, True)

    def test_uses_the_given_ssl_context(
        self, client_factory: Callable[..., EmailClient], imap_server: FakeIMAPServer
    ) -> None:
        """Test that a custom SSL context is used as is"""
        context = ssl.create_default_context()

        client_factory(ssl_context=context).find_emails(USERNAME)

        assert imap_server.connections[0].ssl_context is context

    def test_sets_a_socket_timeout(self, client: EmailClient, imap_server: FakeIMAPServer) -> None:
        """Test that a server that stops responding cannot block a test forever"""
        client.find_emails(USERNAME)

        assert imap_server.connections[0].timeout == SOCKET_TIMEOUT

    def test_closes_connection_after_every_call(self, client: EmailClient, imap_server: FakeIMAPServer) -> None:
        """Test that no connection is kept open between calls"""
        client.find_emails(USERNAME)
        client.find_emails(USERNAME)

        assert len(imap_server.connections) == 2
        assert all(connection.logged_out for connection in imap_server.connections)
