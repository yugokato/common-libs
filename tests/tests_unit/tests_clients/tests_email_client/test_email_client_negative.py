"""Negative tests for common_libs.clients.email_client.client module"""

import imaplib
import re
import socket
import ssl
from collections.abc import Callable
from datetime import date, timedelta
from typing import Any

import pytest

from common_libs.clients.email_client import Email, EmailClient, EmailClientError, EmailNotFoundError, PasswordAuth
from tests.libs.email_client.emails import UNPARSABLE_HEADERS, build_email_with_unparsable_header
from tests.libs.email_client.fake_clock import FakeClock
from tests.libs.email_client.fake_imap import HOST, PASSWORD, USERNAME, FakeIMAPServer, Response

pytestmark = [pytest.mark.unittest]

INVALID_ADDRESSES = [
    pytest.param("", id="empty"),
    pytest.param("qa", id="no-at-sign"),
    pytest.param("@example.com", id="no-local-part"),
    pytest.param("qa@", id="no-domain"),
    pytest.param("qa@@example.com", id="two-at-signs"),
    pytest.param("qa @example.com", id="space-inside"),
    pytest.param(" qa@example.com", id="leading-space"),
    pytest.param("qa@example.com\n", id="trailing-line-break"),
    pytest.param('qa"@example.com', id="double-quote"),
    pytest.param("q\\a@example.com", id="backslash"),
    pytest.param("qa@example.com\r\nBcc: x@y", id="header-injection"),
]
NON_ASCII_ADDRESS = "tëst@example.com"
NON_ASCII_ADDRESS_MESSAGE = f"Non-ASCII email addresses are not supported: {NON_ASCII_ADDRESS!r}"
NOT_FINITE = [float("nan"), float("inf"), float("-inf")]
INVALID_TIMEOUTS = [-1, *NOT_FINITE]
INVALID_POLL_INTERVALS = [0, -0.5, *NOT_FINITE]


class TestEmailClientInit:
    """Tests for `EmailClient()` with invalid arguments"""

    def test_rejects_positional_arguments(self) -> None:
        """Test that all the parameters are keyword-only"""
        with pytest.raises(TypeError, match="positional"):
            EmailClient(HOST, USERNAME, PASSWORD)

    @pytest.mark.parametrize(
        ("name", "value"),
        [("timeout", value) for value in INVALID_TIMEOUTS]
        + [("poll_interval", value) for value in INVALID_POLL_INTERVALS],
        ids=str,
    )
    def test_rejects_invalid_durations(self, name: str, value: float) -> None:
        """Test that a timeout that is negative or not finite, and a poll interval that is zero, negative, or not
        finite, are rejected
        """
        with pytest.raises(ValueError, match=f"{name} must"):
            EmailClient(host=HOST, username=USERNAME, auth=PasswordAuth(PASSWORD), **{name: value})

    def test_rejects_non_ascii_mailbox(self) -> None:
        """Test that a mailbox name that cannot be sent to the server is rejected right away"""
        with pytest.raises(ValueError, match="mailbox must be ASCII"):
            EmailClient(host=HOST, username=USERNAME, auth=PasswordAuth(PASSWORD), mailbox="すべてのメール")

    @pytest.mark.parametrize("port", [0, -1, 65536])
    def test_rejects_port_out_of_range(self, port: int) -> None:
        """Test that a port that no server can listen on is rejected instead of being replaced or mistaken for a host"""
        with pytest.raises(ValueError, match="port must be between 1 and 65535"):
            EmailClient(host=HOST, username=USERNAME, auth=PasswordAuth(PASSWORD), port=port)


@pytest.mark.parametrize("method", ["wait_for_email", "find_emails"])
class TestInputValidation:
    """Tests for `EmailClient.wait_for_email()` and `find_emails()` with invalid input"""

    @pytest.mark.parametrize("to", INVALID_ADDRESSES)
    def test_rejects_invalid_address(
        self, client: EmailClient, imap_server: FakeIMAPServer, method: str, to: str
    ) -> None:
        """Test that an invalid address is rejected before connecting to the server"""
        with pytest.raises(ValueError, match="Not a valid email address"):
            getattr(client, method)(to)

        assert imap_server.connection_attempts == 0

    def test_rejects_non_ascii_address(self, client: EmailClient, imap_server: FakeIMAPServer, method: str) -> None:
        """Test that a non-ASCII address is rejected as unsupported before connecting to the server"""
        with pytest.raises(ValueError, match=re.escape(NON_ASCII_ADDRESS_MESSAGE)):
            getattr(client, method)(NON_ASCII_ADDRESS)

        assert imap_server.connection_attempts == 0

    @pytest.mark.parametrize("subject", ["", " ", "\t\n"], ids=["empty", "space", "tab-and-line-break"])
    def test_rejects_blank_subject(
        self, client: EmailClient, imap_server: FakeIMAPServer, email_address: str, method: str, subject: str
    ) -> None:
        """Test that a blank subject, which would match every email, is rejected before connecting to the server"""
        with pytest.raises(ValueError, match="subject must not be blank"):
            getattr(client, method)(email_address, subject=subject)

        assert imap_server.connection_attempts == 0

    @pytest.mark.parametrize("since", ["2026-10-14", 20261014, "today"], ids=["iso-string", "number", "word"])
    def test_rejects_since_that_is_not_a_date(
        self, client: EmailClient, imap_server: FakeIMAPServer, email_address: str, method: str, since: Any
    ) -> None:
        """Test that a `since` that is not a date is rejected before connecting to the server"""
        with pytest.raises(TypeError, match="since must be a date"):
            getattr(client, method)(email_address, since=since)

        assert imap_server.connection_attempts == 0

    def test_rejects_after_email_without_uid(
        self,
        client: EmailClient,
        imap_server: FakeIMAPServer,
        email_address: str,
        raw_email_factory: Callable[..., bytes],
        method: str,
    ) -> None:
        """Test that `after` needs an email with a UID, and is checked before connecting to the server"""
        parsed = Email.from_bytes(raw_email_factory(to=email_address))

        with pytest.raises(ValueError, match="`after` must be an email returned by this client"):
            getattr(client, method)(email_address, after=parsed)

        assert imap_server.connection_attempts == 0


class TestWaitForEmailNegative:
    """Tests for `EmailClient.wait_for_email()` that do not return an email"""

    @pytest.mark.parametrize(
        ("client_timeout", "call_timeout", "described"),
        [(60.0, 5, "5s"), (7.0, None, "7s"), (7.0, 0, "0s")],
        ids=["given", "default", "zero"],
    )
    def test_raises_when_no_email_arrives(
        self,
        client_factory: Callable[..., EmailClient],
        imap_server: FakeIMAPServer,
        fake_clock: FakeClock,
        email_address: str,
        client_timeout: float,
        call_timeout: float | None,
        described: str,
    ) -> None:
        """Test that the wait ends with an error that names the address and the timeout that was in effect"""
        client = client_factory(timeout=client_timeout, poll_interval=1.0)

        with pytest.raises(EmailNotFoundError) as exc_info:
            client.wait_for_email(email_address, timeout=call_timeout)

        assert str(exc_info.value) == f"No email to {email_address!r} arrived within {described}"

    @pytest.mark.parametrize(
        ("timeout", "poll_interval", "sleeps"),
        [(10, 3, [3, 3, 3, 1]), (6, 3, [3, 3]), (2, 5, [2]), (0, 1, [])],
        ids=["last-sleep-capped", "exact-multiple", "interval-longer-than-timeout", "zero-timeout"],
    )
    def test_polls_until_the_timeout_without_overshooting(
        self,
        client_factory: Callable[..., EmailClient],
        imap_server: FakeIMAPServer,
        fake_clock: FakeClock,
        email_address: str,
        timeout: int,
        poll_interval: int,
        sleeps: list[int],
    ) -> None:
        """Test that the mailbox is checked every poll interval, and the last sleep ends exactly at the timeout"""
        client = client_factory(poll_interval=poll_interval)

        with pytest.raises(EmailNotFoundError):
            client.wait_for_email(email_address, timeout=timeout)

        assert fake_clock.sleeps == sleeps
        assert imap_server.searches == len(sleeps) + 1

    @pytest.mark.parametrize(
        ("subject", "described"),
        [("Reset", "'Reset'"), (re.compile(r"^Reset \w+"), "'^Reset \\\\w+'")],
        ids=["text", "pattern"],
    )
    def test_error_describes_the_subject(
        self, client: EmailClient, email_address: str, subject: str | re.Pattern[str], described: str
    ) -> None:
        """Test that the error tells which subject was being waited for"""
        with pytest.raises(EmailNotFoundError, match=f"with a subject matching {re.escape(described)}"):
            client.wait_for_email(email_address, subject=subject, timeout=0)

    def test_error_names_the_day_and_older_emails_are_not_downloaded(
        self, client: EmailClient, imap_server: FakeIMAPServer, email_address: str, send_email: Callable[..., int]
    ) -> None:
        """Test that an email received before the day is not downloaded, and the error says which day was searched"""
        since = date(2026, 10, 14)
        send_email(email_address, internal_date=since - timedelta(days=1))

        with pytest.raises(EmailNotFoundError) as exc_info:
            client.wait_for_email(email_address, since=since, timeout=0)

        assert str(exc_info.value) == f"No email to {email_address!r} received since 14-Oct-2026 arrived within 0s"
        assert imap_server.count("UID FETCH") == 0

    def test_error_lists_subjects_of_emails_that_did_not_match(
        self, client: EmailClient, email_address: str, send_email: Callable[..., int]
    ) -> None:
        """Test that the error shows what was received instead, to spot a wrong subject or the wrong email"""
        send_email(email_address, subject="Activate account")
        send_email(email_address, subject="Welcome")

        with pytest.raises(EmailNotFoundError) as exc_info:
            client.wait_for_email(email_address, subject="Reset", timeout=0)

        assert str(exc_info.value) == (
            f"No email to {email_address!r} with a subject matching 'Reset' arrived within 0s. "
            "Other emails to this address have the subjects: 'Activate account', 'Welcome'"
        )

    def test_ignores_emails_up_to_the_given_one(
        self, client: EmailClient, email_address: str, send_email: Callable[..., int]
    ) -> None:
        """Test that an email that was already returned is not returned again when waiting for a later one"""
        send_email(email_address)
        first = client.wait_for_email(email_address)

        with pytest.raises(EmailNotFoundError):
            client.wait_for_email(email_address, after=first, timeout=0)

    @pytest.mark.parametrize(
        ("to", "headers"),
        [
            ("x{email_address}", {}),
            ("{email_address}.au", {}),
            ("Somebody <x{email_address}>", {}),
            ("someone@example.com", {"Cc": "{email_address}"}),
        ],
        ids=["prefix", "suffix", "display-name-and-prefix", "cc-only"],
    )
    def test_ignores_server_matches_without_the_exact_recipient(
        self,
        client_factory: Callable[..., EmailClient],
        imap_server: FakeIMAPServer,
        fake_clock: FakeClock,
        email_address: str,
        send_email: Callable[..., int],
        to: str,
        headers: dict[str, str],
    ) -> None:
        """Test that an email a lenient server finds (by substring or in Cc) is dropped when the address is not in To,
        and is not downloaded again at the next poll
        """
        uid = send_email(
            to.format(email_address=email_address),
            **{name: value.format(email_address=email_address) for name, value in headers.items()},
        )
        imap_server.script("SEARCH", ("OK", [str(uid).encode()]), times=2)

        with pytest.raises(EmailNotFoundError):
            client_factory(poll_interval=1).wait_for_email(email_address, timeout=1)

        assert (imap_server.searches, imap_server.count("UID FETCH")) == (2, 1)

    @pytest.mark.parametrize(
        ("command", "data"),
        [("SEARCH", [None]), ("SEARCH", [b""]), ("FETCH", [None])],
        ids=["search-none", "search-empty", "email-deleted"],
    )
    def test_treats_empty_server_response_as_no_email(
        self,
        client: EmailClient,
        imap_server: FakeIMAPServer,
        email_address: str,
        send_email: Callable[..., int],
        command: str,
        data: list[Any],
    ) -> None:
        """Test that a search without any UID, or an email that is gone when fetched, does not raise an IMAP error"""
        send_email(email_address)
        imap_server.script(command, ("OK", data))

        with pytest.raises(EmailNotFoundError):
            client.wait_for_email(email_address, timeout=0)

    @pytest.mark.parametrize("timeout", INVALID_TIMEOUTS, ids=str)
    def test_rejects_invalid_timeout(
        self, client: EmailClient, fake_clock: FakeClock, email_address: str, timeout: float
    ) -> None:
        """Test that a timeout that is negative or not finite is rejected instead of waiting without end"""
        with pytest.raises(ValueError, match="timeout must"):
            client.wait_for_email(email_address, timeout=timeout)


class TestServerErrors:
    """Tests for how `EmailClient` reports IMAP server and connection problems"""

    @pytest.mark.parametrize("method", ["wait_for_email", "find_emails"])
    @pytest.mark.parametrize(
        ("command", "outcome", "message"),
        [
            ("EXAMINE", ("NO", [b"Mailbox does not exist"]), "Unable to open mailbox 'INBOX': Mailbox does not exist"),
            ("SEARCH", ("NO", [b"Search is not available"]), "Unable to search the mailbox: Search is not available"),
            ("FETCH", ("NO", [b"Message is gone"]), "Unable to fetch email 1: Message is gone"),
            ("SEARCH", imaplib.IMAP4.error("BAD Command unknown"), "IMAP error: BAD Command unknown"),
        ],
        ids=["examine-no", "search-no", "fetch-no", "search-bad"],
    )
    def test_raises_when_server_rejects_a_command(
        self,
        client: EmailClient,
        imap_server: FakeIMAPServer,
        email_address: str,
        send_email: Callable[..., int],
        method: str,
        command: str,
        outcome: Response | Exception,
        message: str,
    ) -> None:
        """Test that a command the server answers with NO or BAD is reported with the server's reason, and that the
        connection is closed
        """
        send_email(email_address)
        imap_server.script(command, outcome)

        with pytest.raises(EmailClientError) as exc_info:
            getattr(client, method)(email_address)

        assert str(exc_info.value) == message
        assert [connection.logged_out for connection in imap_server.connections] == [True]

    @pytest.mark.parametrize(
        ("password", "error", "detail"),
        [
            ("wrong-password", None, "AUTHENTICATIONFAILED"),
            ("pässword", UnicodeEncodeError("ascii", "pässword", 1, 2, "ordinal not in range(128)"), "ordinal"),
            (PASSWORD, ConnectionResetError("Connection reset by peer"), "Connection reset by peer"),
            (PASSWORD, TimeoutError("timed out"), "timed out"),
        ],
        ids=["rejected", "not-ascii", "reset", "timeout"],
    )
    def test_raises_when_login_fails(
        self,
        client_factory: Callable[..., EmailClient],
        imap_server: FakeIMAPServer,
        email_address: str,
        password: str,
        error: Exception | None,
        detail: str,
    ) -> None:
        """Test that a login that is rejected or cannot be completed is reported, and the connection is closed"""
        imap_server.script("LOGIN", error)
        client = client_factory(auth=PasswordAuth(password))

        with pytest.raises(EmailClientError, match=f"Unable to log in to {HOST} as {USERNAME!r}.*{detail}"):
            client.wait_for_email(email_address)

        assert imap_server.connection_attempts == 1
        assert imap_server.connections[0].logged_out

    @pytest.mark.parametrize(
        "error",
        [
            ConnectionRefusedError("Connection refused"),
            imaplib.IMAP4.abort("socket error: EOF"),
            imaplib.IMAP4.error("* BYE Too many connections"),
            socket.gaierror(socket.EAI_NONAME, "Name or service not known"),
            ssl.SSLCertVerificationError("certificate verify failed: self-signed certificate"),
            TimeoutError("timed out"),
        ],
        ids=["refused", "closed", "bye", "dns", "certificate", "timeout"],
    )
    def test_raises_when_connecting_fails(
        self, client: EmailClient, imap_server: FakeIMAPServer, email_address: str, error: Exception
    ) -> None:
        """Test that a connection the server refuses, closes, or rejects is reported right away, not retried"""
        imap_server.script("CONNECT", error)

        with pytest.raises(EmailClientError, match=re.escape(f"Unable to connect to {HOST}:993: {error}")):
            client.wait_for_email(email_address)

        assert imap_server.connection_attempts == 1

    def test_raises_when_reconnecting_fails(
        self, client: EmailClient, imap_server: FakeIMAPServer, email_address: str
    ) -> None:
        """Test that a server that cannot be connected to again after the connection dropped is reported"""
        imap_server.script("SEARCH", imaplib.IMAP4.abort("socket error"))
        imap_server.script("CONNECT", None)
        imap_server.script("CONNECT", imaplib.IMAP4.abort("socket error: EOF"))

        with pytest.raises(EmailClientError, match=f"Unable to connect to {HOST}:993"):
            client.wait_for_email(email_address)

        assert imap_server.connection_attempts == 2

    def test_raises_when_connection_drops_again_after_reconnecting(
        self, client: EmailClient, imap_server: FakeIMAPServer, email_address: str
    ) -> None:
        """Test that a connection that keeps failing is reported after one new attempt"""
        imap_server.script("SEARCH", imaplib.IMAP4.abort("socket error"), times=2)

        with pytest.raises(EmailClientError, match="Lost the connection to the IMAP server: socket error"):
            client.wait_for_email(email_address)

        assert imap_server.connection_attempts == 2

    def test_raises_the_last_error_when_polls_keep_failing_until_the_timeout(
        self,
        client_factory: Callable[..., EmailClient],
        imap_server: FakeIMAPServer,
        fake_clock: FakeClock,
        email_address: str,
    ) -> None:
        """Test that a mailbox that could not be checked when the wait ended is reported, not that no email arrived"""
        client = client_factory(timeout=3, poll_interval=1)
        imap_server.script("CONNECT", None)
        imap_server.script("CONNECT", ConnectionRefusedError("Connection refused"), times=3)

        with pytest.raises(EmailClientError, match="Unable to check the mailbox when the 3s wait ended") as exc_info:
            client.wait_for_email(email_address)

        assert not isinstance(exc_info.value, EmailNotFoundError)
        assert isinstance(exc_info.value.__cause__, EmailClientError)
        assert isinstance(exc_info.value.__cause__.__cause__, ConnectionRefusedError)
        assert fake_clock.sleeps == [1, 1, 1]
        assert imap_server.connection_attempts == 4

    def test_raises_not_found_when_the_server_recovers(
        self,
        client_factory: Callable[..., EmailClient],
        imap_server: FakeIMAPServer,
        fake_clock: FakeClock,
        email_address: str,
    ) -> None:
        """Test that a poll that failed earlier does not hide that the last poll found nothing"""
        client = client_factory(timeout=3, poll_interval=1)
        imap_server.script("CONNECT", None)
        imap_server.script("CONNECT", ConnectionRefusedError("Connection refused"))

        with pytest.raises(EmailNotFoundError):
            client.wait_for_email(email_address)

        assert fake_clock.sleeps == [1, 1, 1]
        assert imap_server.connection_attempts == 4

    def test_find_emails_does_not_retry_after_the_mailbox_was_reached(
        self, client: EmailClient, imap_server: FakeIMAPServer, email_address: str
    ) -> None:
        """Test that a poll of `find_emails` that fails is reported right away, even in a client that retries waits"""
        client.find_emails(email_address)
        imap_server.script("CONNECT", ConnectionRefusedError("Connection refused"))

        with pytest.raises(EmailClientError, match=f"Unable to connect to {HOST}:993"):
            client.find_emails(email_address)

        assert imap_server.connection_attempts == 2

    @pytest.mark.parametrize("header", UNPARSABLE_HEADERS)
    def test_raises_library_error_for_email_it_cannot_parse(
        self, client: EmailClient, imap_server: FakeIMAPServer, fake_clock: FakeClock, email_address: str, header: str
    ) -> None:
        """Test that an email the standard library cannot parse is reported with its UID, even in a client that retries
        failing polls, because the same bytes would fail again
        """
        client.find_emails(email_address)
        imap_server.deliver(build_email_with_unparsable_header(header, email_address))

        with pytest.raises(EmailClientError, match="Unable to parse email 1"):
            client.wait_for_email(email_address)

        assert imap_server.searches == 2

    @pytest.mark.parametrize(
        "error", [OSError("Socket is closed"), imaplib.IMAP4.abort("socket error")], ids=["os-error", "abort"]
    )
    def test_ignores_errors_when_logging_out(
        self,
        client: EmailClient,
        imap_server: FakeIMAPServer,
        email_address: str,
        send_email: Callable[..., int],
        error: Exception,
    ) -> None:
        """Test that a connection that is already closed by the server does not fail the call"""
        send_email(email_address)
        imap_server.script("LOGOUT", error)

        assert client.wait_for_email(email_address).to == (email_address,)
