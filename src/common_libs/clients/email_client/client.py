from __future__ import annotations

import imaplib
import math
import re
import ssl
import time
from collections.abc import Callable
from contextlib import suppress
from datetime import date
from typing import Any, TypeAlias

from common_libs.logging import get_logger

from .auth import OAuth2Auth, PasswordAuth
from .exceptions import EmailClientError, EmailNotFoundError
from .models import Email

logger = get_logger(__name__)

IMAP_PORT = 143
IMAPS_PORT = 993
SOCKET_TIMEOUT = 30.0
_ADDRESS_RE = re.compile(r'[^\s@"\\]+@[^\s@"\\]+')
_PORT_RANGE = range(1, 65536)
_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

SubjectFilter: TypeAlias = str | re.Pattern[str]


class EmailClient:
    """Wait for emails in an IMAP mailbox that receives the mail of many addresses (e.g. `qa+<anything>@example.com`)

    Every poll opens its own short-lived connection and closes it right after, so no connection stays open while
    waiting, and one instance can be shared by tests running in threads.

    :param host: IMAP server host
    :param username: Login name
    :param auth: How to log in: `PasswordAuth("my-password")`, or `OAuth2Auth(token_or_function)` for an OAuth2 token
    :param port: IMAP port. Defaults to 993 with SSL and 143 without
    :param use_ssl: Connect over SSL/TLS
    :param ssl_context: Custom SSL context. Defaults to one that verifies the server certificate
    :param mailbox: The mailbox (folder) to look in
    :param timeout: Default number of seconds to wait for an email
    :param poll_interval: Seconds between two checks while waiting
    """

    def __init__(
        self,
        *,
        host: str,
        username: str,
        auth: PasswordAuth | OAuth2Auth,
        port: int | None = None,
        use_ssl: bool = True,
        ssl_context: ssl.SSLContext | None = None,
        mailbox: str = "INBOX",
        timeout: float = 60.0,
        poll_interval: float = 1.0,
    ) -> None:
        if not (math.isfinite(poll_interval) and poll_interval > 0):
            raise ValueError(f"poll_interval must be a finite positive number: {poll_interval}")
        if not mailbox.isascii():
            raise ValueError(f"mailbox must be ASCII: {mailbox!r}")
        if port is not None and port not in _PORT_RANGE:
            raise ValueError(f"port must be between {_PORT_RANGE.start} and {_PORT_RANGE.stop - 1}: {port}")
        self._host = host
        self._username = username
        self._auth = auth
        self._port = (IMAPS_PORT if use_ssl else IMAP_PORT) if port is None else port
        self._use_ssl = use_ssl
        self._ssl_context = ssl_context
        self._mailbox = mailbox
        self._timeout = _validate_timeout(timeout)
        self._poll_interval = poll_interval
        self._reached_mailbox = False

    def wait_for_email(
        self,
        to: str,
        *,
        subject: SubjectFilter | None = None,
        after: Email | None = None,
        since: date | None = None,
        timeout: float | None = None,
    ) -> Email:
        """Wait until an email to the address arrives and return it. If several match, the newest is returned

        :param to: The recipient address
        :param subject: Only consider emails whose subject contains this text or matches this pattern
        :param after: Only consider emails that arrived after this one (e.g. to wait for a resent email)
        :param since: Only consider emails the server received on or after this day, which lets the server skip older
                      mail in a large mailbox. The server compares days and ignores the time and the time zone, so pick
                      a day earlier than needed if the clocks may differ. A `datetime` is reduced to its day
        :param timeout: Seconds to wait. Defaults to the client's timeout
        """
        address = _validate_address(to)
        _validate_subject(subject)
        _validate_since(since)
        timeout = self._timeout if timeout is None else _validate_timeout(timeout)
        after_uid = _get_uid(after)
        deadline = time.monotonic() + timeout
        session = _Session(self._connect, self._mailbox, address, since)
        emails: list[Email] = []
        while True:
            try:
                emails = self._poll(session, after_uid)
                last_error = None
            except _ServerError as e:
                if not self._reached_mailbox:
                    raise
                logger.warning("Unable to check the mailbox for emails to %s. Trying again at the next poll: %s", to, e)
                last_error = e
            else:
                if matches := [email for email in emails if _subject_matches(email, subject)]:
                    return matches[-1]
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                if last_error is not None:
                    raise EmailClientError(
                        f"Unable to check the mailbox when the {timeout:g}s wait ended: {last_error}"
                    ) from last_error
                raise EmailNotFoundError(_not_found_message(address, subject, since, timeout, emails))
            time.sleep(min(self._poll_interval, remaining))

    def find_emails(
        self,
        to: str,
        *,
        subject: SubjectFilter | None = None,
        after: Email | None = None,
        since: date | None = None,
    ) -> list[Email]:
        """Return the emails to the address that are already in the mailbox, oldest first, without waiting

        :param to: The recipient address
        :param subject: Only return emails whose subject contains this text or matches this pattern
        :param after: Only return emails that arrived after this one
        :param since: Only return emails the server received on or after this day. See `wait_for_email()`
        """
        address = _validate_address(to)
        _validate_subject(subject)
        _validate_since(since)
        after_uid = _get_uid(after)
        emails = self._poll(_Session(self._connect, self._mailbox, address, since), after_uid)
        return [email for email in emails if _subject_matches(email, subject)]

    def _poll(self, session: _Session, after_uid: int) -> list[Email]:
        emails = session.emails(after_uid)
        self._reached_mailbox = True
        return emails

    def _connect(self) -> imaplib.IMAP4:
        log_in = self._auth.prepare_login(self._username)
        try:
            if self._use_ssl:
                context = self._ssl_context or ssl.create_default_context()
                conn: imaplib.IMAP4 = imaplib.IMAP4_SSL(
                    self._host, self._port, ssl_context=context, timeout=SOCKET_TIMEOUT
                )
            else:
                conn = imaplib.IMAP4(self._host, self._port, timeout=SOCKET_TIMEOUT)
        except (OSError, imaplib.IMAP4.error) as e:
            raise _ServerError(f"Unable to connect to {self._host}:{self._port}: {e}") from e
        try:
            log_in(conn)
        except (imaplib.IMAP4.error, OSError, UnicodeEncodeError) as e:
            _logout(conn)
            raise _ServerError(f"Unable to log in to {self._host} as {self._username!r}: {e}") from e
        return conn


class _ServerError(EmailClientError):
    """The connection or the server failed, which a later attempt may not repeat"""


class _ConnectionLost(_ServerError):
    """The connection broke while a command was running"""


class _Session:
    """Looks up the emails sent to one address and remembers the ones it fetched already

    Every poll runs on its own short-lived, read-only IMAP connection, so none stays open while waiting. A connection
    that drops during a poll is replaced by a new one and the poll is repeated once.
    """

    def __init__(self, connect: Callable[[], imaplib.IMAP4], mailbox: str, address: str, since: date | None) -> None:
        self._connect = connect
        self._mailbox = mailbox
        self._address = address
        self._since = since
        self._emails: dict[int, Email | None] = {}

    def emails(self, after_uid: int = 0) -> list[Email]:
        """Return the emails sent to the address, oldest first. Each email is fetched only once"""
        try:
            return self._poll(after_uid)
        except _ConnectionLost as e:
            logger.warning("%s. Trying again on a new connection", e)
            return self._poll(after_uid)

    def _poll(self, after_uid: int) -> list[Email]:
        conn = self._connect()
        try:
            return self._read(conn, after_uid)
        except (imaplib.IMAP4.abort, OSError) as e:
            raise _ConnectionLost(f"Lost the connection to the IMAP server: {e}") from e
        except imaplib.IMAP4.error as e:
            raise _ServerError(f"IMAP error: {e}") from e
        finally:
            _logout(conn)

    def _read(self, conn: imaplib.IMAP4, after_uid: int) -> list[Email]:
        found = []
        for uid in self._search(conn):
            if uid <= after_uid:
                continue
            if uid not in self._emails:
                if (raw := self._fetch(conn, uid)) is None:
                    continue
                self._emails[uid] = self._parse(raw, uid)
            if email := self._emails[uid]:
                found.append(email)
        return found

    def _search(self, conn: imaplib.IMAP4) -> list[int]:
        _check(conn.select(_quote(self._mailbox), readonly=True), f"Unable to open mailbox {self._mailbox!r}")
        criteria = ["TO", _quote(self._address)]
        if self._since is not None:
            criteria += ["SINCE", _format_date(self._since)]
        data = _check(conn.uid("SEARCH", *criteria), "Unable to search the mailbox")
        return sorted(int(uid) for uid in (data[0] or b"").split())

    def _fetch(self, conn: imaplib.IMAP4, uid: int) -> bytes | None:
        data = _check(conn.uid("FETCH", str(uid), "(BODY.PEEK[])"), f"Unable to fetch email {uid}")
        return next((item[1] for item in data if isinstance(item, tuple)), None)

    def _parse(self, raw: bytes, uid: int) -> Email | None:
        email = Email.from_bytes(raw, uid=uid)
        # The server-side TO search may match substrings, so the recipient is verified here
        return email if self._address.lower() in {addr.lower() for addr in email.to} else None


def _validate_timeout(timeout: float) -> float:
    if not (math.isfinite(timeout) and timeout >= 0):
        raise ValueError(f"timeout must be a finite number that is not negative: {timeout}")
    return timeout


def _validate_address(address: str) -> str:
    if not address.isascii():
        raise ValueError(f"Non-ASCII email addresses are not supported: {address!r}")
    if _ADDRESS_RE.fullmatch(address) is None:
        raise ValueError(f"Not a valid email address: {address!r}")
    return address


def _validate_subject(subject: SubjectFilter | None) -> None:
    if isinstance(subject, str) and not subject.strip():
        raise ValueError("subject must not be blank")


def _validate_since(since: date | None) -> None:
    if since is not None and not isinstance(since, date):
        raise TypeError(f"since must be a date: {since!r}")


def _format_date(day: date) -> str:
    # strftime would follow the locale, but IMAP requires English month names
    return f"{day.day}-{_MONTHS[day.month - 1]}-{day.year}"


def _get_uid(email: Email | None) -> int:
    if email is None:
        return 0
    if email.uid is None:
        raise ValueError("`after` must be an email returned by this client")
    return email.uid


def _subject_matches(email: Email, subject: SubjectFilter | None) -> bool:
    if subject is None:
        return True
    if isinstance(subject, re.Pattern):
        return subject.search(email.subject) is not None
    return subject in email.subject


def _not_found_message(
    address: str, subject: SubjectFilter | None, since: date | None, timeout: float, others: list[Email]
) -> str:
    message = f"No email to {address!r}"
    if subject is not None:
        message += f" with a subject matching {subject.pattern if isinstance(subject, re.Pattern) else subject!r}"
    if since is not None:
        message += f" received since {_format_date(since)}"
    message += f" arrived within {timeout:g}s"
    if others:
        message += f". Other emails to this address have the subjects: {', '.join(repr(e.subject) for e in others)}"
    return message


def _check(response: tuple[str, list[Any]], error: str) -> list[Any]:
    status, data = response
    if status != "OK":
        detail = b" ".join(item for item in data if isinstance(item, bytes)).decode(errors="replace")
        raise _ServerError(f"{error}: {detail}")
    return data


def _quote(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _logout(conn: imaplib.IMAP4) -> None:
    try:
        conn.logout()
    except (OSError, imaplib.IMAP4.error):
        with suppress(OSError):
            conn.shutdown()
