import imaplib
import ssl
from collections import defaultdict, deque
from collections.abc import Callable
from datetime import date, datetime
from email import message_from_bytes
from typing import Any, TypeAlias

from tests.libs.email_client.addresses import MAILBOX_ADDRESS

HOST = "imap.test"
USERNAME = MAILBOX_ADDRESS
PASSWORD = "secret"
ACCESS_TOKEN = "ya29.access-token"
XOAUTH2_ERROR_CHALLENGE = b'{"status":"401","schemes":"Bearer","scope":"https://mail.google.com/"}'

Response: TypeAlias = tuple[str, list[Any]]


class FakeIMAPServer:
    """An in-memory IMAP server that understands the commands the client uses

    Install `tcp` and `tls` in place of `imaplib.IMAP4` and `imaplib.IMAP4_SSL`.

    :param username: The accepted login name
    :param password: The accepted password
    """

    def __init__(self, username: str = USERNAME, password: str = PASSWORD, access_token: str = ACCESS_TOKEN) -> None:
        self.username = username
        self.password = password
        self.access_token = access_token
        self.messages: dict[int, bytes] = {}
        self.internal_dates: dict[int, date] = {}
        self.commands: list[str] = []
        self.connections: list[FakeConnection] = []
        self.connection_attempts = 0
        self.searches = 0
        self.marked_seen: set[int] = set()
        self.unsolicited_fetch_items: list[bytes] = []
        self.tcp = _Endpoint(self, secure=False)
        self.tls = _Endpoint(self, secure=True)
        self._scheduled: defaultdict[int, list[bytes]] = defaultdict(list)
        self._scripted: defaultdict[str, deque[BaseException | Response | None]] = defaultdict(deque)
        self._next_uid = 1

    def deliver(self, raw: bytes, internal_date: date | None = None) -> int:
        """Add an email to the mailbox and return its UID

        :param raw: The raw email
        :param internal_date: The day the server received the email. Defaults to today
        """
        uid = self._next_uid
        self._next_uid += 1
        self.messages[uid] = raw
        self.internal_dates[uid] = internal_date or date.today()
        return uid

    def deliver_on_search(self, search_count: int, raw: bytes) -> None:
        """Add an email to the mailbox just before the search with the given (1-based, overall) number"""
        self._scheduled[search_count].append(raw)

    def script(self, command: str, outcome: BaseException | Response | None, times: int = 1) -> None:
        """Make the next `times` executions of a command raise the exception or return the response

        A response for `CONNECT` is consumed without effect, so that the connection attempt succeeds. `None` lets the
        execution behave normally, so that a later one can fail.
        """
        self._scripted[command].extend([outcome] * times)

    def count(self, prefix: str) -> int:
        """Return the number of commands received so far that start with the prefix"""
        return sum(command.startswith(prefix) for command in self.commands)

    def pop_scheduled(self, search_count: int) -> list[bytes]:
        """Remove and return the emails that are scheduled for the search with the given number"""
        return self._scheduled.pop(search_count, [])

    def take_scripted(self, command: str) -> Response | None:
        """Return the next scripted response of the command, or raise the next scripted exception

        :param command: The command that is being executed
        """
        if not self._scripted[command]:
            return None
        outcome = self._scripted[command].popleft()
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class FakeConnection:
    """The subset of `imaplib.IMAP4` that the client uses"""

    def __init__(
        self,
        server: FakeIMAPServer,
        host: str,
        port: int,
        secure: bool,
        ssl_context: ssl.SSLContext | None,
        timeout: Any,
    ) -> None:
        self.host = host
        self.port = port
        self.secure = secure
        self.ssl_context = ssl_context
        self.timeout = timeout
        self.credentials: tuple[str, str] | None = None
        self.challenge_replies: list[bytes | None] = []
        self.state = "NONAUTH"
        self.logged_out = False
        self.closed = False
        self._server = server

    def login(self, user: str, password: str) -> Response:
        command = "LOGIN"
        self._server.commands.append(command)
        if response := self._server.take_scripted(command):
            return response
        self.credentials = (user, password)
        if (user, password) != (self._server.username, self._server.password):
            raise imaplib.IMAP4.error("[AUTHENTICATIONFAILED] Invalid credentials")
        self.state = "AUTH"
        return "OK", [b"LOGIN completed"]

    def authenticate(self, mechanism: str, authobject: Callable[[bytes], bytes | None]) -> Response:
        command = "AUTHENTICATE"
        self._server.commands.append(f"{command} {mechanism}")
        if response := self._server.take_scripted(command):
            return response
        if mechanism != "XOAUTH2":
            raise imaplib.IMAP4.error(f"Unsupported authentication mechanism {mechanism}")
        initial_response = authobject(b"")
        assert initial_response is not None, "The client aborted the exchange"
        user, token = _parse_xoauth2(initial_response)
        self.credentials = (user, token)
        if (user, token) != (self._server.username, self._server.access_token):
            self.challenge_replies.append(authobject(XOAUTH2_ERROR_CHALLENGE))
            raise imaplib.IMAP4.error("[AUTHENTICATIONFAILED] Invalid credentials")
        self.state = "AUTH"
        return "OK", [b"Authenticated"]

    def select(self, mailbox: str = "INBOX", readonly: bool = False) -> Response:
        command = "EXAMINE" if readonly else "SELECT"
        self._server.commands.append(f"{command} {mailbox}")
        if response := self._server.take_scripted(command):
            self.state = "AUTH"
            return response
        self.state = "SELECTED"
        return "OK", [str(len(self._server.messages)).encode()]

    def uid(self, command: str, *args: str) -> Response:
        self._server.commands.append(f"UID {command} {' '.join(args)}")
        if self.state != "SELECTED":
            raise imaplib.IMAP4.error(f"command {command} illegal in state {self.state}")
        if command == "SEARCH":
            return self._search(*args)
        if command == "FETCH":
            return self._fetch(*args)
        raise imaplib.IMAP4.error(f"unsupported command {command}")

    def logout(self) -> Response:
        """Like `imaplib.IMAP4.logout()`, the connection is only closed once the server has answered the command"""
        command = "LOGOUT"
        self._server.commands.append(command)
        self.logged_out = True
        response = self._server.take_scripted(command) or ("BYE", [b"Logging out"])
        self.shutdown()
        return response

    def shutdown(self) -> None:
        """Close the connection"""
        self.closed = True

    def _search(self, *criteria: str) -> Response:
        self._server.searches += 1
        for raw in self._server.pop_scheduled(self._server.searches):
            self._server.deliver(raw)
        if response := self._server.take_scripted("SEARCH"):
            return response
        keys = iter(criteria)
        filters = {key: value for key, value in zip(keys, keys, strict=True)}
        assert set(filters) <= {"TO", "SINCE"}, f"unsupported search keys {list(filters)}"
        needle = filters["TO"].strip('"').lower()
        since = _parse_date(filters["SINCE"]) if "SINCE" in filters else None
        uids = [
            uid
            for uid, raw in self._server.messages.items()
            if needle in _header(raw, "To").lower() and (since is None or self._server.internal_dates[uid] >= since)
        ]
        return "OK", [" ".join(map(str, uids)).encode()]

    def _fetch(self, uid: str, items: str) -> Response:
        if response := self._server.take_scripted("FETCH"):
            return response
        raw = self._server.messages.get(int(uid))
        if raw is None:
            return "OK", [None]
        if items != "(BODY.PEEK[])":
            self._server.marked_seen.add(int(uid))
        header = f"{uid} (UID {uid} BODY[] {{{len(raw)}}}".encode()
        return "OK", [*self._server.unsolicited_fetch_items, (header, raw), b")"]


class _Endpoint:
    """Stands in for the `imaplib.IMAP4` and `imaplib.IMAP4_SSL` classes"""

    error = imaplib.IMAP4.error
    abort = imaplib.IMAP4.abort

    def __init__(self, server: FakeIMAPServer, secure: bool) -> None:
        self._server = server
        self._secure = secure

    def __call__(
        self, host: str, port: int, *, ssl_context: ssl.SSLContext | None = None, timeout: Any = None
    ) -> FakeConnection:
        self._server.connection_attempts += 1
        self._server.take_scripted("CONNECT")
        connection = FakeConnection(self._server, host, port, self._secure, ssl_context, timeout)
        self._server.connections.append(connection)
        return connection


def _header(raw: bytes, name: str) -> str:
    return str(message_from_bytes(raw).get(name, ""))


def _parse_xoauth2(response: bytes) -> tuple[str, str]:
    user, auth, *_ = response.decode().split("\x01")
    return user.removeprefix("user="), auth.removeprefix("auth=Bearer ")


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%d-%b-%Y").date()
