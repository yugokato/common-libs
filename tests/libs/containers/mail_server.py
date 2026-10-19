import imaplib
import shlex
import smtplib
import ssl
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any, Self

from common_libs.exceptions import CommandError
from tests.libs.containers.base import HOST, ServiceContainer
from tests.libs.email_client.addresses import MAILBOX_ADDRESS

XDIST_GROUP = "mail-server"
HOSTNAME = "mail.example.com"
USERNAME = MAILBOX_ADDRESS
PASSWORD = "secret"
ACCESS_TOKEN = "ya29.integration-test-token"
SUBMISSION_PORT = 587
IMAPS_PORT = 993
IDP_PORT = 8080
IDP_URL = f"http://127.0.0.1:{IDP_PORT}/userinfo"
# Validates the access token at the identity provider like the mail server does. Fails until the provider answers
IDP_CHECK_SCRIPT = (
    "import urllib.request as r; "
    f"r.urlopen(r.Request('{IDP_URL}', headers={{'Authorization': 'Bearer {ACCESS_TOKEN}'}}))"
)
IDP_SCRIPT = Path(__file__).parents[1] / "email_client" / "fake_idp.py"
DOVECOT_LOCAL_CONFIG = "/etc/dovecot/local.conf"
DOVECOT_SETTINGS = ("auth_failure_delay = 0", "mail_max_userip_connections = 100")
SOCKET_TIMEOUT = 30.0
DELIVERY_TIMEOUT = 30.0
CONNECTION_ATTEMPTS = 3
CONNECTION_LOST_ERRORS = (imaplib.IMAP4.abort, ConnectionError, ssl.SSLEOFError)
SERVICES_NOT_NEEDED = (
    "ENABLE_AMAVIS",
    "ENABLE_SPAMASSASSIN",
    "ENABLE_CLAMAV",
    "ENABLE_RSPAMD",
    "ENABLE_OPENDKIM",
    "ENABLE_OPENDMARC",
    "ENABLE_POLICYD_SPF",
    "ENABLE_POSTGREY",
    "ENABLE_FAIL2BAN",
)
ENVIRONMENT = {
    "ENABLE_OAUTH2": "1",
    "OAUTH2_INTROSPECTION_URL": IDP_URL,
    "SSL_TYPE": "snakeoil",
    **dict.fromkeys(SERVICES_NOT_NEEDED, "0"),
}


def create_unverified_ssl_context() -> ssl.SSLContext:
    """Create an SSL context that accepts the self-signed certificate of the mail server"""
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context


class MailServerContainer(ServiceContainer):
    """A docker-mailserver container that has a single mailbox and is reachable on random ports

    https://docker-mailserver.github.io/docker-mailserver/latest/

    The mailbox accepts a password login and an OAuth2 login. The OAuth2 access token is validated by a small fake
    identity provider that runs inside the container.

    A failed login is not delayed. That also switches off the penalty Dovecot adds to every later login from the same
    IP address once a login has failed. The limit of 10 connections per user and IP address is raised, because every
    poll of a waiting client opens a new connection and the tests wait for several emails at once.
    """

    ready_timeout = 180.0
    transient_errors = (CommandError, OSError, imaplib.IMAP4.error, smtplib.SMTPException)

    def __init__(self) -> None:
        super().__init__("mailserver/docker-mailserver", "16.0.1")

    def run(self, **kwargs: Any) -> Self:
        """Run the container and wait until the mail server accepts logins and emails

        :param kwargs: Extra options for the container, which are passed on to the base class
        """
        return super().run(
            hostname=HOSTNAME,
            ports={f"{port}/tcp": None for port in (SUBMISSION_PORT, IMAPS_PORT)},
            environment=ENVIRONMENT,
            **kwargs,
        )

    def get_flags(self, uid: int) -> bytes:
        """Return the FETCH response with the flags of an email, as another user of the mailbox sees it

        :param uid: The UID of the email
        """
        return bytes(self._run_uid("FETCH", str(uid), "(FLAGS)")[0])

    def wait_until_delivered(self, message_id: str, timeout: float = DELIVERY_TIMEOUT) -> None:
        """Wait until the email with the Message-ID is in the mailbox

        The server accepts an email before it delivers it, and emails sent in quick succession can be delivered in a
        different order.

        :param message_id: The value of the Message-ID header
        :param timeout: Seconds to wait
        """
        deadline = time.monotonic() + timeout
        while True:
            if self._run_uid("SEARCH", "HEADER", "Message-ID", f'"{message_id}"')[0]:
                return
            if time.monotonic() > deadline:
                raise TimeoutError(f"The email {message_id} was not delivered within {timeout:g} seconds")
            time.sleep(0.05)

    def _run_uid(self, command: str, *args: str) -> list[Any]:
        """Run a UID command on the INBOX in a new read-only session and return the response data

        Dovecot cuts the connections of a login process that crashes, so the command is repeated on a new connection.

        :param command: The UID command, such as `SEARCH` or `FETCH`
        :param args: The arguments of the command
        """

        def _run(command: str, *args: str) -> list[Any]:
            with self._login() as imap:
                imap.select("INBOX", readonly=True)
                _, data = imap.uid(command, *args)
            return data

        for _ in range(CONNECTION_ATTEMPTS - 1):
            with suppress(*CONNECTION_LOST_ERRORS):
                return _run(command, *args)
        return _run(command, *args)

    @contextmanager
    def _login(self) -> Iterator[imaplib.IMAP4_SSL]:
        imap = imaplib.IMAP4_SSL(
            HOST, self.host_port(IMAPS_PORT), ssl_context=create_unverified_ssl_context(), timeout=SOCKET_TIMEOUT
        )
        try:
            imap.login(USERNAME, PASSWORD)
            yield imap
        finally:
            # The `with` of imaplib only ignores an `OSError` of the logout, so a connection dropped by the server
            # would leave the socket open
            try:
                imap.logout()
            except (imaplib.IMAP4.error, OSError):
                with suppress(OSError):
                    imap.shutdown()

    def _readiness_steps(self) -> Sequence[Callable[[], object]]:
        return [
            self._add_account,
            self._start_fake_idp,
            self._check_fake_idp,
            self._configure_dovecot,
            self._check_imaps,
            self._check_submission,
        ]

    def _add_account(self) -> None:
        self.exec_run(f"setup email add {USERNAME} {PASSWORD}", quiet=True)

    def _start_fake_idp(self) -> None:
        self.upload_file(IDP_SCRIPT, self.tmp_dir)
        self.exec_run(
            f"python3 {self.tmp_dir}/{IDP_SCRIPT.name} {ACCESS_TOKEN} {USERNAME} {IDP_PORT}", detach=True, quiet=True
        )

    def _check_fake_idp(self) -> None:
        self.exec_run(f"python3 -c {shlex.quote(IDP_CHECK_SCRIPT)}", quiet=True)

    def _configure_dovecot(self) -> None:
        settings = " ".join(shlex.quote(setting) for setting in DOVECOT_SETTINGS)
        self.exec_run(f"printf '%s\\n' {settings} > {DOVECOT_LOCAL_CONFIG} && doveadm reload", quiet=True)

    def _check_imaps(self) -> None:
        with self._login():
            pass

    def _check_submission(self) -> None:
        with smtplib.SMTP(HOST, self.host_port(SUBMISSION_PORT), timeout=SOCKET_TIMEOUT) as smtp:
            smtp.starttls(context=create_unverified_ssl_context())
            smtp.login(USERNAME, PASSWORD)
