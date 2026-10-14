import imaplib
import smtplib
import ssl
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Self

from common_libs.containers.container import BaseContainer
from common_libs.exceptions import CommandError
from tests.libs.email_client.addresses import MAILBOX_ADDRESS

HOST = "127.0.0.1"
HOSTNAME = "mail.example.com"
USERNAME = MAILBOX_ADDRESS
PASSWORD = "secret"
ACCESS_TOKEN = "ya29.integration-test-token"
SUBMISSION_PORT = 587
IMAPS_PORT = 993
IDP_PORT = 8080
IDP_SCRIPT = Path(__file__).with_name("fake_idp.py")
SOCKET_TIMEOUT = 30.0
READY_TIMEOUT = 180.0
DELIVERY_TIMEOUT = 30.0
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
    "OAUTH2_INTROSPECTION_URL": f"http://127.0.0.1:{IDP_PORT}/userinfo",
    "SSL_TYPE": "snakeoil",
    **dict.fromkeys(SERVICES_NOT_NEEDED, "0"),
}


def create_unverified_ssl_context() -> ssl.SSLContext:
    """Create an SSL context that accepts the self-signed certificate of the mail server"""
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context


class MailServerContainer(BaseContainer):
    """A docker-mailserver container that has a single mailbox and is reachable on random ports

    https://docker-mailserver.github.io/docker-mailserver/latest/

    The mailbox accepts a password login and an OAuth2 login. The OAuth2 access token is validated by a small fake
    identity provider that runs inside the container.
    """

    def __init__(self) -> None:
        # The label keeps the containers of parallel test sessions apart
        super().__init__("mailserver/docker-mailserver", tag="16.0.1", labels={"session": uuid.uuid4().hex})

    def run(self, **kwargs: Any) -> Self:
        """Run the container and wait until the mail server accepts logins and emails

        The container is deleted if the mail server does not become ready.

        :param kwargs: Extra options for the container, which are passed on to the base class
        """
        # The container is kept after a crash during the start-up, so that its logs can be read
        super().run(
            remove=False,
            hostname=HOSTNAME,
            ports={f"{port}/tcp": None for port in (SUBMISSION_PORT, IMAPS_PORT)},
            environment=ENVIRONMENT,
            **kwargs,
        )
        try:
            self._wait_until_ready(READY_TIMEOUT)
        except BaseException:
            self.delete()
            raise
        return self

    def get_flags(self, uid: int) -> bytes:
        """Return the FETCH response with the flags of an email, as another user of the mailbox sees it

        :param uid: The UID of the email
        """
        with self._login() as imap:
            imap.select("INBOX", readonly=True)
            _, data = imap.uid("FETCH", str(uid), "(FLAGS)")
        return bytes(data[0])

    def wait_until_delivered(self, message_id: str, timeout: float = DELIVERY_TIMEOUT) -> None:
        """Wait until the email with the Message-ID is in the mailbox

        The server accepts an email before it delivers it, and emails sent in quick succession can be delivered in a
        different order.

        :param message_id: The value of the Message-ID header
        :param timeout: Seconds to wait
        """
        deadline = time.monotonic() + timeout
        while True:
            with self._login() as imap:
                imap.select("INBOX", readonly=True)
                _, data = imap.uid("SEARCH", "HEADER", "Message-ID", f'"{message_id}"')
            if data[0]:
                return
            if time.monotonic() > deadline:
                raise TimeoutError(f"The email {message_id} was not delivered within {timeout:g} seconds")
            time.sleep(0.05)

    @contextmanager
    def _login(self) -> Iterator[imaplib.IMAP4_SSL]:
        with imaplib.IMAP4_SSL(
            HOST, self.host_port(IMAPS_PORT), ssl_context=create_unverified_ssl_context(), timeout=SOCKET_TIMEOUT
        ) as imap:
            imap.login(USERNAME, PASSWORD)
            yield imap

    def _wait_until_ready(self, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        steps: list[Callable[[], object]] = [
            self._add_account,
            self._start_fake_idp,
            self._check_imaps,
            self._check_submission,
        ]
        for step in steps:
            while True:
                try:
                    step()
                    break
                except (CommandError, OSError, imaplib.IMAP4.error, smtplib.SMTPException) as e:
                    if time.monotonic() > deadline:
                        assert self.container is not None
                        logs = self.container.logs().decode(errors="replace")
                        raise RuntimeError(
                            f"The mail server did not become ready in {timeout:g} seconds ({e!r}):\n{logs}"
                        ) from None
                    time.sleep(0.5)

    def _add_account(self) -> None:
        self.exec_run(f"setup email add {USERNAME} {PASSWORD}", quiet=True)

    def _start_fake_idp(self) -> None:
        self.upload_file(IDP_SCRIPT, self.tmp_dir)
        self.exec_run(
            f"python3 {self.tmp_dir}/{IDP_SCRIPT.name} {ACCESS_TOKEN} {USERNAME} {IDP_PORT}", detach=True, quiet=True
        )

    def _check_imaps(self) -> None:
        with self._login():
            pass

    def _check_submission(self) -> None:
        with smtplib.SMTP(HOST, self.host_port(SUBMISSION_PORT), timeout=SOCKET_TIMEOUT) as smtp:
            smtp.starttls(context=create_unverified_ssl_context())
            smtp.login(USERNAME, PASSWORD)
