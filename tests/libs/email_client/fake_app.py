import smtplib
import time
from concurrent.futures import Future, ThreadPoolExecutor
from email import message_from_bytes, policy
from email.headerregistry import Address
from email.message import EmailMessage
from email.utils import make_msgid

from pytest_data_loader import DataLoaderFixture

from tests.libs.email_client.emails import get_template_path
from tests.libs.email_client.mail_server import (
    HOST,
    PASSWORD,
    SOCKET_TIMEOUT,
    SUBMISSION_PORT,
    USERNAME,
    MailServerContainer,
    create_unverified_ssl_context,
)


class FakeApp:
    """Stands in for the web application under test, which sends its emails whenever it decides to

    Every email is delivered to the one mailbox of the mail server, as the plus-addressing of Gmail does. The emails are
    sent through the authenticated submission port, as a web application with an SMTP account would.

    :param mail_server: The mail server that receives the emails
    :param data_loader: The data loader that reads the template emails
    """

    def __init__(self, mail_server: MailServerContainer, data_loader: DataLoaderFixture) -> None:
        self._mail_server = mail_server
        self._data_loader = data_loader
        self._executor = ThreadPoolExecutor()
        self._pending: list[Future[None]] = []

    def send_email(self, template: str, to: str, *, delay: float = 0.0) -> None:
        """Send a sample email to the address

        :param template: The name of the sample email
        :param to: The address to put in the To header
        :param delay: Seconds to wait before sending. The call returns immediately if this is set. Otherwise the
                      email is in the mailbox when the call returns
        """
        msg = message_from_bytes(self._data_loader(get_template_path(template)), policy=policy.SMTP)
        assert isinstance(msg, EmailMessage)
        display_name = msg["To"].addresses[0].display_name
        del msg["To"]
        msg["To"] = Address(display_name=display_name, addr_spec=to)
        del msg["Message-ID"]
        msg["Message-ID"] = make_msgid()
        if delay:
            self._pending.append(self._executor.submit(self._send, msg, delay))
        else:
            self._send(msg)

    def close(self) -> None:
        """Wait for the delayed emails and raise the error if sending one of them failed"""
        self._executor.shutdown()
        for future in self._pending:
            future.result()

    def _send(self, msg: EmailMessage, delay: float = 0.0) -> None:
        time.sleep(delay)
        with smtplib.SMTP(HOST, self._mail_server.host_port(SUBMISSION_PORT), timeout=SOCKET_TIMEOUT) as smtp:
            smtp.starttls(context=create_unverified_ssl_context())
            smtp.login(USERNAME, PASSWORD)
            smtp.send_message(msg, from_addr=USERNAME, to_addrs=[USERNAME])
        self._mail_server.wait_until_delivered(str(msg["Message-ID"]))
