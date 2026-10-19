import smtplib
from email import message_from_bytes, policy
from email.headerregistry import Address
from email.message import EmailMessage
from email.utils import make_msgid

from pytest_data_loader import DataLoaderFixture

from tests.libs.containers.base import HOST
from tests.libs.containers.mail_server import (
    PASSWORD,
    SOCKET_TIMEOUT,
    SUBMISSION_PORT,
    USERNAME,
    MailServerContainer,
    create_unverified_ssl_context,
)
from tests.libs.email_client.emails import get_template_path


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

    def send_email(self, template: str, to: str) -> None:
        """Send a sample email to the address. The email is in the mailbox when the call returns

        :param template: The name of the sample email
        :param to: The address to put in the To header
        """
        msg = message_from_bytes(self._data_loader(get_template_path(template)), policy=policy.SMTP)
        assert isinstance(msg, EmailMessage)
        display_name = msg["To"].addresses[0].display_name
        del msg["To"]
        msg["To"] = Address(display_name=display_name, addr_spec=to)
        del msg["Message-ID"]
        msg["Message-ID"] = make_msgid()
        with smtplib.SMTP(HOST, self._mail_server.host_port(SUBMISSION_PORT), timeout=SOCKET_TIMEOUT) as smtp:
            smtp.starttls(context=create_unverified_ssl_context())
            smtp.login(USERNAME, PASSWORD)
            smtp.send_message(msg, from_addr=USERNAME, to_addrs=[USERNAME])
        self._mail_server.wait_until_delivered(str(msg["Message-ID"]))
