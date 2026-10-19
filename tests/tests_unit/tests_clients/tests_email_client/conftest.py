"""Shared pytest fixtures for email client unit tests"""

import imaplib
from collections.abc import Callable
from datetime import date
from email.message import EmailMessage
from typing import Any

import pytest
import pytest_data_loader
from pytest_data_loader import make_data_loader_fixture
from pytest_mock import MockerFixture

from common_libs.clients.email_client import EmailClient, PasswordAuth
from common_libs.clients.email_client import client as client_module
from tests.libs.common.fake_clock import FakeClock
from tests.libs.email_client.emails import SENDER, TEMPLATE_READ_OPTIONS, read_template
from tests.libs.email_client.fake_imap import HOST, PASSWORD, USERNAME, FakeIMAPServer
from tests.libs.email_client.fixtures import email_address, password_reset, workspace_invitation  # noqa: F401

DEFAULT_ADDRESS = "qa+test@example.com"

pytest_data_loader.register_reader(".eml", read_template, read_options=TEMPLATE_READ_OPTIONS)

# Override the default fixture with a session-scoped one so that loaded data is cached and shared by all tests
data_loader = make_data_loader_fixture(scope="session")


@pytest.fixture
def raw_email_factory() -> Callable[..., bytes]:
    """Build a raw RFC 822 email. Pass `to=None` to omit the To header"""

    def _make_raw_email(
        *,
        to: str | None = DEFAULT_ADDRESS,
        subject: str = "Hello",
        text: str | None = "Hello",
        html: str | None = None,
        **headers: str,
    ) -> bytes:
        msg = EmailMessage()
        msg["From"] = SENDER
        msg["Subject"] = subject
        if to is not None:
            msg["To"] = to
        for name, value in headers.items():
            msg[name.replace("_", "-")] = value
        if text is not None:
            msg.set_content(text)
            if html is not None:
                msg.add_alternative(html, subtype="html")
        elif html is not None:
            msg.set_content(html, subtype="html")
        return msg.as_bytes()

    return _make_raw_email


@pytest.fixture
def imap_server(mocker: MockerFixture) -> FakeIMAPServer:
    """An in-memory IMAP server that replaces the connections made with `imaplib`"""
    server = FakeIMAPServer()
    mocker.patch.object(imaplib, "IMAP4", new=server.tcp)
    mocker.patch.object(imaplib, "IMAP4_SSL", new=server.tls)
    return server


@pytest.fixture
def fake_clock(mocker: MockerFixture) -> FakeClock:
    """A clock that replaces the real one in the client, so that waits take no time and their sleeps can be checked"""
    clock = FakeClock()
    mocker.patch.object(client_module, "time", new=clock)
    return clock


@pytest.fixture
def client_factory(imap_server: FakeIMAPServer, fake_clock: FakeClock) -> Callable[..., EmailClient]:
    """Create a client for the in-memory server that waits on the fake clock, so no wait takes time. Keyword arguments
    override the defaults
    """

    def _make_client(**kwargs: Any) -> EmailClient:
        params: dict[str, Any] = {
            "host": HOST,
            "username": USERNAME,
            "auth": PasswordAuth(PASSWORD),
            "timeout": 2.0,
            "poll_interval": 0.5,
        }
        return EmailClient(**params | kwargs)

    return _make_client


@pytest.fixture
def client(client_factory: Callable[..., EmailClient]) -> EmailClient:
    """A client for the in-memory server that waits on the fake clock"""
    return client_factory()


@pytest.fixture
def send_email(imap_server: FakeIMAPServer, raw_email_factory: Callable[..., bytes]) -> Callable[..., int]:
    """Deliver an email to the in-memory server and return its UID"""

    def _send_email(to: str, *, internal_date: date | None = None, **kwargs: str | None) -> int:
        return imap_server.deliver(raw_email_factory(to=to, **kwargs), internal_date)

    return _send_email


@pytest.fixture
def send_email_during_wait(imap_server: FakeIMAPServer, raw_email_factory: Callable[..., bytes]) -> Callable[..., None]:
    """Deliver an email to the in-memory server just before the search with the given number"""

    def _send_email_during_wait(search_count: int, to: str, **kwargs: str | None) -> None:
        imap_server.deliver_on_search(search_count, raw_email_factory(to=to, **kwargs))

    return _send_email_during_wait
