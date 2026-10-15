"""Shared pytest fixtures for email client integration tests"""

from collections.abc import Callable, Iterator
from typing import Any

import pytest
import pytest_data_loader
from pytest_data_loader import DataLoaderFixture, make_data_loader_fixture

from common_libs.clients.email_client import EmailClient, PasswordAuth
from tests.libs.containers.base import HOST
from tests.libs.containers.mail_server import (
    IMAPS_PORT,
    PASSWORD,
    USERNAME,
    MailServerContainer,
    create_unverified_ssl_context,
)
from tests.libs.email_client.emails import PASSWORD_RESET, TEMPLATE_READ_OPTIONS, WORKSPACE_INVITATION, read_template
from tests.libs.email_client.fake_app import FakeApp
from tests.libs.email_client.fixtures import email_address, password_reset, workspace_invitation  # noqa: F401

pytest_data_loader.register_reader(".eml", read_template, read_options=TEMPLATE_READ_OPTIONS)

# Override the default fixture with a session-scoped one so that loaded data is cached and shared by all tests
data_loader = make_data_loader_fixture(scope="session")


@pytest.fixture(scope="session")
def mail_server(data_loader: DataLoaderFixture) -> Iterator[MailServerContainer]:
    """A mail server with a mailbox that holds emails of earlier runs, started once per run"""
    with MailServerContainer() as container:
        fake_app = FakeApp(container, data_loader)
        for number, template in enumerate([PASSWORD_RESET, WORKSPACE_INVITATION, PASSWORD_RESET]):
            fake_app.send_email(template, to=f"qa+test-{number}@example.com")
        fake_app.close()
        yield container


@pytest.fixture
def app(mail_server: MailServerContainer, data_loader: DataLoaderFixture) -> Iterator[FakeApp]:
    """The web application under test, which sends emails to the mailbox"""
    fake_app = FakeApp(mail_server, data_loader)
    yield fake_app
    fake_app.close()


@pytest.fixture
def client_factory(mail_server: MailServerContainer) -> Callable[..., EmailClient]:
    """Create a client for the mailbox of the mail server over IMAPS. Keyword arguments override the defaults

    The mail server has a self-signed certificate, so the default SSL context accepts it.
    """

    def _make_client(**kwargs: Any) -> EmailClient:
        params: dict[str, Any] = {
            "host": HOST,
            "username": USERNAME,
            "auth": PasswordAuth(PASSWORD),
            "port": mail_server.host_port(IMAPS_PORT),
            "ssl_context": create_unverified_ssl_context(),
            "timeout": 10.0,
            "poll_interval": 0.1,
        }
        return EmailClient(**params | kwargs)

    return _make_client


@pytest.fixture
def client(client_factory: Callable[..., EmailClient]) -> EmailClient:
    """A client for the mailbox of the mail server"""
    return client_factory()
