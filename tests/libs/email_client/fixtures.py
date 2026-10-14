"""Fixtures shared by the email client unit and integration tests

The conftest that imports the spec fixtures must define the session-scoped `data_loader` fixture they depend on.
"""

import pytest
from pytest_data_loader import DataLoaderFixture

from tests.libs.email_client.addresses import generate_email_address
from tests.libs.email_client.emails import PASSWORD_RESET, WORKSPACE_INVITATION, EmailSpec, get_spec_path


@pytest.fixture
def email_address() -> str:
    """A unique address to wait for emails at"""
    return generate_email_address()


@pytest.fixture(scope="session")
def password_reset(data_loader: DataLoaderFixture) -> EmailSpec:
    """The spec of the password reset email"""
    return _load_spec(data_loader, PASSWORD_RESET)


@pytest.fixture(scope="session")
def workspace_invitation(data_loader: DataLoaderFixture) -> EmailSpec:
    """The spec of the workspace invitation email"""
    return _load_spec(data_loader, WORKSPACE_INVITATION)


def _load_spec(data_loader: DataLoaderFixture, name: str) -> EmailSpec:
    return data_loader(get_spec_path(name), onload=EmailSpec.from_json)
