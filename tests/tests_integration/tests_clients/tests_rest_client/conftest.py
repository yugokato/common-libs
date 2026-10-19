"""Shared pytest fixtures for REST client integration tests"""

import ssl
import uuid
from collections.abc import AsyncIterator, Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from pytest_mock import MockerFixture

from common_libs.clients.rest_client import AsyncRestClient, RestClient
from common_libs.clients.rest_client import rate_limit as rate_limit_module
from common_libs.clients.rest_client import retry as retry_module
from tests.libs.common.fake_clock import FakeClock
from tests.libs.containers.http3_proxy import Http3ProxyContainer
from tests.libs.containers.mock_server import MockServerContainer

CLIENT_TIMEOUT = 10.0


@pytest.fixture
def fake_clock(mocker: MockerFixture) -> FakeClock:
    """A clock that replaces the real one in the retry policy and the rate limiter, so that waits take no time and
    their sleeps can be checked
    """
    clock = FakeClock()
    mocker.patch.object(retry_module, "time", new=clock)
    mocker.patch.object(rate_limit_module, "time", new=clock)
    return clock


@pytest.fixture(scope="session")
def mock_server() -> Iterator[MockServerContainer]:
    """A mock server, started once per run"""
    with MockServerContainer() as container:
        yield container


@pytest.fixture(scope="session")
def http3_proxy(mock_server: MockServerContainer) -> Iterator[Http3ProxyContainer]:
    """A proxy that serves HTTP/3 in front of the mock server, started when the first HTTP/3 test needs it"""
    with Http3ProxyContainer(mock_server) as container:
        yield container


@pytest.fixture(autouse=True)
def _reset_mock_server(mock_server: MockServerContainer) -> None:
    """Remove the expectations and the received requests of the previous test"""
    mock_server.reset()


@pytest.fixture
def path() -> str:
    """A request path that is unique to the test, so that a late request of another test can not be mistaken for it"""
    return f"/{uuid.uuid4().hex[:12]}"


@pytest.fixture(scope="session")
def mock_server_ca_file(mock_server: MockServerContainer, tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The file with the certificate authority that signed the HTTPS certificate of the mock server"""
    ca_file = tmp_path_factory.mktemp("mock-server") / "ca.pem"
    ca_file.write_text(mock_server.ca_certificate())
    return ca_file


@pytest.fixture(scope="session")
def http3_ca_file(http3_proxy: Http3ProxyContainer, tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The file with the certificate authority that signed the certificate of the HTTP/3 proxy"""
    ca_file = tmp_path_factory.mktemp("http3-proxy") / "ca.pem"
    ca_file.write_text(http3_proxy.ca_certificate())
    return ca_file


@pytest.fixture
def ssl_context(mock_server_ca_file: Path) -> ssl.SSLContext:
    """A context that trusts the mock server. It is not shared, because httpx2 changes its ALPN protocols"""
    return ssl.create_default_context(cafile=str(mock_server_ca_file))


@pytest.fixture
def client_factory(
    mock_server: MockServerContainer, ssl_context: ssl.SSLContext
) -> Iterator[Callable[..., RestClient]]:
    """Create a sync client for the mock server over HTTPS. Keyword arguments override the defaults

    The retry policy is off by default, because the default policy waits seconds between the attempts. Every client is
    closed after the test.
    """
    clients: list[RestClient] = []

    def _make_client(**kwargs: Any) -> RestClient:
        options = {"base_url": mock_server.url("https"), "retry_policy": None, "timeout": CLIENT_TIMEOUT}
        options |= {"verify": ssl_context} | kwargs
        client = RestClient(options.pop("base_url"), **options)
        clients.append(client)
        return client

    yield _make_client
    for client in clients:
        client.close()


@pytest.fixture
async def async_client_factory(
    mock_server: MockServerContainer, ssl_context: ssl.SSLContext
) -> AsyncIterator[Callable[..., AsyncRestClient]]:
    """Create an async client for the mock server over HTTPS. Keyword arguments override the defaults

    The retry policy is off by default, because the default policy waits seconds between the attempts. Every client is
    closed after the test.
    """
    clients: list[AsyncRestClient] = []

    def _make_client(**kwargs: Any) -> AsyncRestClient:
        options = {"base_url": mock_server.url("https"), "retry_policy": None, "timeout": CLIENT_TIMEOUT}
        options |= {"verify": ssl_context} | kwargs
        client = AsyncRestClient(options.pop("base_url"), **options)
        clients.append(client)
        return client

    yield _make_client
    for client in clients:
        await client.close()
