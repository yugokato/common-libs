"""Shared pytest fixtures for container tests"""

from unittest.mock import MagicMock

import pytest
from pytest_mock import MockFixture


@pytest.fixture(autouse=True)
def mock_docker_from_env(mocker: MockFixture) -> MagicMock:
    """Patch docker.from_env and return the mock docker client with ping() returning True"""
    mock_client = mocker.patch("common_libs.containers.container.docker.from_env").return_value
    mock_client.ping.return_value = True
    return mock_client


@pytest.fixture(autouse=True)
def mock_register_exit_handler(mocker: MockFixture) -> MagicMock:
    """Patch register_exit_handler in the container module"""
    return mocker.patch("common_libs.containers.container.register_exit_handler")


@pytest.fixture
def mock_grpc_stub(mocker: MockFixture) -> MagicMock:
    """Patch grpc.insecure_channel and RuntimeServiceStub, return stub instance"""
    mock_insecure_channel = mocker.patch("common_libs.containers.containerd.grpc.insecure_channel")
    mock_insecure_channel.return_value.__enter__.return_value = mocker.MagicMock()

    mock_stub_class = mocker.patch("common_libs.containers.containerd.RuntimeServiceStub")

    return mock_stub_class.return_value
