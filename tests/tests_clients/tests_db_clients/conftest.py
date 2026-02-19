"""Shared pytest fixtures for database client tests"""

from unittest.mock import MagicMock

import pytest
from pytest_mock import MockFixture


@pytest.fixture(autouse=True)
def mock_connection_pool(mocker: MockFixture) -> MagicMock:
    """Patch ConnectionPool in the postgresql module"""
    return mocker.patch("common_libs.clients.database.postgresql.ConnectionPool")


@pytest.fixture(autouse=True)
def mock_pg_register_exit_handler(mocker: MockFixture) -> MagicMock:
    """Patch register_exit_handler in the postgresql module"""
    return mocker.patch("common_libs.clients.database.postgresql.register_exit_handler")


@pytest.fixture(autouse=True)
def mock_redis_class(mocker: MockFixture) -> MagicMock:
    """Patch redis.Redis in the redis module"""
    return mocker.patch("common_libs.clients.database.redis.redis.Redis")
