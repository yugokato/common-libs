"""Shared pytest fixtures for database client tests"""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockFixture

from common_libs.clients.database.postgresql import PostgreSQLClient


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


@pytest.fixture
def mock_pg_client(
    mocker: MockFixture, request: pytest.FixtureRequest
) -> tuple[PostgreSQLClient, MagicMock, MagicMock]:
    """Set up a PostgreSQLClient with mocked connection and cursor.

    Uses the test name to generate a unique host, avoiding singleton collisions
    between tests that share this fixture.
    """
    client = PostgreSQLClient(
        host=f"pg-query-{request.node.name}",
        port=5432,
        db_name="querydb",
        user="q_user",
        password="q_pass",
        connect=True,
    )

    mock_conn: MagicMock = mocker.MagicMock()
    mock_cursor: MagicMock = mocker.MagicMock()

    @contextmanager
    def mock_get_connection(existing_connection: Any = None) -> Iterator[Any]:
        yield mock_conn

    @contextmanager
    def mock_get_cursor(connection: Any, /, *, logging: bool = False, row_factory: Any = None) -> Iterator[Any]:
        yield mock_cursor

    mocker.patch.object(client, "get_connection", side_effect=mock_get_connection)
    mocker.patch.object(client, "get_cursor", side_effect=mock_get_cursor)

    return client, mock_conn, mock_cursor
