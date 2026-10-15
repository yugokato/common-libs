"""Shared pytest fixtures for database client integration tests"""

import uuid
from collections.abc import Callable, Iterator
from typing import Any

import pytest
import redis

from common_libs.clients.database.postgresql import PostgreSQLClient
from common_libs.clients.database.redis import RedisClient
from tests.libs.containers.postgresql_server import PostgreSQLContainer
from tests.libs.containers.redis_server import RedisContainer

TABLE_COLUMNS = "id serial PRIMARY KEY, doc jsonb, tags int[], amount numeric, uid uuid, label text"


@pytest.fixture(scope="session")
def postgresql_server() -> Iterator[PostgreSQLContainer]:
    """A PostgreSQL server, started once per run when the first test needs it"""
    with PostgreSQLContainer() as container:
        yield container


@pytest.fixture(scope="session")
def redis_server() -> Iterator[RedisContainer]:
    """A Redis server, started once per run when the first test needs it"""
    with RedisContainer() as container:
        yield container


@pytest.fixture
def pg_client_factory(postgresql_server: PostgreSQLContainer) -> Iterator[Callable[..., PostgreSQLClient]]:
    """Create a client for the PostgreSQL server. Keyword arguments override the connection options

    Every client is disconnected after the test.
    """
    clients: list[PostgreSQLClient] = []

    def _make_client(**kwargs: Any) -> PostgreSQLClient:
        client = PostgreSQLClient(**postgresql_server.client_options | kwargs)
        clients.append(client)
        return client

    yield _make_client
    for client in clients:
        client.disconnect()


@pytest.fixture
def pg_client(pg_client_factory: Callable[..., PostgreSQLClient]) -> PostgreSQLClient:
    """A client for the PostgreSQL server"""
    return pg_client_factory()


@pytest.fixture
def execute_sql(pg_client: PostgreSQLClient) -> Callable[[str], None]:
    """Execute a statement without a result, such as DDL, through the client"""

    def _execute(statement: str) -> None:
        with pg_client.get_connection() as connection, pg_client.get_cursor(connection) as cursor:
            cursor.execute(statement)

    return _execute


@pytest.fixture
def table(execute_sql: Callable[[str], None]) -> Iterator[str]:
    """The name of a table that has a column for each type the client customizes adapters for"""
    name = f"t_{uuid.uuid4().hex[:12]}"
    execute_sql(f"CREATE TABLE {name} ({TABLE_COLUMNS})")
    yield name
    execute_sql(f"DROP TABLE {name}")


@pytest.fixture
def schema(execute_sql: Callable[[str], None]) -> Iterator[str]:
    """The name of an empty schema"""
    name = f"s_{uuid.uuid4().hex[:12]}"
    execute_sql(f"CREATE SCHEMA {name}")
    yield name
    execute_sql(f"DROP SCHEMA {name} CASCADE")


@pytest.fixture
def redis_client_factory(redis_server: RedisContainer) -> Callable[..., RedisClient]:
    """Create a client for the Redis server. Keyword arguments override the connection options"""

    def _make_client(**kwargs: Any) -> RedisClient:
        return RedisClient(**redis_server.client_options | kwargs)

    return _make_client


@pytest.fixture
def redis_client(redis_client_factory: Callable[..., RedisClient]) -> RedisClient:
    """A client for the Redis server"""
    return redis_client_factory()


@pytest.fixture
def redis_db(redis_client: RedisClient) -> redis.Redis:
    """The connection of the Redis client, to write test data with"""
    assert redis_client.db is not None
    return redis_client.db


@pytest.fixture
def key_prefix(redis_client: RedisClient, redis_db: redis.Redis) -> Iterator[str]:
    """A key prefix that is unique to the test. The keys that start with it are deleted after the test"""
    prefix = f"{uuid.uuid4().hex[:12]}:"
    yield prefix
    if keys := redis_client.scan_keys(f"{prefix}*"):
        redis_db.delete(*keys)
