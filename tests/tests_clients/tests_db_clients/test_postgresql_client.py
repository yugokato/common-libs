"""Tests for common_libs.clients.database.postgresql module"""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock

from pytest_mock import MockFixture

from common_libs.clients.database.postgresql import PostgreSQLClient


class TestPostgreSQLClientInit:
    """Tests for PostgreSQLClient initialization"""

    def test_init_does_not_connect_by_default(self) -> None:
        """Test that connect=False (default) does not create a connection pool"""
        client = PostgreSQLClient(host="pg-no-connect", port=5432, db_name="testdb", user="user_nc", password="pass_nc")
        assert client._connection_pool is None

    def test_init_connects_when_requested(self, mock_connection_pool: MagicMock) -> None:
        """Test that connect=True creates a connection pool on init"""
        client = PostgreSQLClient(
            host="pg-connect", port=5432, db_name="testdb2", user="user_c", password="pass_c", connect=True
        )
        assert client._connection_pool is not None
        mock_connection_pool.assert_called_once()

    def test_init_stores_dsn(self) -> None:
        """Test that DSN is correctly constructed and stored"""
        client = PostgreSQLClient(host="pg-host", port=5432, db_name="mydb", user="myuser", password="mypassword")
        assert "myuser" in client.dsn
        assert "pg-host" in client.dsn
        assert "mydb" in client.dsn

    def test_singleton_same_args_same_instance(self) -> None:
        """Test that same arguments return the same singleton instance"""
        params = dict(host="pg-single", port=5432, db_name="singledb", user="single_u", password="single_p")
        client1 = PostgreSQLClient(**params)
        client2 = PostgreSQLClient(**params)
        assert client1 is client2

    def test_singleton_different_args_different_instance(self) -> None:
        """Test that different arguments create different instances"""
        client1 = PostgreSQLClient(host="pg-diff1", port=5432, db_name="db1", user="user1", password="pass1")
        client2 = PostgreSQLClient(host="pg-diff2", port=5433, db_name="db2", user="user2", password="pass2")
        assert client1 is not client2


class TestPostgreSQLClientConnection:
    """Tests for PostgreSQLClient connect/disconnect"""

    def test_connect_creates_pool(self, mock_connection_pool: MagicMock) -> None:
        """Test that connect() creates a ConnectionPool"""
        client = PostgreSQLClient(host="pg-conn1", port=5432, db_name="conndb1", user="conn_u1", password="conn_p1")
        assert client._connection_pool is None

        client.connect()
        # Verify pool was created
        mock_connection_pool.assert_called_once()
        assert client._connection_pool is not None

    def test_connect_skips_if_already_connected(self, mock_connection_pool: MagicMock) -> None:
        """Test that connect() does not create a new pool if already connected"""
        client = PostgreSQLClient(host="pg-conn2", port=5432, db_name="conndb2", user="conn_u2", password="conn_p2")
        client.connect()
        first_call_count = mock_connection_pool.call_count

        client.connect()  # second call
        assert mock_connection_pool.call_count == first_call_count  # not called again

    def test_disconnect_closes_pool(self, mock_connection_pool: MagicMock) -> None:
        """Test that disconnect() closes the pool and sets it to None"""
        mock_pool = mock_connection_pool.return_value

        client = PostgreSQLClient(host="pg-disc1", port=5432, db_name="discdb1", user="disc_u1", password="disc_p1")
        client.connect()
        client.disconnect()

        mock_pool.close.assert_called_once()
        assert client._connection_pool is None

    def test_disconnect_skips_if_not_connected(self) -> None:
        """Test that disconnect() is a no-op when not connected"""
        client = PostgreSQLClient(host="pg-disc2", port=5432, db_name="discdb2", user="disc_u2", password="disc_p2")
        # Should not raise
        client.disconnect()
        assert client._connection_pool is None


class TestPostgreSQLClientQueries:
    """Tests for PostgreSQLClient query methods"""

    def test_select_executes_query(self, mocker: MockFixture) -> None:
        """Test that SELECT() executes the query and returns rows"""
        row = {"id": 1, "name": "alice"}
        client, _, mock_cursor = self._setup_mock_client(mocker, "pg-select1", "sel_u1", "sel_p1")
        mock_cursor.fetchall.return_value = [row]

        result = client.SELECT("* FROM users")
        assert len(result) == 1
        assert result[0] == row
        mock_cursor.execute.assert_called()

    def test_select_prepends_select_keyword(self, mocker: MockFixture) -> None:
        """Test that SELECT() prepends 'SELECT' if not present"""
        client, _, mock_cursor = self._setup_mock_client(mocker, "pg-select2", "sel_u2", "sel_p2")
        mock_cursor.fetchall.return_value = []

        client.SELECT("id FROM users WHERE id = 1")
        call_args = mock_cursor.execute.call_args[0]
        assert call_args[0].upper().startswith("SELECT")

    def test_delete_executes_query(self, mocker: MockFixture) -> None:
        """Test that DELETE() executes the query and returns rowcount"""
        client, _, mock_cursor = self._setup_mock_client(mocker, "pg-delete1", "del_u1", "del_p1")
        mock_cursor.rowcount = 3

        result = client.DELETE("FROM users WHERE active = false")
        assert result == 3
        mock_cursor.execute.assert_called()

    def test_update_executes_query(self, mocker: MockFixture) -> None:
        """Test that UPDATE() executes the query and returns rowcount"""
        client, _, mock_cursor = self._setup_mock_client(mocker, "pg-update1", "upd_u1", "upd_p1")
        mock_cursor.rowcount = 5

        result = client.UPDATE("users SET active = true WHERE id = 1")
        assert result == 5
        mock_cursor.execute.assert_called()

    def test_insert_executes_query(self, mocker: MockFixture) -> None:
        """Test that INSERT() executes the query and returns rowcount"""
        client, _, mock_cursor = self._setup_mock_client(mocker, "pg-insert1", "ins_u1", "ins_p1")
        mock_cursor.rowcount = 1

        result = client.INSERT("INTO users (name) VALUES ('bob')")
        assert result == 1
        mock_cursor.execute.assert_called()

    def test_delete_with_returning_returns_value(self, mocker: MockFixture) -> None:
        """Test that DELETE with RETURNING clause returns the fetched value"""
        client, _, mock_cursor = self._setup_mock_client(mocker, "pg-delete2", "del_u2", "del_p2")
        mock_cursor.fetchone.return_value = {"id": 42}

        result = client.DELETE("FROM users WHERE id = 42 RETURNING id")
        assert result == 42

    def _setup_mock_client(
        self, mocker: MockFixture, host: str, user: str, password: str
    ) -> tuple[PostgreSQLClient, Any, Any]:
        """Helper to set up a client with mocked connection and cursor"""
        client = PostgreSQLClient(host=host, port=5432, db_name="querydb", user=user, password=password, connect=True)

        mock_conn = mocker.MagicMock()
        mock_cursor = mocker.MagicMock()

        @contextmanager
        def mock_get_connection(existing_connection: Any = None) -> Iterator[Any]:
            yield mock_conn

        @contextmanager
        def mock_get_cursor(connection: Any, /, *, logging: bool = False, row_factory: Any = None) -> Iterator[Any]:
            yield mock_cursor

        mocker.patch.object(client, "get_connection", side_effect=mock_get_connection)
        mocker.patch.object(client, "get_cursor", side_effect=mock_get_cursor)

        return client, mock_conn, mock_cursor
