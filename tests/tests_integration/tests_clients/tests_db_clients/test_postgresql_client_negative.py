"""Negative integration tests for common_libs.clients.database.postgresql module"""

from collections.abc import Callable

import psycopg
import pytest

from common_libs.clients.database.postgresql import PostgreSQLClient
from tests.libs.containers.postgresql_server import XDIST_GROUP

pytestmark = [pytest.mark.integrationtest, pytest.mark.xdist_group(XDIST_GROUP)]

SLEEP_SECONDS = 3
STATEMENT_TIMEOUT_SECONDS = 1


class TestPostgreSQLClientNegative:
    """Tests for PostgreSQLClient against a real server when a query fails"""

    def test_unknown_table_raises_and_logs_the_failed_query(
        self, pg_client: PostgreSQLClient, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that a query on a table that does not exist raises the server's error and logs the query"""
        with pytest.raises(psycopg.errors.UndefinedTable, match="does_not_exist"):
            pg_client.SELECT("* FROM does_not_exist")

        assert "Encountered an error during the query execution" in caplog.text
        assert "SELECT * FROM does_not_exist;" in caplog.text

    def test_statement_timeout_cancels_a_long_running_query(
        self, pg_client_factory: Callable[..., PostgreSQLClient]
    ) -> None:
        """Test that a query that runs longer than statement_timeout_seconds is cancelled by the server"""
        client = pg_client_factory(statement_timeout_seconds=STATEMENT_TIMEOUT_SECONDS)

        with pytest.raises(psycopg.errors.QueryCanceled, match="statement timeout"):
            client.SELECT(f"pg_sleep({SLEEP_SECONDS})")

    def test_wrong_number_of_parameters_raises_and_logs_the_query(
        self, pg_client: PostgreSQLClient, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that a query with fewer parameters than placeholders raises and logs the query and the parameters"""
        with pytest.raises(psycopg.ProgrammingError):
            pg_client.SELECT("%s AS first, %s AS second", vars=(1,))

        assert "Failed to generate a query with given vars" in caplog.text
        assert "%s AS first, %s AS second" in caplog.text
