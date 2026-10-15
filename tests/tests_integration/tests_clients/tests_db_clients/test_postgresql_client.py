"""Integration tests for common_libs.clients.database.postgresql module"""

import logging
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import psycopg
import pytest

from common_libs.clients.database.postgresql import PostgreSQLClient
from tests.libs.containers.postgresql_server import XDIST_GROUP

pytestmark = [pytest.mark.integrationtest, pytest.mark.xdist_group(XDIST_GROUP)]

THREADS = 8
QUERIES = 32


class TestQueries:
    """Tests for the SELECT(), INSERT(), UPDATE() and DELETE() methods of PostgreSQLClient against a real server"""

    def test_select_returns_rows_as_dicts(self, pg_client: PostgreSQLClient, table: str) -> None:
        """Test that SELECT() returns the rows as dicts, and adds the missing SELECT keyword"""
        pg_client.INSERT(f"INTO {table} (label) VALUES (%s), (%s)", vars=("b", "a"))

        assert pg_client.SELECT(f"label FROM {table} ORDER BY label") == [{"label": "a"}, {"label": "b"}]

    def test_select_binds_parameters(self, pg_client: PostgreSQLClient, table: str) -> None:
        """Test that SELECT() binds the parameters into the query"""
        pg_client.INSERT(f"INTO {table} (label) VALUES (%s), (%s)", vars=("a", "b"))

        assert pg_client.SELECT(f"label FROM {table} WHERE label = %s", vars=("b",)) == [{"label": "b"}]

    def test_select_prints_a_table_on_request(self, pg_client: PostgreSQLClient, capsys: pytest.CaptureFixture) -> None:
        """Test that SELECT(print_table=True) prints the rows as a table"""
        pg_client.SELECT("42 AS answer", print_table=True)

        assert _printed_cells(capsys.readouterr().out) == ["answer", "42"]

    @pytest.mark.parametrize("with_keyword", [False, True], ids=["without-keyword", "with-keyword"])
    @pytest.mark.parametrize(
        ("method", "statement", "expected_row_count"),
        [
            ("INSERT", "INTO {table} (label) VALUES ('c')", 1),
            ("UPDATE", "{table} SET label = 'z' WHERE label IN ('a', 'b')", 2),
            ("DELETE", "FROM {table} WHERE label = 'a'", 1),
        ],
    )
    def test_modifying_methods_return_the_row_count(
        self,
        pg_client: PostgreSQLClient,
        table: str,
        method: str,
        statement: str,
        expected_row_count: int,
        with_keyword: bool,
    ) -> None:
        """Test that INSERT(), UPDATE() and DELETE() return the row count, with or without the statement keyword"""
        pg_client.INSERT(f"INTO {table} (label) VALUES (%s), (%s)", vars=("a", "b"))
        query = statement.format(table=table)
        if with_keyword:
            query = f"{method} {query}"

        assert getattr(pg_client, method)(query) == expected_row_count

    @pytest.mark.parametrize(
        ("columns", "expected"), [("label", "a"), ("label, id", ("a", 1))], ids=["one-column", "two-columns"]
    )
    def test_insert_returning_gives_a_value_or_a_tuple(
        self, pg_client: PostgreSQLClient, table: str, columns: str, expected: str | tuple[str, int]
    ) -> None:
        """Test that INSERT() returns the value of a single RETURNING column, and a tuple of several"""
        result = pg_client.INSERT(f"INTO {table} (label) VALUES (%s) RETURNING {columns}", vars=("a",))

        assert result == expected

    @pytest.mark.parametrize(
        ("method", "statement", "expected"),
        [
            ("UPDATE", "{table} SET label = 'b' RETURNING label", "b"),
            ("UPDATE", "{table} SET label = 'b' RETURNING label, id", ("b", 1)),
            ("DELETE", "FROM {table} RETURNING label", "a"),
            ("DELETE", "FROM {table} RETURNING label, id", ("a", 1)),
        ],
    )
    def test_update_and_delete_returning_give_a_value_or_a_tuple(
        self, pg_client: PostgreSQLClient, table: str, method: str, statement: str, expected: str | tuple[str, int]
    ) -> None:
        """Test that UPDATE() and DELETE() return the value of a single RETURNING column, and a tuple of several"""
        pg_client.INSERT(f"INTO {table} (label) VALUES (%s)", vars=("a",))

        assert getattr(pg_client, method)(statement.format(table=table)) == expected


class TestAdapters:
    """Tests for the customized psycopg adapters of PostgreSQLClient against a real server"""

    def test_python_types_round_trip_through_columns(self, pg_client: PostgreSQLClient, table: str) -> None:
        """Test that a dict, a set, a float and a UUID are stored as jsonb, an array, numeric and uuid and read back"""
        uid = uuid.uuid4()
        pg_client.INSERT(
            f"INTO {table} (doc, tags, amount, uid) VALUES (%s, %s, %s, %s)",
            vars=({"a": [1, {"b": None}]}, {3, 1, 2}, 12.5, uid),
        )

        row = pg_client.SELECT(f"doc, tags, amount, uid FROM {table}")[0]

        assert (row["doc"], sorted(row["tags"]), row["amount"], row["uid"]) == (
            {"a": [1, {"b": None}]},
            [1, 2, 3],
            12.5,
            str(uid),
        )


class TestTransaction:
    """Tests for PostgreSQLClient.transaction() against a real server"""

    def test_transaction_commits_the_changes(self, pg_client: PostgreSQLClient, table: str) -> None:
        """Test that the changes made on the connection of a transaction are committed when it ends"""
        with pg_client.transaction() as connection:
            pg_client.INSERT(f"INTO {table} (label) VALUES (%s)", vars=("a",), connection=connection)

        assert pg_client.SELECT(f"label FROM {table}") == [{"label": "a"}]

    def test_transaction_rolls_back_when_a_statement_fails(self, pg_client: PostgreSQLClient, table: str) -> None:
        """Test that an earlier change of a transaction is rolled back when a later statement of it fails"""
        with pytest.raises(psycopg.errors.UndefinedColumn, match="no_such_column"):
            with pg_client.transaction() as connection:
                pg_client.INSERT(f"INTO {table} (label) VALUES (%s)", vars=("a",), connection=connection)
                pg_client.INSERT(f"INTO {table} (no_such_column) VALUES (%s)", vars=("b",), connection=connection)

        assert pg_client.SELECT(f"label FROM {table}") == []


class TestShowTables:
    """Tests for PostgreSQLClient.show_tables() and show_function_definition() against a real server"""

    def test_show_tables_lists_other_schemas_but_hides_public_by_default(
        self, pg_client: PostgreSQLClient, execute_sql: Callable[[str], None], table: str, schema: str
    ) -> None:
        """Test that the tables of the public schema are not listed unless the schema is given, but others are"""
        execute_sql(f"CREATE TABLE {schema}.orders (id int)")

        tables = pg_client.show_tables(return_result=True)

        listed = {(row["schemaname"], row["tablename"]) for row in tables or []}
        assert (schema, "orders") in listed
        assert ("public", table) not in listed

    def test_show_tables_lists_the_tables_of_a_schema(
        self, pg_client: PostgreSQLClient, execute_sql: Callable[[str], None], schema: str
    ) -> None:
        """Test that the tables of the given schema are returned with the selected columns only"""
        execute_sql(f"CREATE TABLE {schema}.orders (id int)")
        execute_sql(f"CREATE TABLE {schema}.customers (id int)")

        tables = pg_client.show_tables(
            schema_names=schema, columns_to_select=["schemaname", "tablename"], return_result=True
        )

        assert tables == [
            {"schemaname": schema, "tablename": "customers"},
            {"schemaname": schema, "tablename": "orders"},
        ]

    def test_show_tables_prints_the_tables_and_returns_nothing_by_default(
        self,
        pg_client: PostgreSQLClient,
        execute_sql: Callable[[str], None],
        schema: str,
        capsys: pytest.CaptureFixture,
    ) -> None:
        """Test that show_tables() prints the tables as a table, and returns nothing unless return_result is set"""
        execute_sql(f"CREATE TABLE {schema}.orders (id int)")

        result = pg_client.show_tables(schema_names=schema, columns_to_select="tablename")

        assert (result, _printed_cells(capsys.readouterr().out)) == (None, ["tablename", "orders"])

    def test_show_function_definition_prints_the_source(
        self, pg_client: PostgreSQLClient, execute_sql: Callable[[str], None], capsys: pytest.CaptureFixture
    ) -> None:
        """Test that show_function_definition() prints the source of a user-defined function"""
        name = f"f_{uuid.uuid4().hex[:12]}"
        execute_sql(f"CREATE FUNCTION {name}() RETURNS int LANGUAGE sql AS 'SELECT 42'")
        try:
            pg_client.show_function_definition(name)
        finally:
            execute_sql(f"DROP FUNCTION {name}")

        assert _printed_cells(capsys.readouterr().out) == ["prosrc", "SELECT", "42"]


class TestConnectionHandling:
    """Tests for how PostgreSQLClient handles its connections and logging against a real server"""

    def test_query_logging_shows_the_bound_query_and_the_affected_row_count(
        self, pg_client: PostgreSQLClient, table: str, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that the bound query and the number of affected rows are logged for a modifying query"""
        caplog.set_level(logging.DEBUG)

        pg_client.INSERT(f"INTO {table} (label) VALUES (%s)", vars=("a",))

        assert f"INSERT INTO {table} (label) VALUES ('a');" in caplog.text
        assert "Affected row count: 1" in caplog.text

    def test_query_after_disconnect_builds_a_new_pool(self, pg_client: PostgreSQLClient) -> None:
        """Test that a query after disconnect() connects again"""
        pg_client.SELECT("1 AS one")
        pg_client.disconnect()

        assert pg_client.SELECT("1 AS one") == [{"one": 1}]

    def test_concurrent_queries_share_the_pool(self, pg_client: PostgreSQLClient, table: str) -> None:
        """Test that queries from several threads are all executed, over fewer connections than queries"""
        pg_client.connect()

        def insert(_: int) -> int | tuple[Any, ...]:
            return pg_client.INSERT(f"INTO {table} (label) VALUES (pg_backend_pid()::text)")

        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            row_counts = list(executor.map(insert, range(QUERIES)))

        stats = pg_client.SELECT(f"count(*) AS total, count(DISTINCT label) AS backends FROM {table}")[0]
        assert (row_counts, stats["total"]) == ([1] * QUERIES, QUERIES)
        assert stats["backends"] < QUERIES


def _printed_cells(output: str) -> list[str]:
    """Return the words of a printed table, without the line that separates the header from the rows"""
    return [word for word in output.split() if set(word) != {"-"}]
