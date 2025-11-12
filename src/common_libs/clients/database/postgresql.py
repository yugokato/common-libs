from __future__ import annotations

import os
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from functools import wraps
from typing import Any, ParamSpec, TypeVar, cast

import psycopg
import tabulate
from psycopg import ClientCursor, Cursor
from psycopg.abc import Query
from psycopg.connection import Connection
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool, PoolClosed, PoolTimeout, TooManyRequests

from common_libs.decorators import singleton
from common_libs.logging import get_logger
from common_libs.signals import register_exit_handler

R = TypeVar("R")
P = ParamSpec("P")

logger = get_logger(__name__)


MAX_CONNECTIONS = 50


def cursor_factory(client: PostgreSQLClient, logging: bool = False) -> Callable[..., Any]:
    """Returns a custom cursor factory"""

    class Cursor(ClientCursor):
        """Custom cursor that adds ability to enable SQL query logging"""

        def __init__(self, client: PostgreSQLClient, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self.client = client

        def execute(self, sql: str, vars: Sequence[Any] | None = None) -> None:
            client_name = self.client.name.capitalize()
            if isinstance(vars, tuple):
                vars = list(vars)
            query = self.client._generate_query(self, sql, vars=vars)
            if logging:
                logger.debug(f"[{client_name}] {query}")
            try:
                resp = super().execute(cast(Query, sql), vars)
                if query.upper().startswith(("INSERT", "UPDATE", "DELETE")):
                    logger.debug(f"[{client_name}] Affected row count: {self.rowcount}")
                return resp
            except Exception as e:
                logger.error(
                    f"Encountered an error during the query execution:\n"
                    f"- Exception: {type(e).__name__}: {e!s}\n"
                    f"- Query: '{query}'"
                )
                raise

    def create_cursor(*args: Any, **kwargs: Any) -> Cursor:
        return Cursor(client, *args, **kwargs)

    return create_cursor


def check_connection(f: Callable[P, R]) -> Callable[P, R]:
    @wraps(f)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        self = cast(PostgreSQLClient, args[0])
        if not kwargs.get("connection") or self._connection_pool is None:
            self.connect()
        try:
            return f(*args, **kwargs)
        except (psycopg.OperationalError, psycopg.InterfaceError) as e:
            if isinstance(e, PoolTimeout | PoolClosed) or "EOF detected" in str(e):
                time.sleep(3)
                logger.warning("Reconnecting...")
                self.disconnect()
                self.connect()
                return f(*args, **kwargs)
            else:
                raise

    return wrapper


@singleton
class PostgreSQLClient:
    """PostgreSQL Client"""

    name = "postgresql"

    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int = 5432,
        db_name: str,
        user: str,
        password: str,
        connect: bool = False,
        autocommit: bool = True,
        statement_timeout_seconds: int = 60,
    ):
        """Initialize the client

        :param host: Host
        :param port: Port
        :param db_name: Database name
        :param user: Username
        :param password: Passwork
        :param connect: Connect to the DB
        :param autocommit: Enable autocommit
        :param statement_timeout_seconds: Abort a statement when it takes more than the specified amount of time
        """
        logger.info(f"Initializing {self.name} client...")
        self.db_name = db_name
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dsn = f"postgres://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"
        self.autocommit = autocommit
        self.statement_timeout = (statement_timeout_seconds or 0) * 1000
        self._connection_pool: ConnectionPool | None = None

        if connect:
            self.connect()

    def connect(self) -> None:
        """Connect to database"""
        if self._connection_pool is None:
            logger.info(f"Connecting to {self.db_name.capitalize()} as '{self.user}': {self.host}:{self.port}")
            self._connection_pool = ConnectionPool(
                self.dsn,
                min_size=1,
                max_size=MAX_CONNECTIONS,
                max_waiting=5,
                kwargs=dict(options=f"-c statement_timeout={self.statement_timeout}"),
            )
            register_exit_handler(self.disconnect)

    def disconnect(self) -> None:
        """Disconnect from database"""
        if self._connection_pool is not None:
            logger.info(f"Disconnecting from {self.db_name.capitalize()}...")
            self._connection_pool.close()
            self._connection_pool = None

    @contextmanager
    def transaction(self, existing_onnection: Connection = None) -> Iterator[Connection]:
        """Start a transaction

        :param existing_onnection: Existing connection to reuse
        """
        with self.get_connection(existing_onnection) as conn:
            with conn.transaction():
                yield conn

    @contextmanager
    def get_connection(self, existing_connection: Connection | None = None) -> Iterator[Connection]:
        """Get a connection from the connection pool, and return it to the pool at the end

        :param existing_connection: Existing connection to reuse
        """
        if existing_connection:
            yield existing_connection
        else:
            if self._connection_pool is None:
                self.connect()
            assert self._connection_pool is not None
            max_retry = 10
            count = 0
            while count < max_retry:
                try:
                    with self._connection_pool.connection() as conn:
                        conn.set_autocommit(self.autocommit)
                        yield conn
                        break
                except TooManyRequests:
                    if count + 1 == max_retry:
                        raise
                    else:
                        logger.warning(f"Exceeded max connections ({MAX_CONNECTIONS}). Retrying...")
                        time.sleep(1)
                count += 1

    @contextmanager
    def get_cursor(
        self, connection: Connection, /, *, logging: bool = False, row_factory: Callable[..., Any] = dict_row
    ) -> Iterator[Cursor]:
        """Get a custom cursor for the given connection

        :param connection: An existing connection to use
        :param logging: Enable SQL query logging
        :param: row_factory: Custom row factory to use for the cursor
        """
        with cursor_factory(self, logging=logging)(connection, row_factory=row_factory) as cur:
            yield cur

    @check_connection
    def SELECT(
        self,
        query: str,
        vars: Sequence[Any] | None = None,
        connection: Connection | None = None,
        print_table: bool = False,
        logging: bool = False,
    ) -> list[dict[str, Any]]:
        """Execute SELECT query"""
        if os.getenv("ENABLE_SQL_SELECT_QUERY_LOGGING", "false").lower() in ["true", "1"]:
            logging = True
        if not query.strip().upper().startswith("SELECT"):
            query = "SELECT " + query

        with self.get_connection(connection) as conn:
            with self.get_cursor(conn, logging=logging) as cursor:
                cursor.execute(cast(Query, query), vars)
                rows = cursor.fetchall()
                if print_table:
                    print(tabulate.tabulate(rows, headers="keys", tablefmt="presto"))  # noqa: T201
                return [dict(x) for x in rows]

    @check_connection
    def DELETE(
        self,
        query: str,
        vars: Sequence[Any] | None = None,
        connection: Connection | None = None,
        logging: bool = True,
    ) -> int | tuple[Any, ...]:
        """Execute DELETE query"""
        if not query.strip().upper().startswith("DELETE"):
            query = "DELETE " + query
        with self.get_connection(connection) as conn:
            with self.get_cursor(conn, logging=logging) as cursor:
                cursor.execute(cast(Query, query), vars)
                if " RETURNING " in query.upper():
                    resp = cursor.fetchone()
                    return next(iter(resp.values())) if len(resp) == 1 else tuple(resp.values())
                else:
                    return cursor.rowcount

    @check_connection
    def UPDATE(
        self,
        query: str,
        vars: Sequence[Any] | None = None,
        connection: Connection | None = None,
        logging: bool = True,
    ) -> int | tuple[Any, ...]:
        """Execute UPDATE query"""
        if not query.strip().upper().startswith("UPDATE"):
            query = "UPDATE " + query
        with self.get_connection(connection) as conn:
            with self.get_cursor(conn, logging=logging) as cursor:
                cursor.execute(cast(Query, query), vars)
                if " RETURNING " in query.upper():
                    resp = cursor.fetchone()
                    return next(iter(resp.values())) if len(resp) == 1 else tuple(resp.values())
                else:
                    return cursor.rowcount

    @check_connection
    def INSERT(
        self,
        query: str,
        vars: Sequence[Any] | None = None,
        connection: Connection | None = None,
        logging: bool = True,
    ) -> int | tuple[Any, ...]:
        """Execute INSERT query"""
        if not query.strip().upper().startswith("INSERT"):
            query = "INSERT " + query
        with self.get_connection(connection) as conn:
            with self.get_cursor(conn, logging=logging) as cursor:
                cursor.execute(cast(Query, query), vars)
                if " RETURNING " in query.upper():
                    resp = cursor.fetchone()
                    return next(iter(resp.values())) if len(resp) == 1 else tuple(resp.values())
                else:
                    return cursor.rowcount

    @check_connection
    def show_tables(
        self,
        schema_names: str | Sequence[str] | None = None,
        columns_to_select: str | Sequence[str] | None = None,
        return_result: bool = False,
        **kwargs: Any,
    ) -> list[dict[str, Any]] | None:
        """Show tables"""
        if columns_to_select:
            if isinstance(columns_to_select, str):
                columns = columns_to_select
            else:
                columns = ", ".join(columns_to_select)
        else:
            columns = "*"

        sql = f"SELECT {columns} FROM pg_catalog.pg_tables WHERE schemaname"
        if schema_names:
            sql += " = ANY(%s)"
            if isinstance(schema_names, str):
                vars = [schema_names]
            else:
                vars = list(schema_names)
        else:
            sql += " <> ALL(%s)"
            vars = ["pg_catalog", "information_schema", "public"]

        # sort
        col_schema_name = "schemaname"
        col_table_name = "tablename"
        if any(c in columns for c in [col_schema_name, col_table_name]):
            sql += " ORDER BY "
            if col_table_name in columns and col_table_name in columns:
                sql += ", ".join([col_schema_name, col_table_name])
            elif col_schema_name in columns:
                sql += col_schema_name
            else:
                sql += col_table_name

        tables = self.SELECT(sql, vars=[vars], print_table=True, **kwargs)
        if return_result:
            return tables

        return None

    @check_connection
    def show_function_definition(self, func_name: str) -> None:
        """Show user-defined function definition"""
        self.SELECT("SELECT prosrc FROM pg_proc WHERE proname=%s", vars=(func_name,), print_table=True)

    def _generate_query(self, cursor: Cursor | ClientCursor, sql: str, vars: Sequence[Any] | None = None) -> str:
        """Generates a query string after arguments binding"""
        try:
            query = cursor.mogrify(cast(Query, sql), vars).strip()
        except Exception as e:
            logger.error(
                f"Failed to generate a query with given vars:\n"
                f"- query: {sql}\n"
                f"- vars: {vars}\n"
                f"- Exception: {type(e)}\n{e!s}"
            )
            raise

        if not query.endswith(";"):
            query += ";"
        return query


def customize_adapters() -> None:
    """Customize psycopg adapters for specific types"""
    # Dumpers
    psycopg.adapters.register_dumper(dict, psycopg.types.json.JsonbDumper)
    psycopg.adapters.register_dumper(set, psycopg.types.array.ListDumper)

    # Loaders
    psycopg.adapters.register_loader("uuid", psycopg.types.string.TextLoader)
    psycopg.adapters.register_loader("numeric", psycopg.types.numeric.FloatLoader)


customize_adapters()
