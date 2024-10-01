from __future__ import annotations

import os
import time
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from functools import wraps
from select import poll
from typing import Any

import psycopg2
import psycopg2.extensions
import psycopg2.extras
import tabulate
from psycopg2.extensions import POLL_OK, POLL_READ, POLL_WRITE
from psycopg2.extensions import connection as Connection
from psycopg2.pool import ThreadedConnectionPool

from common_libs.decorators import singleton
from common_libs.logging import get_logger
from common_libs.signals import register_exit_handler

logger = get_logger(__name__)


MAX_CONNECTIONS = 50


def get_cursor_factory(client: PostgreSQLClient, logging: bool = False):
    """Returns a custom cursor factory"""

    class Cursor(psycopg2.extras.RealDictCursor):
        """Custom cursor that adds the following capabilities:

        - Returns each row as a dictionary
        - SQL query logging

        https://www.psycopg.org/docs/advanced.html#connection-and-cursor-factories
        """

        def __init__(self, client: PostgreSQLClient, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.client = client

        def execute(self, sql: str, vars: Sequence[Any] = None):
            client_name = self.client.name.capitalize()
            query = self.client._generate_query(self, sql, vars=vars)
            if logging:
                logger.debug(f"[{client_name}] {query}")
            try:
                resp = super().execute(sql, vars=vars)
                if query.upper().startswith(("INSERT", "UPDATE", "DELETE")):
                    logger.debug(f"[{client_name}] Affected row count: {self.rowcount}")
                return resp
            except Exception as e:
                logger.error(
                    f"Encountered an error during the query execution:\n"
                    f"- Exception: {type(e).__name__}: {str(e)}\n"
                    f"- Query: '{query}'"
                )
                raise

    def create_cursor(*args, **kwargs):
        return Cursor(client, *args, **kwargs)

    return create_cursor


def check_connection(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        self: PostgreSQLClient = args[0]
        if not kwargs.get("connection") or self._connection_pool is None:
            self.connect()
        try:
            return f(*args, **kwargs)
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            if any(err in str(e) for err in ["Operation timed out", "connection already closed", "EOF detected"]):
                time.sleep(3)
                logger.warning("Reconnecting...")
                self.conn = None
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
        self.autocommit = autocommit
        self.statement_timeout = (statement_timeout_seconds or 0) * 1000
        self._connection_pool: ThreadedConnectionPool | None = None

        if connect:
            self.connect()

    def connect(self):
        """Connect to database"""
        if self._connection_pool is None:
            logger.info(f"Connecting to {self.db_name.capitalize()} as '{self.user}': {self.host}:{self.port}")
            self._connection_pool = ThreadedConnectionPool(
                minconn=1,
                maxconn=MAX_CONNECTIONS,
                dbname=self.db_name,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                connect_timeout=5,
                options=f"-c statement_timeout={self.statement_timeout}",
            )
            register_exit_handler(self.disconnect)

    def disconnect(self):
        """Disconnect from database"""
        if self._connection_pool is not None:
            logger.info(f"Disconnecting from {self.db_name.capitalize()}...")
            self._connection_pool.closeall()
            self._connection_pool = None

    @contextmanager
    def transaction(self) -> Iterator[Connection]:
        """Start a transaction"""
        with self.get_connection() as conn:
            # Since v2.9, autocommit is automatically ignored when connection is used with `with`
            with conn:
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
            conn = None
            max_retry = 10
            count = 0
            while count < max_retry:
                try:
                    conn = self._connection_pool.getconn()
                    break
                except psycopg2.pool.PoolError as e:
                    if count + 1 == max_retry:
                        raise
                    else:
                        if "connection pool exhausted" in str(e):
                            logger.warning(f"Exceeded max connections ({MAX_CONNECTIONS}). Retrying...")
                            time.sleep(1)
                        else:
                            raise
                count += 1

            assert conn
            try:
                if self.autocommit:
                    conn.set_session(autocommit=True)
                yield conn
            finally:
                if conn:
                    try:
                        conn.reset()
                        self._connection_pool.putconn(conn)
                    except psycopg2.pool.PoolError as e:
                        if "trying to put unkeyed connection" in str(e):
                            pass
                        else:
                            raise

    @contextmanager
    def savepoint(self, connection: Connection, name: str = None, logging: bool = False):
        """Create a savepoint before query executions, and rollback to the savepoint if an error occurs

        NOTE: Use this when you run a large number of queries inside a transaction, and when you want to ignore some
        errors that could be expected. You can stay inside the transaction even an error occurs in the middle.
        Do NOT use this if all queries in the transaction must succeed.

        :param connection: An existing connection in a transaction
        :param name: Savepoint name
        :param logging: Enable logging
        """
        if not name:
            name = f"savepoint_{str(uuid.uuid4()).replace('-', '_')}"
        with self.get_connection(connection) as conn:
            with conn.cursor(cursor_factory=get_cursor_factory(self, logging=logging)) as cursor:
                cursor.execute(f"SAVEPOINT {name}")
                try:
                    yield
                except AssertionError:
                    raise
                except Exception as e:
                    logger.error(f"Encountered an error. Rolling back to the savepoint:\n{type(e)}: {e}")
                    cursor.execute(f"ROLLBACK TO {name}")
                else:
                    cursor.execute(f"RELEASE savepoint {name}")

    @check_connection
    def SELECT(
        self,
        query: str,
        vars: Sequence[Any] = None,
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
            with conn.cursor(cursor_factory=get_cursor_factory(self, logging=logging)) as cursor:
                cursor.execute(query, vars=vars)
                rows = cursor.fetchall()
                if print_table:
                    print(tabulate.tabulate(rows, headers="keys", tablefmt="presto"))

                return [dict(x) for x in rows]

    @check_connection
    def DELETE(
        self,
        query: str,
        vars: Sequence[Any] = None,
        connection: Connection | None = None,
        logging: bool = True,
    ) -> int | tuple[Any, ...]:
        """Execute DELETE query"""
        if not query.strip().upper().startswith("DELETE"):
            query = "DELETE " + query
        with self.get_connection(connection) as conn:
            with conn.cursor(cursor_factory=get_cursor_factory(self, logging=logging)) as cursor:
                cursor.execute(query, vars=vars)
                if " RETURNING " in query.upper():
                    resp = cursor.fetchone()
                    return tuple(resp.values())
                else:
                    return cursor.rowcount

    @check_connection
    def UPDATE(
        self,
        query: str,
        vars: Sequence[Any] = None,
        connection: Connection | None = None,
        logging: bool = True,
    ) -> int | tuple[Any, ...]:
        """Execute UPDATE query"""
        if not query.strip().upper().startswith("UPDATE"):
            query = "UPDATE " + query
        with self.get_connection(connection) as conn:
            with conn.cursor(cursor_factory=get_cursor_factory(self, logging=logging)) as cursor:
                cursor.execute(query, vars=vars)
                if " RETURNING " in query.upper():
                    resp = cursor.fetchone()
                    return tuple(resp.values())
                else:
                    return cursor.rowcount

    @check_connection
    def INSERT(
        self,
        query: str,
        vars: Sequence[Any] = None,
        connection: Connection | None = None,
        logging: bool = True,
    ) -> int | tuple[Any, ...]:
        """Execute INSERT query"""
        if not query.strip().upper().startswith("INSERT"):
            query = "INSERT " + query
        with self.get_connection(connection) as conn:
            with conn.cursor(cursor_factory=get_cursor_factory(self, logging=logging)) as cursor:
                cursor.execute(query, vars=vars)
                if " RETURNING " in query.upper():
                    resp = cursor.fetchone()
                    return tuple(resp.values())
                else:
                    return cursor.rowcount

    @check_connection
    def show_tables(
        self,
        schema_names: str | Sequence[str] = None,
        columns_to_select: str | Sequence[str] = None,
        return_result: bool = False,
        **kwargs,
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
            sql += " IN %s"
            if isinstance(schema_names, str):
                vars = (schema_names,)
            else:
                vars = tuple(schema_names)
        else:
            sql += " NOT IN %s"
            vars = ("pg_catalog", "information_schema", "public")

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

        tables = self.SELECT(sql, vars=(vars,), print_table=True, **kwargs)
        if return_result:
            return tables

    @check_connection
    def show_function_definition(self, func_name: str):
        """Show user-defined function definition"""
        self.SELECT("SELECT prosrc FROM pg_proc WHERE proname=%s", vars=(func_name,), print_table=True)

    def _generate_query(self, cursor: psycopg2.extensions.cursor, sql: str, vars: Sequence[Any] = None) -> str:
        """Generates a query string after arguments binding"""
        try:
            query = cursor.mogrify(sql, vars).decode("utf-8").strip()
        except Exception as e:
            logger.error(
                f"Failed to generate a query with given vars:\n"
                f"- query: {sql}\n"
                f"- vars: {vars}\n"
                f"- Exception: {type(e)}\n{str(e)}"
            )
            raise

        if not query.endswith(";"):
            query += ";"
        return query


def wait_select_inter(conn: Connection):
    """Callback func for cancelling SQL statement on KeyboardInterrupt

    The original code was copied from here: https://www.psycopg.org/articles/2014/07/20/cancelling-postgresql-statements-python/,
    and then was modified to use poll() due to the limitation of FD_SETSIZE for select().
    (https://man7.org/linux/man-pages/man2/select.2.html)
    """
    poller = poll()
    poller.register(conn.fileno())
    try:
        while True:
            try:
                state = conn.poll()
                if state == POLL_OK:
                    break
                elif state in [POLL_READ, POLL_WRITE]:
                    poller.poll()
                else:
                    raise conn.OperationalError(f"bad state from poll: {state}")
            except KeyboardInterrupt:
                conn.cancel()
                # the loop will be broken by a server error
                continue
    finally:
        poller.unregister(conn.fileno())


psycopg2.extensions.set_wait_callback(wait_select_inter)
# Automatically convert a Python object to a json/jsonb type when inserting (json.dumps() is no longer needed)
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
