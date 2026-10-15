from collections.abc import Callable, Sequence
from typing import Any, Self

import psycopg

from tests.libs.containers.base import HOST, ServiceContainer

XDIST_GROUP = "postgresql-server"
DATABASE = "qa"
USER = "qa"
PASSWORD = "secret"
PORT = 5432
CONNECT_TIMEOUT = 5


class PostgreSQLContainer(ServiceContainer):
    """A PostgreSQL container with a single database and user, reachable on a random port of the loopback interface"""

    transient_errors = (psycopg.OperationalError,)

    def __init__(self) -> None:
        super().__init__("postgres", "18.6-alpine")

    @property
    def client_options(self) -> dict[str, Any]:
        """The connection options for `PostgreSQLClient`"""
        return {"host": HOST, "port": self.host_port(PORT), "db_name": DATABASE, "user": USER, "password": PASSWORD}

    def run(self, **kwargs: Any) -> Self:
        """Run the container and wait until the database accepts logins

        :param kwargs: Extra options for the container, which are passed on to the base class
        """
        return super().run(
            ports={f"{PORT}/tcp": (HOST, None)},
            environment={"POSTGRES_DB": DATABASE, "POSTGRES_USER": USER, "POSTGRES_PASSWORD": PASSWORD},
            **kwargs,
        )

    def _readiness_steps(self) -> Sequence[Callable[[], object]]:
        return [self._check_login]

    def _check_login(self) -> None:
        # The server restarts after initializing the database, so the first attempts are refused
        with psycopg.connect(
            host=HOST,
            port=self.host_port(PORT),
            dbname=DATABASE,
            user=USER,
            password=PASSWORD,
            connect_timeout=CONNECT_TIMEOUT,
        ) as connection:
            connection.execute("SELECT 1")
