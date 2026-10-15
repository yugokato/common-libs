from collections.abc import Callable, Sequence
from typing import Any, Self

import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

from tests.libs.containers.base import HOST, ServiceContainer

XDIST_GROUP = "redis-server"
USER = "qa"
PASSWORD = "secret"
PORT = 6379
# Switch off the default user, and allow the ACL user to run any command on any key and channel
ACL_OPTIONS = ["--user", "default", "off", "--user", USER, "on", f">{PASSWORD}", "~*", "&*", "+@all"]


class RedisContainer(ServiceContainer):
    """A Redis container with a single ACL user, reachable on a random port of the loopback interface

    The default user is switched off, so a client must log in as the ACL user.
    """

    transient_errors = (redis.exceptions.ConnectionError,)

    def __init__(self) -> None:
        super().__init__("redis", "8.10.2-alpine")

    @property
    def client_options(self) -> dict[str, Any]:
        """The connection options for `RedisClient`"""
        return {"host": HOST, "port": self.host_port(PORT), "user": USER, "password": PASSWORD}

    def run(self, **kwargs: Any) -> Self:
        """Run the container and wait until the server accepts logins

        :param kwargs: Extra options for the container, which are passed on to the base class
        """
        return super().run(
            ports={f"{PORT}/tcp": (HOST, None)},
            command=["redis-server", *ACL_OPTIONS],
            **kwargs,
        )

    def _readiness_steps(self) -> Sequence[Callable[[], object]]:
        return [self._check_login]

    def _check_login(self) -> None:
        # redis-py retries a failed connection with a backoff by default, which would slow down the polling
        client = redis.Redis(
            host=HOST, port=self.host_port(PORT), username=USER, password=PASSWORD, retry=Retry(NoBackoff(), 0)
        )
        try:
            client.ping()
        except redis.exceptions.AuthenticationError as e:
            # A subclass of ConnectionError, which is retried. Retrying a rejected login would only hide the mistake
            raise RuntimeError(f"The Redis server rejected the login of user '{USER}': {e}") from e
        finally:
            client.close()
