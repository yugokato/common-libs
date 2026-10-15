"""Negative integration tests for common_libs.clients.database.redis module"""

from collections.abc import Callable
from typing import Any

import pytest
import redis

from common_libs.clients.database.redis import RedisClient
from tests.libs.containers.redis_server import XDIST_GROUP

pytestmark = [pytest.mark.integrationtest, pytest.mark.xdist_group(XDIST_GROUP)]

# A privileged port that nothing listens on, which the random host ports of containers never take
UNUSED_PORT = 1


class TestRedisClientNegative:
    """Tests for RedisClient against a real server when the login or the connection fails"""

    @pytest.mark.parametrize(
        "options",
        [{"password": "incorrect-password"}, {"user": "unknown-user"}],
        ids=["wrong-password", "unknown-user"],
    )
    def test_rejected_login_raises_authentication_error(
        self, redis_client_factory: Callable[..., RedisClient], options: dict[str, Any]
    ) -> None:
        """Test that the server's rejection of the login is raised on the first command"""
        client = redis_client_factory(**options)

        with pytest.raises(redis.exceptions.AuthenticationError, match="invalid username-password pair"):
            client.scan_keys("*")

    def test_unreachable_server_raises_connection_error(self, redis_client_factory: Callable[..., RedisClient]) -> None:
        """Test that a connection to a port nobody listens on is raised on the first command"""
        client = redis_client_factory(port=UNUSED_PORT)

        with pytest.raises(redis.exceptions.ConnectionError, match=rf"connecting to 127\.0\.0\.1:{UNUSED_PORT}\b"):
            client.scan_keys("*")
