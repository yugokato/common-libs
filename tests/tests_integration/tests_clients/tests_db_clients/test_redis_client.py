"""Integration tests for common_libs.clients.database.redis module"""

import pytest
import redis
from pytest_mock import MockFixture

from common_libs.clients.database.redis import RedisClient
from tests.libs.containers.redis_server import USER, XDIST_GROUP

pytestmark = [pytest.mark.integrationtest, pytest.mark.xdist_group(XDIST_GROUP)]

KEYS_OVER_SEVERAL_PAGES = 25000


class TestRedisClient:
    """Tests for RedisClient against a real server"""

    def test_connects_as_the_given_user(self, redis_db: redis.Redis) -> None:
        """Test that the client logs in as the given ACL user"""
        assert redis_db.acl_whoami() == USER

    def test_values_are_decoded_to_str(self, redis_db: redis.Redis, key_prefix: str) -> None:
        """Test that the connection returns str values instead of bytes"""
        redis_db.set(f"{key_prefix}name", "alice")

        assert redis_db.get(f"{key_prefix}name") == "alice"


class TestScanKeys:
    """Tests for RedisClient.scan_keys() against a real server"""

    @pytest.mark.parametrize(
        ("pattern", "expected"),
        [
            ("user:*", ["user:1", "user:2", "user:10"]),
            ("user:?", ["user:1", "user:2"]),
            ("user:[2-9]*", ["user:2"]),
            ("order:*", ["order:1"]),
            ("missing:*", []),
        ],
        ids=["star", "single-character", "range", "other-prefix", "no-match"],
    )
    def test_returns_the_keys_matching_the_pattern(
        self, redis_client: RedisClient, redis_db: redis.Redis, key_prefix: str, pattern: str, expected: list[str]
    ) -> None:
        """Test that only the keys that match the glob-style pattern are returned"""
        for key in ["user:1", "user:2", "user:10", "order:1"]:
            redis_db.set(f"{key_prefix}{key}", "x")

        keys = redis_client.scan_keys(f"{key_prefix}{pattern}")

        assert sorted(keys) == sorted(f"{key_prefix}{key}" for key in expected)

    def test_follows_the_cursor_over_several_pages(
        self, mocker: MockFixture, redis_client: RedisClient, redis_db: redis.Redis, key_prefix: str
    ) -> None:
        """Test that all keys are returned when the server needs several SCAN calls to go through them"""
        pipeline = redis_db.pipeline()
        for number in range(KEYS_OVER_SEVERAL_PAGES):
            pipeline.set(f"{key_prefix}{number}", number)
        pipeline.execute()
        scan = mocker.spy(redis_db, "scan")

        keys = redis_client.scan_keys(f"{key_prefix}*")

        assert len(set(keys)) == KEYS_OVER_SEVERAL_PAGES
        assert scan.call_count > 1
