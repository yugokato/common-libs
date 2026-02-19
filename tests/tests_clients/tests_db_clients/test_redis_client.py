"""Tests for common_libs.clients.database.redis module"""

from unittest.mock import MagicMock

from common_libs.clients.database.redis import RedisClient


class TestRedisClient:
    """Tests for RedisClient class"""

    def test_init_connects_on_creation(self, mock_redis_class: MagicMock) -> None:
        """Test that connect() is called on initialization"""
        host = "host-init"
        port = 6379
        user = "user_init"
        password = "pass_init"
        client = RedisClient(host=host, port=port, user=user, password=password)
        mock_redis_class.assert_called_once_with(
            host=host,
            port=port,
            username=user,
            password=password,
            decode_responses=True,
        )
        assert client.db is not None

    def test_singleton_same_args_same_instance(self, mock_redis_class: MagicMock) -> None:
        """Test that the same arguments return the same singleton instance"""
        params = dict(host="host-single", port=6379, user="user_s", password="pass_s")
        client1 = RedisClient(**params)
        client2 = RedisClient(**params)
        assert client1 is client2

    def test_singleton_different_args_different_instance(self, mock_redis_class: MagicMock) -> None:
        """Test that different arguments create different instances"""
        client1 = RedisClient(host="host-a", port=6379, user="user_a", password="pass_a")
        client2 = RedisClient(host="host-b", port=6380, user="user_b", password="pass_b")
        assert client1 is not client2

    def test_connect_skips_if_already_connected(self, mock_redis_class: MagicMock) -> None:
        """Test that connect() does not create a new Redis instance if already connected"""
        client = RedisClient(host="host-skip", port=6379, user="user_skip", password="pass_skip")

        initial_call_count = mock_redis_class.call_count
        client.connect()
        # Should not have called Redis() again
        assert mock_redis_class.call_count == initial_call_count

    def test_scan_keys_single_page(self, mock_redis_class: MagicMock) -> None:
        """Test scan_keys returns keys from a single scan page"""
        keys = ["key:1", "key:2", "key:3"]
        mock_redis_instance = mock_redis_class.return_value
        mock_redis_instance.scan.return_value = (0, keys)

        client = RedisClient(host="host-scan1", port=6379, user="user_sc1", password="pass_sc1")
        result = client.scan_keys("key:*")

        assert sorted(result) == sorted(keys)

    def test_scan_keys_multiple_pages(self, mock_redis_class: MagicMock) -> None:
        """Test scan_keys iterates through multiple cursor pages"""
        expected_keys = ["key:1", "key:2", "key:3"]
        mock_redis_instance = mock_redis_class.return_value
        # First call returns cursor=42 (not done), second returns cursor=0 (done)
        mock_redis_instance.scan.side_effect = [
            (42, ["key:1", "key:2"]),
            (0, ["key:3"]),
        ]

        client = RedisClient(host="host-scan2", port=6379, user="user_sc2", password="pass_sc2")
        result = client.scan_keys("key:*")

        assert sorted(result) == sorted(expected_keys)
        assert mock_redis_instance.scan.call_count == 2

    def test_scan_keys_no_results(self, mock_redis_class: MagicMock) -> None:
        """Test scan_keys returns empty list when no keys match"""
        mock_redis_instance = mock_redis_class.return_value
        mock_redis_instance.scan.return_value = (0, [])

        client = RedisClient(host="host-scan3", port=6379, user="user_sc3", password="pass_sc3")
        result = client.scan_keys("nonexistent:*")

        assert result == []
