"""Tests for common_libs.network module"""

import socket
import threading

import pytest

from common_libs.network import find_open_port, is_port_in_use


class TestIsPortInUse:
    """Tests for is_port_in_use function"""

    def test_is_port_in_use_free_port(self) -> None:
        """Test detecting a free port"""
        # Find a port that's likely free
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]

        # After closing, the port should be free
        assert is_port_in_use(port) is False

    def test_is_port_in_use_occupied_port(self) -> None:
        """Test detecting an occupied port"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
            s.listen(1)
            assert is_port_in_use(port) is True

    def test_is_port_in_use_custom_host(self) -> None:
        """Test with custom host"""
        result = is_port_in_use(65535, host="0.0.0.0")
        assert isinstance(result, bool)


class TestFindOpenPort:
    """Tests for find_open_port function"""

    def test_find_open_port_basic(self) -> None:
        """Test finding an open port"""
        port = find_open_port()
        assert 1024 <= port <= 65535
        assert is_port_in_use(port) is False

    def test_find_open_port_range(self) -> None:
        """Test finding port within specific range"""
        port = find_open_port(start_port=50000, end_port=50100)
        assert 50000 <= port <= 50100

    def test_find_open_port_exclude(self) -> None:
        """Test excluding specific ports"""
        # Find an open port first
        port1 = find_open_port(start_port=50000, end_port=50100)
        # Find another, excluding the first
        port2 = find_open_port(start_port=50000, end_port=50100, exclude=[port1])
        assert port2 != port1

    def test_find_open_port_no_available_raises(self) -> None:
        """Test RuntimeError when no port available"""
        # Use a very narrow range where ports are likely in use
        # This is somewhat fragile but tests the error path
        with pytest.raises(RuntimeError, match="Unable to find"):
            # Try to find port in invalid range (end < start won't work, use occupied range)
            find_open_port(start_port=1, end_port=1, exclude=[1])

    def test_find_open_port_thread_safe(self) -> None:
        """Test that find_open_port is thread-safe"""
        ports: list[int] = []
        errors: list[Exception] = []

        def find_port() -> None:
            try:
                port = find_open_port(start_port=55000, end_port=55100)
                ports.append(port)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=find_port) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(ports) == 5
