"""Shared pytest fixtures for REST client tests"""

from collections.abc import Callable, Generator
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockFixture

from common_libs.clients.rest_client.hooks import get_hooks


@pytest.fixture(autouse=True)
def clear_hooks_cache() -> Generator[None, None, None]:
    """Clear get_hooks lru_cache before and after each test to prevent stale hooks"""
    get_hooks.cache_clear()
    yield
    get_hooks.cache_clear()


@pytest.fixture
def mock_response_factory(mocker: MockFixture) -> Callable[..., MagicMock]:
    """Factory fixture that builds a mock ResponseExt with configurable status_code"""

    def _factory(status_code: int = 200, is_stream: bool = False) -> MagicMock:
        mock_request: MagicMock = mocker.MagicMock()
        mock_request.request_id = "test-request-id"
        mock_request.extensions = {"hooks": {"request": [], "response": []}}
        mock_request.method = "GET"
        mock_request.url = "http://example.com/api"

        mock_response: MagicMock = mocker.MagicMock()
        mock_response.request = mock_request
        mock_response.status_code = status_code
        mock_response.is_stream = is_stream
        mock_response.is_closed = not is_stream
        mock_response.is_success = 200 <= status_code < 300
        mock_response.elapsed.total_seconds.return_value = 0.1
        mock_response.json.return_value = {"ok": True}
        mock_response.headers = {}
        mock_response.reason_phrase = "OK" if 200 <= status_code < 300 else "Error"
        return mock_response

    return _factory


@pytest.fixture
def mock_stream_response_factory(mock_response_factory: Callable[..., MagicMock]) -> Callable[..., MagicMock]:
    """Factory fixture that builds a mock ResponseExt for streaming with configurable status_code"""

    def _factory(status_code: int = 200) -> MagicMock:
        return mock_response_factory(status_code=status_code, is_stream=True)

    return _factory


@pytest.fixture
def mock_hooks_logger(mocker: MockFixture) -> MagicMock:
    """Patch the logger in the hooks module"""
    return mocker.patch("common_libs.clients.rest_client.hooks.logger")
