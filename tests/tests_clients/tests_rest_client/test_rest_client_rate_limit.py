"""Tests for common_libs.clients.rest_client.rate_limit module"""

import json
import time

import httpx
import pytest
from pytest_mock import MockFixture

from common_libs.clients.rest_client import AsyncRestClient, RateLimit, RestClient
from common_libs.clients.rest_client.rate_limit import RateLimiter
from common_libs.clients.rest_client.retry import RetryPolicy

BASE_URL = "https://example.com"


def make_json_response(status_code: int, data: dict[str, object]) -> httpx.Response:
    """Build a stream-backed JSON response for a MockTransport handler

    Unlike passing `json=` (preloaded content), a stream-backed response is read by the client after the
    transport returns it, which sets `elapsed` the same way a real transport does.

    :param status_code: HTTP status code
    :param data: JSON body
    """
    return httpx.Response(
        status_code,
        stream=httpx.ByteStream(json.dumps(data).encode()),
        headers={"Content-Type": "application/json"},
    )


def ok_handler(request: httpx.Request) -> httpx.Response:
    """Serve a canned 200 response"""
    return make_json_response(200, {"ok": True})


class TestRateLimit:
    """Tests for the RateLimit config dataclass"""

    def test_rejects_non_positive_max_requests(self) -> None:
        """Test that a non-positive max_requests raises ValueError"""
        with pytest.raises(ValueError, match="max_requests must be a positive integer"):
            RateLimit(0)

    def test_rejects_non_positive_interval(self) -> None:
        """Test that a non-positive interval raises ValueError"""
        with pytest.raises(ValueError, match="interval must be a positive number"):
            RateLimit(1, interval=0)

    def test_rejects_float_max_requests(self) -> None:
        """Test that a float max_requests raises ValueError"""
        with pytest.raises(ValueError, match="max_requests must be a positive integer"):
            RateLimit(2.5)  # type: ignore[arg-type]

    def test_rejects_non_numeric_max_requests(self) -> None:
        """Test that a non-numeric max_requests raises ValueError"""
        with pytest.raises(ValueError, match="max_requests must be a positive integer"):
            RateLimit("2")  # type: ignore[arg-type]

    def test_rejects_non_numeric_interval(self) -> None:
        """Test that a non-numeric interval raises ValueError"""
        with pytest.raises(ValueError, match="interval must be a positive number"):
            RateLimit(1, interval="1")  # type: ignore[arg-type]

    def test_rejects_bool_max_requests(self) -> None:
        """Test that a bool max_requests raises ValueError"""
        with pytest.raises(ValueError, match="max_requests must be a positive integer"):
            RateLimit(True)  # type: ignore[arg-type]

    def test_rejects_bool_interval(self) -> None:
        """Test that a bool interval raises ValueError"""
        with pytest.raises(ValueError, match="interval must be a positive number"):
            RateLimit(1, interval=True)  # type: ignore[arg-type]


class TestRateLimiter:
    """Tests for the token-bucket RateLimiter"""

    def test_burst_allowed_up_to_max_requests(self) -> None:
        """Test that up to max_requests tokens are immediately available"""
        limiter = RateLimiter(RateLimit(2, interval=100))
        assert limiter.try_acquire() == 0.0
        assert limiter.try_acquire() == 0.0

    def test_wait_time_returned_when_budget_exhausted(self) -> None:
        """Test that try_acquire returns a positive wait time once the budget is exhausted"""
        limiter = RateLimiter(RateLimit(2, interval=100))
        limiter.try_acquire()
        limiter.try_acquire()
        wait_secs = limiter.try_acquire()
        assert 0 < wait_secs <= 50

    def test_tokens_refill_over_time(self, mocker: MockFixture) -> None:
        """Test that tokens refill according to the configured rate"""
        clock = {"now": 0.0}
        mocker.patch("time.monotonic", lambda: clock["now"])

        limiter = RateLimiter(RateLimit(2, interval=0.2))
        assert limiter.try_acquire() == 0.0
        assert limiter.try_acquire() == 0.0
        assert limiter.try_acquire() > 0

        clock["now"] += 0.15
        assert limiter.try_acquire() == 0.0


class TestRestClientRateLimiting:
    """Tests for rate limiting applied through the REST clients"""

    def test_sync_requests_throttled(self) -> None:
        """Test that sync requests beyond the budget are delayed by the rate limiter"""
        with RestClient(
            BASE_URL,
            retry_policy=None,
            rate_limit=RateLimit(2, interval=0.2),
            transport=httpx.MockTransport(ok_handler),
        ) as client:
            start = time.monotonic()
            for _ in range(4):
                client.get("/api", quiet=True)
            elapsed = time.monotonic() - start

        # 4 requests with a budget of 2 per 0.2s: the 3rd and 4th each wait ~0.1s
        assert elapsed >= 0.15

    def test_sync_requests_within_budget_not_throttled(self) -> None:
        """Test that requests within the budget are not delayed"""
        with RestClient(
            BASE_URL,
            retry_policy=None,
            rate_limit=RateLimit(10, interval=1.0),
            transport=httpx.MockTransport(ok_handler),
        ) as client:
            start = time.monotonic()
            for _ in range(3):
                client.get("/api", quiet=True)
            elapsed = time.monotonic() - start

        assert elapsed < 0.1

    async def test_async_requests_throttled(self) -> None:
        """Test that async requests beyond the budget are delayed by the rate limiter"""
        async with AsyncRestClient(
            BASE_URL,
            retry_policy=None,
            rate_limit=RateLimit(2, interval=0.2),
            transport=httpx.MockTransport(ok_handler),
        ) as client:
            start = time.monotonic()
            for _ in range(4):
                await client.get("/api", quiet=True)
            elapsed = time.monotonic() - start

        assert elapsed >= 0.15

    def test_retries_count_against_budget(self) -> None:
        """Test that automatic retries also pass the rate limiter"""
        num_requests = 0

        def flaky_handler(request: httpx.Request) -> httpx.Response:
            nonlocal num_requests
            num_requests += 1
            if num_requests < 3:
                return make_json_response(503, {"error": "unavailable"})
            return make_json_response(200, {"ok": True})

        with RestClient(
            BASE_URL,
            retry_policy=RetryPolicy(condition=503, num_retries=2, retry_after=0),
            rate_limit=RateLimit(1, interval=0.15),
            transport=httpx.MockTransport(flaky_handler),
        ) as client:
            start = time.monotonic()
            r = client.get("/api", quiet=True)
            elapsed = time.monotonic() - start

        # 3 sends (initial + 2 retries) with a budget of 1 per 0.15s: the 2nd and 3rd each wait ~0.15s
        assert r.status_code == 200
        assert num_requests == 3
        assert elapsed >= 0.25
