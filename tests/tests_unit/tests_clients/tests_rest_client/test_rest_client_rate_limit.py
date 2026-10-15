"""Tests for common_libs.clients.rest_client.rate_limit module"""

import json

import httpx2
import pytest
from pytest_mock import MockFixture

from common_libs.clients.rest_client import AsyncRestClient, RateLimit, RestClient
from common_libs.clients.rest_client import rate_limit as rate_limit_module
from common_libs.clients.rest_client.rate_limit import RateLimiter
from common_libs.clients.rest_client.retry import RetryPolicy
from tests.libs.common.fake_clock import FakeClock

pytestmark = [pytest.mark.unittest]

BASE_URL = "https://example.com"
RATE_LIMIT = RateLimit(2, interval=1.0)
TOKEN_WAIT = RATE_LIMIT.interval / RATE_LIMIT.max_requests


def make_json_response(status_code: int, data: dict[str, object]) -> httpx2.Response:
    """Build a stream-backed JSON response for a MockTransport handler

    Unlike passing `json=` (preloaded content), a stream-backed response is read by the client after the
    transport returns it, which sets `elapsed` the same way a real transport does.

    :param status_code: HTTP status code
    :param data: JSON body
    """
    return httpx2.Response(
        status_code,
        stream=httpx2.ByteStream(json.dumps(data).encode()),
        headers={"Content-Type": "application/json"},
    )


def ok_handler(request: httpx2.Request) -> httpx2.Response:
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
            RateLimit(2.5)

    def test_rejects_non_numeric_max_requests(self) -> None:
        """Test that a non-numeric max_requests raises ValueError"""
        with pytest.raises(ValueError, match="max_requests must be a positive integer"):
            RateLimit("2")

    def test_rejects_non_numeric_interval(self) -> None:
        """Test that a non-numeric interval raises ValueError"""
        with pytest.raises(ValueError, match="interval must be a positive number"):
            RateLimit(1, interval="1")

    def test_rejects_bool_max_requests(self) -> None:
        """Test that a bool max_requests raises ValueError"""
        with pytest.raises(ValueError, match="max_requests must be a positive integer"):
            RateLimit(True)

    def test_rejects_bool_interval(self) -> None:
        """Test that a bool interval raises ValueError"""
        with pytest.raises(ValueError, match="interval must be a positive number"):
            RateLimit(1, interval=True)


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


@pytest.fixture
def fake_clock(mocker: MockFixture) -> FakeClock:
    """A clock that replaces the real one in the rate limiter, so waits take no time and their sleeps can be checked"""
    clock = FakeClock()
    mocker.patch.object(rate_limit_module, "time", new=clock)
    mocker.patch("asyncio.sleep", new=clock.async_sleep)
    return clock


class TestRestClientRateLimiting:
    """Tests for rate limiting applied through the REST clients"""

    def test_sync_requests_throttled(self, fake_clock: FakeClock) -> None:
        """Test that sync requests beyond the budget are delayed by the rate limiter"""
        with RestClient(
            BASE_URL,
            retry_policy=None,
            rate_limit=RATE_LIMIT,
            transport=httpx2.MockTransport(ok_handler),
        ) as client:
            for _ in range(4):
                client.get("/api", quiet=True)

        # 4 requests with a budget of 2: the 3rd and 4th each wait for a token
        assert fake_clock.sleeps == [TOKEN_WAIT] * 2

    def test_sync_requests_within_budget_not_throttled(self, fake_clock: FakeClock) -> None:
        """Test that requests within the budget are not delayed"""
        with RestClient(
            BASE_URL,
            retry_policy=None,
            rate_limit=RateLimit(10, interval=1.0),
            transport=httpx2.MockTransport(ok_handler),
        ) as client:
            for _ in range(3):
                client.get("/api", quiet=True)

        assert fake_clock.sleeps == []

    async def test_async_requests_throttled(self, fake_clock: FakeClock) -> None:
        """Test that async requests beyond the budget are delayed by the rate limiter"""
        async with AsyncRestClient(
            BASE_URL,
            retry_policy=None,
            rate_limit=RATE_LIMIT,
            transport=httpx2.MockTransport(ok_handler),
        ) as client:
            for _ in range(4):
                await client.get("/api", quiet=True)

        assert fake_clock.sleeps == [TOKEN_WAIT] * 2

    def test_retries_count_against_budget(self, fake_clock: FakeClock) -> None:
        """Test that automatic retries also pass the rate limiter"""
        num_requests = 0

        def flaky_handler(request: httpx2.Request) -> httpx2.Response:
            nonlocal num_requests
            num_requests += 1
            if num_requests < 3:
                return make_json_response(503, {"error": "unavailable"})
            return make_json_response(200, {"ok": True})

        with RestClient(
            BASE_URL,
            retry_policy=RetryPolicy(condition=503, num_retries=2, retry_after=0),
            rate_limit=RateLimit(1, interval=TOKEN_WAIT),
            transport=httpx2.MockTransport(flaky_handler),
        ) as client:
            r = client.get("/api", quiet=True)

        # 3 sends (initial + 2 retries) with a budget of 1: the 2nd and 3rd each wait for a token
        assert r.status_code == 200
        assert num_requests == 3
        assert fake_clock.sleeps == [TOKEN_WAIT] * 2
