"""Integration tests for the retry and rate limit features of common_libs.clients.rest_client"""

from collections.abc import Callable

import httpx2
import pytest

from common_libs.clients.rest_client import BackoffStrategy, RateLimit, RestClient, RetryPolicy
from tests.libs.common.fake_clock import FakeClock
from tests.libs.containers.mock_server import XDIST_GROUP, MockResponse, MockServerContainer

pytestmark = [pytest.mark.integrationtest, pytest.mark.xdist_group(XDIST_GROUP)]

# The default policy waits 5 seconds between the attempts, which would make the tests slow
RETRY_DELAY = 0.05
RETRY_AFTER_SECONDS = 1.0
RATE_LIMIT_REQUESTS = 2
RATE_LIMIT_INTERVAL = 1.0
TOKEN_WAIT = RATE_LIMIT_INTERVAL / RATE_LIMIT_REQUESTS
SERVER_BUSY = MockResponse(status=503)
SUCCESS = MockResponse(body={"ok": True})


class TestRetry:
    """Tests for the retry policy of RestClient against a server that fails before it succeeds"""

    def test_request_is_retried_until_the_server_succeeds(
        self, client_factory: Callable[..., RestClient], mock_server: MockServerContainer, path: str
    ) -> None:
        """Test that a 503 is retried up to the number of retries, and the successful response is returned"""
        mock_server.expect(path, SERVER_BUSY, SERVER_BUSY, SUCCESS)
        client = client_factory(retry_policy=RetryPolicy(num_retries=2, retry_after=RETRY_DELAY))

        response = client.get(path)

        assert (response.status_code, len(mock_server.received(path))) == (200, 3)

    @pytest.mark.parametrize(
        ("honor_retry_after", "expected_wait"),
        [(True, RETRY_AFTER_SECONDS), (False, RETRY_DELAY)],
        ids=["honored", "ignored"],
    )
    def test_retry_after_header_is_used_only_when_honored(
        self,
        client_factory: Callable[..., RestClient],
        mock_server: MockServerContainer,
        path: str,
        fake_clock: FakeClock,
        honor_retry_after: bool,
        expected_wait: float,
    ) -> None:
        """Test that the wait before a retry is the Retry-After value of the server only if the strategy honors it"""
        mock_server.expect(path, MockResponse(status=503, headers={"Retry-After": f"{RETRY_AFTER_SECONDS:g}"}), SUCCESS)
        strategy = BackoffStrategy(base=RETRY_DELAY, jitter=False, honor_retry_after=honor_retry_after)
        client = client_factory(retry_policy=RetryPolicy(retry_after=strategy))

        response = client.get(path)

        assert response.status_code == 200
        assert fake_clock.sleeps == [expected_wait], "The client did not wait as long as the retry strategy decides"

    @pytest.mark.parametrize(
        ("safe_methods_only", "status_code", "attempts"), [(True, 503, 1), (False, 200, 2)], ids=["safe", "any"]
    )
    def test_post_is_retried_only_when_unsafe_methods_are_allowed(
        self,
        client_factory: Callable[..., RestClient],
        mock_server: MockServerContainer,
        path: str,
        safe_methods_only: bool,
        status_code: int,
        attempts: int,
    ) -> None:
        """Test that a POST, which is not a safe method, is only retried when the policy allows it"""
        mock_server.expect(path, SERVER_BUSY, SUCCESS)
        policy = RetryPolicy(retry_after=RETRY_DELAY, safe_methods_only=safe_methods_only)

        response = client_factory(retry_policy=policy).post(path, name="alice")

        assert (response.status_code, len(mock_server.received(path))) == (status_code, attempts)

    def test_request_is_retried_after_the_connection_is_dropped(
        self, client_factory: Callable[..., RestClient], mock_server: MockServerContainer, path: str
    ) -> None:
        """Test that an exception named in the retry condition, here the error of a dropped connection, is retried"""
        mock_server.expect_dropped_connection(path, times=1)
        mock_server.expect(path, SUCCESS)
        policy = RetryPolicy(condition=httpx2.RemoteProtocolError, retry_after=RETRY_DELAY)

        response = client_factory(retry_policy=policy).get(path)

        assert (response.status_code, len(mock_server.received(path))) == (200, 2)


class TestRateLimit:
    """Tests for the client-side rate limit of RestClient against a real server"""

    def test_requests_over_the_limit_wait_for_the_bucket_to_refill(
        self,
        client_factory: Callable[..., RestClient],
        mock_server: MockServerContainer,
        path: str,
        fake_clock: FakeClock,
    ) -> None:
        """Test that a burst of 2 requests passes at once and the next two wait for tokens at 2 requests per second"""
        mock_server.expect(path)
        client = client_factory(rate_limit=RateLimit(max_requests=RATE_LIMIT_REQUESTS, interval=RATE_LIMIT_INTERVAL))

        responses = [client.get(path) for _ in range(RATE_LIMIT_REQUESTS + 2)]

        assert [response.status_code for response in responses] == [200] * (RATE_LIMIT_REQUESTS + 2)
        assert fake_clock.sleeps == [TOKEN_WAIT] * 2, "The requests over the limit did not wait for a new token each"
