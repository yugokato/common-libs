"""Tests for common_libs.clients.rest_client.retry module"""

import inspect
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from pytest import FixtureRequest
from pytest_mock import MockFixture

from common_libs.clients.rest_client.retry import (
    DEFAULT_RETRY_POLICY,
    BackoffStrategy,
    RetryPolicy,
    _parse_retry_after,
    retry_on,
)
from common_libs.clients.rest_client.types import Request, RestResponse
from common_libs.clients.rest_client.utils import set_request_to_exception


class TestRetryPolicy:
    """Tests for RetryPolicy dataclass and DEFAULT_RETRY_POLICY"""

    def test_default_policy_field_values(self) -> None:
        """Test that DEFAULT_RETRY_POLICY has the expected field values"""
        assert DEFAULT_RETRY_POLICY.condition == 503
        assert DEFAULT_RETRY_POLICY.num_retries == 1
        assert DEFAULT_RETRY_POLICY.retry_after == 5
        assert DEFAULT_RETRY_POLICY.safe_methods_only is True

    def test_custom_policy_fields(self) -> None:
        """Test that RetryPolicy stores custom field values correctly"""
        policy = RetryPolicy(condition=429, num_retries=3, retry_after=0.5, safe_methods_only=False)
        assert policy.condition == 429
        assert policy.num_retries == 3
        assert policy.retry_after == 0.5
        assert policy.safe_methods_only is False

    def test_policy_is_frozen(self) -> None:
        """Test that RetryPolicy instances are immutable"""
        policy = RetryPolicy()
        with pytest.raises((AttributeError, TypeError)):
            policy.num_retries = 5

    def test_policy_with_exception_class_condition(self) -> None:
        """Test that RetryPolicy accepts an exception class as condition"""
        policy = RetryPolicy(condition=ConnectionError)
        assert policy.condition is ConnectionError

    def test_policy_with_sequence_condition(self) -> None:
        """Test that RetryPolicy accepts a sequence of status codes as condition"""
        policy = RetryPolicy(condition=[502, 503, 504])
        assert policy.condition == [502, 503, 504]

    def test_policy_with_callable_condition(self) -> None:
        """Test that RetryPolicy accepts a callable as condition"""
        predicate = lambda r: r.status_code >= 500
        policy = RetryPolicy(condition=predicate)
        assert policy.condition is predicate


class TestBackoffStrategy:
    """Tests for BackoffStrategy"""

    def test_delay_increases_exponentially(self) -> None:
        """Test that delay grows exponentially with each attempt (jitter off)"""
        strategy = BackoffStrategy(base=1.0, factor=2.0, jitter=False)
        assert strategy.delay(0, ValueError()) == pytest.approx(1.0)
        assert strategy.delay(1, ValueError()) == pytest.approx(2.0)
        assert strategy.delay(2, ValueError()) == pytest.approx(4.0)

    def test_delay_clamped_to_max_delay(self) -> None:
        """Test that delay is clamped to max_delay"""
        strategy = BackoffStrategy(base=1.0, factor=2.0, max_delay=3.0, jitter=False)
        assert strategy.delay(5, ValueError()) == pytest.approx(3.0)

    def test_delay_falls_back_to_exponential_on_exception(self) -> None:
        """Test that BackoffStrategy.delay falls back to exponential formula when context is an Exception"""
        strategy = BackoffStrategy(base=1.0, factor=2.0, jitter=False, respect_retry_after=True)
        result = strategy.delay(0, ConnectionError("timeout"))
        assert result == pytest.approx(1.0)

    def test_delay_uses_retry_after_header_when_respect_retry_after_enabled(self, mocker: MockFixture) -> None:
        """Test that delay uses the Retry-After header value when respect_retry_after=True"""
        strategy = BackoffStrategy(base=1.0, factor=2.0, jitter=False, respect_retry_after=True)
        mock_httpx_response = mocker.MagicMock()
        mock_httpx_response.headers.get.return_value = "30"
        response = RestResponse(mock_httpx_response)
        assert strategy.delay(0, response) == pytest.approx(30.0)

    def test_delay_ignores_retry_after_header_when_respect_retry_after_disabled(self, mocker: MockFixture) -> None:
        """Test that delay ignores the Retry-After header when respect_retry_after=False"""
        strategy = BackoffStrategy(base=1.0, factor=2.0, jitter=False, respect_retry_after=False)
        mock_httpx_response = mocker.MagicMock()
        mock_httpx_response.headers.get.return_value = "999"
        response = RestResponse(mock_httpx_response)
        assert strategy.delay(0, response) == pytest.approx(1.0)

    def test_as_callable_produces_independent_fresh_counters(self) -> None:
        """Test that each as_callable() call produces an independent counter starting at 0"""
        strategy = BackoffStrategy(base=1.0, factor=2.0, jitter=False)
        fn1 = strategy.as_callable()
        fn2 = strategy.as_callable()
        ctx = ValueError()
        assert fn1(ctx) == pytest.approx(1.0)
        assert fn1(ctx) == pytest.approx(2.0)
        assert fn2(ctx) == pytest.approx(1.0)
        assert fn2(ctx) == pytest.approx(2.0)

    def test_delay_returns_max_delay_on_overflow(self) -> None:
        """Test that delay returns max_delay when the exponential computation overflows"""
        strategy = BackoffStrategy(base=1.0, factor=2.0, max_delay=60.0, jitter=False)
        assert strategy.delay(100000, ValueError()) == pytest.approx(60.0)


class TestBackoffStrategyJitterAndFallback:
    """Tests for BackoffStrategy jitter and header-fallback behavior"""

    def test_jitter_delay_within_bounds(self) -> None:
        """Test that jitter=True produces a delay in [0, computed] across several attempts"""
        strategy = BackoffStrategy(base=1.0, factor=2.0, max_delay=60.0, jitter=True)
        ctx = ValueError()
        for attempt in range(5):
            computed = min(60.0, 1.0 * (2.0**attempt))
            result = strategy.delay(attempt, ctx)
            assert 0.0 <= result <= computed

    def test_respect_retry_after_falls_back_to_exponential_when_header_missing(self, mocker: MockFixture) -> None:
        """Test that delay falls back to the exponential formula when the Retry-After header is absent"""
        strategy = BackoffStrategy(base=1.0, factor=2.0, jitter=False, respect_retry_after=True)
        mock_httpx_response = mocker.MagicMock()
        mock_httpx_response.headers.get.return_value = None
        response = RestResponse(mock_httpx_response)
        assert strategy.delay(0, response) == pytest.approx(1.0)

    def test_respect_retry_after_falls_back_to_exponential_when_header_unparseable(self, mocker: MockFixture) -> None:
        """Test that delay falls back to the exponential formula when the Retry-After header is unparseable"""
        strategy = BackoffStrategy(base=1.0, factor=2.0, jitter=False, respect_retry_after=True)
        mock_httpx_response = mocker.MagicMock()
        mock_httpx_response.headers.get.return_value = "not-a-valid-value"
        response = RestResponse(mock_httpx_response)
        assert strategy.delay(0, response) == pytest.approx(1.0)

    def test_respect_retry_after_reads_header_from_raw_httpx_response(self) -> None:
        """Test that delay reads Retry-After from a raw httpx Response (the production path through ext.py)"""
        strategy = BackoffStrategy(base=1.0, factor=2.0, jitter=False, respect_retry_after=True)
        raw = httpx.Response(503, headers={"Retry-After": "45"}, request=httpx.Request("GET", "http://x"))
        assert strategy.delay(0, raw) == pytest.approx(45.0)

    def test_retry_after_header_not_capped_by_max_delay(self, mocker: MockFixture) -> None:
        """Test that a Retry-After header value exceeding max_delay is honored verbatim, not capped"""
        strategy = BackoffStrategy(base=1.0, factor=2.0, max_delay=30.0, jitter=False, respect_retry_after=True)
        mock_httpx_response = mocker.MagicMock()
        mock_httpx_response.headers.get.return_value = "999"
        response = RestResponse(mock_httpx_response)
        assert strategy.delay(0, response) == pytest.approx(999.0)

    def test_respect_retry_after_via_retry_on_with_raw_response_factory(
        self, mocker: MockFixture, mock_response_factory: Callable[..., MagicMock]
    ) -> None:
        """Test that retry_on honors the `Retry-After` header end-to-end when `retry_after` is a
        `BackoffStrategy` and the wrapped function returns a raw httpx `Response`."""
        sleep_mock = mocker.patch("time.sleep")
        strategy = BackoffStrategy(base=1.0, factor=2.0, jitter=False, respect_retry_after=True)
        mock_err = mock_response_factory(503)
        mock_err.headers = {"Retry-After": "30"}
        mock_ok = mock_response_factory(200)
        call_count = 0

        @retry_on(503, num_retries=1, retry_after=strategy)
        def f() -> Any:
            nonlocal call_count
            call_count += 1
            return mock_err if call_count == 1 else mock_ok

        f()
        sleep_mock.assert_called_once_with(pytest.approx(30.0))


class TestRetryOn:
    """Tests for retry_on decorator"""

    @pytest.fixture(params=["sync", "async"])
    def mode(self, request: FixtureRequest) -> str:
        """Fixture parametrized over sync and async inner function modes."""
        return request.param

    @pytest.fixture(autouse=True)
    def _mock_async_sleep(self, mocker: MockFixture) -> MagicMock:
        return mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    @pytest.fixture(autouse=True)
    def _mock_time_sleep(self, mocker: MockFixture) -> MagicMock:
        return mocker.patch("time.sleep")

    @pytest.fixture
    def sleep_mock(self, mode: str, _mock_async_sleep: MagicMock, _mock_time_sleep: MagicMock) -> MagicMock:
        """Return the sleep mock that matches the current mode."""
        return _mock_async_sleep if mode == "async" else _mock_time_sleep

    def make_retried(
        self, mode: str, *condition_args: Any, body: Callable[[], Any], **retry_kwargs: Any
    ) -> Callable[[], Any]:
        """Return a `retry_on`-decorated function that delegates to `body()` on each attempt.

        :param mode: `"sync"` or `"async"`, matching the `mode` fixture.
        :param condition_args: Positional arguments forwarded to `retry_on`.
        :param body: Callable invoked on every attempt; may raise to trigger exception-based retries.
        :param retry_kwargs: Keyword arguments forwarded to `retry_on`.
        """
        if mode == "sync":

            @retry_on(*condition_args, **retry_kwargs)
            def f() -> Any:
                return body()

        else:

            @retry_on(*condition_args, **retry_kwargs)
            async def f() -> Any:
                return body()

        return f

    async def invoke(self, f: Callable[[], Any]) -> Any:
        """Call function and await the result if it is a coroutine."""
        result = f()
        if inspect.iscoroutine(result):
            return await result
        return result

    async def test_no_retry_when_condition_doesnt_match(
        self, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that no retry happens when condition doesn't match"""
        mock_resp = mock_response_factory(200)
        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            return mock_resp

        result = await self.invoke(self.make_retried(mode, 500, body=body))
        assert result is mock_resp
        assert call_count == 1

    async def test_retries_on_matching_int_condition(
        self, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that retry happens when condition matches an int status code"""
        status_to_retry = 500
        mock_err = mock_response_factory(status_to_retry)
        mock_ok = mock_response_factory(200)
        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            return mock_err if call_count == 1 else mock_ok

        result = await self.invoke(self.make_retried(mode, status_to_retry, body=body))
        assert result is mock_ok
        assert call_count == 2

    async def test_retries_on_matching_list_condition(
        self, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that retry happens when condition matches a list of status codes"""
        status_to_retry = 502
        mock_err = mock_response_factory(status_to_retry)
        mock_ok = mock_response_factory(200)
        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            return mock_err if call_count == 1 else mock_ok

        result = await self.invoke(self.make_retried(mode, [status_to_retry, 503], body=body))
        assert result is mock_ok
        assert call_count == 2

    async def test_retries_on_callable_condition(
        self, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that retry happens when callable condition returns True"""
        mock_err = mock_response_factory(503)
        mock_ok = mock_response_factory(200)
        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            return mock_err if call_count == 1 else mock_ok

        result = await self.invoke(self.make_retried(mode, lambda r: r.status_code >= 500, body=body))
        assert result is mock_ok
        assert call_count == 2

    async def test_retries_with_num_retries(self, mock_response_factory: Callable[..., MagicMock], mode: str) -> None:
        """Test that retry stops after num_retries attempts"""
        mock_err = mock_response_factory(500)
        call_count = 0
        num_retries = 3

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            return mock_err

        await self.invoke(self.make_retried(mode, 500, num_retries=num_retries, body=body))
        assert call_count == num_retries + 1  # 1 initial + 3 retries

    async def test_retries_with_safe_methods_only_option(
        self, mocker: MockFixture, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that safe_methods_only=True skips retry for non-safe methods like POST"""
        mock_logger = mocker.patch("common_libs.clients.rest_client.retry.logger")
        mock_resp = mock_response_factory(500)
        mock_resp.request.method = "POST"
        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            return mock_resp

        result = await self.invoke(self.make_retried(mode, 500, safe_methods_only=True, body=body))
        assert result is mock_resp
        assert call_count == 1
        assert mock_logger.warning.call_count == 1
        assert "safe_methods_only" in mock_logger.warning.call_args[0][0]

    async def test_retry_after_callable_not_called_when_condition_doesnt_match(
        self, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that retry_after callable is not called when condition doesn't match"""
        mock_retry_after: MagicMock = MagicMock()
        mock_200 = mock_response_factory(200)

        await self.invoke(self.make_retried(mode, 429, retry_after=mock_retry_after, body=lambda: mock_200))
        mock_retry_after.assert_not_called()

    async def test_retry_after_callable_called_when_condition_matches(
        self, sleep_mock: MagicMock, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that retry_after callable is called and its return value is used for sleep"""
        retry_after_val = 10
        mock_err = mock_response_factory(429)
        mock_err.headers = {"Retry-After": retry_after_val}

        await self.invoke(
            self.make_retried(mode, 429, retry_after=lambda r: r.headers["Retry-After"], body=lambda: mock_err)
        )
        sleep_mock.assert_called_once_with(retry_after_val)

    async def test_retry_after_static_value_used_for_sleep(
        self, sleep_mock: MagicMock, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that static retry_after value is passed directly to the sleep function"""
        retry_after = 30
        mock_err = mock_response_factory(500)
        mock_ok = mock_response_factory(200)
        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            return mock_err if call_count == 1 else mock_ok

        await self.invoke(self.make_retried(mode, 500, retry_after=retry_after, body=body))
        sleep_mock.assert_called_once_with(retry_after)

    async def test_retried_response_chains_retried_attribute(self, mode: str) -> None:
        """Test that after a retry, the successful response's request.retried is a snapshot of the
        failed-attempt request."""
        shared_request = cast(Request, httpx.Request("GET", "http://example.com/api"))
        shared_request.request_id = "test-request-id"
        shared_request.start_time = None
        shared_request.end_time = None
        shared_request.retried = None

        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            mock_resp = MagicMock()
            mock_resp.status_code = 500 if call_count == 1 else 200
            mock_resp.is_stream = False
            mock_resp.is_success = call_count != 1
            mock_resp.json.return_value = {"status": mock_resp.status_code}
            mock_resp.request = shared_request
            return mock_resp

        result = await self.invoke(self.make_retried(mode, 500, body=body))
        assert call_count == 2
        assert result.request.retried is not None
        assert result.request.retried is not shared_request
        assert result.request.retried.request_id == shared_request.request_id
        assert result.request.retried.retried is None

    async def test_retried_response_chains_retried_attribute_over_multiple_retries(self, mode: str) -> None:
        """Test that request.retried chains correctly across multiple retry attempts."""
        shared_request = cast(Request, httpx.Request("GET", "http://example.com/api"))
        shared_request.request_id = "test-request-id"
        shared_request.start_time = None
        shared_request.end_time = None
        shared_request.retried = None

        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            mock_resp = MagicMock()
            mock_resp.status_code = 500 if call_count <= 2 else 200
            mock_resp.is_stream = False
            mock_resp.is_success = call_count > 2
            mock_resp.json.return_value = {"status": mock_resp.status_code}
            mock_resp.request = shared_request
            return mock_resp

        result = await self.invoke(self.make_retried(mode, 500, num_retries=2, body=body))
        assert call_count == 3

        snapshot1 = result.request.retried
        assert snapshot1 is not None
        assert snapshot1 is not shared_request
        assert snapshot1.request_id == shared_request.request_id

        snapshot0 = snapshot1.retried
        assert snapshot0 is not None
        assert snapshot0 is not shared_request
        assert snapshot0.request_id == shared_request.request_id
        assert snapshot0.retried is None

    async def test_snapshot_preserves_timestamps_from_failed_attempt(self, mode: str) -> None:
        """Test that request.retried freezes the failed attempt's start_time and end_time independently
        of the live request object, which is reused and overwritten on the next retry."""
        shared_request = cast(Request, httpx.Request("GET", "http://example.com/api"))
        shared_request.request_id = "test-request-id"
        shared_request.retried = None

        call_count = 0
        attempt_times: list[tuple[datetime, datetime]] = []

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            start = datetime(2024, 1, 1, 0, 0, call_count, tzinfo=UTC)
            end = datetime(2024, 1, 1, 0, 1, call_count, tzinfo=UTC)
            shared_request.start_time = start
            shared_request.end_time = end
            attempt_times.append((start, end))
            mock_resp = MagicMock()
            mock_resp.status_code = 500 if call_count == 1 else 200
            mock_resp.is_stream = False
            mock_resp.is_success = call_count != 1
            mock_resp.json.return_value = {"status": mock_resp.status_code}
            mock_resp.request = shared_request
            return mock_resp

        result = await self.invoke(self.make_retried(mode, 500, body=body))
        assert call_count == 2

        assert result.request.start_time == attempt_times[1][0]
        assert result.request.end_time == attempt_times[1][1]

        snapshot = result.request.retried
        assert snapshot is not None
        assert snapshot.start_time == attempt_times[0][0]
        assert snapshot.end_time == attempt_times[0][1]
        assert snapshot.start_time != result.request.start_time

    async def test_retried_exception_chains_retried_attribute(
        self, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that after a retry following an exception, the successful response's request.retried
        is a snapshot of the failed-attempt request."""
        original_request = cast(Request, httpx.Request("GET", "http://example.com/api"))
        original_request.request_id = "test-request-id"
        original_request.start_time = None
        original_request.end_time = None
        original_request.retried = None

        mock_ok = mock_response_factory(200)
        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                exc = ValueError("transient error")
                set_request_to_exception(exc, original_request)
                raise exc
            return mock_ok

        result = await self.invoke(self.make_retried(mode, ValueError, body=body))
        assert result is mock_ok
        assert mock_ok.request.retried is not None
        assert mock_ok.request.retried is not original_request
        assert mock_ok.request.retried.request_id == original_request.request_id
        assert mock_ok.request.retried.retried is None

    async def test_retried_response_chains_retried_attribute_when_retries_exhausted(self, mode: str) -> None:
        """Test that the final failing response's request.retried is a snapshot of the prior attempt,
        not the live request object, when all retries are exhausted."""
        shared_request = cast(Request, httpx.Request("GET", "http://example.com/api"))
        shared_request.request_id = "test-request-id"
        shared_request.start_time = None
        shared_request.end_time = None
        shared_request.retried = None

        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            mock_resp = MagicMock()
            mock_resp.status_code = 503
            mock_resp.is_stream = False
            mock_resp.is_success = False
            mock_resp.json.return_value = {"error": "unavailable"}
            mock_resp.request = shared_request
            return mock_resp

        result = await self.invoke(self.make_retried(mode, 503, num_retries=1, body=body))
        assert call_count == 2
        assert result.request.retried is not None
        assert result.request.retried is not shared_request
        assert result.request.retried.request_id == shared_request.request_id
        assert result.request.retried.retried is None

    async def test_invalid_condition_raises_value_error(self, mode: str) -> None:
        """Test that an invalid condition type raises ValueError at decoration time"""
        with pytest.raises(ValueError, match="Invalid condition: "):
            self.make_retried(mode, "invalid_condition", body=lambda: None)

    async def test_retries_on_single_exception_class(self, mode: str) -> None:
        """Test that retry happens when a single exception class is raised"""
        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("transient error")
            return "ok"

        result = await self.invoke(self.make_retried(mode, ValueError, body=body))
        assert result == "ok"
        assert call_count == 2

    async def test_retries_on_sequence_of_exception_classes(self, mode: str) -> None:
        """Test that retry happens when condition is a list of exception classes and a matching one is raised"""
        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("transient error")
            return "ok"

        result = await self.invoke(self.make_retried(mode, [ValueError, RuntimeError], body=body))
        assert result == "ok"
        assert call_count == 2

    async def test_no_retry_on_non_matching_exception(self, mode: str) -> None:
        """Test that non-matching exceptions propagate immediately without retry"""
        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            raise RuntimeError("unexpected error")

        with pytest.raises(RuntimeError, match="unexpected error"):
            await self.invoke(self.make_retried(mode, ValueError, body=body))
        assert call_count == 1

    async def test_exception_retry_exhausted_reraises(self, mode: str) -> None:
        """Test that after num_retries retries, the last exception is re-raised"""
        call_count = 0
        num_retries = 2

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            raise ValueError("persistent error")

        with pytest.raises(ValueError, match="persistent error"):
            await self.invoke(self.make_retried(mode, ValueError, num_retries=num_retries, body=body))
        assert call_count == num_retries + 1  # 1 initial + num_retries retries

    async def test_exception_retry_with_static_retry_after(self, sleep_mock: MagicMock, mode: str) -> None:
        """Test that static retry_after value is passed to the sleep function on exception retry"""
        retry_after = 10
        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("transient")
            return "ok"

        await self.invoke(self.make_retried(mode, ValueError, retry_after=retry_after, body=body))
        sleep_mock.assert_called_once_with(retry_after)

    async def test_exception_retry_with_callable_retry_after(self, sleep_mock: MagicMock, mode: str) -> None:
        """Test that callable retry_after receives the raised exception and its return value is used for sleep"""
        expected_wait = 15
        retry_after_func: MagicMock = MagicMock(return_value=expected_wait)
        call_count = 0
        raised_exc = ValueError("transient error")

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise raised_exc
            return "ok"

        await self.invoke(self.make_retried(mode, ValueError, retry_after=retry_after_func, body=body))
        retry_after_func.assert_called_once_with(raised_exc)
        sleep_mock.assert_called_once_with(expected_wait)

    async def test_safe_methods_only_skips_exception_retry_for_non_safe_method(
        self, mocker: MockFixture, mode: str
    ) -> None:
        """Test that safe_methods_only=True skips exception retry for non-safe methods like POST"""
        mock_logger = mocker.patch("common_libs.clients.rest_client.retry.logger")
        mock_request = MagicMock(spec=Request)
        mock_request.method = "POST"
        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            exc = ValueError("transient error")
            set_request_to_exception(exc, mock_request)
            raise exc

        with pytest.raises(ValueError, match="transient error"):
            await self.invoke(self.make_retried(mode, ValueError, safe_methods_only=True, body=body))
        assert call_count == 1
        assert mock_logger.warning.call_count == 1
        assert "safe_methods_only" in mock_logger.warning.call_args[0][0]

    async def test_safe_methods_only_retries_on_exception_for_safe_method(
        self, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that safe_methods_only=True allows exception retry for safe methods like GET"""
        mock_request = MagicMock(spec=Request)
        mock_request.method = "GET"
        mock_ok = mock_response_factory(200)
        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                exc = ValueError("transient error")
                set_request_to_exception(exc, mock_request)
                raise exc
            return mock_ok

        result = await self.invoke(self.make_retried(mode, ValueError, safe_methods_only=True, body=body))
        assert result is mock_ok
        assert call_count == 2

    async def test_safe_methods_only_skips_exception_retry_when_no_request_attached(
        self, mocker: MockFixture, mode: str
    ) -> None:
        """Test that safe_methods_only=True skips exception retry when no request is attached to the exception"""
        mock_logger = mocker.patch("common_libs.clients.rest_client.retry.logger")
        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            raise ValueError("transient error")

        with pytest.raises(ValueError, match="transient error"):
            await self.invoke(self.make_retried(mode, ValueError, safe_methods_only=True, body=body))
        assert call_count == 1
        mock_logger.warning.assert_called_once()
        assert "safe_methods_only" in mock_logger.warning.call_args[0][0]

    async def test_callable_condition_retries_on_exception(self, mode: str) -> None:
        """Test that a callable condition can match a raised exception and trigger a retry"""
        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("transient error")
            return "ok"

        result = await self.invoke(self.make_retried(mode, lambda x: isinstance(x, ValueError), body=body))
        assert result == "ok"
        assert call_count == 2

    async def test_callable_condition_does_not_retry_on_exception_for_response_only_callable(self, mode: str) -> None:
        """Test that a response-only callable that errors when given an exception does not cause a retry"""
        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            raise RuntimeError("unexpected error")

        with pytest.raises(RuntimeError, match="unexpected error"):
            await self.invoke(self.make_retried(mode, lambda r: r.status_code == 503, body=body))
        assert call_count == 1

    async def test_retry_after_callable_not_called_when_retry_skipped_by_safe_methods_only(
        self, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that a callable retry_after is not invoked when a retry is skipped by safe_methods_only=True"""
        mock_retry_after = MagicMock(return_value=1)
        mock_err = mock_response_factory(503)
        mock_err.request.method = "POST"

        result = await self.invoke(
            self.make_retried(mode, 503, retry_after=mock_retry_after, safe_methods_only=True, body=lambda: mock_err)
        )
        assert result is mock_err
        mock_retry_after.assert_not_called()

    async def test_callable_condition_invoked_once_per_attempt_when_retries_exhausted(
        self, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that a callable condition is evaluated exactly once per call attempt even when retries are exhausted"""
        eval_count = 0

        def condition(r: Any) -> bool:
            nonlocal eval_count
            eval_count += 1
            return r.status_code == 503

        mock_err = mock_response_factory(503)

        result = await self.invoke(self.make_retried(mode, condition, num_retries=2, body=lambda: mock_err))
        assert result is mock_err
        assert eval_count == 3  # initial + 2 retries

    async def test_sequence_condition_supports_arbitrary_sequence(
        self, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that any homogeneous Sequence[int] (e.g. range) is accepted as a condition"""
        mock_err = mock_response_factory(502)
        mock_ok = mock_response_factory(200)
        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            return mock_err if call_count == 1 else mock_ok

        result = await self.invoke(self.make_retried(mode, range(500, 504), body=body))
        assert result is mock_ok
        assert call_count == 2

    async def test_mixed_sequence_condition_retries_on_status_code(
        self, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that a mixed sequence retries when the status code matches"""
        mock_err = mock_response_factory(500)
        mock_ok = mock_response_factory(200)
        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            return mock_err if call_count == 1 else mock_ok

        result = await self.invoke(self.make_retried(mode, [500, ValueError], body=body))
        assert result is mock_ok
        assert call_count == 2

    async def test_mixed_sequence_condition_retries_on_exception(
        self, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that a mixed sequence retries when the matching exception is raised"""
        mock_ok = mock_response_factory(200)
        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("transient error")
            return mock_ok

        result = await self.invoke(self.make_retried(mode, [500, ValueError], body=body))
        assert result is mock_ok
        assert call_count == 2

    def test_invalid_sequence_element_raises_value_error(self) -> None:
        """Test that a sequence containing an invalid element raises ValueError at decoration time"""
        with pytest.raises(
            ValueError, match="condition sequence must contain only status codes and/or exception classes"
        ):

            @retry_on([500, "foo"])
            def f() -> Any:
                return "ok"

    def test_empty_sequence_condition_raises_value_error(self) -> None:
        """Test that passing an empty sequence as condition raises ValueError immediately"""
        with pytest.raises(ValueError, match="condition sequence must not be empty"):

            @retry_on([])
            def f() -> Any:
                return "ok"

    def test_bool_condition_raises_value_error(self) -> None:
        """Test that passing True or False as condition raises ValueError at decoration time"""
        with pytest.raises(ValueError, match="Invalid condition: "):

            @retry_on(True)
            def f() -> Any:
                return "ok"

    async def test_num_retries_zero_disables_retries(
        self, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that num_retries=0 performs no retries even when the condition matches"""
        mock_err = mock_response_factory(503)
        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            return mock_err

        result = await self.invoke(self.make_retried(mode, 503, num_retries=0, body=body))
        assert result is mock_err
        assert call_count == 1

    async def test_returns_none_when_wrapped_function_returns_none(self, mode: str) -> None:
        """Test that retry_on does not crash and returns None when the wrapped function returns None"""
        result = await self.invoke(self.make_retried(mode, 503, body=lambda: None))
        assert result is None

    async def test_no_crash_when_response_request_is_none(self, mode: str) -> None:
        """Test that retry_on handles a matching response whose request attribute is None without crashing"""
        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            mock_resp = MagicMock()
            mock_resp.status_code = 503
            mock_resp.is_stream = False
            mock_resp.is_success = False
            mock_resp.json.return_value = {"error": "service unavailable"}
            mock_resp.request = None
            return mock_resp

        result = await self.invoke(self.make_retried(mode, 503, num_retries=1, body=body))
        assert call_count == 2
        assert result.status_code == 503

    async def test_safe_methods_only_skips_callable_condition_exception_retry_when_no_request_attached(
        self, mocker: MockFixture, mode: str
    ) -> None:
        """Test that safe_methods_only=True skips a callable-condition exception retry when no request is attached"""
        mock_logger = mocker.patch("common_libs.clients.rest_client.retry.logger")
        call_count = 0

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            raise ValueError("transient error")

        with pytest.raises(ValueError, match="transient error"):
            await self.invoke(
                self.make_retried(mode, lambda x: isinstance(x, ValueError), safe_methods_only=True, body=body)
            )
        assert call_count == 1
        mock_logger.warning.assert_called_once()
        assert "safe_methods_only" in mock_logger.warning.call_args[0][0]

    async def test_logs_exhausted_warning_with_status_code(
        self, mocker: MockFixture, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that the exhausted-retry warning includes the status code when retries run out"""
        mock_logger = mocker.patch("common_libs.clients.rest_client.retry.logger")
        mock_err = mock_response_factory(503)

        await self.invoke(self.make_retried(mode, 503, num_retries=1, body=lambda: mock_err))
        assert mock_logger.warning.call_count == 2
        exhausted_msg = mock_logger.warning.call_args_list[-1][0][0]
        assert "503" in exhausted_msg

    async def test_logs_exhausted_warning_with_exception_type(self, mocker: MockFixture, mode: str) -> None:
        """Test that the exhausted-retry warning includes the exception type when retries run out"""
        mock_logger = mocker.patch("common_libs.clients.rest_client.retry.logger")

        def body() -> Any:
            raise ValueError("persistent")

        with pytest.raises(ValueError):
            await self.invoke(self.make_retried(mode, ValueError, num_retries=1, body=body))
        assert mock_logger.warning.call_count == 2
        exhausted_msg = mock_logger.warning.call_args_list[-1][0][0]
        assert "ValueError" in exhausted_msg

    async def test_status_code_condition_does_not_swallow_exceptions(self, mode: str) -> None:
        """Test that an exception raised from a function with a status-code-only condition propagates without retry"""

        def body() -> Any:
            raise ConnectionError("network error")

        with pytest.raises(ConnectionError, match="network error"):
            await self.invoke(self.make_retried(mode, 503, body=body))

    async def test_exception_condition_does_not_retry_on_returned_response(
        self, mock_response_factory: Callable[..., MagicMock], mode: str
    ) -> None:
        """Test that an exception-class condition does not trigger a retry when the function returns a response"""
        call_count = 0
        mock_resp = mock_response_factory(503)

        def body() -> Any:
            nonlocal call_count
            call_count += 1
            return mock_resp

        result = await self.invoke(self.make_retried(mode, ValueError, body=body))
        assert result is mock_resp
        assert call_count == 1


class TestBackoffStrategyValidation:
    """Tests for BackoffStrategy constructor validation"""

    def test_negative_base_raises_value_error(self) -> None:
        """Test that a negative base raises ValueError"""
        with pytest.raises(ValueError, match="non-negative"):
            BackoffStrategy(base=-1.0)

    def test_negative_factor_raises_value_error(self) -> None:
        """Test that a negative factor raises ValueError"""
        with pytest.raises(ValueError, match="non-negative"):
            BackoffStrategy(factor=-1.0)

    def test_negative_max_delay_raises_value_error(self) -> None:
        """Test that a negative max_delay raises ValueError"""
        with pytest.raises(ValueError, match="non-negative"):
            BackoffStrategy(max_delay=-1.0)

    def test_zero_values_are_valid(self) -> None:
        """Test that zero values for base, factor, and max_delay are accepted"""
        strategy = BackoffStrategy(base=0.0, factor=0.0, max_delay=0.0)
        assert strategy.base == 0.0
        assert strategy.factor == 0.0
        assert strategy.max_delay == 0.0


class TestRetryOnBackoff:
    """Integration tests for retry_on with BackoffStrategy"""

    def test_uses_exponential_backoff_delays(
        self, mocker: MockFixture, mock_response_factory: Callable[..., MagicMock]
    ) -> None:
        """Test that retry_on uses the computed backoff delays when BackoffStrategy is passed"""
        sleep_mock = mocker.patch("time.sleep")
        strategy = BackoffStrategy(base=1.0, factor=2.0, jitter=False)
        responses = iter([mock_response_factory(503), mock_response_factory(503), mock_response_factory(200)])

        @retry_on(503, num_retries=2, retry_after=strategy)
        def f() -> Any:
            return next(responses)

        f()
        assert sleep_mock.call_count == 2
        assert sleep_mock.call_args_list[0][0][0] == pytest.approx(1.0)  # attempt 0
        assert sleep_mock.call_args_list[1][0][0] == pytest.approx(2.0)  # attempt 1

    def test_fresh_counter_per_invocation(
        self, mocker: MockFixture, mock_response_factory: Callable[..., MagicMock]
    ) -> None:
        """Test that each top-level call to a BackoffStrategy-decorated function gets a fresh attempt counter"""
        sleep_mock = mocker.patch("time.sleep")
        strategy = BackoffStrategy(base=1.0, factor=2.0, jitter=False)
        responses = iter(
            [
                mock_response_factory(503),
                mock_response_factory(200),
                mock_response_factory(503),
                mock_response_factory(200),
            ]
        )

        @retry_on(503, num_retries=1, retry_after=strategy)
        def f() -> Any:
            return next(responses)

        f()  # call 1: one retry at attempt 0 → 1.0s
        f()  # call 2: one retry at attempt 0 again → 1.0s (fresh counter)

        assert sleep_mock.call_count == 2
        assert sleep_mock.call_args_list[0][0][0] == pytest.approx(1.0)
        assert sleep_mock.call_args_list[1][0][0] == pytest.approx(1.0)

    async def test_async_uses_exponential_backoff_delays(
        self, mocker: MockFixture, mock_response_factory: Callable[..., MagicMock]
    ) -> None:
        """Test that async retry_on uses BackoffStrategy delays correctly"""
        sleep_mock = mocker.patch("asyncio.sleep", new_callable=AsyncMock)
        strategy = BackoffStrategy(base=0.5, factor=2.0, jitter=False)
        responses = iter([mock_response_factory(503), mock_response_factory(200)])

        @retry_on(503, num_retries=1, retry_after=strategy)
        async def f() -> Any:
            return next(responses)

        await f()
        sleep_mock.assert_called_once()
        assert sleep_mock.call_args_list[0][0][0] == pytest.approx(0.5)  # attempt 0

    def test_exception_retry_uses_exponential_backoff_delays(self, mocker: MockFixture) -> None:
        """Test that exception-based retries use the computed backoff delays from BackoffStrategy."""
        sleep_mock = mocker.patch("time.sleep")
        strategy = BackoffStrategy(base=1.0, factor=2.0, jitter=False)
        call_count = 0

        @retry_on(ValueError, num_retries=2, retry_after=strategy)
        def f() -> Any:
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise ValueError("transient")
            return "ok"

        result = f()
        assert result == "ok"
        assert sleep_mock.call_count == 2
        assert sleep_mock.call_args_list[0][0][0] == pytest.approx(1.0)  # attempt 0
        assert sleep_mock.call_args_list[1][0][0] == pytest.approx(2.0)  # attempt 1

    async def test_async_exception_retry_uses_exponential_backoff_delays(self, mocker: MockFixture) -> None:
        """Test that async exception-based retries use the computed backoff delays from BackoffStrategy."""
        sleep_mock = mocker.patch("asyncio.sleep", new_callable=AsyncMock)
        strategy = BackoffStrategy(base=1.0, factor=2.0, jitter=False)
        call_count = 0

        @retry_on(ValueError, num_retries=2, retry_after=strategy)
        async def f() -> Any:
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise ValueError("transient")
            return "ok"

        result = await f()
        assert result == "ok"
        assert sleep_mock.call_count == 2
        assert sleep_mock.call_args_list[0][0][0] == pytest.approx(1.0)  # attempt 0
        assert sleep_mock.call_args_list[1][0][0] == pytest.approx(2.0)  # attempt 1

    def test_jitter_applied_through_retry_on(
        self, mocker: MockFixture, mock_response_factory: Callable[..., MagicMock]
    ) -> None:
        """Test that jitter=True causes random.uniform to be consulted and its return value is slept on."""
        expected_delay = 0.42
        uniform_mock = mocker.patch("common_libs.clients.rest_client.retry.random.uniform", return_value=expected_delay)
        sleep_mock = mocker.patch("time.sleep")
        strategy = BackoffStrategy(base=1.0, factor=2.0, jitter=True)
        responses = iter([mock_response_factory(503), mock_response_factory(200)])

        @retry_on(503, num_retries=1, retry_after=strategy)
        def f() -> Any:
            return next(responses)

        f()
        uniform_mock.assert_called_once()
        sleep_mock.assert_called_once_with(expected_delay)

    def test_retry_after_header_advances_backoff_counter_when_header_absent_on_next_attempt(
        self, mocker: MockFixture, mock_response_factory: Callable[..., MagicMock]
    ) -> None:
        """Test that the backoff attempt counter increments even when Retry-After was used, so the
        next attempt without the header falls back to the correct exponential position."""
        sleep_mock = mocker.patch("time.sleep")
        strategy = BackoffStrategy(base=1.0, factor=2.0, jitter=False, respect_retry_after=True)
        mock_503_with_header = mock_response_factory(503)
        mock_503_with_header.headers = {"Retry-After": "10"}
        mock_503_no_header = mock_response_factory(503)
        mock_503_no_header.headers = {}
        mock_ok = mock_response_factory(200)
        responses = iter([mock_503_with_header, mock_503_no_header, mock_ok])

        @retry_on(503, num_retries=2, retry_after=strategy)
        def f() -> Any:
            return next(responses)

        f()
        assert sleep_mock.call_count == 2
        assert sleep_mock.call_args_list[0][0][0] == pytest.approx(10.0)  # attempt 0: header wins
        assert sleep_mock.call_args_list[1][0][0] == pytest.approx(2.0)  # attempt 1: exponential (1.0 * 2.0**1)


class TestParseRetryAfter:
    """Tests for _parse_retry_after"""

    def test_delta_seconds_integer_string(self) -> None:
        """Test that an integer delta-seconds string is parsed correctly"""
        assert _parse_retry_after("30") == pytest.approx(30.0)

    def test_delta_seconds_float_string(self) -> None:
        """Test that a float delta-seconds string is parsed correctly"""
        assert _parse_retry_after("1.5") == pytest.approx(1.5)

    def test_delta_seconds_zero(self) -> None:
        """Test that zero delta-seconds yields 0.0"""
        assert _parse_retry_after("0") == pytest.approx(0.0)

    def test_delta_seconds_negative_clamped_to_zero(self) -> None:
        """Test that a negative delta-seconds value is clamped to 0.0"""
        assert _parse_retry_after("-5") == pytest.approx(0.0)

    def test_http_date_in_future(self) -> None:
        """Test that an HTTP-date header in the future returns a positive delay"""
        future = datetime.now(tz=UTC) + timedelta(seconds=60)
        result = _parse_retry_after(format_datetime(future))
        assert result is not None
        assert result > 0.0

    def test_http_date_in_past_clamped_to_zero(self) -> None:
        """Test that an HTTP-date header in the past is clamped to 0.0"""
        past = datetime.now(tz=UTC) - timedelta(seconds=60)
        result = _parse_retry_after(format_datetime(past))
        assert result == pytest.approx(0.0)

    def test_garbage_string_returns_none(self) -> None:
        """Test that an unparseable string returns None"""
        assert _parse_retry_after("not-a-date-or-number") is None

    def test_none_returns_none(self) -> None:
        """Test that None input returns None"""
        assert _parse_retry_after(None) is None

    def test_empty_string_returns_none(self) -> None:
        """Test that an empty string returns None"""
        assert _parse_retry_after("") is None

    def test_inf_returns_none(self) -> None:
        """Test that 'inf' returns None so callers fall back to exponential backoff"""
        assert _parse_retry_after("inf") is None

    def test_nan_returns_none(self) -> None:
        """Test that 'nan' returns None so callers fall back to exponential backoff"""
        assert _parse_retry_after("nan") is None

    def test_tz_naive_http_date_returns_none(self) -> None:
        """Test that a tz-naive HTTP-date string returns None (subtraction with tz-aware datetime raises TypeError)"""
        naive_date = "Mon, 01 Jan 2024 00:00:00"
        assert _parse_retry_after(naive_date) is None
