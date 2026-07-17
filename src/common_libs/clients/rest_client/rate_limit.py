from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass

from common_libs.logging import get_logger

__all__ = ["RateLimit", "RateLimiter"]

logger = get_logger(__name__)


@dataclass(frozen=True)
class RateLimit:
    """Client-side rate limit configuration (token bucket)

    Pass an instance to `RestClient` or `AsyncRestClient` via the `rate_limit` parameter. Requests that would
    exceed the budget wait until a token is available (blocking sleep in sync mode, `asyncio.sleep` in async mode).
    Automatic retries and reconnects also count against the budget.

    This is a token bucket, not a strict fixed window: it bursts up to `max_requests` immediately, then admits at
    an average rate of `max_requests / interval` per second as tokens refill.

    :param max_requests: Maximum number of requests allowed within `interval` seconds (also the burst size)
    :param interval: Length of the time window in seconds
    """

    max_requests: int
    interval: float = 1.0

    def __post_init__(self) -> None:
        if isinstance(self.max_requests, bool) or not isinstance(self.max_requests, int) or self.max_requests < 1:
            raise ValueError("max_requests must be a positive integer")
        if isinstance(self.interval, bool) or not isinstance(self.interval, int | float) or self.interval <= 0:
            raise ValueError("interval must be a positive number")


class RateLimiter:
    """Token-bucket rate limiter offering both sync and async acquisition over shared state

    The internal lock guards only the (non-blocking) bucket bookkeeping and is never held while waiting, so the
    same limiter instance can be shared between sync and async callers.
    """

    def __init__(self, rate_limit: RateLimit) -> None:
        """
        :param rate_limit: The rate limit configuration to enforce
        """
        self.rate_limit = rate_limit
        self._capacity = float(rate_limit.max_requests)
        self._refill_rate = rate_limit.max_requests / rate_limit.interval
        self._tokens = self._capacity
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Take a token, blocking until one is available"""
        while (wait_secs := self.try_acquire()) > 0:
            time.sleep(wait_secs)

    async def aacquire(self) -> None:
        """Take a token, asynchronously waiting until one is available"""
        while (wait_secs := self.try_acquire()) > 0:
            await asyncio.sleep(wait_secs)

    def try_acquire(self) -> float:
        """Take a token if one is available and return `0.0`, otherwise return the seconds to wait before retrying"""
        with self._lock:
            now = time.monotonic()
            self._tokens = min(self._capacity, self._tokens + (now - self._last_refill) * self._refill_rate)
            self._last_refill = now
            if self._tokens >= 1:
                self._tokens -= 1
                return 0.0
            wait_secs = (1 - self._tokens) / self._refill_rate
            if wait_secs > 0:
                logger.debug(
                    f"Rate limit reached ({self.rate_limit.max_requests} requests/{self.rate_limit.interval}s). "
                    f"Waiting {wait_secs:.3f} seconds..."
                )
            return wait_secs
