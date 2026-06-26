from __future__ import annotations

import asyncio
import copy
import inspect
import itertools
import math
import random
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from functools import wraps
from typing import Any, ParamSpec, TypeAlias, TypeVar, cast

from common_libs.logging import get_logger

from .types import Request, Response, RestResponse
from .utils import SAFE_HTTP_METHODS, get_request_from_exception, process_response

P = ParamSpec("P")
R = TypeVar("R", bound="RestResponse")

RetryCondition: TypeAlias = int | type[Exception] | Sequence[int | type[Exception]] | Callable[..., bool]

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class BackoffStrategy:
    """Exponential-backoff configuration for `retry_on`.

    Pass an instance as the `retry_after` argument of `retry_on()`. Each top-level call mints a
    fresh attempt counter via `as_callable()`, so retries within a single call follow the correct exponential
    sequence without state leaking across calls.

    :param base: Base wait time in seconds for the first retry
    :param factor: Multiplicative growth factor per attempt
    :param max_delay: Upper bound on the computed exponential wait time in seconds.
                      This cap does not apply when `honor_retry_after=True` and the response
                      carries a `Retry-After` header — the server's explicit value is used verbatim.
    :param jitter: When `True`, applies full jitter: actual delay is `uniform(0, computed)`
    :param honor_retry_after: When `True` and the response carries a `Retry-After` header, use that value instead of
                                the exponential formula. The header value is honored verbatim — it is neither jittered
                                nor capped by `max_delay`.
    """

    base: float = 0.5
    factor: float = 2.0
    max_delay: float = 60.0
    jitter: bool = True
    honor_retry_after: bool = False

    def __post_init__(self) -> None:
        if self.base < 0 or self.factor < 0 or self.max_delay < 0:
            raise ValueError("base, factor, and max_delay must be non-negative")

    def delay(self, attempt: int, context: RestResponse | Response | Exception) -> float:
        """Compute the wait time for the given attempt.

        :param attempt: Zero-based retry attempt index
        :param context: The response or exception that triggered the retry
        """
        if self.honor_retry_after and not isinstance(context, Exception):
            headers = context._response.headers if isinstance(context, RestResponse) else context.headers
            parsed = _parse_retry_after(headers.get("Retry-After"))
            if parsed is not None:
                return parsed
        try:
            computed = min(self.max_delay, self.base * (self.factor**attempt))
        except OverflowError:
            computed = self.max_delay
        return random.uniform(0, computed) if self.jitter else computed

    def as_callable(self) -> Callable[[RestResponse | Response | Exception], float]:
        """Return a fresh per-call callable that increments its own attempt counter.

        Each invocation of this method produces an independent closure with its own `itertools.count()` starting at 0,
        so repeated calls to `retry_on(retry_after=...)` each get an isolated, correctly-sequenced backoff.
        """
        counter = itertools.count()
        strategy = self

        def _backoff_callable(context: RestResponse | Response | Exception) -> float:
            return strategy.delay(next(counter), context)

        return _backoff_callable


@dataclass(frozen=True)
class RetryPolicy:
    """Policy controlling automatic HTTP request retry behavior.

    Pass an instance to `RestClient` or `AsyncRestClient` via the `retry` parameter.
    Use `retry=None` to disable automatic retries entirely.

    :param condition: Status code(s), exception class(es), or a callable matching the retry trigger.
                      Accepts the same forms as `retry_on`.
    :param num_retries: Maximum number of retry attempts.
    :param retry_after: Seconds to wait before retrying, a callable that receives the response or
                        exception and returns the wait time, or a `BackoffStrategy` instance.
    :param safe_methods_only: When `True`, only retries requests with safe HTTP methods (GET, HEAD, OPTIONS).
    """

    condition: RetryCondition = 503
    num_retries: int = 1
    retry_after: float | int | Callable[..., float | int] | BackoffStrategy = 5
    safe_methods_only: bool = True


DEFAULT_RETRY_POLICY = RetryPolicy()


def retry_on(
    condition: int | type[Exception] | Sequence[int | type[Exception]] | Callable[[R | Exception], bool],
    num_retries: int = 1,
    retry_after: float | int | Callable[[R | Exception], float | int] | BackoffStrategy = 5,
    safe_methods_only: bool = False,
    _async_mode: bool = False,
) -> Callable[[Callable[P, R | Awaitable[R]]], Callable[P, R | Awaitable[R]]]:
    """Retry the request if the given condition matches

    :param condition: Status code(s), exception class(es), or a callable taking the response or exception to retry on.
                      A sequence may contain status codes, exception classes, or a mix of both.
                      When a callable is passed, it receives the value returned by the wrapped function on
                      HTTP-level retries (a raw `httpx.Response` when wrapping `RestClient`/`AsyncRestClient`),
                      or the raised exception when an exception-based retry occurs; the callable must handle both
                      cases gracefully. When evaluated against a response, the callable must not raise — errors
                      are not caught.
    :param num_retries: Max number of retries
    :param retry_after: Wait time before retrying in seconds; when the condition matched on an exception, a callable
                        `retry_after` receives the raised exception instead of a response. A callable receives the
                        same value as a callable `condition` would (see above). A `BackoffStrategy` instance is
                        also accepted — `as_callable()` is invoked per wrapped-function invocation so each
                        top-level call gets a fresh attempt counter.
    :param safe_methods_only: Retry only for safe HTTP methods (GET/HEAD/OPTIONS). For exception-based retries,
                              when the method cannot be determined (no request attached), the retry is skipped.
    :param _async_mode: Explicitly signal that the wrapped function executes async code
    """
    status_codes: frozenset[int] | None = None
    exc_types: tuple[type[Exception], ...] | None = None
    predicate: Callable[[R | Exception], bool] | None = None

    if isinstance(condition, bool):
        raise ValueError(f"Invalid condition: {condition!r}")
    elif isinstance(condition, int):
        status_codes = frozenset({condition})
    elif isinstance(condition, type) and issubclass(condition, Exception):
        exc_types = (condition,)
    elif callable(condition) and not isinstance(condition, type):
        predicate = condition
    elif isinstance(condition, Sequence) and not isinstance(condition, (str, bytes, bytearray)):
        items = tuple(condition)
        if not items:
            raise ValueError("condition sequence must not be empty")
        codes = [x for x in items if isinstance(x, int) and not isinstance(x, bool)]
        excs = [x for x in items if isinstance(x, type) and issubclass(x, Exception)]
        if len(codes) + len(excs) != len(items):
            raise ValueError(
                f"condition sequence must contain only status codes and/or exception classes: {condition!r}"
            )
        if codes:
            status_codes = frozenset(codes)
        if excs:
            exc_types = cast("tuple[type[Exception], ...]", tuple(excs))
    else:
        raise ValueError(f"Invalid condition: {condition!r}")

    is_callable_condition = predicate is not None

    def matches_condition(resp: R | None, exc: Exception | None) -> bool:
        if exc is not None:
            if exc_types is not None:
                return True  # already type-filtered in call()
            if predicate is not None:
                try:
                    return bool(predicate(exc))
                except Exception:
                    return False
            return False
        # exc is None — evaluate against the response
        if resp is None:
            return False
        if status_codes is not None:
            return resp.status_code in status_codes
        if predicate is not None:
            return bool(predicate(resp))
        return False  # exception-based condition but no exception raised (e.g. after a successful retry)

    def _get_retry_context(
        request: Request | None,
        resp: R | None,
        exc: Exception | None,
        resolved_retry_after: float | int | Callable[[R | Exception], float | int],
    ) -> tuple[float | int, str, dict[str, Any]]:
        """Compute the wait duration, log message, and log extras for a pending retry attempt."""
        if exc is not None:
            wait_secs = resolved_retry_after(exc) if callable(resolved_retry_after) else resolved_retry_after
            msg = "Retry condition matched." if is_callable_condition else f"Exception {type(exc).__name__} raised."
            log_extra: dict[str, Any] = {"exception": f"{type(exc)}: {exc!s}"}
        else:
            assert resp is not None
            wait_secs = resolved_retry_after(resp) if callable(resolved_retry_after) else resolved_retry_after
            msg = "Retry condition matched." if is_callable_condition else f"Received status code {resp.status_code}."
            log_extra = {"status_code": resp.status_code, "response": process_response(resp, prettify=True)}
        if request is not None and (request_id := getattr(request, "request_id", None)) is not None:
            log_extra["request_id"] = request_id
        return wait_secs, msg, log_extra

    def _log_retry(wait_secs: float | int, msg: str, log_extra: dict[str, Any]) -> None:
        logger.warning(f"{msg} Retrying in {wait_secs} seconds...", extra=log_extra)

    def _log_exhausted(num_retried: int, condition_matched: bool, resp: R | None, exc: Exception | None) -> None:
        if num_retried > 0 and condition_matched:
            text = f"{num_retried} times" if num_retried > 1 else "once"
            if exc is not None:
                reason = f"raised {type(exc).__name__}"
            else:
                assert resp is not None
                reason = (
                    "matches the condition" if is_callable_condition else f"received status code {resp.status_code}"
                )
            logger.warning(f"Retried {text} but request still {reason}")

    def _should_swallow(e: Exception) -> bool:
        """Return True when `e` matches the retry condition and should be captured rather than propagated."""
        if exc_types is not None:
            return isinstance(e, exc_types)
        return is_callable_condition

    def _prepare_retry(
        resp: R | None,
        exc: Exception | None,
        num_retried: int,
        resolved_retry_after: float | int | Callable[[R | Exception], float | int],
    ) -> tuple[Request | None, float | int] | None:
        """Decide whether to retry, logging the outcome.

        Return `(request, wait_secs)` when a retry should happen, or `None` to stop.
        """
        matched = matches_condition(resp, exc)
        if num_retried >= num_retries or not matched:
            _log_exhausted(num_retried, matched, resp, exc)
            return None
        if exc is not None:
            request = get_request_from_exception(exc)
        else:
            assert resp is not None
            request = resp.request
        if safe_methods_only and (request is None or request.method.upper() not in SAFE_HTTP_METHODS):
            logger.warning("Retry condition matched but skipped (safe_methods_only=True).")
            return None
        wait_secs, msg, log_extra = _get_retry_context(request, resp, exc, resolved_retry_after)
        _log_retry(wait_secs, msg, log_extra)
        return request, wait_secs

    def _snapshot_request(request: Request | None, prev: Request | None) -> Request | None:
        """Return a shallow copy of `request` that captures this attempt's state.

        The live request object is reused and mutated across retry attempts, so a snapshot
        taken before each retry preserves the failed attempt's scalar attributes (`request_id`,
        `start_time`, `end_time`) independently of later mutations.

        :param request: The request from the failed attempt, or `None` when unavailable.
        :param prev: The snapshot from the preceding attempt, used to build the `.retried` chain.
        """
        if request is None:
            return None
        snap = copy.copy(request)
        snap.retried = prev
        return snap

    def _finalize(resp: R | None, exc: Exception | None) -> R:
        """Raise the final exception if one is set, otherwise return the final response."""
        if exc is not None:
            raise exc
        return cast("R", resp)

    def _retry_sync(f: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> R:
        resolved_retry_after = retry_after.as_callable() if isinstance(retry_after, BackoffStrategy) else retry_after

        def call() -> tuple[R | None, Exception | None]:
            try:
                return f(*args, **kwargs), None
            except Exception as e:
                if not _should_swallow(e):
                    raise
                return None, e

        resp, exc = call()
        num_retried = 0
        prev_snapshot: Request | None = None
        while (plan := _prepare_retry(resp, exc, num_retried, resolved_retry_after)) is not None:
            request, wait_secs = plan
            snapshot = _snapshot_request(request, prev_snapshot)
            time.sleep(wait_secs)
            resp, exc = call()
            if resp is not None and snapshot is not None:
                resp.request.retried = snapshot
            prev_snapshot = snapshot
            num_retried += 1
        return _finalize(resp, exc)

    async def _retry_async(f: Callable[P, R | Awaitable[R]], *args: P.args, **kwargs: P.kwargs) -> R:
        resolved_retry_after = retry_after.as_callable() if isinstance(retry_after, BackoffStrategy) else retry_after

        async def call() -> tuple[R | None, Exception | None]:
            try:
                r = f(*args, **kwargs)
                if inspect.isawaitable(r):
                    r = await r
                return r, None
            except Exception as e:
                if not _should_swallow(e):
                    raise
                return None, e

        resp, exc = await call()
        num_retried = 0
        prev_snapshot: Request | None = None
        while (plan := _prepare_retry(resp, exc, num_retried, resolved_retry_after)) is not None:
            request, wait_secs = plan
            snapshot = _snapshot_request(request, prev_snapshot)
            await asyncio.sleep(wait_secs)
            resp, exc = await call()
            if resp is not None and snapshot is not None:
                resp.request.retried = snapshot
            prev_snapshot = snapshot
            num_retried += 1
        return _finalize(resp, exc)

    def decorator(f: Callable[P, R | Awaitable[R]]) -> Callable[P, R | Awaitable[R]]:
        @wraps(f)
        async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return await _retry_async(f, *args, **kwargs)

        @wraps(f)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return _retry_sync(f, *args, **kwargs)  # type: ignore[arg-type]

        if _async_mode or inspect.iscoroutinefunction(f):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


def _parse_retry_after(value: str | None) -> float | None:
    """Parse an HTTP `Retry-After` header value per RFC 7231.

    Accepts either a delta-seconds integer string or an HTTP-date string. Returns the
    number of seconds to wait (clamped to >= 0), or `None` if the value is absent or
    cannot be parsed.

    :param value: The raw `Retry-After` header value, or `None` if the header is absent
    """
    if not value:
        return None
    try:
        seconds = float(value)
        if not math.isfinite(seconds):
            return None
        return max(0.0, seconds)
    except ValueError:
        pass
    try:
        retry_at = parsedate_to_datetime(value)
        now = datetime.now(tz=UTC)
        delta = (retry_at - now).total_seconds()
        return max(0.0, delta)
    except Exception:
        return None
