import asyncio
import threading
import weakref
from pathlib import Path
from typing import Any, Self

from filelock import AsyncFileLock, FileLock

from common_libs.logging import get_logger

logger = get_logger(__name__)

LOCK_DIR = Path(Path.home(), ".locks").resolve()

# Per-event-loop, per-name reentrant locks used by AsyncLock. The outer WeakKeyDictionary drops a
# loop's entry once the loop is collected; the inner WeakValueDictionary drops a name entry once no
# AsyncLock instance holds a strong ref. asyncio.Lock must never be shared across event loops.
_async_locks: weakref.WeakKeyDictionary[
    asyncio.AbstractEventLoop, weakref.WeakValueDictionary[str, "_AsyncReentrantLock"]
] = weakref.WeakKeyDictionary()
_async_locks_guard = threading.Lock()


class _BaseLock:
    """Base class providing shared setup for sync and async file locks."""

    LOCK_FILE = "{name}.lock"

    def __init__(self, name: str = "lock") -> None:
        self.name = name
        LOCK_DIR.mkdir(parents=True, exist_ok=True)
        self._lock_file = LOCK_DIR / self.LOCK_FILE.format(name=self.name)


class Lock(_BaseLock):
    """A class to manage lock among multiple processes."""

    def __init__(self, name: str = "lock", timeout: float = -1, is_singleton: bool = True, **kwargs: Any) -> None:
        """
        Create a cross-process file lock.

        :param name: Lock name, used to derive the lock file path.
        :param timeout: Seconds to wait before giving up; -1 means wait forever.
        :param is_singleton: Reuse the same `FileLock` instance for the same file path across calls.
        :param kwargs: Extra keyword arguments forwarded to `filelock.FileLock`.
        """
        super().__init__(name)
        self._lock = FileLock(self._lock_file, timeout=timeout, is_singleton=is_singleton, **kwargs)

    def __enter__(self) -> Self:
        if not self._lock.is_locked:
            logger.debug(f"Acquiring lock: {self._lock_file.name}")
        try:
            self._lock.acquire()
        except FileNotFoundError:
            LOCK_DIR.mkdir(parents=True, exist_ok=True)
            self._lock.acquire()
        return self

    def __exit__(self, *args: Any) -> None:
        self._lock.release()
        if not self._lock.is_locked:
            logger.debug(f"Released lock: {self._lock_file.name}")


class AsyncLock(_BaseLock):
    """A class to manage lock among multiple processes without blocking the event loop.

    Use as an async context manager: `async with AsyncLock("name"): ...`.

    Provides two layers of mutual exclusion:
    - **Intra-process**: a reentrant asyncio lock serializes concurrent coroutines
      within the same event loop; the same task may re-enter without deadlocking.
    - **Cross-process**: `filelock.AsyncFileLock` serializes different processes via
      OS-level file locking, using `asyncio.sleep` between retries so the event loop
      remains responsive while waiting.

    **Concurrency boundary:** a given lock name is safe within a single event loop and
    across *sequential* event loops (e.g. successive `asyncio.run()` calls on the same
    instance). It does **not** provide mutual exclusion when two event loops in different
    threads run *concurrently* using the same lock name in the same process — cross-process
    exclusion is unaffected.
    """

    def __init__(self, name: str = "lock", timeout: float = -1, is_singleton: bool = True, **kwargs: Any) -> None:
        """
        Create an async cross-process file lock.

        :param name: Lock name, used to derive the lock file path.
        :param timeout: Seconds to wait before giving up; -1 means wait forever.
        :param is_singleton: Reuse the same `AsyncFileLock` instance for the same file path across calls.
        :param kwargs: Extra keyword arguments forwarded to `filelock.AsyncFileLock`.
        """
        super().__init__(name)
        self._lock = AsyncFileLock(self._lock_file, timeout=timeout, is_singleton=is_singleton, **kwargs)
        self._asyncio_lock: _AsyncReentrantLock | None = None

    async def __aenter__(self) -> Self:
        asyncio_lock = self._get_asyncio_lock()
        if not asyncio_lock.held_by_current_task:
            logger.debug(f"Acquiring lock: {self._lock_file.name}")
        await asyncio_lock.acquire()
        try:
            try:
                await self._lock.acquire()
            except FileNotFoundError:
                LOCK_DIR.mkdir(parents=True, exist_ok=True)
                await self._lock.acquire()
        except BaseException:
            asyncio_lock.release()
            raise
        self._asyncio_lock = asyncio_lock
        return self

    async def __aexit__(self, *args: Any) -> None:
        asyncio_lock = self._asyncio_lock
        if asyncio_lock is None:
            return
        try:
            await self._lock.release()
        finally:
            asyncio_lock.release()
            if not asyncio_lock.is_locked:
                logger.debug(f"Released lock: {self._lock_file.name}")
                self._asyncio_lock = None

    def _get_asyncio_lock(self) -> "_AsyncReentrantLock":
        """Return the per-(loop, name) reentrant lock for the running event loop."""
        loop = asyncio.get_running_loop()
        with _async_locks_guard:
            loop_locks = _async_locks.get(loop)
            if loop_locks is None:
                loop_locks = weakref.WeakValueDictionary()
                _async_locks[loop] = loop_locks
            asyncio_lock = loop_locks.get(self.name)
            if asyncio_lock is None:
                asyncio_lock = _AsyncReentrantLock()
                loop_locks[self.name] = asyncio_lock
        return asyncio_lock


class _AsyncReentrantLock:
    """A reentrant asyncio lock that tracks ownership by the current asyncio Task.

    Unlike `asyncio.Lock`, the same task may acquire this lock multiple times without
    deadlocking; each acquisition increments an internal depth counter, and the
    underlying lock is released only when that counter reaches zero.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._owner: asyncio.Task[Any] | None = None
        self._depth = 0

    async def acquire(self) -> None:
        """Acquire the lock, blocking until it is available."""
        current = asyncio.current_task()
        if current is None:
            raise RuntimeError("AsyncLock can only be acquired from within an asyncio task")
        if self._owner is current:
            self._depth += 1
            return
        await self._lock.acquire()
        self._owner = current
        self._depth = 1

    def release(self) -> None:
        """Release the lock."""
        if self._depth == 0:
            return
        if self._owner is not asyncio.current_task():
            raise RuntimeError("AsyncLock can only be released by the task that holds it")
        self._depth -= 1
        if self._depth == 0:
            self._owner = None
            self._lock.release()

    @property
    def is_locked(self) -> bool:
        """Returns True when the lock is currently held."""
        return self._lock.locked()

    @property
    def held_by_current_task(self) -> bool:
        """Returns True when the current task already holds the lock."""
        return self._owner is not None and self._owner is asyncio.current_task()
