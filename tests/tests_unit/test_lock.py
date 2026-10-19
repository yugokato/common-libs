"""Tests for common_libs.lock module"""

import asyncio
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor

import pytest
from filelock import Timeout

from common_libs.lock import LOCK_DIR, AsyncLock, Lock, _AsyncReentrantLock

pytestmark = [pytest.mark.unittest]

# Seconds a wait may take before the test gives up. A passing test never waits this long
WAIT_TIMEOUT = 10.0


@pytest.fixture
def lock_name_factory() -> Iterator[Callable[[], str]]:
    """Create lock names that are unique to the test, and remove their lock files afterwards"""
    names: list[str] = []

    def _make_name() -> str:
        names.append(f"test_{uuid.uuid4().hex}")
        return names[-1]

    yield _make_name

    for name in names:
        (LOCK_DIR / f"{name}.lock").unlink(missing_ok=True)


@pytest.fixture
def lock_name(lock_name_factory: Callable[[], str]) -> str:
    """A lock name that is unique to the test, so that no other test or process holds the lock"""
    return lock_name_factory()


class TestLock:
    """Tests for Lock class"""

    def test_lock_context_manager(self, lock_name: str) -> None:
        """Test using Lock as context manager"""
        with Lock(lock_name) as lock:
            assert lock is not None

    def test_lock_creates_lock_directory(self, lock_name: str) -> None:
        """Test that lock directory is created"""
        with Lock(lock_name):
            assert LOCK_DIR.exists()

    def test_lock_creates_lock_file(self, lock_name: str) -> None:
        """Test that lock file is created"""
        with Lock(lock_name):
            lock_file = LOCK_DIR / f"{lock_name}.lock"
            assert lock_file.exists()

    def test_lock_mutual_exclusion(self, lock_name: str) -> None:
        """Test that only one thread at a time can hold a lock"""
        counter = 0

        def increment_with_lock() -> None:
            nonlocal counter
            with Lock(lock_name, timeout=WAIT_TIMEOUT):
                current = counter
                time.sleep(0.01)  # Widens the window in which a broken lock lets another thread in
                counter = current + 1

        num_threads = 5
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(increment_with_lock) for _ in range(num_threads)]
        for future in futures:
            future.result()

        assert counter == num_threads, "Increments were lost, so threads held the lock at the same time"

    def test_lock_excludes_other_threads(self, lock_name: str) -> None:
        """Test that a lock held by one thread cannot be taken by another thread until it is released"""
        with Lock(lock_name, timeout=0):
            taken_while_held = _try_acquire_in_another_thread(lock_name)
        taken_after_release = _try_acquire_in_another_thread(lock_name)

        assert not taken_while_held, "Another thread took the lock while it was held"
        assert taken_after_release, "The lock could not be taken after it was released"

    def test_lock_different_names_no_conflict(self, lock_name_factory: Callable[[], str]) -> None:
        """Test that a lock can be taken while a lock with a different name is held"""
        with Lock(lock_name_factory(), timeout=0):
            taken = _try_acquire_in_another_thread(lock_name_factory())

        assert taken, "A lock with a different name could not be taken while the first one was held"

    def test_lock_default_name(self) -> None:
        """Test that a lock without a name uses the default name and lock file"""
        lock = Lock()
        assert (lock.name, lock._lock_file.name) == ("lock", "lock.lock")

    def test_lock_with_timeout(self, lock_name: str) -> None:
        """Test lock with custom timeout"""
        # Just verify it doesn't raise immediately
        with Lock(lock_name, timeout=1):
            pass

    def test_lock_reentrant_same_thread(self, lock_name: str) -> None:
        """Test that lock can be re-acquired in same thread (FileLock is reentrant)"""
        with Lock(lock_name, timeout=0):
            taken_again = _try_acquire(lock_name)

        assert taken_again, "The thread that holds the lock could not take it again"

    def test_lock_settings_preserved_after_file_not_found_fallback(self) -> None:
        """Test that Lock reuses its FileLock instance with original settings after a FileNotFoundError."""
        lock = Lock(f"test_fnf_{uuid.uuid4()}", timeout=42)
        original_filelock = lock._lock
        real_acquire = lock._lock.acquire
        calls: list[int] = []

        def acquire_with_one_failure(*args: object, **kwargs: object) -> object:
            calls.append(1)
            if len(calls) == 1:
                raise FileNotFoundError("simulated missing directory")
            return real_acquire(*args, **kwargs)

        lock._lock.acquire = acquire_with_one_failure

        with lock:
            pass

        assert lock._lock is original_filelock, "FileLock instance was unexpectedly replaced"
        assert lock._lock.timeout == 42, "timeout was reset after fallback"


class TestAsyncLock:
    """Tests for AsyncLock class"""

    async def test_async_lock_context_manager(self) -> None:
        """Test using AsyncLock as async context manager"""
        async with AsyncLock(f"test_async_ctx_{uuid.uuid4()}") as lock:
            assert lock is not None

    async def test_async_lock_creates_lock_file(self) -> None:
        """Test that lock file is created"""
        lock_name = f"test_async_file_{uuid.uuid4()}"
        async with AsyncLock(lock_name):
            lock_file = LOCK_DIR / f"{lock_name}.lock"
            assert lock_file.exists()

    async def test_async_lock_mutual_exclusion(self) -> None:
        """Test that async locks provide mutual exclusion among concurrent coroutines"""
        shared_counter = {"value": 0}
        lock_name = f"test_async_mutex_{uuid.uuid4()}"

        async def increment_with_lock() -> None:
            async with AsyncLock(lock_name, timeout=5):
                current = shared_counter["value"]
                await asyncio.sleep(0.01)  # Yield to let other coroutines attempt to acquire
                shared_counter["value"] = current + 1

        num_tasks = 5
        await asyncio.gather(*[increment_with_lock() for _ in range(num_tasks)])

        assert shared_counter["value"] == num_tasks

    async def test_async_lock_does_not_block_event_loop(self, lock_name: str) -> None:
        """Test that waiting for an async lock that another thread holds does not block the event loop"""
        lock_held, loop_ran = threading.Event(), threading.Event()

        def hold_lock() -> bool:
            with Lock(lock_name, timeout=WAIT_TIMEOUT):
                lock_held.set()
                return loop_ran.wait(WAIT_TIMEOUT)

        async def take_lock() -> None:
            async with AsyncLock(lock_name):
                pass

        with ThreadPoolExecutor(max_workers=1) as executor:
            holder = executor.submit(hold_lock)
            assert lock_held.wait(WAIT_TIMEOUT), "The holder thread did not get the lock"
            waiter = asyncio.create_task(take_lock())
            await asyncio.sleep(0)  # Lets the waiter run until it waits for the lock
            waited = not waiter.done()
            loop_ran.set()
            await waiter

        assert holder.result(), "The event loop was blocked while the async lock waited"
        assert waited, "The async lock was taken while another thread held it"

    async def test_async_lock_different_names_no_conflict(self, lock_name_factory: Callable[[], str]) -> None:
        """Test that an async lock can be taken while an async lock with a different name is held"""
        async with AsyncLock(lock_name_factory(), timeout=0):
            # A new task, because the task that holds a lock may take it again
            taken = await asyncio.create_task(_try_acquire_async(lock_name_factory()))

        assert taken, "An async lock with a different name could not be taken while the first one was held"

    async def test_async_lock_reentrant_same_name(self) -> None:
        """Test that an async lock can be re-acquired within the same task without deadlocking."""
        lock_name = f"test_async_reentrant_{uuid.uuid4()}"

        async def acquire_nested() -> None:
            async with AsyncLock(lock_name):
                async with AsyncLock(lock_name):
                    pass

        # asyncio.wait_for raises TimeoutError if this deadlocks
        await asyncio.wait_for(acquire_nested(), timeout=WAIT_TIMEOUT)

    async def test_async_lock_asyncio_lock_released_on_file_acquire_failure(self) -> None:
        """Test that the intra-process asyncio lock is released when the file lock acquisition fails."""
        lock_name = f"test_async_leak_{uuid.uuid4()}"

        async def always_fail() -> None:
            raise RuntimeError("injected failure")

        lock_instance = AsyncLock(lock_name)
        lock_instance._lock.acquire = always_fail

        # Capture the asyncio lock before __aenter__ to keep a strong ref across the failure;
        # without it the WeakValueDictionary could drop the entry and _get_asyncio_lock() would
        # return a fresh (always-unlocked) instance, masking a real leak.
        asyncio_lock = lock_instance._get_asyncio_lock()

        try:
            await lock_instance.__aenter__()
        except RuntimeError:
            pass

        # The asyncio lock must have been released; a leaked lock would leave this True.
        assert not asyncio_lock.is_locked, "asyncio lock was not released after file lock acquisition failure"

    async def test_async_lock_settings_preserved_after_file_not_found_fallback(self) -> None:
        """Test that AsyncLock reuses its AsyncFileLock instance with original settings after a FileNotFoundError."""
        lock = AsyncLock(f"test_async_fnf_{uuid.uuid4()}", timeout=42)
        original_filelock = lock._lock
        real_acquire = lock._lock.acquire
        calls: list[int] = []

        async def acquire_with_one_failure(*args: object, **kwargs: object) -> object:
            calls.append(1)
            if len(calls) == 1:
                raise FileNotFoundError("simulated missing directory")
            return await real_acquire(*args, **kwargs)

        lock._lock.acquire = acquire_with_one_failure

        async with lock:
            pass

        assert lock._lock is original_filelock, "AsyncFileLock instance was unexpectedly replaced"
        assert lock._lock.timeout == 42, "timeout was reset after fallback"

    async def test_async_lock_asyncio_lock_cleared_after_exit(self) -> None:
        """Test that _asyncio_lock is reset to None after a normal async with exit."""
        lock_name = f"test_async_cleared_{uuid.uuid4()}"
        lock = AsyncLock(lock_name)
        async with lock:
            pass
        assert lock._asyncio_lock is None, "_asyncio_lock should be None after full release"

    async def test_async_lock_reentrant_same_instance(self) -> None:
        """Test that the same AsyncLock instance can be re-entered without deadlocking."""
        lock_name = f"test_async_reentrant_same_inst_{uuid.uuid4()}"
        lock = AsyncLock(lock_name)

        async def acquire_nested() -> None:
            async with lock:
                async with lock:
                    pass

        await asyncio.wait_for(acquire_nested(), timeout=WAIT_TIMEOUT)
        assert lock._asyncio_lock is None, "_asyncio_lock should be None after full release"

    async def test_async_reentrant_lock_acquire_without_task_raises(self, mocker: pytest.FixtureRequest) -> None:
        """Test that _AsyncReentrantLock.acquire raises when there is no current task."""
        lock = _AsyncReentrantLock()
        mocker.patch("common_libs.lock.asyncio.current_task", return_value=None)
        with pytest.raises(RuntimeError, match="asyncio task"):
            await lock.acquire()

    async def test_async_reentrant_lock_release_by_non_owner_raises(self) -> None:
        """Test that _AsyncReentrantLock.release raises when called by a task that doesn't hold the lock."""
        lock = _AsyncReentrantLock()
        owner_acquired = asyncio.Event()
        can_release = asyncio.Event()
        non_owner_error: list[Exception] = []

        async def owner() -> None:
            await lock.acquire()
            owner_acquired.set()
            await can_release.wait()
            lock.release()

        async def non_owner() -> None:
            await owner_acquired.wait()
            try:
                lock.release()
            except RuntimeError as exc:
                non_owner_error.append(exc)
            can_release.set()

        await asyncio.gather(
            asyncio.create_task(owner()),
            asyncio.create_task(non_owner()),
        )

        assert len(non_owner_error) == 1
        assert "task that holds it" in str(non_owner_error[0])
        assert not lock.is_locked

    async def test_async_lock_safe_across_event_loops(self) -> None:
        """Test that a long-lived AsyncLock works correctly when used across different event loops."""
        name = f"test_cross_loop_{uuid.uuid4()}"

        class Service:
            def __init__(self) -> None:
                self._lock = AsyncLock(name)

            async def contend(self) -> None:
                shared = {"value": 0}

                async def worker() -> None:
                    async with self._lock:
                        current = shared["value"]
                        await asyncio.sleep(0.01)
                        shared["value"] = current + 1

                await asyncio.gather(worker(), worker())
                assert shared["value"] == 2

        svc = Service()
        errors: list[Exception] = []

        def run_session() -> None:
            try:
                asyncio.run(svc.contend())
            except Exception as exc:
                errors.append(exc)

        # Two sequential asyncio.run() calls (distinct event loops) sharing the same Service.
        # Previously raised: RuntimeError: asyncio.Lock is bound to a different event loop.
        t1 = threading.Thread(target=run_session)
        t2 = threading.Thread(target=run_session)
        t1.start()
        t1.join()
        t2.start()
        t2.join()

        assert not errors, f"AsyncLock broke across event loops: {errors}"


def _try_acquire(name: str) -> bool:
    """Try to take the lock without waiting, and release it right away. Return whether it was taken"""
    try:
        with Lock(name, timeout=0):
            return True
    except Timeout:
        return False


def _try_acquire_in_another_thread(name: str) -> bool:
    """Try to take the lock from a new thread without waiting. Return whether it was taken"""
    executor = ThreadPoolExecutor(max_workers=1)
    try:
        return executor.submit(_try_acquire, name).result(timeout=WAIT_TIMEOUT)
    finally:
        executor.shutdown(wait=False)  # Do not wait for a worker that is stuck on the lock this thread holds


async def _try_acquire_async(name: str) -> bool:
    """Try to take the async lock without waiting for the file lock, and release it right away. Return whether it was
    taken
    """
    try:
        async with asyncio.timeout(WAIT_TIMEOUT), AsyncLock(name, timeout=0):
            return True
    except TimeoutError:  # The wait for the asyncio lock of this process ended, or the file lock is held elsewhere
        return False
