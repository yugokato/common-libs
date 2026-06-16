"""Tests for common_libs.lock module"""

import asyncio
import signal
import threading
import time
import uuid

import pytest

from common_libs.lock import LOCK_DIR, AsyncLock, Lock, _AsyncReentrantLock


class TestLock:
    """Tests for Lock class"""

    def test_lock_context_manager(self) -> None:
        """Test using Lock as context manager"""
        with Lock("test_lock_context") as lock:
            assert lock is not None

    def test_lock_creates_lock_directory(self) -> None:
        """Test that lock directory is created"""
        with Lock("test_dir_creation"):
            assert LOCK_DIR.exists()

    def test_lock_creates_lock_file(self) -> None:
        """Test that lock file is created"""
        lock_name = "test_file_creation"
        with Lock(lock_name):
            lock_file = LOCK_DIR / f"{lock_name}.lock"
            assert lock_file.exists()

    def test_lock_mutual_exclusion(self) -> None:
        """Test that locks provide mutual exclusion"""
        # Use a read-modify-write pattern that requires mutual exclusion
        # Without lock, race conditions would cause lost updates
        shared_counter = {"value": 0}
        lock_name = "test_mutex"

        def increment_with_lock() -> None:
            with Lock(lock_name, timeout=5):
                # Read-modify-write (not atomic without lock)
                current = shared_counter["value"]
                time.sleep(0.01)  # Small delay to ensure overlap attempts
                shared_counter["value"] = current + 1

        num_threads = 5
        threads = [threading.Thread(target=increment_with_lock) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # With proper locking, all increments should be preserved
        # Without lock, some updates would be lost due to race conditions
        assert shared_counter["value"] == num_threads

    def test_lock_different_names_no_conflict(self) -> None:
        """Test that locks with different names don't conflict (allow concurrent execution)"""

        execution_times: list[float] = []
        lock_hold_time = 0.1

        def acquire_lock(name: str) -> None:
            start = time.time()
            with Lock(name):
                time.sleep(lock_hold_time)
            execution_times.append(time.time() - start)

        # Use different lock names - should allow concurrent execution
        t1 = threading.Thread(target=acquire_lock, args=(f"lock_a_{uuid.uuid4()}",))
        t2 = threading.Thread(target=acquire_lock, args=(f"lock_b_{uuid.uuid4()}",))

        overall_start = time.time()
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        overall_duration = time.time() - overall_start

        # If locks don't conflict, they run concurrently: total time ≈ lock_hold_time
        # If they conflicted (same lock), they'd serialize: total time ≈ 2 * lock_hold_time
        # Allow some overhead for thread scheduling (use 1.5x as threshold)
        assert overall_duration < lock_hold_time * 1.5, (
            f"Locks with different names should allow concurrent execution. "
            f"Expected ~{lock_hold_time}s, got {overall_duration:.2f}s"
        )

    def test_lock_default_name(self) -> None:
        """Test lock with default name"""
        with Lock() as lock:
            assert lock.name == "lock"

    def test_lock_with_timeout(self) -> None:
        """Test lock with custom timeout"""
        # Just verify it doesn't raise immediately
        with Lock("test_timeout", timeout=1):
            pass

    def test_lock_reentrant_same_thread(self) -> None:
        """Test that lock can be re-acquired in same thread (FileLock is reentrant)"""

        def timeout_handler(signum: int, frame: object) -> None:
            raise TimeoutError("Deadlock detected: Lock is not reentrant")

        lock_name = "test_reentrant"

        # Set a 2-second timeout
        old_handler = signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(2)

        try:
            with Lock(lock_name):
                with Lock(lock_name):
                    pass  # Should not deadlock
        finally:
            # Cancel the alarm and restore original handler
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)

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

    async def test_async_lock_does_not_block_event_loop(self) -> None:
        """Test that waiting to acquire an async lock does not block the event loop"""
        lock_name = f"test_async_nonblocking_{uuid.uuid4()}"
        ticks = 0

        async def heartbeat() -> None:
            nonlocal ticks
            while True:
                ticks += 1
                await asyncio.sleep(0.01)

        async def holder() -> None:
            async with AsyncLock(lock_name):
                await asyncio.sleep(0.2)

        async def waiter() -> None:
            await asyncio.sleep(0.02)  # Ensure holder grabs lock first
            async with AsyncLock(lock_name):
                pass

        hb_task = asyncio.create_task(heartbeat())
        try:
            await asyncio.gather(holder(), waiter())
        finally:
            hb_task.cancel()

        # If the loop was blocked during contention, the heartbeat would barely tick.
        # With asyncio.sleep-based polling, it should tick many times.
        assert ticks > 10, f"Event loop appeared to be blocked (ticks={ticks})"

    async def test_async_lock_different_names_no_conflict(self) -> None:
        """Test that async locks with different names allow concurrent execution"""
        lock_hold_time = 0.1
        results: list[float] = []

        async def acquire_lock(name: str) -> None:
            start = time.time()
            async with AsyncLock(name):
                await asyncio.sleep(lock_hold_time)
            results.append(time.time() - start)

        overall_start = time.time()
        await asyncio.gather(
            acquire_lock(f"async_lock_a_{uuid.uuid4()}"),
            acquire_lock(f"async_lock_b_{uuid.uuid4()}"),
        )
        overall_duration = time.time() - overall_start

        assert overall_duration < lock_hold_time * 1.5, (
            f"Locks with different names should allow concurrent execution. "
            f"Expected ~{lock_hold_time}s, got {overall_duration:.2f}s"
        )

    async def test_async_lock_reentrant_same_name(self) -> None:
        """Test that an async lock can be re-acquired within the same task without deadlocking."""
        lock_name = f"test_async_reentrant_{uuid.uuid4()}"

        async def acquire_nested() -> None:
            async with AsyncLock(lock_name):
                async with AsyncLock(lock_name):
                    pass

        # asyncio.wait_for raises TimeoutError if this deadlocks
        await asyncio.wait_for(acquire_nested(), timeout=2.0)

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

        await asyncio.wait_for(acquire_nested(), timeout=2.0)
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
