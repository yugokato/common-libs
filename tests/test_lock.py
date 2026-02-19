"""Tests for common_libs.lock module"""

import signal
import threading
import time
import uuid

from common_libs.lock import LOCK_DIR, Lock


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
