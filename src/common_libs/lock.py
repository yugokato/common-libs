import os
from pathlib import Path

from filelock import FileLock

from common_libs.logging import get_logger
from common_libs.signals import register_exit_handler

logger = get_logger(__name__)

LOCK_DIR = Path(Path.home(), ".locks").resolve()


class Lock:
    """A class to manage lock among multiple processes"""

    LOCK_FILE = "{name}.lock"

    def __init__(self, name: str = "lock", timeout: float = -1):
        self.name = name
        if not LOCK_DIR.exists():
            os.makedirs(LOCK_DIR, exist_ok=True)
        self._lock_file = LOCK_DIR / self.LOCK_FILE.format(name=self.name)
        self._lock = FileLock(self._lock_file, timeout=timeout, is_singleton=True)

        register_exit_handler(self.__del__)

    def __enter__(self):
        if not self._lock.is_locked:
            logger.debug(f"Acquiring lock: {self._lock_file.name}")
        try:
            self._lock.acquire()
        except FileNotFoundError:
            self._lock = FileLock(self._lock_file)
            self._lock.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._lock.release()
        if not self._lock.is_locked:
            logger.debug(f"Released lock: {self._lock_file.name}")

    def __del__(self):
        try:
            with self._lock:
                self._lock.__del__()
                self._lock_file.unlink(missing_ok=True)
        except FileNotFoundError:
            pass
