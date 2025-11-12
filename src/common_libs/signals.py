import atexit
import signal
from collections.abc import Callable
from functools import wraps
from threading import current_thread, main_thread
from typing import Any


def register_exit_handler(func: Callable[..., Any], *func_args: Any, **func_kwargs: Any) -> None:
    """Register an exit handler to make sure the function is always called when a program terminates"""

    def wrap_signal_handler(f: Callable[..., Any]) -> Callable[..., Any]:
        orig_handler = signal.getsignal(signal.SIGTERM)

        @wraps(f)
        def wrapper(signum: Any, frame: Any) -> None:
            try:
                f(*func_args, **func_kwargs)
            finally:
                if callable(orig_handler):
                    orig_handler(signum, frame)

        return wrapper

    if current_thread() is main_thread():
        atexit.register(func, *func_args, **func_kwargs)
        signal.signal(signal.SIGTERM, wrap_signal_handler(func))
