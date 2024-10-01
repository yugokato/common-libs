import atexit
import signal
from collections.abc import Callable
from functools import wraps
from threading import current_thread, main_thread


def register_exit_handler(func: Callable, *func_args, **func_kwargs):
    """Register an exit handler to make sure the function is always called when a program terminates"""

    def wrap_signal_handler(f):
        @wraps(f)
        def wrapper(signum, frame):
            return f(*func_args, **func_kwargs)

        return wrapper

    if current_thread() is main_thread():
        atexit.register(func, *func_args, **func_kwargs)
        signal.signal(signal.SIGTERM, wrap_signal_handler(func))
