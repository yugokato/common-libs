import inspect
from functools import wraps
from threading import RLock
from typing import Any, Callable, ParamSpec, Type, TypeVar
from weakref import WeakValueDictionary

from common_libs.hash import freeze, generate_hash

T = TypeVar("T")
P = ParamSpec("P")


def singleton(cls: Type[T]) -> Type[T]:
    """A decorator to make the decorated class to work as singleton.

    If the class takes arguments in __init__(), the same instance will be reused only when the instance attributes after
    initialization are identical.

    Examples:
    >>> @singleton
    >>> class Singleton:
    >>>     def __init__(self, param1: int, param2: int = 2, param3: int = None, **kwargs):
    >>>         self.param1 = param1
    >>>         self.param2 = param2
    >>>         self.param3 = param3
    >>>
    >>> s1 = Singleton(1)
    >>> assert s1 is Singleton(1)
    >>> assert s1 is Singleton(1, param2=2)
    >>> assert s1 is Singleton(1, param2=2, param3=None)
    >>> assert s1 is not Singleton(2)
    >>> assert s1 is not Singleton(1, param2=22)
    >>> assert s1 is not Singleton(1, param3=3)
    >>> assert s1 is not Singleton(1, foo="bar")
    >>>
    >>> # The singleton class can be inherited by another singleton class
    >>> @singleton
    >>> class Singleton2(Singleton):
    >>>     def __init__(self, param1: int, **kwargs):
    >>>         super().__init__(param1, **kwargs)
    >>>
    >>> s2 = Singleton2(1)
    >>> assert s2 is Singleton2(1)
    >>> assert s2 is not Singleton2(2)
    >>>
    """
    orig_new = cls.__new__
    orig_init = cls.__init__
    instances: WeakValueDictionary[tuple[Type[T], int], T] = WeakValueDictionary()
    cls._lock = RLock()

    @wraps(orig_new)
    def __new__(cls: Type[T], *args: Any, **kwargs: Any) -> T:
        sig = inspect.signature(orig_init)
        try:
            bound_args = sig.bind(cls, *args, **kwargs)
        except TypeError:
            # Passed arguments can not be bound. Let it surfice it as an error from __new__()
            return orig_new(cls)
        else:
            bound_args.apply_defaults()
            key = (cls, generate_hash(bound_args.arguments))
            with cls._lock:
                if key not in instances:
                    instance = orig_new(cls)
                    instances[key] = instance
            return instances[key]

    @wraps(orig_init)
    def __init__(self: T, *args: Any, **kwargs: Any):
        with cls._lock:
            # Ensure init is called only once
            if getattr(self, "__initialized", False):
                return
            orig_init(self, *args, **kwargs)
            self.__initialized = True

    cls.__new__ = __new__
    cls.__init__ = __init__
    return cls


def freeze_args(f: Callable[P, T]) -> Callable[P, T]:
    """A decorator to freeze function arguments

    This is useful for making lru_cache to work on a function that takes mutable arguments (eg. dictionary)

    Usage:
        @freeze_args
        @lru_cache
        def do_something(*args, **kwargs):
            ...
    """

    @wraps(f)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        args = freeze(args)
        kwargs = {k: freeze(v) for k, v in kwargs.items()}
        return f(*args, **kwargs)

    return wrapper
