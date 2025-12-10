from collections.abc import Callable, Collection, Mapping
from typing import Any


class HashableDict(dict[Any, Any]):
    """A hashable dictionary"""

    def __hash__(self) -> int:  # type: ignore[override]
        return hash(frozenset((k, freeze(v)) for k, v in self.items()))


def freeze(obj: Any) -> Any:
    """Recursively convert an object to be immutable and hashable

    :param obj: Any object
    """
    if isinstance(obj, HashableDict | Mapping):
        return HashableDict({k: freeze(v) for k, v in obj.items()})
    elif isinstance(obj, Collection) and not isinstance(obj, str | bytes):
        return tuple(freeze(x) for x in obj)
    else:
        return obj


def generate_hash(obj: Any, fallback_hasher: Callable[..., Any] = repr) -> int:
    """Generate a hash for the given object.

    This function first attempts to recursively convert common unhashable types to hashable ones before generating a
    hash. If it is still not hashable, the fallback_hasher will be used.

    :param obj: Any object
    :param fallback_hasher: A function to convert a complex custom unhashable object to a hashable
    """
    try:
        return hash(freeze(obj))
    except TypeError:
        if fallback_hasher is None:
            raise ValueError("The object is not hashable as is. Please provide a custom logic as fallback_hasher")
        elif not callable(fallback_hasher):
            raise TypeError("fallback_hasher must be a callable")
        return hash(fallback_hasher(obj))
