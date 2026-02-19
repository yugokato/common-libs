from collections.abc import Callable, Hashable, Mapping
from typing import Any


class HashableDict(dict[Any, Any]):
    """A hashable dictionary

    NOTE: This obj is still mutable
    """

    def __hash__(self) -> int:  # type: ignore[override]
        return self._hash(frozenset())

    def _hash(self, seen: frozenset[int]) -> int:
        obj_id = id(self)
        if obj_id in seen:
            return 0  # circular reference sentinel
        seen = seen | {obj_id}

        def _h(obj: Any) -> int:
            if isinstance(obj, HashableDict):
                return obj._hash(seen)
            return hash(obj)

        return hash(frozenset((_h(k), _h(v)) for k, v in self.items()))


def freeze(obj: Any) -> Any:
    """Convert the object to hashable

    :param obj: Any object
    """

    seen: dict[int, Any] = {}

    def _freeze(o: Any) -> Any:
        obj_id = id(o)
        if obj_id in seen:
            return seen[obj_id]

        if isinstance(o, Mapping):
            hashable_dict = HashableDict()
            seen[obj_id] = hashable_dict
            for k, v in o.items():
                hashable_dict[_freeze(k)] = _freeze(v)
            return hashable_dict
        elif isinstance(o, list | tuple):
            placeholder_list: list[Any] = []
            seen[obj_id] = placeholder_list
            placeholder_list.extend(_freeze(x) for x in o)
            frozen_tuple = tuple(placeholder_list)
            seen[obj_id] = frozen_tuple
            return frozen_tuple
        elif isinstance(o, set):
            placeholder_set: set[Any] = set()
            seen[obj_id] = placeholder_set
            placeholder_set.update(_freeze(x) for x in o)
            frozen_set = frozenset(placeholder_set)
            seen[obj_id] = frozen_set
            return frozen_set
        else:
            if not isinstance(o, Hashable):
                raise TypeError(f"Unhashable object of type {type(o).__name__}")
            return o

    return _freeze(obj)


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
