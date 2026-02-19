"""Tests for common_libs.decorators module"""

import threading
from functools import lru_cache
from typing import Any

import pytest

from common_libs.decorators import conditional_lru_cache, freeze_args, singleton


class TestSingleton:
    """Tests for singleton decorator"""

    def test_singleton_same_instance(self) -> None:
        """Test that same args return same instance"""

        @singleton
        class MySingleton:
            def __init__(self, value: int) -> None:
                self.value = value

        s1 = MySingleton(1)
        s2 = MySingleton(1)
        assert s1 is s2

    def test_singleton_different_instance_different_args(self) -> None:
        """Test that different args return different instances"""

        @singleton
        class MySingleton:
            def __init__(self, value: int) -> None:
                self.value = value

        val1, val2 = 1, 2
        s1 = MySingleton(val1)
        s2 = MySingleton(val2)
        assert s1 is not s2
        assert s1.value == val1
        assert s2.value == val2

    def test_singleton_with_kwargs(self) -> None:
        """Test singleton with keyword arguments"""

        @singleton
        class MySingleton:
            def __init__(self, value: int, name: str = "default") -> None:
                self.value = value
                self.name = name

        s1 = MySingleton(1)
        s2 = MySingleton(1, name="default")
        assert s1 is s2

        s3 = MySingleton(1, name="other")
        assert s1 is not s3

    def test_singleton_inheritance(self) -> None:
        """Test singleton with class inheritance"""

        @singleton
        class Parent:
            def __init__(self, value: int) -> None:
                self.value = value

        @singleton
        class Child(Parent):
            def __init__(self, value: int, extra: Any = None) -> None:
                super().__init__(value)
                self.extra = extra

        p1 = Parent(1)
        c1 = Child(1)
        assert p1 is not c1  # Different classes

        c2 = Child(1)
        assert c1 is c2

    def test_singleton_thread_safety(self) -> None:
        """Test singleton is thread-safe"""

        @singleton
        class MySingleton:
            def __init__(self, value: int) -> None:
                self.value = value

        instances: list[MySingleton] = []

        def create_instance() -> None:
            instances.append(MySingleton(42))

        threads = [threading.Thread(target=create_instance) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All instances should be the same
        assert all(inst is instances[0] for inst in instances)

    def test_singleton_init_called_once(self) -> None:
        """Test that __init__ is only called once per unique args"""
        call_count = 0

        @singleton
        class MySingleton:
            def __init__(self, value: int) -> None:
                nonlocal call_count
                call_count += 1
                self.value = value

        # Keep references to prevent garbage collection (WeakValueDictionary)
        s1 = MySingleton(1)
        s2 = MySingleton(1)
        s3 = MySingleton(1)
        assert call_count == 1
        assert s1 is s2 is s3


class TestFreezeArgs:
    """Tests for freeze_args decorator"""

    def test_freeze_args_with_dict(self) -> None:
        """Test freezing dictionary arguments"""
        call_count = 0

        @freeze_args
        @lru_cache
        def func(data: dict[str, int]) -> int:
            nonlocal call_count
            call_count += 1
            return sum(data.values())

        result1 = func({"a": 1, "b": 2})
        result2 = func({"a": 1, "b": 2})
        assert result1 == result2 == 3
        assert call_count == 1  # Second call should use cache

    def test_freeze_args_with_list(self) -> None:
        """Test freezing list arguments"""
        call_count = 0

        @freeze_args
        @lru_cache
        def func(items: list[int]) -> int:
            nonlocal call_count
            call_count += 1
            return sum(items)

        result1 = func([1, 2, 3])
        result2 = func([1, 2, 3])
        assert result1 == result2 == 6
        assert call_count == 1

    def test_freeze_args_with_nested_structures(self) -> None:
        """Test freezing nested structures enables caching without raising TypeError"""
        call_count = 0

        @freeze_args
        @lru_cache
        def func(data: dict[str, Any]) -> int:
            nonlocal call_count
            call_count += 1
            return call_count

        result1 = func({"nested": [1, 2, {"inner": "value"}]})
        result2 = func({"nested": [1, 2, {"inner": "value"}]})
        assert result1 == result2 == 1  # Second call should use cache
        assert call_count == 1

    def test_freeze_args_with_kwargs(self) -> None:
        """Test freezing keyword arguments"""

        call_count = 0

        @freeze_args
        @lru_cache
        def func(a: int, **kwargs: Any) -> int:
            nonlocal call_count
            call_count += 1
            return a

        func(1, data={"key": "value"})
        func(1, data={"key": "value"})
        assert call_count == 1


class TestConditionalLruCache:
    """Tests for conditional_lru_cache decorator"""

    def test_conditional_cache_always_cache(self) -> None:
        """Test caching when condition is None (always cache)"""
        call_count = 0

        @conditional_lru_cache()
        def func(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        assert func(5) == 10
        assert func(5) == 10
        assert call_count == 1

    def test_conditional_cache_with_condition_true(self) -> None:
        """Test caching when condition returns True"""
        call_count = 0

        @conditional_lru_cache(condition=lambda x: x > 0)
        def func(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        assert func(5) == 10
        assert func(5) == 10
        assert call_count == 1  # Cached because x > 0

    def test_conditional_cache_with_condition_false(self) -> None:
        """Test bypassing cache when condition returns False"""
        call_count = 0

        @conditional_lru_cache(condition=lambda x: x > 0)
        def func(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        assert func(-1) == -2
        assert func(-1) == -2
        assert call_count == 2  # Not cached because x <= 0

    def test_conditional_cache_ignore_unhashable(self) -> None:
        """Test ignore_if_unhashable option"""
        call_count = 0

        @conditional_lru_cache(ignore_if_unhashable=True)
        def func(data: dict[str, int]) -> int:
            nonlocal call_count
            call_count += 1
            return sum(data.values())

        assert func({"a": 1}) == 1
        assert func({"a": 1}) == 1
        assert call_count == 2  # Called twice, not cached due to unhashable

    def test_conditional_cache_unhashable_raises(self) -> None:
        """Test that unhashable arguments raise TypeError when ignore_if_unhashable=False"""

        @conditional_lru_cache(ignore_if_unhashable=False)
        def func(data: dict[str, int]) -> int:
            return sum(data.values())

        with pytest.raises(TypeError):
            func({"a": 1})

    def test_conditional_cache_maxsize(self) -> None:
        """Test maxsize parameter"""
        call_count = 0

        @conditional_lru_cache(maxsize=2)
        def func(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x

        func(1)
        func(2)
        func(3)  # Evicts 1 from cache
        func(1)  # Should be recomputed
        assert call_count == 4
