"""Tests for common_libs.hash module"""

from typing import Any

import pytest

from common_libs.hash import HashableDict, freeze, generate_hash


class TestHashableDict:
    """Tests for HashableDict class"""

    def test_hashable_dict_is_hashable(self) -> None:
        """Test that HashableDict can be hashed"""
        d = HashableDict({"a": 1, "b": 2})
        h = hash(d)
        assert isinstance(h, int)

    def test_hashable_dict_same_content_same_hash(self) -> None:
        """Test that same content produces same hash"""
        d1 = HashableDict({"a": 1, "b": 2})
        d2 = HashableDict({"a": 1, "b": 2})
        assert hash(d1) == hash(d2)

    def test_hashable_dict_different_content_different_hash(self) -> None:
        """Test that different content produces different hash"""
        d1 = HashableDict({"a": 1})
        d2 = HashableDict({"a": 2})
        assert hash(d1) != hash(d2)

    def test_hashable_dict_still_mutable(self) -> None:
        """Test that HashableDict is still mutable"""
        d = HashableDict({"a": 1})
        d["b"] = 2
        assert d["b"] == 2

    def test_hashable_dict_as_dict_key(self) -> None:
        """Test using HashableDict as dictionary key"""
        d = HashableDict({"a": 1})
        container = {d: "value"}
        assert container[d] == "value"

    def test_hashable_dict_nested(self) -> None:
        """Test HashableDict with nested structures"""
        d = HashableDict({"nested": HashableDict({"inner": 1})})
        h = hash(d)
        assert isinstance(h, int)


class TestFreeze:
    """Tests for freeze function"""

    def test_freeze_dict(self) -> None:
        """Test freezing a dictionary"""
        result = freeze({"a": 1, "b": 2})
        assert isinstance(result, HashableDict)
        assert isinstance(hash(result), int)

    def test_freeze_list(self) -> None:
        """Test freezing a list"""
        data = [1, 2, 3]
        result = freeze(data)
        assert isinstance(result, tuple)
        assert result == tuple(data)

    def test_freeze_tuple(self) -> None:
        """Test freezing a tuple"""
        data = (1, 2, 3)
        result = freeze(data)
        assert isinstance(result, tuple)
        assert result == data

    def test_freeze_set(self) -> None:
        """Test freezing a set"""
        data = {1, 2, 3}
        result = freeze(data)
        assert isinstance(result, frozenset)
        assert result == frozenset(data)

    def test_freeze_nested_structures(self) -> None:
        """Test freezing nested structures"""
        data = {"list": [1, 2], "set": {3, 4}, "nested": {"inner": [5]}}
        result = freeze(data)
        assert isinstance(result, HashableDict)
        assert isinstance(result["list"], tuple)
        assert isinstance(result["set"], frozenset)
        assert isinstance(result["nested"], HashableDict)
        assert isinstance(result["nested"]["inner"], tuple)

    def test_freeze_primitive_passthrough(self) -> None:
        """Test that primitive types pass through unchanged"""
        assert freeze(42) == 42
        assert freeze("string") == "string"
        assert freeze(3.14) == 3.14
        assert freeze(None) is None
        assert freeze(True) is True

    def test_freeze_circular_reference(self) -> None:
        """Test handling of circular references"""

        d: dict[str, Any] = {"a": 1}
        d["self"] = d  # Circular reference
        result = freeze(d)
        assert isinstance(result, HashableDict)
        # Should not raise RecursionError
        hash(result)

    def test_freeze_unhashable_object_raises(self) -> None:
        """Test that unhashable objects raise TypeError"""

        class Unhashable:
            __hash__ = None  # type: ignore[assignment]

        with pytest.raises(TypeError):
            freeze(Unhashable())


class TestGenerateHash:
    """Tests for generate_hash function"""

    def test_generate_hash_dict(self) -> None:
        """Test generating hash for dictionary"""
        result = generate_hash({"a": 1, "b": 2})
        assert isinstance(result, int)

    def test_generate_hash_list(self) -> None:
        """Test generating hash for list"""
        result = generate_hash([1, 2, 3])
        assert isinstance(result, int)

    def test_generate_hash_same_content_same_hash(self) -> None:
        """Test that same content produces same hash"""
        h1 = generate_hash({"a": 1})
        h2 = generate_hash({"a": 1})
        assert h1 == h2

    def test_generate_hash_with_fallback(self) -> None:
        """Test hash generation with fallback hasher"""

        class Unhashable:
            __hash__ = None  # type: ignore[assignment]

            def __repr__(self) -> str:
                return "Unhashable()"

        result = generate_hash(Unhashable(), fallback_hasher=repr)
        assert isinstance(result, int)

    def test_generate_hash_fallback_none_raises(self) -> None:
        """Test that None fallback raises ValueError for unhashable"""

        class Unhashable:
            __hash__ = None  # type: ignore[assignment]

        with pytest.raises(ValueError, match="not hashable"):
            generate_hash(Unhashable(), fallback_hasher=None)

    def test_generate_hash_fallback_not_callable_raises(self) -> None:
        """Test that non-callable fallback raises TypeError"""

        class Unhashable:
            __hash__ = None  # type: ignore[assignment]

        with pytest.raises(TypeError, match="must be a callable"):
            generate_hash(Unhashable(), fallback_hasher="not_callable")

    def test_generate_hash_nested_structures(self) -> None:
        """Test hash generation for nested structures"""
        data = {"a": [1, 2], "b": {"c": 3}}
        result = generate_hash(data)
        assert isinstance(result, int)
