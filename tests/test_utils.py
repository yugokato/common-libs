"""Tests for common_libs.utils module"""

from collections.abc import Callable
from functools import wraps
from typing import Any

import pytest
from pytest_mock import MockFixture

from common_libs.utils import (
    dedup,
    is_decorator_with_args,
    list_items,
    log_section,
    merge_dicts,
    prompt_confirmation,
    wait_until,
)


class TestPromptConfirmation:
    """Tests for prompt_confirmation function"""

    def test_prompt_confirmation_yes(self, mocker: MockFixture) -> None:
        """Test confirmation with 'Y' input"""
        mocker.patch("builtins.input", return_value="Y")
        # Should not raise or exit
        prompt_confirmation("Continue?")

    def test_prompt_confirmation_no_exits(self, mocker: MockFixture) -> None:
        """Test confirmation with 'N' input exits"""
        mocker.patch("builtins.input", return_value="N")
        with pytest.raises(SystemExit):
            prompt_confirmation("Continue?")

    def test_prompt_confirmation_keyboard_interrupt(self, mocker: MockFixture) -> None:
        """Test confirmation with KeyboardInterrupt exits"""
        mocker.patch("builtins.input", side_effect=KeyboardInterrupt)
        with pytest.raises(SystemExit):
            prompt_confirmation("Continue?")

    def test_prompt_confirmation_custom_yes_no(self, mocker: MockFixture) -> None:
        """Test confirmation with custom yes/no values"""
        mocker.patch("builtins.input", return_value="yes")
        prompt_confirmation("Continue?", yes="yes", no="no")


class TestMergeDicts:
    """Tests for merge_dicts function"""

    def test_merge_dicts_basic(self) -> None:
        """Test basic dictionary merge"""
        dict1 = {"a": 1}
        dict2 = {"b": 2}
        result = merge_dicts(dict1, dict2)
        assert result == {"a": 1, "b": 2}

    def test_merge_dicts_overwrite(self) -> None:
        """Test that dict2 serves as base and dict1 adds missing keys"""
        # Note: merge_dicts uses dict2 as base, dict1 values only add missing keys
        dict1 = {"a": "from_dict1", "b": "only_in_dict1"}
        dict2 = {"a": "from_dict2", "c": "only_in_dict2"}
        result = merge_dicts(dict1, dict2)
        # dict2 values are preserved when keys exist in both
        assert result["a"] == "from_dict2"
        # dict1 keys not in dict2 are added
        assert result["b"] == "only_in_dict1"
        # dict2 keys are preserved
        assert result["c"] == "only_in_dict2"

    def test_merge_dicts_nested(self) -> None:
        """Test merging nested dictionaries"""
        dict1 = {"nested": {"a": 1}}
        dict2 = {"nested": {"b": 2}}
        result = merge_dicts(dict1, dict2)
        assert result["nested"]["a"] == 1
        assert result["nested"]["b"] == 2

    def test_merge_dicts_deep_nested(self) -> None:
        """Test deeply nested merge"""
        dict1 = {"l1": {"l2": {"l3": "value1"}}}
        dict2 = {"l1": {"l2": {"other": "value2"}}}
        result = merge_dicts(dict1, dict2)
        assert result["l1"]["l2"]["l3"] == "value1"
        assert result["l1"]["l2"]["other"] == "value2"

    def test_merge_dicts_preserves_originals(self) -> None:
        """Test that original dicts are not modified"""
        dict1 = {"a": 1}
        dict2 = {"b": 2}
        merge_dicts(dict1, dict2)
        assert dict1 == {"a": 1}
        assert dict2 == {"b": 2}


class TestListItems:
    """Tests for list_items function"""

    def test_list_items_basic(self) -> None:
        """Test basic list formatting"""
        result = list_items(["a", "b", "c"])
        assert result == "- a\n- b\n- c"

    def test_list_items_custom_style(self) -> None:
        """Test custom bullet style"""
        result = list_items(["a", "b"], style="*")
        assert result == "* a\n* b"

    def test_list_items_with_indent(self) -> None:
        """Test indentation"""
        result = list_items(["a", "b"], indent=2)
        assert result == "  - a\n  - b"

    def test_list_items_multiline(self) -> None:
        """Test multiline items"""
        result = list_items(["line1\nline2", "item2"])
        assert "line1" in result
        assert "line2" in result

    def test_list_items_empty(self) -> None:
        """Test empty list"""
        result = list_items([])
        assert result == ""


class TestLogSection:
    """Tests for log_section function"""

    def test_log_section_basic(self, mocker: MockFixture) -> None:
        """Test basic section logging"""
        mock_logger = mocker.patch("common_libs.utils.logger")
        log_section("Test Section")
        mock_logger.info.assert_called_once()
        call_arg = mock_logger.info.call_args[0][0]
        assert "Test Section" in call_arg
        assert "#" in call_arg

    def test_log_section_sub_section(self, mocker: MockFixture) -> None:
        """Test sub-section logging"""
        mock_logger = mocker.patch("common_libs.utils.logger")
        log_section("Test Sub", sub_section=True)
        mock_logger.info.assert_called_once()
        call_arg = mock_logger.info.call_args[0][0]
        assert "Test Sub" in call_arg
        assert "-" in call_arg


class TestDedup:
    """Tests for dedup function"""

    def test_no_duplicates(self) -> None:
        """Test that unique values are returned unchanged"""
        assert dedup(1, 2, 3) == (1, 2, 3)

    def test_duplicates_removed_order_preserved(self) -> None:
        """Test that duplicates are removed while preserving the original order"""
        assert dedup(1, 2, 1, 3, 2) == (1, 2, 3)

    def test_strings(self) -> None:
        """Test deduplication with string values"""
        assert dedup("a", "b", "a", "c") == ("a", "b", "c")

    def test_mixed_hashable_types(self) -> None:
        """Test deduplication with mixed hashable types"""
        assert dedup(1, "a", (1, 2), 1, "a") == (1, "a", (1, 2))

    def test_empty(self) -> None:
        """Test that no arguments returns an empty tuple"""
        assert dedup() == ()

    def test_single_object(self) -> None:
        """Test that a single object is returned as a one-element tuple"""
        assert dedup(42) == (42,)

    def test_returns_tuple(self) -> None:
        """Test that the return type is always a tuple"""
        result = dedup(1, 2, 3)
        assert isinstance(result, tuple)


class TestWaitUntil:
    """Tests for wait_until function"""

    def test_wait_until_immediate_success(self) -> None:
        """Test immediate success"""
        expected = 42
        result = wait_until(lambda: expected, timeout=1)
        assert result == expected

    def test_wait_until_delayed_success(self) -> None:
        """Test delayed success"""
        counter = [0]
        expected = "done"

        def func() -> str | None:
            counter[0] += 1
            return expected if counter[0] >= 2 else None

        result = wait_until(func, interval=0.1, timeout=5)
        assert result == expected

    def test_wait_until_timeout(self) -> None:
        """Test timeout raises TimeoutError"""
        with pytest.raises(TimeoutError):
            wait_until(lambda: None, interval=0.1, timeout=0.3)

    def test_wait_until_with_args(self) -> None:
        """Test function with arguments"""
        result = wait_until(lambda x, y: x + y, func_args=(1, 2), timeout=1)
        assert result == 3

    def test_wait_until_with_kwargs(self) -> None:
        """Test function with keyword arguments"""
        expected = 10
        result = wait_until(lambda x=0: x, func_kwargs={"x": expected}, timeout=1)
        assert result == expected

    def test_wait_until_custom_stop_condition(self) -> None:
        """Test custom stop condition"""
        counter = [0]

        def func() -> int:
            counter[0] += 1
            return counter[0]

        result = wait_until(func, interval=0.1, timeout=5, stop_condition=lambda x: x >= 3)
        assert result == 3


class TestIsDecoratorWithArgs:
    """Tests for is_decorator_with_args function"""

    def test_regular_decorator(self) -> None:
        """Test detecting regular decorator"""

        def decorator(f: Callable[..., Any]) -> Callable[..., Any]:
            @wraps(f)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                return f(*args, **kwargs)

            return wrapper

        assert is_decorator_with_args(decorator) is False

    def test_decorator_with_args(self) -> None:
        """Test detecting decorator with arguments"""

        def decorator_with_args(
            arg1: Any, /, *, arg2: str = "default"
        ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
            def decorator(f: Callable[..., Any]) -> Callable[..., Any]:
                @wraps(f)
                def wrapper(*args: Any, **kwargs: Any) -> Any:
                    return f(*args, **kwargs)

                return wrapper

            return decorator

        assert is_decorator_with_args(decorator_with_args) is True

    def test_non_callable(self) -> None:
        """Test non-callable returns False"""
        assert is_decorator_with_args("not a function") is False
        assert is_decorator_with_args(123) is False

    def test_decorator_returning_non_callable(self) -> None:
        """Test decorator returning non-callable"""

        def not_a_decorator(x: int) -> int:
            return 42

        assert is_decorator_with_args(not_a_decorator) is False
