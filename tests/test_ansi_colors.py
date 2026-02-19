"""Tests for common_libs.ansi_colors module"""

import pytest

from common_libs.ansi_colors import ColorCodes, color, escape_color_code, remove_color_code


class TestColorCodes:
    """Tests for ColorCodes class"""

    def test_color_codes_exist(self) -> None:
        """Verify all expected color codes are defined"""
        assert ColorCodes.DEFAULT == "\x1b[0m"
        assert ColorCodes.RED == "\x1b[31m"
        assert ColorCodes.GREEN == "\x1b[32m"
        assert ColorCodes.YELLOW == "\x1b[33m"
        assert ColorCodes.BLUE == "\x1b[34m"
        assert ColorCodes.BOLD == "\x1b[1m"
        assert ColorCodes.UNDERLINE == "\x1b[4m"


class TestColor:
    """Tests for color function"""

    def test_color_default_green(self) -> None:
        """Test default color is green"""
        text = "test"
        result = color(text)
        assert result == f"{ColorCodes.GREEN}{text}{ColorCodes.DEFAULT}"

    def test_color_with_specific_color(self) -> None:
        """Test applying specific color"""
        text = "test"
        result = color(text, color_code=ColorCodes.RED)
        assert result == f"{ColorCodes.RED}{text}{ColorCodes.DEFAULT}"

    def test_color_with_bold(self) -> None:
        """Test applying bold style"""
        text = "test"
        result = color(text, color_code=ColorCodes.GREEN, bold=True)
        assert result == f"{ColorCodes.GREEN}{ColorCodes.BOLD}{text}{ColorCodes.DEFAULT}"

    def test_color_with_underline(self) -> None:
        """Test applying underline style"""
        text = "test"
        result = color(text, color_code=ColorCodes.GREEN, underline=True)
        assert result == f"{ColorCodes.GREEN}{ColorCodes.UNDERLINE}{text}{ColorCodes.DEFAULT}"

    def test_color_with_bold_and_underline(self) -> None:
        """Test applying both bold and underline"""
        text = "test"
        result = color(text, bold=True, underline=True)
        assert result == f"{ColorCodes.GREEN}{ColorCodes.UNDERLINE}{ColorCodes.BOLD}{text}{ColorCodes.DEFAULT}"

    def test_color_none_color_code(self) -> None:
        """Test with None color code"""
        text = "test"
        result = color(text, color_code=None)
        assert result == text

    def test_color_with_escape(self) -> None:
        """Test escape mode for terminal prompt compatibility"""
        text = "test"
        result = color(text, color_code=ColorCodes.GREEN, escape=True)
        assert result == f"\x01{ColorCodes.GREEN}\x02{text}\x01{ColorCodes.DEFAULT}\x02"

    @pytest.mark.parametrize("value", [123, 45.67, True, None, ["foo"], {"k": "v"}])
    def test_color_non_string_input(self, value: object) -> None:
        """Test that non-string input is converted to string"""
        result = color(value, color_code=ColorCodes.GREEN)
        assert result == f"{ColorCodes.GREEN}{value!s}{ColorCodes.DEFAULT}"


class TestRemoveColorCode:
    """Tests for remove_color_code function"""

    def test_remove_color_code_basic(self) -> None:
        """Test removing basic color codes"""
        colored = f"{ColorCodes.GREEN}test{ColorCodes.DEFAULT}"
        result = remove_color_code(colored)
        assert result == "test"

    def test_remove_color_code_multiple(self) -> None:
        """Test removing multiple color codes"""
        colored = f"{ColorCodes.RED}red{ColorCodes.DEFAULT}{ColorCodes.BLUE}blue{ColorCodes.DEFAULT}"
        result = remove_color_code(colored)
        assert result == "redblue"

    def test_remove_color_code_no_codes(self) -> None:
        """Test string without color codes"""
        text = "plain text"
        result = remove_color_code(text)
        assert result == text

    def test_remove_color_code_styles(self) -> None:
        """Test removing style codes (bold, underline)"""
        colored = f"{ColorCodes.BOLD}bold{ColorCodes.UNDERLINE}underline{ColorCodes.DEFAULT}"
        result = remove_color_code(colored)
        assert result == "boldunderline"


class TestEscapeColorCode:
    """Tests for escape_color_code function"""

    def test_escape_color_code_basic(self) -> None:
        """Test escaping basic color codes"""
        colored = f"{ColorCodes.GREEN}test{ColorCodes.DEFAULT}"
        result = escape_color_code(colored)
        assert f"\x01{ColorCodes.GREEN}\x02" in result
        assert f"\x01{ColorCodes.DEFAULT}\x02" in result

    def test_escape_color_code_no_codes(self) -> None:
        """Test string without color codes"""
        text = "plain text"
        result = escape_color_code(text)
        assert result == text
