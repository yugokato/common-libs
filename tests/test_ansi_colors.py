"""Tests for common_libs.ansi_colors module"""

import io

import pytest

from common_libs.ansi_colors import ColorCodes, color, escape_color_code, remove_color_code, should_color


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
    """Tests for color function

    Styling assertions run with `FORCE_COLOR=1` (autouse) so they're deterministic regardless of whether the
    test run's `sys.stdout` is a terminal. The `should_color()`-driven gate itself is covered by
    `test_color_returns_plain_text_when_color_disabled` below and by `TestShouldColor`.
    """

    @pytest.fixture(autouse=True)
    def _force_color(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Force `should_color()` to `True` so styling assertions don't depend on the test run's `sys.stdout`"""
        monkeypatch.setenv("FORCE_COLOR", "1")

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

    def test_color_returns_plain_text_when_color_disabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that color() returns the text unchanged when should_color() is False, even with
        color/bold/underline/escape set"""
        monkeypatch.setenv("FORCE_COLOR", "0")
        text = "test"
        result = color(text, color_code=ColorCodes.RED, bold=True, underline=True, escape=True)
        assert result == text

    def test_color_non_string_input_converted_when_color_disabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that non-string input is still converted to string when color is disabled"""
        monkeypatch.setenv("FORCE_COLOR", "0")
        result = color(123, color_code=ColorCodes.GREEN)
        assert result == "123"


class TestShouldColor:
    """Tests for should_color function"""

    def test_should_color_true_when_stream_is_a_tty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that color is enabled when the stream is a terminal and no env vars are set"""
        monkeypatch.delenv("FORCE_COLOR", raising=False)
        monkeypatch.delenv("NO_COLOR", raising=False)
        stream = io.StringIO()
        monkeypatch.setattr(stream, "isatty", lambda: True)
        assert should_color(stream) is True

    def test_should_color_false_when_stream_is_not_a_tty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that color is disabled when the stream is not a terminal and no env vars are set"""
        monkeypatch.delenv("FORCE_COLOR", raising=False)
        monkeypatch.delenv("NO_COLOR", raising=False)
        assert should_color(io.StringIO()) is False

    def test_should_color_false_when_stream_has_no_isatty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that color is disabled when the stream has no `isatty` attribute"""
        monkeypatch.delenv("FORCE_COLOR", raising=False)
        monkeypatch.delenv("NO_COLOR", raising=False)

        class StreamWithoutIsatty:
            pass

        assert should_color(StreamWithoutIsatty()) is False

    def test_should_color_defaults_to_stdout(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that `should_color()` checks `sys.stdout` when no stream is specified"""
        monkeypatch.delenv("FORCE_COLOR", raising=False)
        monkeypatch.delenv("NO_COLOR", raising=False)
        monkeypatch.setattr("sys.stdout.isatty", lambda: True)
        assert should_color() is True

    def test_should_color_false_when_no_color_env_var_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that `NO_COLOR` disables color even on a terminal"""
        monkeypatch.setenv("NO_COLOR", "1")
        stream = io.StringIO()
        monkeypatch.setattr(stream, "isatty", lambda: True)
        assert should_color(stream) is False

    @pytest.mark.parametrize("value", ["0", "false", "False"], ids=["zero", "false", "false-capitalized"])
    def test_should_color_false_when_force_color_env_var_is_falsy(
        self, monkeypatch: pytest.MonkeyPatch, value: str
    ) -> None:
        """Test that a falsy `FORCE_COLOR` disables color even on a terminal"""
        monkeypatch.setenv("FORCE_COLOR", value)
        stream = io.StringIO()
        monkeypatch.setattr(stream, "isatty", lambda: True)
        assert should_color(stream) is False

    def test_should_color_true_when_force_color_env_var_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that a truthy `FORCE_COLOR` enables color even when the stream is not a terminal"""
        monkeypatch.setenv("FORCE_COLOR", "1")
        assert should_color(io.StringIO()) is True

    def test_should_color_force_color_overrides_no_color(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that a truthy `FORCE_COLOR` takes precedence over `NO_COLOR`"""
        monkeypatch.setenv("NO_COLOR", "1")
        monkeypatch.setenv("FORCE_COLOR", "1")
        assert should_color(io.StringIO()) is True


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
