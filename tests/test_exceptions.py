"""Tests for common_libs.exceptions module"""

import pytest

from common_libs.exceptions import CommandError, NotFound


class TestNotFound:
    """Tests for NotFound exception"""

    def test_not_found_raise(self) -> None:
        """Test raising NotFound exception"""
        with pytest.raises(NotFound):
            raise NotFound("item not found")

    def test_not_found_message(self) -> None:
        """Test NotFound exception message"""
        message = "test message"
        with pytest.raises(NotFound, match=message):
            raise NotFound(message)


class TestCommandError:
    """Tests for CommandError exception"""

    def test_command_error_with_message(self) -> None:
        """Test CommandError with message only"""
        message = "command failed"
        error = CommandError(message)
        assert str(error) == message
        assert error.exit_code is None

    def test_command_error_with_exit_code(self) -> None:
        """Test CommandError with exit code"""
        message = "command failed"
        error = CommandError(message, exit_code=1)
        assert str(error) == message
        assert error.exit_code == 1

    def test_command_error_raise(self) -> None:
        """Test raising CommandError"""
        with pytest.raises(CommandError) as exc_info:
            raise CommandError("failed", exit_code=127)

        assert exc_info.value.exit_code == 127  # type: ignore[unreachable]

    def test_command_error_non_string_message(self) -> None:
        """Test CommandError with non-string message"""
        error = CommandError({"error": "details"})
        assert "error" in str(error)
