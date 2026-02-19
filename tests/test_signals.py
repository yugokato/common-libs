"""Tests for common_libs.signals module"""

import signal
import threading

from pytest_mock import MockFixture

from common_libs.signals import register_exit_handler


class TestRegisterExitHandler:
    """Tests for register_exit_handler function"""

    def test_register_exit_handler_basic(self, mocker: MockFixture) -> None:
        """Test basic handler registration"""
        handler = mocker.MagicMock()

        mock_atexit = mocker.patch("atexit.register")
        mock_signal = mocker.patch("signal.signal")
        mocker.patch("signal.getsignal", return_value=signal.SIG_DFL)

        register_exit_handler(handler, "arg1", key="value")
        mock_atexit.assert_called_once()
        mock_signal.assert_called_once()

    def test_register_exit_handler_with_args(self, mocker: MockFixture) -> None:
        """Test handler registration with arguments"""
        handler = mocker.MagicMock()

        mock_atexit = mocker.patch("atexit.register")
        mocker.patch("signal.signal")
        mocker.patch("signal.getsignal", return_value=signal.SIG_DFL)

        register_exit_handler(handler, 1, 2, 3, a="b")
        mock_atexit.assert_called_once_with(handler, 1, 2, 3, a="b")

    def test_register_exit_handler_not_main_thread(self, mocker: MockFixture) -> None:
        """Test that handler is not registered from non-main thread"""

        handler = mocker.MagicMock()
        registered: list[bool] = []

        def register_in_thread() -> None:
            mock_atexit = mocker.patch("atexit.register")
            register_exit_handler(handler)
            registered.append(mock_atexit.called)

        thread = threading.Thread(target=register_in_thread)
        thread.start()
        thread.join()

        # atexit.register should NOT have been called from non-main thread
        assert registered == [False]

    def test_register_exit_handler_sigterm_wrapper(self, mocker: MockFixture) -> None:
        """Test that SIGTERM handler wraps the function properly"""
        handler = mocker.MagicMock()
        original_handler = mocker.MagicMock()

        mocker.patch("atexit.register")
        mock_signal = mocker.patch("signal.signal")
        mocker.patch("signal.getsignal", return_value=original_handler)

        register_exit_handler(handler)
        # Get the wrapper function passed to signal.signal
        call_args = mock_signal.call_args
        assert call_args[0][0] == signal.SIGTERM
        wrapper = call_args[0][1]

        # Call the wrapper and verify both handlers are called
        wrapper(signal.SIGTERM, None)
        handler.assert_called_once()
        original_handler.assert_called_once_with(signal.SIGTERM, None)

    def test_register_exit_handler_preserves_original_sig_dfl(self, mocker: MockFixture) -> None:
        """Test that SIG_DFL original handler doesn't cause error"""
        handler = mocker.MagicMock()

        mocker.patch("atexit.register")
        mock_signal = mocker.patch("signal.signal")
        mocker.patch("signal.getsignal", return_value=signal.SIG_DFL)

        register_exit_handler(handler)
        wrapper = mock_signal.call_args[0][1]

        # Should not raise when original handler is SIG_DFL (not callable)
        wrapper(signal.SIGTERM, None)
        handler.assert_called_once()
