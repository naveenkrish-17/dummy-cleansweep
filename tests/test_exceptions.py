"""Test suite for the `cleansweep.exceptions` module."""

import sys
from unittest import mock

from cleansweep.utils.exceptions import (
    ExceptionHandlerSingleton,
    initialize_except_hook,
)


class TestInitializeExceptHook:
    """Test suite for the `initialize_except_hook` function."""

    def test_initialize_except_hook_with_default_parameters(self):
        """Test the `initialize_except_hook` function with default parameters."""
        with (
            mock.patch.object(
                ExceptionHandlerSingleton, "set_error_hook"
            ) as mock_set_error_hook,
            mock.patch.object(
                ExceptionHandlerSingleton, "set_critical_hook"
            ) as mock_set_critical_hook,
            mock.patch.object(
                ExceptionHandlerSingleton, "set_uncaught_hook"
            ) as mock_set_uncaught_hook,
            mock.patch.object(
                ExceptionHandlerSingleton, "set_errors"
            ) as mock_set_errors,
            mock.patch.object(
                ExceptionHandlerSingleton, "set_critical"
            ) as mock_set_critical,
        ):
            initialize_except_hook()
            mock_set_error_hook.assert_not_called()
            mock_set_critical_hook.assert_not_called()
            mock_set_uncaught_hook.assert_not_called()
            mock_set_errors.assert_called_once_with(None)
            mock_set_critical.assert_called_once_with(None)

    def test_initialize_except_hook_with_custom_hooks(self):
        """Test the `initialize_except_hook` function with custom hooks."""
        custom_error_hook = mock.Mock()
        custom_critical_hook = mock.Mock()
        custom_uncaught_hook = mock.Mock()
        with (
            mock.patch.object(
                ExceptionHandlerSingleton, "set_error_hook"
            ) as mock_set_error_hook,
            mock.patch.object(
                ExceptionHandlerSingleton, "set_critical_hook"
            ) as mock_set_critical_hook,
            mock.patch.object(
                ExceptionHandlerSingleton, "set_uncaught_hook"
            ) as mock_set_uncaught_hook,
        ):
            initialize_except_hook(
                error_hook=custom_error_hook,
                critical_hook=custom_critical_hook,
                uncaught_hook=custom_uncaught_hook,
            )
            mock_set_error_hook.assert_called_once_with(custom_error_hook)
            mock_set_critical_hook.assert_called_once_with(custom_critical_hook)
            mock_set_uncaught_hook.assert_called_once_with(custom_uncaught_hook)

    def test_initialize_except_hook_with_error_and_critical_lists(self):
        """Test the `initialize_except_hook` function with error and critical lists."""
        errors = (ValueError,)
        critical = (SystemExit,)
        with (
            mock.patch.object(
                ExceptionHandlerSingleton, "set_errors"
            ) as mock_set_errors,
            mock.patch.object(
                ExceptionHandlerSingleton, "set_critical"
            ) as mock_set_critical,
        ):
            initialize_except_hook(errors=errors, critical=critical)
            mock_set_errors.assert_called_once_with(errors)
            mock_set_critical.assert_called_once_with(critical)


class TestExceptionHandlerSingleton:
    """Test suite for the `ExceptionHandlerSingleton` class."""

    def test_singleton_instance(self):
        """Test that `ExceptionHandlerSingleton` always returns the same instance."""
        instance1 = ExceptionHandlerSingleton()
        instance2 = ExceptionHandlerSingleton()
        assert (
            instance1 is instance2
        ), "ExceptionHandlerSingleton should return the same instance"

    def test_set_errors(self):
        """Test setting errors in `ExceptionHandlerSingleton`."""
        errors = (ValueError, TypeError)
        ExceptionHandlerSingleton().set_errors(errors)
        assert (
            ExceptionHandlerSingleton().errors == errors
        ), "Errors should be set correctly"

    def test_set_critical(self):
        """Test setting critical errors in `ExceptionHandlerSingleton`."""
        critical = (SystemExit, KeyboardInterrupt)
        ExceptionHandlerSingleton().set_critical(critical)
        assert (
            ExceptionHandlerSingleton().critical == critical
        ), "Critical errors should be set correctly"

    def test_set_error_hook(self):
        """Test setting an error hook in `ExceptionHandlerSingleton`."""

        def error_hook(exctype, value, traceback):
            pass

        ExceptionHandlerSingleton().set_error_hook(error_hook)
        assert (
            ExceptionHandlerSingleton().error_hook() == error_hook
        ), "Error hook should be set correctly"

    def test_set_critical_hook(self):
        """Test setting a critical hook in `ExceptionHandlerSingleton`."""

        def critical_hook(exctype, value, traceback):
            pass

        ExceptionHandlerSingleton().set_critical_hook(critical_hook)
        assert (
            ExceptionHandlerSingleton().critical_hook() == critical_hook
        ), "Critical hook should be set correctly"

    def test_set_uncaught_hook(self):
        """Test setting an uncaught hook in `ExceptionHandlerSingleton`."""

        def uncaught_hook(exctype, value, traceback):
            pass

        original_hook = sys.excepthook
        ExceptionHandlerSingleton().set_uncaught_hook(uncaught_hook)
        assert (
            ExceptionHandlerSingleton().uncaught_hook() == uncaught_hook
        ), "Uncaught hook should be set correctly"
        # Restore the original excepthook after the test
        sys.excepthook = original_hook

    def test_except_hook_with_no_errors_or_critical(self, mocker):
        """Test `except_hook` method when no errors or critical are set."""
        mocker.patch.object(
            ExceptionHandlerSingleton, "uncaught_hook", return_value=lambda *args: None
        )
        ExceptionHandlerSingleton().set_errors(None)
        ExceptionHandlerSingleton().set_critical(None)
        ExceptionHandlerSingleton().except_hook(ValueError, ValueError(), None)
        ExceptionHandlerSingleton.uncaught_hook.assert_called_once()

    def test_except_hook_with_critical_error(self, mocker):
        """Test `except_hook` method with a critical error."""
        critical_hook_mock = mocker.Mock()
        ExceptionHandlerSingleton().set_critical_hook(critical_hook_mock)
        ExceptionHandlerSingleton().set_critical((SystemExit,))
        ExceptionHandlerSingleton().except_hook(SystemExit, SystemExit(), None)
        critical_hook_mock.assert_called_once()

    def test_except_hook_with_error(self, mocker):
        """Test `except_hook` method with an error."""
        error_hook_mock = mocker.Mock()
        ExceptionHandlerSingleton().set_error_hook(error_hook_mock)
        ExceptionHandlerSingleton().set_errors((ValueError,))
        ExceptionHandlerSingleton().except_hook(ValueError, ValueError(), None)
        error_hook_mock.assert_called_once()
