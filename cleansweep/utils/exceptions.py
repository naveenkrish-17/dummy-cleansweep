"""Utilities for handling exceptions."""

import logging
import sys
import traceback
from pathlib import Path
from types import TracebackType
from typing import Callable, Optional, TypeAlias

from cleansweep import __app_name__
from cleansweep.settings.base import settings
from cleansweep.utils.logging import LoggerManager
from cleansweep.utils.slack import send_error_message

HookFunction: TypeAlias = Callable[
    [type[BaseException], BaseException, TracebackType | None], None
]


class ExceptionHandlerSingleton:
    """Singleton class for custom exception handling."""

    __instance = None
    __errors: Optional[tuple[type[BaseException]]] = None
    __critical: Optional[tuple[type[BaseException]]] = None
    __uncaught_hook: HookFunction = sys.excepthook
    __error_hook: Optional[HookFunction] = None
    __critical_hook: Optional[HookFunction] = None

    def __new__(cls):
        """Create a new instance of the class."""
        if cls.__instance is None:
            cls.__instance = super(ExceptionHandlerSingleton, cls).__new__(cls)
        return cls.__instance

    @classmethod
    def uncaught_hook(cls) -> HookFunction:
        """Return the uncaught hook."""
        return cls.__uncaught_hook

    @classmethod
    def error_hook(cls) -> HookFunction | None:
        """Return the uncaught hook."""
        return cls.__error_hook

    @classmethod
    def critical_hook(cls) -> HookFunction | None:
        """Return the uncaught hook."""
        return cls.__critical_hook

    @classmethod
    def except_hook(
        cls,
        exctype: type[BaseException],
        value: BaseException,
        traceback: TracebackType | None,  # pylint: disable=redefined-outer-name
        /,
    ) -> None:
        """Handle exceptions.

        Args:
            exctype (type[BaseException]): The exception type.
            value (BaseException): The exception value.
            traceback (TracebackType | None): The traceback.

        """
        # Ensure __errors and __critical are tuples when used
        errors = cls.__errors if cls.__errors is not None else ()
        critical = cls.__critical if cls.__critical is not None else ()

        if not errors and not critical:
            return cls.uncaught_hook()(exctype, value, traceback)

        if isinstance(value, critical):  # Check against the value, not the type
            if isinstance(cls.critical_hook(), Callable):
                return cls.critical_hook()(  # pylint: disable=not-callable # pyright: ignore[reportOptionalCall]
                    exctype, value, traceback
                )
            logger.critical("A critical error occurred: %s", value)
            sys.exit(1)

        if isinstance(value, errors):  # Check against the value, not the type
            if isinstance(cls.error_hook(), Callable):
                return cls.error_hook()(  # pylint: disable=not-callable # pyright: ignore[reportOptionalCall]
                    exctype, value, traceback
                )
            logger.error("An error occurred: %s", value)
            sys.exit(1)

        return cls.uncaught_hook()(exctype, value, traceback)

    @classmethod
    def set_errors(cls, errors: Optional[tuple[type[BaseException]]]) -> None:
        """Set the errors."""
        cls.__errors = errors

    @classmethod
    def set_critical(cls, critical: Optional[tuple[type[BaseException]]]) -> None:
        """Set the critical errors."""
        cls.__critical = critical

    @classmethod
    def set_error_hook(cls, hook: HookFunction) -> None:
        """Set the error hook."""
        cls.__error_hook = hook

    @classmethod
    def set_critical_hook(cls, hook: HookFunction) -> None:
        """Set the critical hook."""
        cls.__critical_hook = hook

    @classmethod
    def set_uncaught_hook(cls, hook: HookFunction) -> None:
        """Set the uncaught hook."""
        cls.__uncaught_hook = hook

    @property
    def errors(self) -> Optional[tuple[type[BaseException]]]:
        """Return the errors."""
        return self.__errors

    @property
    def critical(self) -> Optional[tuple[type[BaseException]]]:
        """Return the critical errors."""
        return self.__critical


class ErrorLogger(logging.Logger):
    """Custom logger for error handling purposes."""

    def handle(self, record: logging.LogRecord) -> None:
        """Handle the log record.

        Manipulates the log record to include the filename and function name of the exception.

        Args:
            record (logging.LogRecord): The log record.

        """
        if record.exc_info:
            _, _, exc_traceback = record.exc_info

            while exc_traceback:
                if (
                    exc_traceback.tb_next is None
                    or exc_traceback.tb_next.tb_frame.f_globals.get(
                        "__name__", __app_name__
                    )
                    != __app_name__
                ):
                    # if we're at the last frame or the next frame is not in the app
                    # this should give us the frame for the function that raised the exception
                    break
                exc_traceback = exc_traceback.tb_next

            if exc_traceback is not None:
                record.filename = Path(exc_traceback.tb_frame.f_code.co_filename).name
                record.funcName = exc_traceback.tb_frame.f_code.co_name
                record.name = exc_traceback.tb_frame.f_globals.get(
                    "__name__", record.name
                )

        super().handle(record)


logger = LoggerManager(ErrorLogger).getLogger(__app_name__)
"""Module logger."""


def initialize_except_hook(
    errors: Optional[tuple[type[BaseException]]] = None,
    critical: Optional[tuple[type[BaseException]]] = None,
    error_hook: Optional[HookFunction] = None,
    critical_hook: Optional[HookFunction] = None,
    uncaught_hook: Optional[HookFunction] = None,
):
    """Initialize the exception hook.

    Args:
        errors (Optional[tuple[type[BaseException]]], optional): The errors to catch. Defaults to
            None.
        critical (Optional[tuple[type[BaseException]]], optional): The critical errors to catch.
            Defaults to None.
        error_hook (Optional[HookFunction], optional): The error hook. Defaults to None.
        critical_hook (Optional[HookFunction], optional): The critical hook. Defaults to None.
        uncaught_hook (Optional[HookFunction], optional): The uncaught hook. Defaults to None.

    """
    if error_hook:
        ExceptionHandlerSingleton().set_error_hook(error_hook)

    if critical_hook:
        ExceptionHandlerSingleton().set_critical_hook(critical_hook)

    if uncaught_hook:
        ExceptionHandlerSingleton().set_uncaught_hook(uncaught_hook)

    ExceptionHandlerSingleton().set_errors(errors)
    ExceptionHandlerSingleton().set_critical(critical)
    sys.excepthook = ExceptionHandlerSingleton().except_hook


def error_handler(
    exc_type: type[BaseException],
    exc_value: BaseException,
    exc_traceback: TracebackType | None,
):
    """Handle uncaught exceptions.

    Args:
        exc_type (type[BaseException]): The exception type.
        exc_value (BaseException): The exception value.
        exc_traceback (TracebackType | None): The traceback.

    """
    logger.error(
        "%s: %s",
        exc_type.__name__,
        exc_value,
        exc_info=(exc_type, exc_value, exc_traceback),
    )
    if isinstance(exc_value, ExceptionGroup):
        for i, exception in enumerate(exc_value.exceptions):
            logger.error(
                "Sub-exception %s - %s: %s",
                i + 1,
                exception.__class__.__name__,
                exception,
                exc_info=(exc_type, exc_value, exc_traceback),
            )
    logger.error(
        "Traceback: %s",
        traceback.format_tb(exc_traceback),
        exc_info=(exc_type, exc_value, exc_traceback),
    )
    send_error_message(settings.default_channel, error=exc_value)
