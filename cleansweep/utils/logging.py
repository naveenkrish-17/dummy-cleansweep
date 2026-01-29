"""Logging utilities."""

import logging

from cleansweep.utils.google.logging import (
    CloudHandler,
    SyncTransport,
    get_job_labels,
    initialize_cloud_logging,
)

EXCLUDED_LOGGERS: tuple[str] = ("azure", "httpx")  # type: ignore


def setup_logging(
    name: str, log_level: str = "INFO", dev_mode: bool = True
) -> logging.Logger:
    """Set up logging configuration.

    Args:
        name (str): The name of the logger.
        log_level (str, optional): The log level. Defaults to "INFO".
        dev_mode (bool, optional): Whether to enable development mode. Defaults to True.

    Returns:
        logging.Logger: The configured logger.

    """
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    excl_loggers = EXCLUDED_LOGGERS
    if dev_mode or log_level == "DEBUG":
        logging.basicConfig(level=log_level)
        excl_loggers = None

    if not dev_mode:
        _ = initialize_cloud_logging(
            log_level,
            labels=get_job_labels(),
            transport=SyncTransport,
            excluded_loggers=excl_loggers,
        )

    return logger


class LoggerManager(logging.Manager):
    """Custom logger manager for error handling purposes.

    Sets the logger class for loggers created with this manager only.

    To set the logger class for all loggers, use `logging.setLoggerClass`.
    """

    def __init__(self, logger_class: type[logging.Logger] | None = None):
        super().__init__(logging.Logger.root)
        self.loggerClass = logger_class


def set_app_labels(labels: dict) -> None:
    """Set the application labels.

    Args:
        labels (dict): The application labels.

    """
    for handler in logging.getLogger().handlers:
        if isinstance(handler, CloudHandler):
            _labels = handler.labels
            _labels.update(labels)
            handler.labels = _labels
