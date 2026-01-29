"""Module for writing logs to Google Cloud Logging."""

__all__ = [
    "BackgroundThreadTransport",
    "CloudHandler",
    "detect_resource",
    "get_job_labels",
    "initialize_cloud_logging",
    "SyncTransport",
]

import logging
import os
import re
import time
from logging import Handler, LogRecord
from typing import Any, Type

from google.cloud.logging import Client
from google.cloud.logging_v2.handlers._monitored_resources import detect_resource
from google.cloud.logging_v2.handlers.handlers import EXCLUDED_LOGGER_DEFAULTS
from google.cloud.logging_v2.handlers.transports import (
    BackgroundThreadTransport,
    Transport,
)
from google.cloud.logging_v2.handlers.transports.sync import SyncTransport
from google.cloud.logging_v2.resource import Resource


def get_job_labels() -> dict:
    """Get the Cloud Run Job labels from the environment variables.

    Returns
    -------
      A dictionary containing the job labels.

    """
    return {
        "run.googleapis.com/execution_name": os.getenv(
            "CLOUD_RUN_EXECUTION", "unknown"
        ),
        "run.googleapis.com/task_index": os.getenv("CLOUD_RUN_TASK_INDEX", "unknown"),
        "run.googleapis.com/task_attempt": os.getenv(
            "CLOUD_RUN_TASK_ATTEMPT", "unknown"
        ),
    }


class CloudHandler(Handler):
    """A handler class that writes logs to Google Cloud Logging."""

    def __init__(
        self,
        name: str = __name__,
        resource: Resource | None = None,
        labels: dict | None = None,
        transport: Type[Transport] = BackgroundThreadTransport,
        project: str | None = None,
    ):
        Handler.__init__(self)
        if labels is None:
            labels = {}

        self._cloud_logging_client = Client(project=project)
        self.set_name(name)
        self._transport = transport(self._cloud_logging_client, name)
        self._resource = resource if resource else Resource(type="global", labels={})
        self._labels = labels

    @property
    def labels(self) -> dict:
        """Return the labels associated with the cloud logging instance.

        Returns
        -------
            dict: A dictionary containing the labels.

        """
        return self._labels

    @labels.setter
    def labels(self, labels: dict) -> None:
        """Set the labels for the logger.

        Args:
        ----
            labels (dict): The `labels` parameter is a dictionary that contains the labels to set
                for the logger.

        """
        self._labels = labels

    @property
    def resource(self) -> Resource:
        """Return the resource associated with the cloud logging instance.

        Returns
        -------
            Resource: The resource associated with the cloud logging instance.

        """
        return self._resource

    @resource.setter
    def resource(self, resource: Resource) -> None:
        """Set the resource for the logger.

        Args:
        ----
            resource (Resource): The `resource` parameter is an instance of the `Resource` class
                that contains information about the resource associated with the logger.

        """
        self._resource = resource

    def _format_time(self, epoch: float, fmt: str = "%Y-%m-%dT%H:%M:%SZ") -> str:
        """Return a formatted string representation of the given epoch time.

        Args:
        ----
            epoch (float): The epoch parameter is a timestamp representing the number of seconds
                that have elapsed since January 1, 1970, 00:00:00 UTC.
            fmt (str): The format parameter is a string that specifies the desired format for the
                output time string. It uses the same format codes as the strftime() method in the
                datetime module. Defaults to %Y-%m-%dT%H:%M:%SZ

        Returns:
        -------
          a formatted string representation of the given epoch time.

        """
        return time.strftime(fmt, time.gmtime(epoch))

    def _format_as_dict(self, record: LogRecord) -> tuple[
        dict[str, str],
        dict[str, Any],
        dict[str, Any],
        Resource | None,
    ]:
        """Format a log record as a dictionary.

        Args:
        ----
            record (LogRecord): The `record` parameter is an instance of the `LogRecord` class. It
                contains information about the log message being emitted, such as the log level, log
                message, and other attributes.

        Returns:
        -------
            A tuple containing four elements:
            1. A dictionary with keys "severity" and "message".
            2. A dictionary with keys "file", "line", and "function".
            3. A dictionary of labels.
            4. A Resource object or None.

        """
        resource: dict[str, str] | None = getattr(record, "resource", None)
        labels = getattr(record, "labels", {})
        return (
            {
                "severity": record.levelname,
                "message": re.sub(r" {2,}", " ", record.getMessage()),
            },
            {
                "file": record.filename,
                "line": getattr(  # pyright: ignore[reportCallIssue]
                    record,
                    "line_no",
                    record.lineno,  # pyright: ignore[reportArgumentType]
                ),
                "function": f"{record.name}.{record.funcName}",
            },
            labels if labels is not None else {},
            resource if resource is None else Resource(**resource),
        )

    def emit(self, record: LogRecord) -> None:
        """Write a log record to Google Cloud Logging.

        Args:
        ----
            record (LogRecord): The `record` parameter is an instance of the `LogRecord` class. It
                contains information about the log message being emitted, such as the log level, log
                message, and other attributes.

        """
        message, source_location, labels, resource = self._format_as_dict(record)
        if self._labels:
            labels.update(self._labels)
        try:
            self._transport.send(
                record,
                message,
                resource=self._resource if resource is None else resource,
                labels={k: v for k, v in labels.items() if v is not None},
                source_location=source_location,
            )
        except RecursionError:  # See issue 36272
            raise
        except Exception:  # pylint: disable=broad-except
            self.handleError(record)


def initialize_cloud_logging(
    log_level: int | str = logging.INFO,
    labels: dict | None = None,
    excluded_loggers: tuple[str] | None = None,
    transport: Type[Transport] = BackgroundThreadTransport,
    project: str | None = None,
) -> CloudHandler:
    """Initialize the Google Cloud Logging handler and attach it to the root logger.

    Args:
    ----
        log_level (int): The `log_level` parameter is an optional parameter that specifies the
            logging level for the logger. It determines the severity of the messages that will be
            logged. The available logging levels are defined in the `logging` module and include
            `DEBUG`, `INFO`, `WARNING`, `ERROR`, and `CRITICAL`. Defaults to `logging.INFO`.
        labels (dict): The `labels` parameter is an optional parameter that specifies the labels for
            the logger. It is a dictionary that contains the labels to set for the logger. Defaults
            to `None`.
        excluded_loggers (tuple[str]): The `excluded_loggers` parameter is an optional parameter that
            specifies the list of loggers to exclude from logging. Defaults to `None`.
        transport (Transport): The `transport` parameter is an optional parameter that specifies the
            transport to use for logging. Defaults to `BackgroundThreadTransport`. The other
            option is `SyncTransport`.
        project (str): The `project` parameter is an optional parameter that specifies the Google
            Cloud project ID to use for logging. Defaults to `None`.

    Returns:
    -------
        CloudHandler: The function returns a `CloudHandler` instance that logs records to the Google
        Cloud Logging service.

    """
    root = logging.getLogger()

    # exclude loggers from logging, this is to avoid recursion errors
    if excluded_loggers is None:
        all_excluded_loggers = EXCLUDED_LOGGER_DEFAULTS
    else:
        all_excluded_loggers = set(excluded_loggers + EXCLUDED_LOGGER_DEFAULTS)

    resource = detect_resource()

    # remove built-in handlers on App Engine or Cloud Functions environments
    if resource.type in (
        "gae_app",
        "cloud_function",
        "cloud_run_job",
        "cloud_run_revision",
    ):
        root.handlers.clear()

    cloud_handler = CloudHandler(
        resource=resource, labels=labels, transport=transport, project=project
    )
    root.addHandler(cloud_handler)
    root.setLevel(log_level)

    for logger_name in all_excluded_loggers:
        # prevent excluded loggers from propagating logs to handler
        logger = logging.getLogger(logger_name)
        logger.propagate = False

    return cloud_handler
