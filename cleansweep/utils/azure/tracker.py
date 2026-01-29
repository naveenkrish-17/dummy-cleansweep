"""Stores metadata about the script's progress. Only one instance per model is created."""

__all__ = ["Tracker"]

import time
from typing import Dict, TypeAlias
from uuid import uuid4

from cleansweep.utils.threading import Threadsafe


class StatusTracker(object):
    """Stores metadata about the script's progress. Only one instance is created."""

    time_of_last_rate_limit_error: float = (
        0  # used to cool off after hitting rate limits
    )
    time_of_last_api_call: float = 0  # used to calculate timeout

    def __init__(self):
        self._in_progress: list[str] = []
        self._succeeded: list[str] = []
        self._failed: list[str] = []

        self._rate_limit_errors: list[str] = []
        self._api_errors: list[str] = []
        self._auth_errors: list[str] = []
        self._other_errors: list[str] = []

    @property
    def num_tasks_started(self) -> int:
        """Total number of tasks started."""
        return len(self._in_progress) + len(self._succeeded) + len(self._failed)

    @property
    def num_tasks_in_progress(self) -> int:
        """Number of tasks currently in progress."""
        return len(self._in_progress)

    @property
    def num_tasks_succeeded(self) -> int:
        """Number of tasks that have succeeded."""
        return len(self._succeeded)

    @property
    def num_tasks_failed(self) -> int:
        """Number of tasks that have failed."""
        return len(self._failed)

    @property
    def num_rate_limit_errors(self) -> int:
        """Number of rate limit errors encountered."""
        return len(self._rate_limit_errors)

    @property
    def num_api_errors(self) -> int:
        """Number of API errors encountered."""
        return len(self._api_errors)

    @property
    def num_auth_errors(self) -> int:
        """Number of authentication errors encountered."""
        return len(self._auth_errors)

    @property
    def num_other_errors(self) -> int:
        """Number of other errors encountered."""
        return len(self._other_errors)

    def add_task(self, task_id: str):
        """Add a task to the list of in-progress tasks."""
        self._in_progress.append(task_id)

    def mark_task_as_succeeded(self, task_id: str):
        """Mark a task as succeeded."""
        if task_id in self._in_progress:
            self._in_progress.remove(task_id)
        self._succeeded.append(task_id)

    def mark_task_as_failed(self, task_id: str):
        """Mark a task as failed."""
        if task_id in self._in_progress:
            self._in_progress.remove(task_id)
        self._failed.append(task_id)

    def add_rate_limit_error(self, error: str):
        """Add a rate limit error."""
        self._rate_limit_errors.append(error)

    def add_api_error(self, error: str):
        """Add an API error."""
        self._api_errors.append(error)

    def add_auth_error(self, error: str):
        """Add an authentication error."""
        self._auth_errors.append(error)

    def add_other_error(self, error: str):
        """Add an other error."""
        self._other_errors.append(error)


ModelName: TypeAlias = str

_status_trackers: Dict[ModelName, StatusTracker] = {}


class Tracker(Threadsafe):
    """Provides an interface to the status tracker for a specific model.

    This ensures that the status tracker is thread-safe and that only one instance is created per
    model, meaning that the status tracker is shared across all threads.

    Args:
        model_name (ModelName): The name of the model deployment.
        total_tasks (int): The total number of tasks to be processed.

    """

    def __init__(self, model_name: ModelName, total_tasks: int):
        self._model_name = model_name
        self._id = uuid4().hex

        self._total_tasks = total_tasks
        self._tasks_started: int = 0
        self._tasks_in_progress: int = 0  # script ends when this reaches 0
        self._tasks_succeeded: int = 0
        self._tasks_failed: int = 0

    def _get_tracker(self) -> StatusTracker:
        if self._model_name not in _status_trackers:
            _status_trackers[self._model_name] = StatusTracker()
        return _status_trackers[self._model_name]

    @property
    def num_tasks_started(self) -> int:
        """Total number of tasks started."""
        return self._get_tracker().num_tasks_started

    @property
    def num_tasks_in_progress(self) -> int:
        """Number of tasks currently in progress."""
        return self._get_tracker().num_tasks_in_progress

    @property
    def num_tasks_succeeded(self) -> int:
        """Number of tasks that have succeeded."""
        return self._get_tracker().num_tasks_succeeded

    @property
    def num_tasks_failed(self) -> int:
        """Number of tasks that have failed."""
        return self._get_tracker().num_tasks_failed

    @property
    def num_rate_limit_errors(self) -> int:
        """Number of rate limit errors encountered."""
        return self._get_tracker().num_rate_limit_errors

    @property
    def num_api_errors(self) -> int:
        """Number of API errors encountered."""
        return self._get_tracker().num_api_errors

    @property
    def num_auth_errors(self) -> int:
        """Number of authentication errors encountered."""
        return self._get_tracker().num_auth_errors

    @property
    def num_other_errors(self) -> int:
        """Number of other errors encountered."""
        return self._get_tracker().num_other_errors

    @property
    def remaining_tasks(self) -> int:
        """Get the number of tasks remaining."""
        return (self._total_tasks - self._tasks_started) + self._tasks_in_progress

    @property
    def time_of_last_rate_limit_error(self) -> float:
        """Get the time of the last rate limit error."""
        return self._get_tracker().time_of_last_rate_limit_error

    @time_of_last_rate_limit_error.setter
    def time_of_last_rate_limit_error(self, value: float):
        """Set the time of the last rate limit error."""
        self._get_tracker().time_of_last_rate_limit_error = value

    @property
    def time_of_last_api_call(self) -> float:
        """Get the time of the last API call."""
        return self._get_tracker().time_of_last_api_call

    @time_of_last_api_call.setter
    def time_of_last_api_call(self, value: float):
        """Set the time of the last API call."""
        self._get_tracker().time_of_last_api_call = value

    @property
    def total_tasks(self) -> int:
        """Get the total number of tasks."""
        return self._total_tasks

    def add_task(self):
        """Add a task to the list of in-progress tasks."""
        self._tasks_started += 1
        self._tasks_in_progress += 1
        self._get_tracker().add_task(self._id)

    def mark_task_as_succeeded(self):
        """Mark a task as succeeded."""
        self._tasks_succeeded += 1
        self._tasks_in_progress -= 1
        self._get_tracker().mark_task_as_succeeded(self._id)

    def mark_task_as_failed(self):
        """Mark a task as failed."""
        self._tasks_failed += 1
        self._tasks_in_progress -= 1
        self._get_tracker().mark_task_as_failed(self._id)

    def add_rate_limit_error(self):
        """Add a rate limit error."""
        self._get_tracker().time_of_last_rate_limit_error = time.time()
        self._get_tracker().add_rate_limit_error(self._id)

    def add_api_error(self):
        """Add an API error."""
        self._get_tracker().add_api_error(self._id)

    def add_auth_error(self):
        """Add an authentication error."""
        self._get_tracker().add_auth_error(self._id)

    def add_other_error(self):
        """Add an other error."""
        self._get_tracker().add_other_error(self._id)
