"""Azure API utilities."""

import asyncio
import inspect
import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Callable, List, Literal, Sequence

import httpx
from azure.core.exceptions import ClientAuthenticationError
from openai import (
    APIConnectionError,
    AuthenticationError,
    BadRequestError,
    InternalServerError,
    NotFoundError,
    PermissionDeniedError,
    RateLimitError,
    UnprocessableEntityError,
)
from openai.types.chat import ChatCompletionMessageParam

from cleansweep._types import Deployment, Texts
from cleansweep.exceptions import (
    APIRequestError,
    ChatError,
    MetadataGenerationError,
    PipelineError,
    TranslationError,
)
from cleansweep.settings.base import settings
from cleansweep.utils.azure.tracker import Tracker
from cleansweep.utils.azure.utils import (
    create_message,
    num_tokens_from_messages,
    num_tokens_from_strings,
)

logger = logging.getLogger(__name__)
"""The logger for this module."""

AUTHENTICATION_ERRORS = (
    AuthenticationError,
    ClientAuthenticationError,
)

OPENAI_API_ERRORS = (
    BadRequestError,
    InternalServerError,
    NotFoundError,
    PermissionDeniedError,
    UnprocessableEntityError,
)
"""Errors that can be raised by the OpenAI API."""

PIPELINE_ERRORS = (PipelineError, TranslationError, ChatError, MetadataGenerationError)


def generate_task_id():
    """Generate integers 0, 1, 2, and so on."""
    task_id = 0
    while True:
        yield task_id
        task_id += 1


TOKEN_CONSUMPTION_FUNC = {
    "chat": num_tokens_from_messages,
    "embed": num_tokens_from_strings,
}
"""Maps API endpoints to functions that calculate the number of tokens consumed by a request to that
endpoint."""


TPM_PER_MODEL = {
    "gpt-3.5-turbo": 300000,
    "gpt-3.5-turbo-16k": 300000,
    "gpt-4": 40000,
    "gpt-4-turbo": 150000,
    "gpt-4-turbo-v": 30000,
    "gpt-4-32k": 80000,
    "gpt-4o": 1000000,
    "text-embedding-ada-002": 350000,
}


async def process_api_calls(
    func: Callable,
    tasks: Sequence[Sequence[ChatCompletionMessageParam | Texts]],
    end_point: Literal["chat", "embed"],
    model: Deployment,
    *args,
    **kwargs,
) -> List[str]:
    """Process API calls concurrently.

    Args:
        func (Callable): The function to call.
        tasks (Union[list[list[Union[str, dict]], Iterator[list[Union[str, dict]]]): The tasks to
            process.
        end_point (str): The endpoint to call.
        model (Deployment): The model to use.
        *args: Additional arguments to pass to the function.
        **kwargs: Additional keyword arguments to pass to the function.

    Raises:
        NotImplementedError: If the endpoint is not supported.

    Returns:
        list: The results of the API calls.

    """
    # constants
    seconds_to_pause_after_rate_limit_error = 60
    seconds_to_sleep_each_loop = (
        0.001  # 1 ms limits max throughput to 1,000 requests per second
    )

    # initialize trackers
    queue_of_requests_to_retry = asyncio.Queue()
    queue_of_results = asyncio.Queue()
    task_id_generator = generate_task_id()
    status_tracker = Tracker(model_name=model.model, total_tasks=len(list(tasks)))
    next_request: APIRequest | None = None

    max_tokens_per_minute = model.tpm
    max_requests_per_minute = (model.tpm / 1000) * 6

    logger.debug("Max tokens per minute: %d", max_tokens_per_minute)
    logger.debug("Max requests per minute: %d", max_requests_per_minute)

    max_requests_per_second = max_requests_per_minute / 60
    logger.debug("Max request per second: %d", max_requests_per_second)

    max_requests_per_period = (
        max_requests_per_second * settings.rpm_calculation_period_seconds
    )
    logger.debug(
        "RPM calculation period: %d seconds", settings.rpm_calculation_period_seconds
    )
    logger.debug("Max requests per period: %d", max_requests_per_period)

    # update seconds to sleep based on the RPM, RPM is checked every 1 or 10 seconds so
    # the maximum requests for second must be managed
    seconds_to_sleep_each_loop = max(
        (1 / settings.rpm_calculation_period_seconds) / max_requests_per_second,
        seconds_to_sleep_each_loop,
    )

    available_request_capacity = max_requests_per_period
    available_token_capacity = max_tokens_per_minute
    last_update_time = time.time()

    # initialize flags
    tasks_not_finished = True  # after file is empty, we'll skip reading it

    calculate_tokens = TOKEN_CONSUMPTION_FUNC.get(end_point)
    if calculate_tokens is None:
        raise NotImplementedError(f"Unknown endpoint: {end_point}")

    if isinstance(tasks, list):
        tasks_iter = iter(tasks)
    else:
        tasks_iter = tasks

    api_call_time = time.time()  # initialise

    while True:
        # get next request (if one is not already waiting for capacity)
        if next_request is None:
            if not queue_of_requests_to_retry.empty():
                next_request = queue_of_requests_to_retry.get_nowait()
                if next_request:
                    logger.debug("Retrying request %s", next_request.task_id)
            elif tasks_not_finished:
                try:
                    next_task = next(tasks_iter)  # pyright: ignore[reportArgumentType]
                    next_request = APIRequest(
                        task_id=next(task_id_generator),
                        token_consumption=calculate_tokens(next_task, model.model),
                        attempts_left=settings.max_attempts,
                        func=func,
                        task=next_task,
                        args=args,
                        model=model.name,
                        kwargs=kwargs,
                    )
                    status_tracker.add_task()
                    logger.debug("Creating request %s", next_request.task_id)
                except StopIteration:
                    # if file runs out, set flag to stop reading it
                    logger.debug("Task list exhausted")
                    tasks_not_finished = False

        # update available capacity
        current_time = time.time()
        seconds_since_update = current_time - last_update_time
        seconds_since_api_call = current_time - api_call_time
        if seconds_since_api_call > settings.rpm_calculation_period_seconds:
            available_request_capacity = max_requests_per_period

        available_token_capacity = min(
            available_token_capacity
            + max_tokens_per_minute * seconds_since_update / 60.0,
            max_tokens_per_minute,
        )
        last_update_time = current_time

        # if enough capacity available, call API
        if next_request:
            next_request_tokens = next_request.token_consumption
            if (
                available_request_capacity >= 1
                and available_token_capacity >= next_request_tokens
            ):
                # update counters
                available_request_capacity -= 1
                available_token_capacity -= next_request_tokens
                next_request.attempts_left -= 1

                # call API
                asyncio.create_task(
                    next_request.call_api(
                        queue_of_requests_to_retry, queue_of_results, status_tracker
                    )
                )
                api_call_time = time.time()
                next_request = None  # reset next_request to empty

        # if auth error occurred, raise
        if status_tracker.num_auth_errors > 0:
            raise PipelineError("Authentication error occurred")

        # if all tasks are finished, break
        if status_tracker.num_tasks_in_progress == 0:
            break

        # main loop sleeps briefly so concurrent tasks can run
        await asyncio.sleep(seconds_to_sleep_each_loop)

        # if a rate limit error was hit recently, pause to cool down
        seconds_since_rate_limit_error = (
            time.time() - status_tracker.time_of_last_rate_limit_error
        )
        if seconds_since_rate_limit_error < seconds_to_pause_after_rate_limit_error:
            remaining_seconds_to_pause = (
                seconds_to_pause_after_rate_limit_error - seconds_since_rate_limit_error
            )
            logger.debug(
                "Pausing to cool down until %s",
                time.ctime(
                    status_tracker.time_of_last_rate_limit_error
                    + seconds_to_pause_after_rate_limit_error
                ),
            )
            await asyncio.sleep(remaining_seconds_to_pause)

        # if we've been waiting for a while, check if we should just give up
        seconds_since_last_api_call = time.time() - status_tracker.time_of_last_api_call
        if seconds_since_last_api_call > settings.timeouts.process_api_calls:
            logger.error(
                "API calls timed out after %d seconds", seconds_since_last_api_call
            )
            break

    return get_all_results(
        queue_of_results, status_tracker
    )  # pyright: ignore[reportReturnType]


# region dataclasses


@dataclass
class APIRequest:
    """A request to OpenAI API."""

    task_id: int
    token_consumption: int
    attempts_left: int
    func: Callable
    task: Sequence[ChatCompletionMessageParam | Texts]
    args: tuple
    kwargs: dict
    model: str
    result: list = field(default_factory=list)

    async def call_api(
        self,
        retry_queue: asyncio.Queue,
        result_queue: asyncio.Queue,
        status_tracker: Tracker,
    ):
        """Call the API and handle errors.

        Args:
            retry_queue (Queue): The queue to put the request in if it fails.
            result_queue (Queue): The queue to put the result in if it succeeds.
            status_tracker (Tracker): The status tracker to update.

        """
        logger.debug(
            "Calling API for task %s of %s", self.task_id, status_tracker.total_tasks
        )

        error = None
        err_log_func = logger.debug
        result = None
        try:
            status_tracker.time_of_last_api_call = time.time()
            if inspect.iscoroutinefunction(self.func):
                result = await self.func(
                    self.model, self.task, *self.args, **self.kwargs
                )
            else:
                result = self.func(self.model, self.task, *self.args, **self.kwargs)

        except httpx.HTTPStatusError as e:
            error = e
            if e.response.status_code == 429:
                status_tracker.add_rate_limit_error()
            else:
                status_tracker.add_api_error()
        except RateLimitError as e:
            message = (
                e.body.get("message", e.message)
                if isinstance(e.body, dict)
                else e.message
            )
            error = APIRequestError(message)

            status_tracker.add_rate_limit_error()
        except (APIConnectionError, httpx.HTTPError, APIRequestError) as e:
            error = e
            status_tracker.add_api_error()
        except OPENAI_API_ERRORS as e:
            message = (
                e.body.get("message", e.message)
                if isinstance(e.body, dict)
                else e.message
            )
            error = APIRequestError(message)

            status_tracker.add_api_error()
        except json.JSONDecodeError as e:
            error = f"API returned an invalid response: {e}"
            status_tracker.add_api_error()

        except PIPELINE_ERRORS as e:
            error = e
            status_tracker.add_other_error()

        except RuntimeError as e:
            error = e
            status_tracker.add_other_error()

        except AUTHENTICATION_ERRORS as e:
            error = e
            status_tracker.add_auth_error()
            self.attempts_left = 0

        if error:
            if self.attempts_left > 0:
                err_log_func(
                    "Task %s failed with '%s'. %d attempts left.",
                    self.task_id,
                    error,
                    self.attempts_left,
                )
                retry_queue.put_nowait(self)
            else:
                logger.error("Task %s failed with '%s'.", self.task_id, error)
                status_tracker.mark_task_as_failed()
                result_queue.put_nowait((self.task_id, None))
        else:
            status_tracker.mark_task_as_succeeded()
            result_queue.put_nowait((self.task_id, result))
            logger.debug(
                "Task %s succeeded. %s remaining.",
                self.task_id,
                status_tracker.remaining_tasks,
            )


# endregion


# region functions
def get_all_results(
    result_queue: asyncio.Queue, status_tracker: Tracker | None = None
) -> list:
    """Get all results from the result queue.

    Args:
        result_queue (Queue): The queue to get results from.
        status_tracker (StatusTracker, optional): The status tracker to update.

    """
    results = []
    while not result_queue.empty():
        results.append(result_queue.get_nowait())

    if status_tracker and status_tracker.num_tasks_in_progress > 0:
        logger.warning(
            "Tasks still in progress: %d", status_tracker.num_tasks_in_progress
        )

        # identify in progress tasks and insert failed results
        returned_tasks = sorted([result[0] for result in results])
        expected_task_count = status_tracker.num_tasks_started
        missing_tasks = sorted(
            set(range(0, expected_task_count)).difference(returned_tasks)
        )
        for task_id in missing_tasks:
            results.append((task_id, None))
            status_tracker.mark_task_as_failed()

    return [result[1] for result in sorted(results)]


def create_messages(
    system_prompt: str | None = None,
    user_input: str | None = None,
    assistant_prompt: str | None = None,
) -> list[ChatCompletionMessageParam]:
    """Create a message for the OpenAI API.

    Args:
        system_prompt (str, optional): The system prompt.
        user_input (str, optional): The user input.
        assistant_prompt (str, optional): The assistant prompt.

    Returns:
        list: The messages.

    """
    messages = []
    if system_prompt:
        messages.append(create_message(system_prompt, role="system"))

    if user_input:
        messages.append(create_message(user_input, role="user"))

    if assistant_prompt:
        messages.append(create_message(assistant_prompt, role="assistant"))

    if not messages:
        raise ValueError("No messages provided")

    return messages


def get_prompt_size(
    prompt: str,
    model: str,
    prompt_type: Literal["system", "user", "assistant"] = "system",
) -> int:
    """Get the size of a prompt in tokens.

    Args:
        prompt (str): The prompt.
        model (str): The model to use.
        prompt_type (str, optional): The type of prompt. Defaults to "system".

    Returns:
        int: The size of the prompt in tokens.

    """
    args = ["", "", ""]
    if prompt_type == "system":
        args[0] = prompt

    if prompt_type == "user":
        args[1] = prompt

    if prompt_type == "assistant":
        args[2] = prompt

    return num_tokens_from_messages(create_messages(system_prompt=prompt), model)


def raise_error(
    error: Callable[..., Exception],
    message: str,
    request_id: str | None = None,
    body: str | None = None,
    status_code: int | None = None,
):
    """Raise an error with the specified message and response details.

    Args:
        error (Callable[..., Exception]): The error class or function to raise.
        message (str): The error message.
        request_id (str | None, optional): The request ID. Defaults to None.
        body (str | None, optional): The response body. Defaults to None.
        status_code (int | None, optional): The response status code. Defaults to None.

    Raises:
        Exception: The specified error with the provided message, response, and body.

    """
    if body is None:
        body = "No body provided"

    if status_code is None:
        status_code = 500

    headers = None
    if request_id is not None:
        headers = {"x-request-id": request_id}

    resp = httpx.Response(
        status_code,
        content=json.dumps({"message": message, "body": body}),
        headers=headers,
        request=httpx.Request("GET", "https://example.com"),
    )

    raise error(message, response=resp, body=resp.json())


def check_for_refusal(response: str):
    """Check if the response indicates that the request was refused.

    Args:
        response (str): The response to check.

    Raises:
        APIRequestError: If the request was refused.

    """
    patterns = [
        (
            r"^.*?\bi(?:(?: am|\\?'?m)\b (?:(?:not |un)able) to|(?:\b can(?:\\?'?t|not))) "
            r"(?:(?:complete|fulfill|provide|(?:help|assist)(?: with)?|perform|translate) )"
            r"(?:(?:a|the|this|that|your) )?(?:request|provided|translat(?:ion|ed)|task).*?$"
        ),
        (
            r"^.*?sorry,? (?:but )?it (?:seems|looks|appears).*?(?:(?:provided \w+|\w+ "
            r"(?:is|provided))|there (?:was|might|may).*?(?:typo|mistake|error)|(?:sent"
            r"|entered|provided) the wrong \w+|you have).*?$"
        ),
        (
            r"^.*?(?:\w+ (?:requested|provided)|(?:requested|provided) \w+|request|text|content"
            r"|this) is (?:not (?:possible|clear|available)|too (?:long|complex|lengthy)|"
            r"incomplete).*?$"
        ),
        (
            r"^.*?sorry,? (?:but )?i need.*? to (?:provide|complete|fulfill|help "
            r"(?:with)?|perform).*?$"
        ),
        r".*?sorry,? (?:but )?(?:it is not possible to ).*?without.*?$",
        (
            r"^.*?sorry,? (?:but )?it (?:seems|looks|appears) (?:that|like)? ? (?:you haven\\?'?t "
            r"provided|I haven\\?'?t received).*?$"
        ),
        r"^.*?\w+ is not provided.*?provide the \w+ to be \w+.*?$",
        r"^.*?the \w+ (?:could not|can(?:\\?'?t|not)) be translated.*?$",
        r"^.*?the \w+.*?is not in \w+.*?it can(?:\\?'?t|not) be \w+.*?$",
    ]

    for pattern in patterns:
        m = re.match(pattern, response, re.IGNORECASE | re.MULTILINE)
        if m:
            raise APIRequestError(f"Request refused: {m.group(0)}")


# endregion
