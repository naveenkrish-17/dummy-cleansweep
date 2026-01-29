"""Test suite for cleansweep.utils.azure.api module."""

import asyncio
import json

import httpx
import pytest

from cleansweep.deployments.deployments import DEPLOYMENTS
from cleansweep.utils.azure.api import (
    APIConnectionError,
    APIRequest,
    AuthenticationError,
    BadRequestError,
    ClientAuthenticationError,
    InternalServerError,
    NotFoundError,
    PermissionDeniedError,
    PipelineError,
    RateLimitError,
    TranslationError,
    UnprocessableEntityError,
    create_messages,
    generate_task_id,
    get_all_results,
    get_prompt_size,
    process_api_calls,
)
from cleansweep.utils.azure.tracker import Tracker


class TestGenerateTaskId:
    """Test suite for generate_task_id function."""

    def test_generate_task_id(self):
        """Test generate_task_id function."""
        task_id_generator = generate_task_id()
        assert next(task_id_generator) == 0
        assert next(task_id_generator) == 1


class TestGetAllResults:
    """Test suite for get_all_results function."""

    @pytest.mark.asyncio
    async def test_get_all_results(self):
        """Test get_all_results function."""
        # Create a queue and add some items
        queue = asyncio.Queue()
        await queue.put((2, "second"))
        await queue.put((1, "first"))
        await queue.put((3, "third"))

        # Call the function to test
        results = get_all_results(queue)

        # Check the results
        assert results == ["first", "second", "third"]


class TestGetPromptSize:
    """Test suite for get_prompt_size function."""

    def test_get_prompt_size(self):
        """Test get_prompt_size function."""

        result = get_prompt_size("This is a system prompt", "gpt-4")

        assert result == 12


class TestCreateMessage:
    """Test suite for create_message function."""

    def test_create_message(self):
        """Test create_message function."""
        messages = create_messages("This is a system prompt", "This is a user prompt")

        assert messages == [
            {"role": "system", "content": "This is a system prompt"},
            {"role": "user", "content": "This is a user prompt"},
        ]


class TestAPIRequest:
    """Test suite for APIRequest class."""

    def test_init(self):
        """Test APIRequest class."""

        def some_callable():
            return 1

        api_request = APIRequest(1, 1, 1, some_callable, ["task"], (), {}, "test_model")

        assert api_request.task_id == 1
        assert api_request.token_consumption == 1
        assert api_request.attempts_left == 1
        assert (
            api_request.func  # pylint: disable=comparison-with-callable
            == some_callable
        )
        assert api_request.task == ["task"]
        assert not api_request.args
        assert api_request.model == "test_model"

    @pytest.mark.asyncio
    async def test_call_api(self):
        """Test call method of APIRequest class."""

        def some_callable(model, tasks):  # pylint: disable=unused-argument
            return 1

        retry_queue = asyncio.Queue()
        result_queue = asyncio.Queue()
        status_tracker = Tracker(model_name="test_model", total_tasks=1)

        api_request = APIRequest(1, 1, 1, some_callable, ["task"], (), {}, "test_model")

        await api_request.call_api(
            retry_queue=retry_queue,
            result_queue=result_queue,
            status_tracker=status_tracker,
        )

        assert get_all_results(result_queue) == [1]

    @pytest.mark.parametrize(
        "err",
        [
            pytest.param(APIConnectionError(request=""), id="APIConnectionError"),
            pytest.param(httpx.HTTPError(""), id="httpx.HTTPError"),
            pytest.param(
                httpx.HTTPStatusError("", request="", response=httpx.Response(400)),
                id="httpx.HTTPStatusError",
            ),
            pytest.param(json.JSONDecodeError("", "", 0), id="json.JSONDecodeError"),
            pytest.param(
                BadRequestError(
                    "",
                    response=httpx.Response(400, request=httpx.Request("GET", "")),
                    body={},
                ),
                id="BadRequestError",
            ),
            pytest.param(
                InternalServerError(
                    "",
                    response=httpx.Response(500, request=httpx.Request("GET", "")),
                    body={},
                ),
                id="InternalServerError",
            ),
            pytest.param(
                NotFoundError(
                    "",
                    response=httpx.Response(404, request=httpx.Request("GET", "")),
                    body={},
                ),
                id="NotFoundError",
            ),
            pytest.param(
                PermissionDeniedError(
                    "",
                    response=httpx.Response(403, request=httpx.Request("GET", "")),
                    body={},
                ),
                id="PermissionDeniedError",
            ),
            pytest.param(
                UnprocessableEntityError(
                    "",
                    response=httpx.Response(422, request=httpx.Request("GET", "")),
                    body={},
                ),
                id="UnprocessableEntityError",
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_api_errors(self, err):
        """Test API errors are handled correctly."""

        def some_callable(model, tasks):  # pylint: disable=unused-argument
            raise err

        retry_queue = asyncio.Queue()
        result_queue = asyncio.Queue()
        status_tracker = Tracker(model_name="test_model", total_tasks=1)

        api_request = APIRequest(1, 1, 1, some_callable, ["task"], (), {}, "test_model")

        await api_request.call_api(
            retry_queue=retry_queue,
            result_queue=result_queue,
            status_tracker=status_tracker,
        )

        assert status_tracker.num_api_errors > 0
        assert get_all_results(result_queue) == []

    @pytest.mark.parametrize(
        "err",
        [
            pytest.param(
                AuthenticationError(
                    "",
                    response=httpx.Response(400, request=httpx.Request("GET", "")),
                    body={},
                ),
                id="AuthenticationError",
            ),
            pytest.param(
                ClientAuthenticationError(
                    "ClientSecretCredential.get_token failed",
                ),
                id="ClientAuthenticationError",
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_auth_errors(self, err):
        """Test API errors are handled correctly."""

        def some_callable(model, tasks):  # pylint: disable=unused-argument
            raise err

        retry_queue = asyncio.Queue()
        result_queue = asyncio.Queue()
        status_tracker = Tracker(model_name="test_model", total_tasks=1)

        api_request = APIRequest(1, 1, 1, some_callable, ["task"], (), {}, "test_model")

        await api_request.call_api(
            retry_queue=retry_queue,
            result_queue=result_queue,
            status_tracker=status_tracker,
        )

        assert status_tracker.num_auth_errors > 0

    @pytest.mark.parametrize(
        "err",
        [
            pytest.param(
                RateLimitError(
                    "",
                    response=httpx.Response(404, request=httpx.Request("GET", "")),
                    body={},
                ),
                id="RateLimitError",
            ),
            pytest.param(
                httpx.HTTPStatusError("", request="", response=httpx.Response(429)),
                id="httpx.HTTPStatusError",
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_rate_limit_errors(self, err):
        """Test API errors are handled correctly."""

        def some_callable(model, tasks):  # pylint: disable=unused-argument
            raise err

        retry_queue = asyncio.Queue()
        result_queue = asyncio.Queue()
        status_tracker = Tracker(model_name="test_model", total_tasks=1)

        api_request = APIRequest(1, 1, 1, some_callable, ["task"], (), {}, "test_model")

        await api_request.call_api(
            retry_queue=retry_queue,
            result_queue=result_queue,
            status_tracker=status_tracker,
        )

        assert status_tracker.num_rate_limit_errors > 0
        assert get_all_results(result_queue) == []

    @pytest.mark.parametrize(
        "err",
        [
            pytest.param(
                TranslationError(""),
                id="TranslationError",
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_other_errors(self, err):
        """Test API errors are handled correctly."""

        def some_callable(model, tasks):  # pylint: disable=unused-argument
            raise err

        retry_queue = asyncio.Queue()
        result_queue = asyncio.Queue()
        status_tracker = Tracker(model_name="test_model", total_tasks=1)

        api_request = APIRequest(1, 1, 1, some_callable, ["task"], (), {}, "test_model")

        await api_request.call_api(
            retry_queue=retry_queue,
            result_queue=result_queue,
            status_tracker=status_tracker,
        )

        assert status_tracker.num_other_errors > 0
        assert get_all_results(result_queue) == []


class TestProcessApi:
    """Test suite for process_api function."""

    @pytest.mark.asyncio
    async def test_process_api(self):
        """Test process_api function."""

        def some_callable(model, tasks):
            return 1

        tasks = [
            [
                {"role": "system", "content": "This is a system prompt"},
                {"role": "user", "content": "This is a user prompt"},
            ]
        ]
        results = await process_api_calls(
            some_callable, tasks, "chat", DEPLOYMENTS.get_by_model("gpt-4o")
        )

        assert results == [1]

    @pytest.mark.parametrize(
        "err",
        [
            pytest.param(
                AuthenticationError(
                    "",
                    response=httpx.Response(400, request=httpx.Request("GET", "")),
                    body={},
                ),
                id="AuthenticationError",
            ),
            pytest.param(
                ClientAuthenticationError(
                    "ClientSecretCredential.get_token failed",
                ),
                id="ClientAuthenticationError",
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_auth_errors(self, err):
        """Test API errors are handled correctly."""

        def some_callable(model, tasks):  # pylint: disable=unused-argument
            raise err

        tasks = [
            [
                {"role": "system", "content": "This is a system prompt"},
                {"role": "user", "content": "This is a user prompt"},
            ]
        ]
        with pytest.raises(PipelineError):
            await process_api_calls(
                some_callable, tasks, "chat", DEPLOYMENTS.get_by_model("gpt-4o")
            )
