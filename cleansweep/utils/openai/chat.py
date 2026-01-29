"""Functions for generating chat completions using the OpenAI chat model."""

from typing import Any

from openai import InternalServerError
from openai._types import (
    NOT_GIVEN,  # pyright: ignore[reportPrivateImportUsage]
    NotGiven,  # pyright: ignore[reportPrivateImportUsage]
)
from openai.types.chat import ChatCompletion, ChatCompletionMessageParam
from openai.types.chat.chat_completion_tool_choice_option_param import (
    ChatCompletionToolChoiceOptionParam,
)
from openai.types.chat.chat_completion_tool_param import ChatCompletionToolParam
from openai.types.chat.completion_create_params import ResponseFormat
from openai.types.chat.parsed_chat_completion import ParsedChatCompletion
from pydantic import BaseModel, ValidationInfo, field_validator

from cleansweep.exceptions import ChatError
from cleansweep.utils.azure.api import check_for_refusal, raise_error
from cleansweep.utils.azure.auth import AzureCredentials
from cleansweep.utils.azure.client import get_open_ai_client, get_open_ai_client_async


class ChatCompletionArgs(BaseModel, arbitrary_types_allowed=True):
    """Define and validate the arguments for the chat completion."""

    model: str
    messages: list[ChatCompletionMessageParam]
    timeout: float | NotGiven = NOT_GIVEN
    temperature: float | NotGiven = NOT_GIVEN
    tool_choice: ChatCompletionToolChoiceOptionParam | NotGiven = NOT_GIVEN
    tools: list[ChatCompletionToolParam] | NotGiven = NOT_GIVEN

    @field_validator("timeout", "temperature", "tool_choice", "tools", mode="before")
    @classmethod
    def convert_not_given(
        cls, value: Any, info: ValidationInfo  # pylint: disable=unused-argument
    ) -> Any:
        """Convert None to NOT_GIVEN."""
        if value is None:
            return NOT_GIVEN
        return value


def prepare_args(
    model: str,
    messages: list[ChatCompletionMessageParam],
    timeout: float | None = None,
    temperature: float | None = None,
    tool_choice: ChatCompletionToolChoiceOptionParam | None = None,
    tools: list[ChatCompletionToolParam] | None = None,
) -> dict[str, Any]:
    """Prepare the arguments for the chat completion.

    Args:
        model (str): The name or ID of the model to use.
        messages (list[ChatCompletionMessageParam]): The list of messages in the conversation.
        timeout (float | None, optional): The maximum time in seconds to wait for the completion.
            Defaults to None.
        temperature (float | None, optional): Controls the randomness of the output. Higher values
            make the output more random. Defaults to None.
        tool_choice (ChatCompletionToolChoiceOptionParam | None, optional): The choice of tool to
            use for completion. Defaults to None.
        tools (list[ChatCompletionToolParam] | None, optional): The list of tools to use for
            completion. Defaults to None.

    Returns:
        dict[str, Any]: The prepared arguments for the chat completion.

    """
    args = ChatCompletionArgs(
        model=model,
        messages=messages,
        timeout=timeout,  # pyright: ignore[reportArgumentType]
        temperature=temperature,  # pyright: ignore[reportArgumentType]
        tool_choice=tool_choice,  # pyright: ignore[reportArgumentType]
        tools=tools,  # pyright: ignore[reportArgumentType]
    )

    return args.model_dump()


def get_response(
    response: ChatCompletion | ParsedChatCompletion,
    tool_choice: ChatCompletionToolChoiceOptionParam | None = None,
    response_format: ResponseFormat | BaseModel | None = None,
) -> str:
    """Get the response from the chat completion.

    Args:
        response (ChatCompletion | ParsedChatCompletion): The response from the chat completion.
        tool_choice (ChatCompletionToolChoiceOptionParam | None, optional): The tool choice option
            for the chat completion. Defaults to None.
        response_format (ResponseFormat | BaseModel | None, optional): The response format for the
            chat completion. Defaults to None.

    Returns:
        str: The response from the chat completion.

    Raises:
        ChatError: If no response is returned.

    """
    if not response.choices:
        raise ChatError("No response returned")

    choice = response.choices[0]

    if choice.finish_reason == "content_filter":
        raise_error(
            InternalServerError,
            "Chat failed due to content filter.",
            response.id,
            choice.message.content,
            500,
        )
    elif choice.finish_reason == "length":
        raise_error(
            InternalServerError,
            "Chat failed due to length.",
            response.id,
            choice.message.content,
            500,
        )

    if choice.message.refusal:
        raise ChatError(choice.message.refusal)

    if tool_choice:
        if choice.message.tool_calls:
            _message = choice.message.tool_calls[0]
            assert hasattr(_message, "function"), "No function found in tool call"
            return (
                _message.function.arguments  # pyright: ignore[reportAttributeAccessIssue]
            )
        else:
            raise ChatError("No tool calls in response from OpenAI")

    if response_format:
        if not hasattr(choice.message, "parsed"):
            raise ChatError("No response returned")

        if (
            choice.message.parsed  # pyright: ignore[reportAttributeAccessIssue]
            is not None
        ):
            raise ChatError("No response returned")

        return (
            choice.message.parsed  # pyright: ignore[reportAttributeAccessIssue, reportReturnType]
        )

    if choice.message.tool_calls:
        _message = choice.message.tool_calls[0]
        assert hasattr(_message, "function"), "No function found in tool call"
        return (
            _message.function.arguments  # pyright: ignore[reportAttributeAccessIssue]
        )

    if choice.message.content is None:
        raise ChatError("No response returned")

    return choice.message.content


async def chat_completion_async(
    model: str,
    messages: list[ChatCompletionMessageParam],
    credentials: AzureCredentials,
    ignore_refusals: bool = False,
    timeout: float | None = None,
    temperature: float | None = None,
    tool_choice: ChatCompletionToolChoiceOptionParam | None = None,
    tools: list[ChatCompletionToolParam] | None = None,
    response_format: ResponseFormat | BaseModel | None = None,
) -> str:
    """Asynchronously generates a chat completion using the OpenAI API.

    Args:
        model (str): The model to use for chat completion.
        messages (list[ChatCompletionMessageParam]): A list of messages to use as input for the
            chat completion.
        credentials (AzureCredentials): The Azure credentials.
        ignore_refusals (bool, optional): Whether to ignore refusals in the chat completion.
        timeout (float | None, optional): The maximum time in seconds to wait for the completion.
            Defaults to None.
        temperature (float | None, optional): Controls the randomness of the output. Higher values
            make the output more random. Defaults to None.
        tool_choice (ChatCompletionToolChoiceOptionParam | None, optional): The tool choice option
            for the chat completion. Defaults to None.
        tools (list[ChatCompletionToolParam] | None, optional): A list of tools to use for the
            chat completion. Defaults to None.
        response_format (ResponseFormat | BaseModel | None, optional): The response format for the
            chat completion. Defaults to None.

    Returns:
        str: The generated chat completion.

    Raises:
        ChatError: If no response is returned.
        InternalServerError: If the chat fails due to content filter.
        RateLimitError: If the chat fails due to length.

    """
    client = get_open_ai_client_async(credentials, max_retries=0)

    if response_format is None:
        coro = client.chat.completions.create
        args = prepare_args(
            model=model,
            messages=messages,
            timeout=timeout,
            temperature=temperature,
            tool_choice=tool_choice,
            tools=tools,
        )
    else:
        coro = client.beta.chat.completions.parse
        args = prepare_args(
            model=model, messages=messages, timeout=timeout, temperature=temperature
        )
        args["response_format"] = response_format
        tool_choice = None

    response = await coro(**args)

    result = get_response(response, tool_choice, response_format)

    if not ignore_refusals:
        check_for_refusal(result)

    return result


def create_chat(
    model: str,
    messages: list[ChatCompletionMessageParam],
    credentials: AzureCredentials,
    timeout: float | None = None,
    temperature: float | None = None,
    tool_choice: ChatCompletionToolChoiceOptionParam | None = None,
    tools: list[ChatCompletionToolParam] | None = None,
    response_format: ResponseFormat | BaseModel | None = None,
) -> ChatCompletion:
    """Create a chat completion using the OpenAI API.

    Args:
        model (str): The model to use for chat completion.
        messages (list[ChatCompletionMessageParam]): The list of messages for the chat
            conversation.
        credentials (AzureCredentials): The Azure credentials.
        timeout (float | None, optional): The maximum time in seconds to wait for the completion.
            Defaults to None.
        temperature (float | None, optional): Controls the randomness of the output. Higher values
            make the output more random. Defaults to None.
        tool_choice (ChatCompletionToolChoiceOptionParam | None, optional): The tool choice option
            for the chat completion. Defaults to None.
        tools (list[ChatCompletionToolParam] | None, optional): The list of tools to use for the
            chat completion. Defaults to None.
        response_format (ResponseFormat | BaseModel | None, optional): The response format for the
            chat completion. Defaults to None.

    Returns:
        ChatCompletion: The chat completion response.

    """
    client = get_open_ai_client(credentials, max_retries=0)

    if response_format is None:
        func = client.chat.completions.create
        args = prepare_args(
            model=model,
            messages=messages,
            timeout=timeout,
            temperature=temperature,
            tool_choice=tool_choice,
            tools=tools,
        )
    else:
        func = client.beta.chat.completions.parse
        args = prepare_args(
            model=model, messages=messages, timeout=timeout, temperature=temperature
        )
        args["response_format"] = response_format
        tool_choice = None

    return func(**args)


def chat_completion(
    model: str,
    messages: list[ChatCompletionMessageParam],
    credentials: AzureCredentials,
    ignore_refusals: bool = False,
    timeout: float | None = None,
    temperature: float | None = None,
    tool_choice: ChatCompletionToolChoiceOptionParam | None = None,
    tools: list[ChatCompletionToolParam] | None = None,
    response_format: ResponseFormat | BaseModel | None = None,
) -> str:
    """Generate a chat completion using the OpenAI API.

    Args:
        model (str): The model to use for chat completion.
        messages (list[ChatCompletionMessageParam]): A list of messages to use as input for the
            chat completion.
        credentials (AzureCredentials): The Azure credentials.
        ignore_refusals (bool, optional): Whether to ignore refusals in the chat completion.
        timeout (float | None, optional): The maximum time in seconds to wait for the completion.
            Defaults to None.
        temperature (float | None, optional): Controls the randomness of the output. Higher values
            make the output more random. Defaults to None.
        tool_choice (ChatCompletionToolChoiceOptionParam | None, optional): The tool choice option
            for the chat completion. Defaults to None.
        tools (list[ChatCompletionToolParam] | None, optional): A list of tools to use for the
            chat completion. Defaults to None.
        response_format (ResponseFormat | BaseModel | None, optional): The response format for the
            chat completion. Defaults to None.

    Returns:
        str: The generated chat completion.

    Raises:
        ChatError: If no response is returned.
        InternalServerError: If the chat fails due to content filter.
        RateLimitError: If the chat fails due to length.

    """
    response = create_chat(
        model=model,
        messages=messages,
        credentials=credentials,
        timeout=timeout,
        temperature=temperature,
        tool_choice=tool_choice,
        tools=tools,
    )

    result = get_response(response, tool_choice, response_format)

    if not ignore_refusals:
        check_for_refusal(result)

    return result
