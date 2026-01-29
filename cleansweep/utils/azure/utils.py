"""Utility functions for the Azure module."""

import json
import logging
from typing import (
    Any,
    Iterable,
    Literal,
    Protocol,
    Sequence,
    Type,
    TypeVar,
    Union,
    overload,
)

import tiktoken
from langchain_text_splitters import RecursiveCharacterTextSplitter
from openai.types.chat import (
    ChatCompletionAssistantMessageParam,
    ChatCompletionContentPartParam,
    ChatCompletionContentPartTextParam,
    ChatCompletionFunctionMessageParam,
    ChatCompletionMessageParam,
    ChatCompletionMessageToolCallParam,
    ChatCompletionSystemMessageParam,
    ChatCompletionToolMessageParam,
    ChatCompletionUserMessageParam,
)
from pydantic import BaseModel, ValidationError

from cleansweep._types import Texts

logger = logging.getLogger(__name__)


def batch_texts(
    texts: Sequence[Texts], token_limit: int, model_name: str
) -> list[list[Texts]]:
    """Split a list of texts into multiple chunks based on a token limit.

    Args:
        texts (list[Texts]): The list of texts to be batched.
        token_limit (int): The maximum number of tokens allowed in each chunk.
        model_name (str): The name of the model used for token encoding.

    Returns:
        list[list[Texts]]: A list of chunks, where each chunk is a list of texts.

    """
    texts = list(texts)

    encoding = tiktoken.encoding_for_model(model_name)
    output = []
    # add the first document to the first chunk to avoid empty chunks
    working_chunk = [texts.pop(0)]
    working_chunk_length = len(encoding.encode(str(working_chunk[0])))
    for text in texts:
        tokens = len(encoding.encode(str(text)))
        if working_chunk_length + tokens > token_limit:
            output.append(working_chunk)
            working_chunk = []
            working_chunk_length = 0

        working_chunk.append(text)
        working_chunk_length += tokens

    if working_chunk:
        output.append(working_chunk)

    return output


def min_chunk_documents(
    documents: list[str], token_limit: int, model_name: str
) -> list[list[str]]:
    """Split a list of documents into chunks based on a token limit.

    Fills each "chunk" with as few documents as possible.

    Args:
        documents (list[str]): The documents to chunk.
        token_limit (int): The token limit.
        model_name (str): The model name.

    Returns:
        list[list[str]]: The chunked documents.

    """
    encoding = tiktoken.encoding_for_model(model_name)
    text_splitter = RecursiveCharacterTextSplitter.from_tiktoken_encoder(
        model_name=model_name,
        chunk_size=token_limit,
        chunk_overlap=0,
    )
    output = []

    for document in documents:
        tokens = len(encoding.encode(document))
        if tokens > token_limit:
            output.append(text_splitter.split_text(document))
        else:
            output.append([document])

    return output


def num_tokens_from_messages(
    messages: list[ChatCompletionMessageParam], model: str
) -> int:
    """Return the number of tokens used by a list of messages.

    Args:
        messages (list[ChatCompletionMessageParam]): The messages.
        model (str): The model name.

    """
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        encoding = tiktoken.get_encoding("cl100k_base")

    num_tokens = 0
    for message in messages:
        num_tokens += (
            4  # every message follows <im_start>{role/name}\n{content}<im_end>\n
        )
        for key, value in message.items():
            if not isinstance(value, str):
                continue
            num_tokens += len(encoding.encode(value))
            if key == "name":  # if there's a name, the role is omitted
                num_tokens += -1  # role is always required and always 1 token
    num_tokens += 2  # every reply is primed with <im_start>assistant
    return num_tokens


def num_tokens_from_strings(strings: str | list[str], model: str | None = None) -> int:
    """Return the number of tokens in a text string.

    Args:
        strings (Union[str, list[str])): The text strings.
        model (str, Optional): The model name.

    Returns:
        int: The number of tokens in the text strings.

    """
    if strings is None:
        return 0

    if model is None:
        encoding = tiktoken.get_encoding("cl100k_base")
    else:
        try:
            encoding = tiktoken.encoding_for_model(model)
        except KeyError:
            encoding = tiktoken.get_encoding("cl100k_base")

    if isinstance(strings, str):
        strings = [strings]
    tokens = [len(encoding.encode(string)) for string in strings]
    return sum(tokens)


@overload
def create_message(
    content: Union[str, Iterable[ChatCompletionContentPartTextParam]],
    role: Literal["user"],
    name: Union[str, None] = None,
    tool_call_id: None = None,
    refusal: None = None,
    tool_calls: None = None,
) -> ChatCompletionUserMessageParam: ...


@overload
def create_message(
    content: str,
    role: Literal["assistant"],
    name: Union[str, None] = None,
    tool_call_id: None = None,
    refusal: Union[str, None] = None,
    tool_calls: Union[Iterable[ChatCompletionMessageToolCallParam], None] = None,
) -> ChatCompletionAssistantMessageParam: ...


@overload
def create_message(
    content: Union[str, Iterable[ChatCompletionContentPartTextParam]],
    role: Literal["system"],
    name: Union[str, None] = None,
    tool_call_id: None = None,
    refusal: None = None,
    tool_calls: None = None,
) -> ChatCompletionSystemMessageParam: ...


@overload
def create_message(
    content: Union[str, Iterable[ChatCompletionContentPartTextParam]],
    role: Literal["tool"],
    name: str,
    tool_call_id: str,
    refusal: None = None,
    tool_calls: None = None,
) -> ChatCompletionToolMessageParam: ...


@overload
def create_message(
    content: Union[str, Iterable[ChatCompletionContentPartTextParam]],
    role: Literal["tool"],
    name: None,
    tool_call_id: str,
    refusal: None = None,
    tool_calls: None = None,
) -> ChatCompletionToolMessageParam: ...


@overload
def create_message(
    content: str,
    role: Literal["function"],
    name: Union[str, None] = None,
    tool_call_id: None = None,
    refusal: None = None,
    tool_calls: None = None,
) -> ChatCompletionFunctionMessageParam: ...


def create_message(  # pylint: disable=unused-argument, line-too-long
    content: Union[
        str,
        Iterable[ChatCompletionContentPartTextParam],
        Iterable[ChatCompletionContentPartParam],
    ],
    role: Literal["system", "user", "assistant", "tool", "function"],
    name: Union[str, None] = None,
    tool_call_id: Union[str, None] = None,
    refusal: Union[str, None] = None,
    tool_calls: Union[Iterable[ChatCompletionMessageToolCallParam], None] = None,
    **kwargs,
) -> ChatCompletionMessageParam:
    """Create a message parameter for the OpenAI API.

    Args:
        content (Union[str, Iterable[ChatCompletionContentPartTextParam], Iterable[ChatCompletionContentPartParam]]):
            The content of the message.
        role (Literal["system", "user", "assistant", "tool", "function"]): The role of the message.
        name (Union[str, None]): The name of the message.
        tool_call_id (Union[str, None]): The tool call ID.
        refusal (Union[str, None]): The refusal message.
        tool_calls (Union[Iterable[ChatCompletionMessageToolCallParam], None]): The tool calls.
        **kwargs: Additional keyword arguments.

    Returns:
            ChatCompletionMessageParam: The message parameter.

    """
    args = {
        "content": content,
        "role": role,
        "name": name,
        "tool_call_id": tool_call_id,
        "refusal": refusal,
        "tool_calls": tool_calls,
    }
    args = {k: v for k, v in args.items() if v}

    if role == "system":
        return ChatCompletionSystemMessageParam(**args)

    if role == "user":
        return ChatCompletionUserMessageParam(**args)

    if role == "assistant":
        return ChatCompletionAssistantMessageParam(**args)

    if role == "tool":
        return ChatCompletionToolMessageParam(**args)

    if role == "function":
        return ChatCompletionFunctionMessageParam(**args)

    raise ValueError("Invalid role")


class HasItems(Protocol):
    """Protocol for objects with an items attribute."""

    items: list[Any]


T = TypeVar("T", bound=HasItems)


def process_results(
    results: list[str],
    item_type: Type[BaseModel],
    response_type: Type[T],
) -> list[T]:
    """Process the results obtained from a list of strings.

    Args:
        results (list[str]): The list of strings to be processed.
        item_type (type[BaseModel]): The type of the items to be created.
        response_type (Type[T]): The type of the response to be returned.

    Returns:
        list[T]: The processed results as a list of response objects.

    """
    processed_results = []
    for i, result in enumerate(results):

        try:
            processed_result = json.loads(result)
        except (json.JSONDecodeError, TypeError) as exc:
            logger.error(
                "Error decoding JSON for aticle index %s: %s (%s)", i, exc, result
            )
            processed_results.append(None)
            continue

        items = []
        for j, item in enumerate(processed_result.get("items", [])):
            if item is None:
                obj = item
            elif not isinstance(item, dict):
                logger.error(
                    "item %s of article %s is not a valid dictionary: %s", j, i, item
                )
                obj = None
            else:
                try:
                    obj = item_type(**item)
                except (ValidationError, TypeError) as exc:
                    logger.error(
                        "Error creating %s for item %s of article %s: %s (%s)",
                        j,
                        i,
                        item_type.__name__,
                        exc,
                        item,
                    )
                    obj = None
            items.append(obj)
        processed_results.append(response_type(items=items))  # type: ignore
    return processed_results
