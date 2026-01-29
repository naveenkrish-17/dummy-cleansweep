"""Utility functions for creating tool choices and functions."""

from typing import Any

from openai.types.chat.chat_completion_named_tool_choice_param import (
    ChatCompletionNamedToolChoiceParam,
    Function,
)
from openai.types.chat.chat_completion_tool_param import ChatCompletionToolParam
from openai.types.shared_params import FunctionDefinition
from pydantic import BaseModel

from cleansweep.utils.pydantic import is_basemodel_type


def create_function(
    name: str,
    description: str,
    parameters: dict[str, Any] | type[BaseModel],
    strict: bool = False,
) -> ChatCompletionToolParam:
    """Create a function definition to be used in the OpenAI API.

    Args:
        name (str): The name of the function to be called.
        description (str): A description of what the function does.
        parameters (dict[str, Any] | BaseModel): The parameters of the function.
        strict (bool, optional): Whether the function should be called strictly. Defaults to False.

    Returns:
        ChatCompletionToolParam: The function definition.

    """
    schema: dict[str, Any] = {}
    if is_basemodel_type(parameters):
        schema = parameters.model_json_schema()
    elif isinstance(parameters, dict):
        schema = parameters

    if not schema:
        raise ValueError("Parameters must be a Pydantic model or a dictionary.")

    if strict:
        # make sure additionalProperties is set to False
        schema["additionalProperties"] = False

        if "$defs" in schema:
            for _, value in schema["$defs"].items():
                value["additionalProperties"] = False

    return ChatCompletionToolParam(
        type="function",
        function=FunctionDefinition(
            name=name,
            description=description,
            parameters=schema,
            strict=strict,
        ),
    )


def tool_choice(name: str) -> ChatCompletionNamedToolChoiceParam:
    """Create a named tool choice to be used in the OpenAI API.

    Args:
        name (str): The name of the tool choice.

    Returns:
        ChatCompletionNamedToolChoiceParam: The named tool choice.

    """
    return ChatCompletionNamedToolChoiceParam(
        type="function", function=Function(name=name)
    )
