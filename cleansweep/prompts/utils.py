"""Utility functions for prompts."""

from typing import Any

from jinja2 import Environment, FileSystemLoader

from cleansweep._types import PromptVariable, SeriesLike, StrPath
from cleansweep.exceptions import PromptError


def create_prompt(
    prompt_dir: StrPath | None = None,
    template_name: str | None = None,
    prompt: str | None = None,
    **kwargs: Any,
) -> str:
    """Create a prompt by rendering a template or using a provided prompt string.

    Args:
        prompt_dir (str | None): The directory where the template is located. Defaults to None.
        template_name (str | None): The name of the template to use. Defaults to None.
        prompt (str | None): The prompt string to use. Defaults to None.
        **kwargs: Additional keyword arguments to be passed to the template.

    Returns:
        str: The final rendered prompt.

    Raises:
        PromptError: If neither prompt nor template_name is provided.

    """
    final_prompt: str | None = None
    env = Environment(
        loader=FileSystemLoader(prompt_dir) if prompt_dir is not None else None
    )

    if prompt is not None:
        template = env.from_string(prompt)
    elif template_name is not None:
        template = env.get_template(template_name)
    else:
        raise PromptError("Prompt not provided")

    final_prompt = template.render(**kwargs)

    return final_prompt


def row_to_prompt_kwargs(
    row: SeriesLike, variables: list[PromptVariable]
) -> dict[str, Any]:
    """Convert a row of data to keyword arguments for a prompt.

    Args:
        row (SeriesLike): The row of data to convert.
        variables (list[PromptVariable]): The list of prompt variables.

    Returns:
        dict[str, Any]: The keyword arguments for the prompt.

    """
    return {
        var.name: row.get(var.value, var.value)
        for var in variables
        if var.name is not None and var.value is not None
    }
