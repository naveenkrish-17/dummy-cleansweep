"""Utilities for loading configuration files and converting their contents into objects."""

from typing import Any, TypeVar

from pydantic import BaseModel
from pydantic_core import ValidationError

from cleansweep.core.fileio import read_file_to_dict
from cleansweep.exceptions import PipelineError
from cleansweep.model.network import PathLikeUrl, convert_to_url

T = TypeVar("T", bound=BaseModel)


def load(
    config_uri: PathLikeUrl | str,
    config_object: str,
    target_object: type[T] | None = None,
    name_field: str | None = "name",
) -> dict[Any, type[T]]:
    """Load a configuration file and convert its contents into a dictionary of objects.

    Args:
        config_uri (PathLikeUrl | str): The URI of the configuration file.
        config_object (str): The key in the configuration file that contains the list of objects to
            load.
        target_object (type[T]): The type of objects to instantiate from the configuration data.
        name_field (str | None, optional): The field name to use as the key in the resulting
            dictionary. Defaults to "name".

    Returns:
        dict[Any, type[T]]: A dictionary where the keys are the values of the specified name_field
            and the values are instances of target_object.

    Raises:
        PipelineError: If the configuration file is empty or if there is a validation error when
            creating an object.

    """
    config_uri = convert_to_url(config_uri)

    if name_field is None:
        name_field = "name"

    content = read_file_to_dict(config_uri)
    if not content:
        raise PipelineError("No settings found in the configuration file")

    content = content[0]

    repository = {}
    for cfg in content.get(config_object, []):
        if name_field not in cfg:
            raise PipelineError(f"Missing {name_field} in configuration")
        try:
            o = target_object(**cfg) if target_object else cfg
        except ValidationError as exc:
            raise PipelineError(f"Invalid {config_object} configuration") from exc
        repository[getattr(o, name_field)] = o

    return repository
