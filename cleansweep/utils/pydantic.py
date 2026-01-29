"""Utility functions for working with Pydantic models."""

import inspect
from types import GenericAlias
from typing import Any, Sequence, TypeGuard, TypeVar, get_origin

import pydantic

from cleansweep._types import SeriesLike

T = TypeVar("T", bound=pydantic.BaseModel)


def is_basemodel_type(cls: Any) -> TypeGuard[type[pydantic.BaseModel]]:
    """Check if the given type is a Pydantic BaseModel.

    Args:
        cls (Any): The type to check.

    Returns:
        TypeGuard[type[pydantic.BaseModel]]: True if the type is a Pydantic BaseModel, False
            otherwise.

    """
    if not inspect.isclass(cls):
        return False

    return issubclass(cls, pydantic.BaseModel)


def cast_to(
    row: SeriesLike, target_type: type[pydantic.BaseModel]
) -> pydantic.BaseModel:
    """Cast a dictionary-like row to a specified Pydantic BaseModel type.

    Args:
        row (SeriesLike): The input data to be cast, typically a dictionary or a Pandas Series.
        target_type (pydantic.BaseModel): The Pydantic BaseModel class to cast the row to.

    Returns:
        pydantic.BaseModel: An instance of the target_type populated with data from the row.

    Raises:
        ValidationError: If the row cannot be cast to the target_type due to validation issues.

    """
    row_mapped_to_class = {}
    for key, value in target_type.model_fields.items():

        if isinstance(value.annotation, GenericAlias):
            origin = get_origin(value.annotation)
            val = row.get(key)
            if origin is list and not isinstance(val, list):
                val = [val]

            row_mapped_to_class[key] = val
        elif is_basemodel_type(value.annotation):
            row_mapped_to_class[key] = cast_to(row, value.annotation)
        else:
            row_mapped_to_class[key] = row.get(key)

    return target_type(**row_mapped_to_class)


def create_simple_model(
    model_name: str, fields: Sequence[str]
) -> type[T]:  # pyright: ignore[reportInvalidTypeVarUse]
    """Dynamically create a Pydantic model with the specified fields.

    **It is assumed that all fields type are strings, are required and have no default.**

    Use `create_model` from `pydantic` directly if you need more control over the field definitions.

    Args:
        model_name (str): The name of the model to be created.
        fields (Sequence[str]): A sequence of field names to be included in the model.

    Returns:
        T: A dynamically created Pydantic model class with the specified fields.

    """
    field_definitions = {field: (str, ...) for field in fields}

    return pydantic.create_model(model_name, **field_definitions)  # type: ignore
