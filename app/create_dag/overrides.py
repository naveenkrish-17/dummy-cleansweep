"""A module for managing environment overrides."""

import json
from typing import Any

from pydantic import BaseModel, field_validator


class Override(BaseModel):
    """A class to store and manage environment overrides."""

    name: str
    value: Any


class Overrides(BaseModel, validate_assignment=True):
    """A class to store and manage environment overrides."""

    env: list[Override]

    @field_validator("env")
    @classmethod
    def ensure_unique_objects(cls, v):
        """Ensure that all objects in the provided list have unique names.

        Args:
            cls: The class reference (not used in the method).
            v (list): A list of objects, each expected to have a 'name' attribute.

        Returns:
            list: The original list if all objects have unique names.

        Raises:
            ValueError: If any two objects in the list have the same 'name' attribute.

        """
        seen = set()
        for obj in v:
            if obj.name in seen:
                raise ValueError(f"Duplicate object with name '{obj.name}' found.")
            seen.add(obj.name)
        return v

    def add(self, name: str, value: Any):
        """Add a new override to the environment or updates an existing one.

        Args:
            name (str): The name of the override.
            value (Any): The value of the override.

        Returns:
            None

        """
        for override in self.env:
            if override.name == name:
                override.value = value
                return

        self.env.append(Override(name=name, value=value))

    def get(self, name: str, default: Any | None = None) -> Any:
        """Get the value of an override by its name.

        Args:
            name (str): The name of the override to get the value of.
            default (Any | None, optional): The default value to return if the override is not found.
                Defaults to None.

        Returns:
            Any: The value of the override.

        """
        for override in self.env:
            if override.name == name:
                return override.value

        return default

    def dump(self) -> list[dict[str, Any]]:
        """Dump the environment overrides into a list of dictionaries.

        Returns:
            list[dict[str, Any]]: A list of dictionaries containing the override names and their
                values.

        """
        overrides = []

        for override in self.env:
            if isinstance(override.value, (list, dict, bool)):
                overrides.append(
                    {"name": f"{override.name}", "value": json.dumps(override.value)}
                )
            else:
                overrides.append(
                    {"name": f"{override.name}", "value": str(override.value)}
                )

        return overrides

    def extend(self, overrides: list[Override]):
        """Extend the current collection of overrides.

        Args:
            overrides (list[Override]): A list of Override objects to be added to the current
                collection.

        """
        for override in overrides:
            self.add(override.name, override.value)


def create_overrides(config: dict[str, Any], prefix: str | None = None) -> Overrides:
    """Create an Overrides object from a given configuration dictionary.

    Args:
        config (dict[str, Any]): A dictionary containing configuration key-value pairs.
        prefix (str | None, optional): A prefix to be added to each key in the configuration.
            If None, no prefix is added. If provided and does not end with "__", "__" is appended.

    Returns:
        Overrides: An Overrides object containing the environment overrides.

    """
    if prefix is None:
        prefix = ""
    elif not prefix.endswith("__"):
        prefix += "__"

    return Overrides(
        env=[Override(name=f"{prefix}{k}", value=v) for k, v in config.items()]
    )
