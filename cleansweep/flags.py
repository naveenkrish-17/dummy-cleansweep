"""Feature flags module."""

import functools
from typing import Any, Callable

from cleansweep.settings.base import settings


def flag(
    name: str,
    arg_pos: int | None = None,
    arg_name: str | None = None,
    default: Any = None,
) -> Callable:
    """Feature flag decorator.

    Feature flags are defined as attributes of the object that are instances of BaseModel and have
    an "enabled" attribute. Nested objects are supported by passing the parent object name and
    attribute name with "__" as the separator.

    Args:
        name: The name of the feature flag.
        arg_pos: The position of the argument to return if the feature flag is disabled.
        arg_name: The name of the argument to return if the feature flag is disabled.
        default: The default value to return if the feature flag is disabled.

    Returns:
        function: The wrapped function.

    Example:
        @flag("my_feature", default="Hello, world!")
        def my_func():
            return "Hello, world!"

    """

    def wrapper(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            if settings.feature.model_dump().get(name.lower(), False):
                return func(*args, **kwargs)
            else:
                if arg_name:
                    return kwargs.get(arg_name, default)
                if arg_pos is not None:
                    return args[arg_pos]
                return default

        return wrapped

    return wrapper
