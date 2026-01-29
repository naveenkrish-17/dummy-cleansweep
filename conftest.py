"""Pytest fixtures and plugins."""

import functools

from pydantic import BaseModel, SecretStr

# region cache_call
# Cache the call to the function or method.

# This provides a way to assert that a function or method was called with the correct arguments
# without using a mock or a spy.

# This is useful when you are patching a function and replacing it with a new function or class.

# The cache is GLOBAL and will persist between test cases.

# Usage:
# ```python
# @cache_call
# def my_function():
#     pass

# my_function()
# assert_called(my_function)
# ```

CACHE = {}


def cache_call(func):
    """Cache the call to the function or method.

    Usage:
    ```python
    @cache_call
    def my_function():
        pass

    my_function()
    assert my_function in CACHE
    ```

    """

    @functools.wraps(func)
    def wrapped(*args, **kwargs):

        if func.__qualname__ not in CACHE:
            CACHE[func.__qualname__] = []

        CACHE[func.__qualname__].append((args, kwargs))

        result = func(*args, **kwargs)

        return result

    return wrapped


def assert_called(func):
    """Assert that the function or method was called."""
    assert func.__qualname__ in CACHE


def assert_called_with(func, *args, **kwargs):
    """Assert that the function or method was called with the given arguments.

    Callables must be decorated with `@cache_call`.

    Class methods must have their parent class passed as the first argument.
    """
    assert (args, kwargs) in CACHE[func.__qualname__]


def assert_called_once(func):
    """Assert that a function has been called exactly once.

    Args:
    ----
        func: The function to check.

    Raises:
    ------
        AssertionError: If the function has not been called exactly once.

    """
    assert len(CACHE[func.__qualname__]) == 1


def assert_called_once_with(func, *args, **kwargs):
    """Assert that a function has been called exactly once with the specified arguments.

    Args:
    ----
        func: The function to check.
        *args: The positional arguments passed to the function.
        **kwargs: The keyword arguments passed to the function.

    Raises:
    ------
        AssertionError: If the function has not been called exactly once with the specified
            arguments.

    """
    assert len(CACHE[func.__qualname__]) == 1
    assert (args, kwargs) == CACHE[func.__qualname__][0]


def assert_called_n_times(func, n):
    """Assert that a function has been called exactly n times.

    Args:
    ----
        func (callable): The function to check.
        n (int): The expected number of times the function should have been called.

    Raises:
    ------
        AssertionError: If the function has not been called exactly n times.

    """
    assert len(CACHE[func.__qualname__]) == n


# endregion


class MockAzureCredentials(BaseModel):
    """Mock class for the Azure ClientSecretCredential class."""

    openai_api_base: str = "https://api.openai.com"
    openai_api_version: str = "2020-05-10"

    api_key: SecretStr = SecretStr("api_key")

    def __hash__(self):
        return hash(self.api_key)
