"""Utility functions for working with collections."""

from functools import reduce
from typing import Any, Dict, List, Set, Tuple, TypeVar, overload


def dict_not_none(**kwargs: Any) -> dict:
    """Return a dictionary of the given keyword arguments, excluding any None values.

    Args:
        **kwargs: The keyword arguments to include in the dictionary.

    Returns:
        dict: A dictionary of the given keyword arguments, excluding any None values.

    """
    return {k: v for k, v in kwargs.items() if v is not None}


T = TypeVar("T", Dict, List, Set, Tuple)


@overload
def safe_get(collection: Dict[Any, Any], key: Any, default: Any = None) -> Any: ...
@overload
def safe_get(collection: List[Any], key: Any, default: Any = None) -> Any: ...
@overload
def safe_get(collection: Set[Any], key: Any, default: Any = None) -> Any: ...
@overload
def safe_get(collection: Tuple[Any], key: Any, default: Any = None) -> Any: ...
def safe_get(collection: T, key: Any, default: Any = None) -> Any:
    """Safely get a value from a collection, returning a default value if the key is not found.

    Args:
        collection (T): The collection to get the value from.
        key (Any): The key to look up in the collection.
        default (Any): The default value to return if the key is not found.

    Returns:
        The value of the key in the collection.

    """
    if isinstance(collection, (list, tuple)):
        try:
            return collection[key]
        except IndexError:
            return default

    if isinstance(collection, (dict)):
        return collection.get(key, default)

    if isinstance(collection, (set)):
        l_of_s = list(collection)
        try:
            return l_of_s[key]
        except IndexError:
            return default


@overload
def dig(collection: Dict[Any, Any], *keys: Any, default: Any = None) -> Any: ...
@overload
def dig(collection: List[Any], *keys: Any, default: Any = None) -> Any: ...
@overload
def dig(collection: Set[Any], *keys: Any, default: Any = None) -> Any: ...
@overload
def dig(collection: Tuple[Any], *keys: Any, default: Any = None) -> Any: ...
def dig(collection: T, *keys: Any, default: Any = None) -> Any:
    """Dig into a collection to find a value, returning a default value if the key is not found.

    Args:
        collection (T): The collection to get the value from.
        keys (Any): The keys to look up in the collection.
        default (Any): The default value to return if the key is not found.

    Returns:
        The value of the key in the collection.

    """
    return reduce(lambda x, y: safe_get(x, y, default), keys, collection)


@overload
def plant(collection: Dict[Any, Any], *keys: Any, value: Any): ...
@overload
def plant(collection: List[Any], *keys: Any, value: Any): ...
@overload
def plant(collection: Set[Any], *keys: Any, value: Any): ...
@overload
def plant(collection: Tuple[Any], *keys: Any, value: Any): ...
def plant(collection: T, *keys: Any, value: Any):
    """Dig into a collection to set a value.

    Args:
        collection (T): The collection to set the value in.
        keys (Any): The keys to look up in the collection.
        value (Any): The value to set.

    """
    target = dig(collection, *keys[:-1])
    target[keys[-1]] = value
