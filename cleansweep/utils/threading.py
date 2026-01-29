"""Utilities for threading."""

import threading
from functools import wraps

_tracker_lock = threading.Lock()


def with_lock(func):
    """Wrap a function with a lock."""

    @wraps(func)
    def wrapped(*args, **kwargs):
        with _tracker_lock:
            return func(*args, **kwargs)

    return wrapped


class Threadsafe:
    """Metaclass that wraps all methods and properties of a class with a lock.

    Internal methods and properties are not wrapped as this can call deadlock, and properties are
    wrapped manually to ensure that the getter, setter, and deleter are all wrapped.

    """

    def __init_subclass__(cls, **kwargs):
        """Wrap all methods and properties of a class with a lock."""
        super().__init_subclass__(**kwargs)
        for attr, value in cls.__dict__.items():
            if isinstance(value, property):
                # If it's a property, we need to manually wrap the getter, setter, and deleter
                new_prop = property(
                    fget=with_lock(value.fget) if value.fget else None,
                    fset=with_lock(value.fset) if value.fset else None,
                    fdel=with_lock(value.fdel) if value.fdel else None,
                )
                setattr(cls, attr, new_prop)
            elif callable(value) and not attr.startswith("_"):
                setattr(cls, attr, with_lock(value))
