"""The substrings module contains functions for working with substrings."""

__all__ = ["replace_substrings", "remove_substrings"]

import re

from cleansweep.utils.regex import is_regex


def replace_substrings(input_string: str, old: list[str], new: str = "") -> str:
    """Replace all occurrences of substrings in a string with a new substring.

    Args:
        input_string (str): The string to search for substrings.
        old (list[str]): The substrings to search for.
        new (str, optional): The substring to replace the old substrings with. Defaults to "".

    Returns:
        str: The input string with all occurrences of the old substrings replaced with the new
        substring.

    """
    if not isinstance(old, list):
        old = [old]

    for substring in old:
        input_string = (
            input_string.replace(substring, new)
            if is_regex(substring) is False
            else re.sub(substring, new, input_string)
        )

    return input_string


def remove_substrings(input_string: str, substrings: list[str]) -> str:
    """Remove all occurrences of substrings in a string.

    Args:
        input_string (str): The string to search for substrings.
        substrings (list[str]): The substrings to remove.

    Returns:
        str: The input string with all occurrences of the substrings removed.

    """
    if not isinstance(substrings, list):
        substrings = [substrings]
    return replace_substrings(input_string, substrings, "")
