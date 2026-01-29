"""Utility functions for working with regular expressions."""

import re


def is_regex(pattern: str) -> bool:
    """Check if the given pattern is a valid regular expression.

    Args:
        pattern (str): The regex pattern to be validated.

    Returns:
        bool: True if the pattern is a valid regex, False otherwise.

    """
    try:
        re.compile(pattern)
        return True
    except re.error:
        return False
