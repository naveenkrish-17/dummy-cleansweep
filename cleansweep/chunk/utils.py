"""Utility functions for text splitters."""

from typing import Callable

from langchain_text_splitters import (
    NLTKTextSplitter,
    RecursiveCharacterTextSplitter,
    SpacyTextSplitter,
)

from cleansweep.chunk.html import HTMLSectionSplitter
from cleansweep.enumerations import TextSplitter


def get_text_splitter(splitter: str | TextSplitter) -> Callable:
    """Get the text splitter from the given string.

    Args:
        splitter (Union[str, TextSplitter]): The text splitter to use

    Returns:
        Callable: The text splitter

    """
    if not isinstance(splitter, TextSplitter):
        splitter = TextSplitter(splitter.lower())

    if splitter == TextSplitter.RECURSIVE:
        return RecursiveCharacterTextSplitter
    if splitter == TextSplitter.NLTK:
        return NLTKTextSplitter
    if splitter == TextSplitter.SPACY:
        return SpacyTextSplitter
    if splitter == TextSplitter.HTML:
        return HTMLSectionSplitter

    raise ValueError(f"Invalid text splitter: {splitter}")


def get_text_splitter_string(splitter: Callable) -> str:
    """Get the text splitter string from the given text splitter.

    Args:
        splitter (Callable): The text splitter

    Returns:
        str: The text splitter string

    """
    if splitter == RecursiveCharacterTextSplitter:
        return TextSplitter.RECURSIVE.value
    if splitter == NLTKTextSplitter:
        return TextSplitter.NLTK.value
    if splitter == SpacyTextSplitter:
        return TextSplitter.SPACY.value
    if splitter == HTMLSectionSplitter:
        return TextSplitter.HTML.value

    raise ValueError(f"Invalid text splitter: {splitter}")


def get_paragraph_delimiter(
    strategy: str, repository: dict | None = None
) -> str | None:
    """Get the paragraph delimiter for the strategy.

    Args:
        strategy (str): The strategy to check.
        repository (dict): The repository to check.

    Returns:
        str: The paragraph delimiter for the strategy.

    """
    if repository is None:
        return None

    if strategy not in repository:
        strategy = "default"

    strategy_settings = repository.get(strategy, {})

    if "paragraph_delimiter" in strategy_settings:
        return strategy_settings["paragraph_delimiter"]
