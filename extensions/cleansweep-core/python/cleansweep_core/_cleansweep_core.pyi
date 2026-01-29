"""Module for Cleansweep Core API."""

# pylint: disable=W0613
from os import PathLike
from typing import Any, Tuple, TypeAlias

def process_merge_results(
    results: list[str],
    frame_records: list[list[dict[str, Any]]],
    cluster_ids: list[str],
) -> list[dict[str, Any]]:
    """Process merge results from API and return merged DataFrame.

    Args:
        results (list): List of results from API
        frame_records (list): List of DataFrame records
        cluster_ids (list): List of cluster IDs

    Returns:
        list: List of merged DataFrame records

    """

Token: TypeAlias = Tuple[str, bool | str | int | float]
"""Each Token is a tuple containing a JSON Path and the value of the element in the document."""

StrPath: TypeAlias = str | PathLike[str]

class Tokenizer:
    """Tokenizer class for document transformation."""

    def tokenize_document(self, path: StrPath, root: str | None = None) -> list[Token]:
        """Tokenize a document using the given path.

        Args:
            path (StrPath): Path of file for tokenization.
            root (str, Optional): Root JSON Path value for tokenization.

        Returns:
            list: List of tokens

        """

class Transformer:
    """Transformer class for document transformation."""

    def transform_document(
        self, mapping_path: StrPath, document_path: StrPath, root: str | None = None
    ) -> list[dict[str, Any]]:
        """Transform a document using the given mapping and document paths.

        Args:
            mapping_path (StrPath): Path of mapping file.
            document_path (StrPath): Path of document file.
            root (str, Optional): Root JSON Path value for transformation.

        Returns:
            list: List of transformed documents

        """
