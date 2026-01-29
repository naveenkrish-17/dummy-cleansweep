"""Hook specifications for the Cleansweep plugin system."""

from typing import Any

import pandas as pd
import pluggy

# pylint: disable=unused-argument

hookspec = pluggy.HookspecMarker("cleansweep")


# Hooks implemented by the package after transforming the documents and metadata
@hookspec
def post_transform(
    documents: list[dict],
) -> list[dict[str, Any]]:  # pyright: ignore[reportReturnType]
    """Post transform hook implementation.

    Perform additional transformations on documents after they have been extracted from
    the source.

    Args:
        documents (list[dict]): The extracted documents.

    Returns:
        list[dict]: The transformed documents.

    """


@hookspec
def metadata_transform(
    documents: pd.DataFrame,
) -> pd.DataFrame:  # pyright: ignore[reportReturnType]
    """Metadata transform hook implementation.

    Create additional metadata fields or modify existing metadata fields which can be
    added to the EmbeddedChunk objects written to the embedding file.

    Args:
        documents (DataFrame): The documents DataFrame that contains the extracted documents.

    Returns:
        DataFrame: The documents DataFrame with additional or modified metadata fields.

    """


# Hooks implemented by the package after cleaning the documents
@hookspec
def documents_clean(
    documents: pd.DataFrame,
) -> pd.DataFrame:  # pyright: ignore[reportReturnType]
    """Clean documents hook implementation.

    Perform additional cleaning on the documents after they have had standard cleaning
    procedures applied.

    Args:
        documents (DataFrame): The cleaned documents DataFrame.

    Returns:
        DataFrame: The cleaned documents DataFrame.

    """


@hookspec
def pre_chunk(
    documents: pd.DataFrame,
) -> pd.DataFrame:  # pyright: ignore[reportReturnType]
    """Pre chunk hook implementation.

    Perform additional transformations on documents before they are chunked.

    Args:
        documents (DataFrame): The documents to be chunked.

    Returns:
        DataFrame: The transformed documents.

    """


@hookspec
def post_chunk(
    documents: pd.DataFrame,
) -> pd.DataFrame:  # pyright: ignore[reportReturnType]
    """Post chunk hook implementation.

    Perform additional transformations on documents after they have been chunked.

    Args:
        documents (DataFrame): The chunked documents.

    Returns:
        DataFrame: The transformed documents.

    """


@hookspec
def pre_embed(
    documents: pd.DataFrame,
) -> pd.DataFrame:  # pyright: ignore[reportReturnType]
    """Embed transform hook implementation.

    Perform additional transformations on documents after they have been embedded.

    Args:
        documents (DataFrame): The embedded documents.

    Returns:
        DataFrame: The transformed documents.

    """


@hookspec
def post_embed(
    documents: pd.DataFrame,
) -> pd.DataFrame:  # pyright: ignore[reportReturnType]
    """Embed transform hook implementation.

    Perform additional transformations on documents after they have been embedded.

    Args:
        documents (DataFrame): The embedded documents.

    Returns:
        DataFrame: The transformed documents.

    """


@hookspec
def run(*args, **kwargs) -> None:
    """Run custom code during the pipeline.

    *args and **kwargs are passed from the pipeline config. All code must be self-contained, no
    return value is expected.
    """
