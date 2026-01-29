"""Embedding module."""

__all__ = [
    "create_df_to_embed",
    "create_embeddings",
]

import asyncio
import json
import logging
from typing import Any, Optional

import pandas as pd
from pydantic import BaseModel, ValidationError

from cleansweep._types import Deployment
from cleansweep.chunk.utils import get_paragraph_delimiter
from cleansweep.embed.embedder import get_embedder
from cleansweep.enumerations import EmbedderType
from cleansweep.exceptions import EmbeddingError
from cleansweep.utils.pydantic import cast_to

logger = logging.getLogger(__name__)


def get_columns_to_embed(
    default_columns_to_embed: list[str], extra_columns_to_embed: Optional[list[str]]
) -> list[str]:
    """Get the columns to embed.

    The base list of columns is appended to the end of the extra list of columns.

    Args:
        default_columns_to_embed (list[str]): a list of default columns to embed
        extra_columns_to_embed (list[str], Optional): a list of extra columns to embed

    Returns:
        list[str]: a list of columns to embed

    """
    if not extra_columns_to_embed:
        return default_columns_to_embed

    for col in default_columns_to_embed:
        if col not in extra_columns_to_embed:
            extra_columns_to_embed.append(col)

    return extra_columns_to_embed


def create_df_to_embed(
    df: pd.DataFrame,
    columns_to_embed: list[str],
    strategy_settings: tuple[str, dict] | None = None,
) -> pd.DataFrame:
    """Create a DataFrame to embed.

    Args:
        df (DataFrame): a pandas DataFrame
        columns_to_embed (list[str]): a list of columns to embed
        strategy_settings (tuple[str, dict], Optional): a tuple of strategy settings

    Returns:
        DataFrame: a pandas DataFrame with text to embed

    """

    def create_text_to_embed(row: pd.Series) -> str:
        """Create text to embed from a row.

        Args:
            row (Series): a row from a pandas DataFrame

        Returns:
            str: the text to embed

        """
        text_to_embed = ""
        for column in columns_to_embed:

            if pd.isnull(row[column]) is True:
                continue

            prefix = f"**{column.replace('metadata_', '').capitalize()}**: "
            if text_to_embed:
                text_to_embed += "\n"
            text_to_embed += prefix + row[column]

        delimiter = None
        if strategy_settings:
            delimiter = get_paragraph_delimiter(*strategy_settings)
        if delimiter:
            text_to_embed = text_to_embed.replace(delimiter, "")

        return text_to_embed

    errors = []
    for column in columns_to_embed:
        if column not in df.columns:
            errors.append(
                EmbeddingError(f"Column {column} not found in documents dataframe")
            )

    if errors:
        raise ExceptionGroup("Errors in creating text to embed", errors)  # noqa: F821

    df["text_to_embed"] = df.apply(create_text_to_embed, axis=1)

    return df


def create_embeddings(
    df: pd.DataFrame,
    embedder_type: EmbedderType,
    model: Deployment,
    embedding_column: str | None = "embedding",
    **kwargs,
) -> pd.DataFrame:
    """Embeds the text_to_embed field in articles_df_to_embed dataframe.

    Args:
        df (DataFrame): a pandas dataframe without embeddings
        embedder_type (EmbedderType): the type of embedder
        model (Deployment): the deployment model
        embedding_column (str, Optional): the column name for embeddings in the dataframe. Defaults
            to "embedding".
        **kwargs: additional keyword arguments

    Returns:
        DataFrame: a pandas dataframe with embeddings

    """
    return asyncio.run(
        acreate_embeddings(df, embedder_type, model, embedding_column, **kwargs)
    )


async def acreate_embeddings(
    df: pd.DataFrame,
    embedder_type: EmbedderType,
    model: Deployment,
    embedding_column: str | None = "embedding",
    **kwargs,
) -> pd.DataFrame:
    """Asynchronously embeds the text_to_embed field in articles_df_to_embed dataframe.

    Args:
        df (DataFrame): a pandas dataframe without embeddings
        embedder_type (EmbedderType): the type of embedder
        model (Deployment): the deployment model
        embedding_column (str, Optional): the column name for embeddings in the dataframe. Defaults
            to "embedding".
        **kwargs: additional keyword arguments

    Returns:
        DataFrame: a pandas dataframe with embeddings

    """
    if df.empty:
        raise ValueError("Input DataFrame is empty")
    if embedding_column is None:
        embedding_column = "embedding"

    embedder = get_embedder(embedder_type)

    # if embedding is already present, remove defaults and filter to None
    if embedding_column in df.columns:
        df[embedding_column] = df[embedding_column].apply(
            lambda x: None if isinstance(x, list) and all(v == 0.0 for v in x) else x
        )
        filtered_df = df[df[embedding_column].isnull()]
        remainder_df = df[df[embedding_column].notnull()]
    else:
        filtered_df = df
        remainder_df = pd.DataFrame(columns=df.columns)

    if filtered_df.empty:
        logger.info("No records to embed.")
        return df
    # create unique list of text to embed
    text_to_embed = filtered_df["text_to_embed"].unique().tolist()

    embeddings = await embedder.embed_documents(
        model,
        text_to_embed,
        **kwargs,
    )

    # create a dictionary of text to embed and embeddings
    text_to_embed_dict = dict(zip(text_to_embed, embeddings))

    # map embeddings to text_to_embed
    filtered_df[embedding_column] = filtered_df["text_to_embed"].map(
        pd.Series(text_to_embed_dict)
    )

    output_df = pd.concat([remainder_df, filtered_df], ignore_index=True)
    output_df.reset_index(drop=True, inplace=True)
    return output_df


def create_embedding_file_content(
    df: pd.DataFrame, api: type[BaseModel]
) -> tuple[list[dict[str, Any]], list[ValidationError]]:
    """Transform a DataFrame into a list of JSON strings using a specified API model.

    Args:
        df (pd.DataFrame): The DataFrame containing the data to be transformed.
        api (type[BaseModel]): The API model class used for casting rows.

    Returns:
        tuple[list[dict[str, Any]], list[ValidationError]]: A tuple containing a list of dictionaries
            representing the transformed rows and a list of validation errors encountered during
            the transformation.

    """
    errors = []
    transformed = []

    for index, row in df.iterrows():
        try:
            transformed.append(cast_to(row, api))
        except ValidationError as exc:
            logger.error("Error casting row %s to API %s: %s", index, api.__name__, exc)
            errors.append(exc)
            continue

    return [json.loads(row.model_dump_json()) for row in transformed], errors
