"""Utility functions and protocols for processing results obtained from API calls."""

import logging

import pandas as pd

from cleansweep._types import ClusterDefinition, Deployment
from cleansweep.embed.embedding import acreate_embeddings, create_df_to_embed
from cleansweep.enumerations import EmbedderType

logger = logging.getLogger(__name__)


async def embed_dataframe(
    df: pd.DataFrame,
    definition: ClusterDefinition,
    embedder_type: EmbedderType,
    model: Deployment,
    token_limit: int | None = None,
) -> pd.DataFrame:
    """Embed the columns in the DataFrame based on the given ClusterDefinition.

    Args:
        df (pd.DataFrame): The input DataFrame.
        definition (ClusterDefinition): The ClusterDefinition object containing the cluster
            information.
        embedder_type (EmbedderType): The type of embedder to use for embeddings.
        model (Deployment): The deployment model to use for embeddings.
        token_limit (int | None, optional): The token limit for embeddings. Defaults to None.

    Returns:
        pd.DataFrame: The modified DataFrame with the embedded columns added.

    """
    if df.empty:
        raise ValueError("Input DataFrame is empty")

    logger.debug("Embedding columns: %s", definition.columns_to_embed)
    df = create_df_to_embed(df, definition.columns_to_embed)

    embedded_df = await acreate_embeddings(
        df,
        embedder_type,
        model,
        embedding_column=definition.cluster_name,
        token_limit=token_limit,
    )
    return embedded_df
