from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from cleansweep._types import ClusterDefinition, Deployment
from cleansweep.chunk.semantic._utils import embed_dataframe
from cleansweep.enumerations import EmbedderType
from cleansweep.exceptions import EmbeddingError
from cleansweep.settings.chunk import ChunkSettings
from cleansweep.settings.load import load_settings

settings = load_settings(ChunkSettings)


@pytest.mark.asyncio
class TestEmbedDataFrame:
    """Test suite for the embed_dataframe function"""

    @pytest.fixture
    def documents(self):
        """Fixture that creates a DataFrame"""
        # Assuming documents is a DataFrame with a column 'text_to_embed'
        data = {  # pylint: disable=redefined-outer-name
            "chunk_id": [1, 2],
            "title": ["Title 1", "Title 2"],
            "metadata_url": ["http://test.com/test", "https://test.com/2"],
            "metadata_tags": ["Tag 1", "Tag 2"],
            "metadata_description": ["Description 1", "Description 2"],
            "chunk": ["Chunk 1", "Chunk 2"],
        }
        return pd.DataFrame(data)

    @pytest.mark.asyncio
    async def test_embed_dataframe(self, documents, mocker):
        """Test that the create_embeddings function returns a DataFrame when given valid input."""

        embedded_df = documents.copy()
        embedded_df["cluster_chunk"] = [1, 2]

        mocker.patch(
            "cleansweep.chunk.semantic._utils.acreate_embeddings",
            return_value=embedded_df,
        )

        embeddings = await embed_dataframe(
            documents,
            ClusterDefinition(columns_to_embed=["chunk"]),
            settings.semantic.embedder_type,
            settings.semantic.embedding_model,
        )

        assert isinstance(embeddings, pd.DataFrame), "Output is not a DataFrame"
        assert (
            "cluster_chunk" in embeddings.columns
        ), "Output DataFrame does not contain 'cluster_chunk' column"
        assert (
            embeddings.shape[0] == documents.shape[0]
        ), "Number of rows in input and output DataFrames do not match"

    @pytest.fixture
    def empty_df(self):
        """Fixture that creates an empty DataFrame."""
        return pd.DataFrame()

    @pytest.fixture
    def df_missing_column(self):
        """Fixture that creates a DataFrame with a column that is not 'text_to_embed'."""
        data = {
            "some_other_column": ["text1", "text2", "text3"],
        }
        return pd.DataFrame(data)

    @pytest.mark.asyncio
    async def test_create_embeddings_with_empty_df(self, empty_df):
        """Test that the create_embeddings function raises a ValueError when given an empty
        DataFrame.
        """
        with pytest.raises(ValueError, match="Input DataFrame is empty"):
            await embed_dataframe(
                empty_df,
                ClusterDefinition(columns_to_embed=[""]),
                settings.semantic.embedder_type,
                settings.semantic.embedding_model,
            )

    @pytest.mark.asyncio
    async def test_create_embeddings_with_missing_column(self, df_missing_column):
        """Test that the create_embeddings function raises a KeyError when the input DataFrame does
        not have a 'text_to_embed' column.
        """
        with pytest.raises(ExceptionGroup) as exc_info:
            await embed_dataframe(
                df_missing_column,
                ClusterDefinition(columns_to_embed=[""]),
                settings.semantic.embedder_type,
                settings.semantic.embedding_model,
            )

        assert exc_info.value.exceptions[0].__class__ == EmbeddingError
