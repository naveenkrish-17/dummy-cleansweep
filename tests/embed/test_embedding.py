"""Test Embeddings"""

import pandas as pd
import pytest

from cleansweep.embed.embedding import (
    create_df_to_embed,
    create_embeddings,
    get_columns_to_embed,
)
from cleansweep.embed.utils import add_root_document_to_df
from cleansweep.settings.embedding import EmbeddingSettings
from cleansweep.settings.load import load_settings

settings = load_settings(EmbeddingSettings)


class TestCreateDocumentsDfToEmbed:
    """Test suite for the create_df_to_embed function."""

    @pytest.fixture
    def input_df(self):
        """Fixture that creates a DataFrame with a column 'text_to_embed'."""
        data = {
            "chunk_id": [1, 2],
            "title": ["Title 1", "Title 2"],
            "metadata_url": ["http://test.com/test", "https://test.com/2"],
            "metadata_tags": ["Tag 1", "Tag 2"],
            "metadata_description": ["Description 1", "Description 2"],
            "chunk": ["Chunk 1", "Chunk 2"],
        }

        return pd.DataFrame(data)

    def test_create_df_to_embed(self, input_df):
        """Test that the create_df_to_embed function returns a DataFrame when given valid
        input.
        """
        output_df = create_df_to_embed(input_df, ["chunk"])

        # Check that the output DataFrame has the expected columns.
        assert isinstance(output_df, pd.DataFrame), "Output is not a DataFrame"
        assert (
            "text_to_embed" in output_df.columns
        ), "Output DataFrame does not contain 'text_to_embed' column"

    def test_create_documenbts_df_to_embed_invalid_extra_columns(self, input_df):
        """Test that the create_df_to_embed function raises a KeyError when the input
        DataFrame contains columns that are not in the settings.embed.extra_metadata_columns list.
        """

        with pytest.raises(ExceptionGroup):
            create_df_to_embed(input_df, get_columns_to_embed(["chunk"], ["invalid"]))


class TestCreateEmbeddings:
    """Test suite for the create_embeddings function."""

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

    @pytest.fixture
    def documents_df_to_embed(self, documents):
        """Fixture that creates a DataFrame with a 'text_to_embed' column."""
        output_df = create_df_to_embed(documents, ["chunk"])
        return output_df

    def test_create_embeddings(self, documents_df_to_embed, mocker):
        """Test that the create_embeddings function returns a DataFrame when given valid input."""

        mock_embedder = mocker.AsyncMock()
        mocker.patch(
            "cleansweep.embed.embedding.get_embedder", return_value=mock_embedder
        )

        embeddings = create_embeddings(
            documents_df_to_embed, settings.embedder_type, settings.model
        )

        assert isinstance(embeddings, pd.DataFrame), "Output is not a DataFrame"
        assert (
            "text_to_embed" in embeddings.columns
        ), "Output DataFrame does not contain 'text_to_embed' column"
        assert (
            "embedding" in embeddings.columns
        ), "Output DataFrame does not contain 'embedding' column"
        assert (
            embeddings.shape[0] == documents_df_to_embed.shape[0]
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

    def test_create_embeddings_with_empty_df(self, empty_df):
        """Test that the create_embeddings function raises a ValueError when given an empty
        DataFrame.
        """
        with pytest.raises(ValueError, match="Input DataFrame is empty"):
            create_embeddings(empty_df, settings.embedder_type, settings.model)

    def test_create_embeddings_with_missing_column(self, df_missing_column):
        """Test that the create_embeddings function raises a KeyError when the input DataFrame does
        not have a 'text_to_embed' column.
        """
        with pytest.raises(KeyError, match="'text_to_embed'"):
            create_embeddings(df_missing_column, settings.embedder_type, settings.model)


class TestAddRootDocumentToDF:
    """Test suite for the add_root_document_to_df function."""

    def test_add_root_document_to_df(self):
        """Test that the add_root_document_to_df function returns a DataFrame with the root document
        columns when given valid input.
        """
        data = {
            "id": ["1", "2"],
            "metadata_root_document_id": ["2", None],
            "title": ["Title 1", "Title 2"],
            "metadata_description": ["Description 1", "Description 2"],
            "metadata_url": ["http://test.com/test", "https://test.com/2"],
            "content_type": [1, 1],
            "content": ["Content 1", "Content 2"],
        }
        docs = pd.DataFrame(data)
        output_df = add_root_document_to_df(docs)

        # Check that the output DataFrame has the expected columns.
        assert isinstance(output_df, pd.DataFrame), "Output is not a DataFrame"
        assert (
            "metadata_root_document_title" in output_df.columns
        ), "Output DataFrame does not contain 'root_document_title' column"
        assert (
            "metadata_root_document_description" in output_df.columns
        ), "Output DataFrame does not contain 'root_document_metadata_description' column"
        assert (
            "metadata_root_document_url" in output_df.columns
        ), "Output DataFrame does not contain 'root_document_metadata_url' column"
        assert (
            "metadata_root_document_content_type" in output_df.columns
        ), "Output DataFrame does not contain 'root_document_content_type' column"
        assert (
            "metadata_root_document_id" in output_df.columns
        ), "Output DataFrame does not contain 'root_document_id' column"
        assert (
            output_df.shape[0] == docs.shape[0]
        ), "Number of rows in input and output DataFrames do not match"
        assert (
            output_df.shape[1] == docs.shape[1] + 4
        ), "Number of columns in input and output DataFrames do not match"
        assert output_df["metadata_root_document_id"].equals(
            docs["metadata_root_document_id"]
        ), "root_document_id column values do not match"
        assert list(output_df["metadata_root_document_title"].dropna()) == [
            "Title 2",
        ], "root_document_title column values do not match"


class TestGetColumnsToEmbed:
    """Test suite for the get_columns_to_embed function."""

    scenarios = [
        pytest.param([], ["chunk"], id="no extra columns"),
        pytest.param(None, ["chunk"], id="none extra columns"),
        pytest.param(
            ["metadata_url"],
            ["metadata_url", "chunk"],
            id="extra column, no chunk",
        ),
        pytest.param(
            ["chunk", "metadata_url", "title"],
            ["chunk", "metadata_url", "title"],
            id="extra column with title and chunk",
        ),
        pytest.param(
            ["chunk"],
            ["chunk"],
            id="extra column with chunk",
        ),
    ]

    @pytest.mark.parametrize("extra_columns_to_embed, expected_columns", scenarios)
    def test_func(self, extra_columns_to_embed, expected_columns):
        assert (
            get_columns_to_embed(["chunk"], extra_columns_to_embed) == expected_columns
        )
