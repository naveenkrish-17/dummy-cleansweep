# pylint: disable=protected-access


import pytest
from asyncmock import AsyncMock

from cleansweep._types import Deployment
from cleansweep.embed.embedder import OpenAIEmbedder, get_embedder
from cleansweep.enumerations import EmbedderType

TEST_MODELS = {
    "test-model": Deployment(name="test-model", tpm=2, model="gpt-4o", dimensions=2),
    "text-embedding-ada-002": Deployment(
        name="text-embedding-ada-002",
        tpm=2,
        model="text-embedding-ada-002",
        dimensions=2,
    ),
}


class TestGetEmbedder:
    """Test suite for the get_embedder function."""

    def test_get_openai(self):
        """Test the get_embedder function with OpenAI."""
        embedder = get_embedder(EmbedderType.OPENAI)
        assert isinstance(embedder, OpenAIEmbedder)

    def test_get_embedder_error(self):
        """Test the get_embedder function with an invalid embedder type."""
        with pytest.raises(ValueError):
            get_embedder("invalid_embedder")


# region OpenAI


class TestOpenAIEmbedder:
    """Test suite for the OpenAIEmbedder class."""

    def test_init(self):
        """Test the initialization of the OpenAIEmbedder class."""
        embedder = OpenAIEmbedder()
        assert isinstance(embedder, OpenAIEmbedder)

    @pytest.mark.asyncio
    async def test_embed_documents(self, mocker):
        """Test the embed_documents method of the OpenAIEmbedder class."""

        embedder = OpenAIEmbedder()

        mock_api_call = mocker.MagicMock()
        mock_api_call = [[[0.79, -0.321]]]

        documents = ["This is a test document."]

        mocker.patch(
            "cleansweep.embed.embedder.process_api_calls",
            return_value=mock_api_call,
        )
        mock_chunk = mocker.patch(
            "cleansweep.embed.embedder.batch_texts",
            return_value=[documents],
        )

        result = await embedder.embed_documents(
            TEST_MODELS["text-embedding-ada-002"], documents
        )

        assert result == [[0.79, -0.321]]
        mock_chunk.assert_called_once_with(
            documents, 8000, TEST_MODELS["text-embedding-ada-002"].model
        )

    @pytest.mark.asyncio
    async def test_embed_documents_error(self, mocker):
        """Test the embed_documents method defaults when an empty results is provided."""

        embedder = OpenAIEmbedder()

        mock_api_call = mocker.MagicMock()
        mock_api_call = [[[0.79, -0.321]], []]

        documents = ["This is a test document.", "Another document."]

        mocker.patch(
            "cleansweep.embed.embedder.process_api_calls",
            return_value=mock_api_call,
        )
        mock_chunk = mocker.patch(
            "cleansweep.embed.embedder.batch_texts",
            return_value=[["This is a test document."], ["Another document."]],
        )

        result = embedder.embed_documents(
            TEST_MODELS["text-embedding-ada-002"], documents
        )

        assert await result == [[0.79, -0.321], [0.0, 0.0]]
        mock_chunk.assert_called_once_with(
            documents, 8000, TEST_MODELS["text-embedding-ada-002"].model
        )

    @pytest.mark.asyncio
    async def test_get_embedding(self, mocker):
        """Test the _get_embedding method of the OpenAIEmbedder class."""

        mock_response_data = mocker.MagicMock()
        mock_response_data.embedding = [[0.79, -0.321]]
        mock_response = mocker.MagicMock()
        mock_response.data = [mock_response_data]
        mock_client = mocker.MagicMock()
        mock_client.embeddings.create = AsyncMock(return_value=mock_response)

        mocker.patch(
            "cleansweep.embed.embedder.get_open_ai_client_async",
            return_value=mock_client,
        )

        embedder = OpenAIEmbedder()
        result = await embedder._get_embedding(
            "test-model", ["This is a test document."]
        )
        assert result == [[[0.79, -0.321]]]


# endregion
