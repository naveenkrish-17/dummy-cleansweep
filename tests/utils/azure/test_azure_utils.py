"""Tests for the utils.azure.utils module."""

from cleansweep.utils.azure.utils import (
    batch_texts,
    min_chunk_documents,
    num_tokens_from_messages,
)


class TestChunkDocuments:
    """Test suite for the batch_texts function."""

    def test_chunk_documents(self):
        """Test the _chunk_documents method of the OpenAIEmbedder class."""
        documents = [
            "This is a test document.",
            "This is another document with more tokens.",
        ]
        result = batch_texts(documents, 10, "gpt-4")

        assert result == [
            ["This is a test document."],
            ["This is another document with more tokens."],
        ]


class TestMinChunkDocuments:
    """Test suite for the min_chunk_documents function."""

    def test_min_chunk_documents(self):
        """Test the _min_chunk_documents method of the OpenAIEmbedder class."""

        documents = [
            "This is a test document.",
            "This is another document with more tokens.",
        ]
        result = min_chunk_documents(documents, 6, "gpt-4")

        assert result == [
            ["This is a test document."],
            ["This is another document with more", "tokens."],
        ]


class TestNumTokensFromMessages:
    """Test suite for the num_tokens_from_messages function."""

    def test_num_tokens_from_messages(self):
        """Test the num_tokens_from_messages function."""
        messages = [
            {"role": "system", "content": "This is a system prompt"},
            {"role": "user", "content": "This is a user prompt"},
        ]
        result = num_tokens_from_messages(messages, "gpt-4")

        assert result == 22
