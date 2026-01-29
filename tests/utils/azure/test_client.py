"""Test suite for the Azure utilities client module."""

import pytest

from cleansweep import settings
from cleansweep.utils.azure.client import (
    AsyncAzureOpenAI,
    AzureOpenAI,
    get_open_ai_client,
    get_open_ai_client_async,
)
from conftest import MockAzureCredentials


class TestGetOpenAIClient:
    """Test suite for the get_open_ai_client function."""

    @pytest.mark.order(2)
    def test_func(self, mocker):
        """Test the get_open_ai_client function."""
        client = get_open_ai_client(MockAzureCredentials())
        assert isinstance(client, AzureOpenAI)

    @pytest.mark.order(3)
    def test_cache(self, mocker):
        """Test that the get_open_ai_client function caches the client."""

        client1 = get_open_ai_client(MockAzureCredentials())
        client2 = get_open_ai_client(MockAzureCredentials())

        assert client1 is client2


class TestGetOpenAIClientAsync:
    """Test suite for the get_open_ai_client_async function."""

    @pytest.mark.order(2)
    @pytest.mark.asyncio
    def test_func(self, mocker):
        """Test the get_open_ai_client function."""
        client = get_open_ai_client_async(MockAzureCredentials())
        assert isinstance(client, AsyncAzureOpenAI)

    @pytest.mark.order(3)
    @pytest.mark.asyncio
    def test_cache(self, mocker):
        """Test that the get_open_ai_client function caches the client."""

        client1 = get_open_ai_client_async(MockAzureCredentials())
        client2 = get_open_ai_client_async(MockAzureCredentials())

        assert client1 is client2
