"""The embedder module contains the Embedder class and its concrete implementations."""

import logging
from abc import ABC, abstractmethod

from openai import NOT_GIVEN

from cleansweep._types import Deployment
from cleansweep.enumerations import EmbedderType
from cleansweep.exceptions import UnsupportedEmbedderError
from cleansweep.settings.base import settings
from cleansweep.utils.azure.api import process_api_calls
from cleansweep.utils.azure.auth import AzureCredentials
from cleansweep.utils.azure.client import get_open_ai_client_async
from cleansweep.utils.azure.utils import batch_texts

logger = logging.getLogger(__name__)
"""module level logger"""


class Embedder(ABC):
    """The Embedder class is an abstract class that defines the interface for an embedder."""

    @abstractmethod
    async def embed_documents(
        self, model: Deployment, documents: list[str], **kwargs
    ) -> list[list[float]]:
        """Abstract method to embed the documents."""


class OpenAIEmbedder(Embedder):
    """The OpenAIEmbedder class is a concrete class that implements the Embedder interface."""

    async def _get_embedding(
        self, model: str, documents: list[str], **kwargs
    ) -> list[list[float]]:
        """Get the embedding.

        Args:
            model (str): The model name.
            documents (list[str]): The documents to embed.
            **kwargs: Additional keyword arguments.

        Returns:
            list[float]: The embedding of the text.

        """
        credentials = kwargs.get("credentials", None)
        if credentials is None:
            credentials = AzureCredentials()
        dimensions = kwargs.get("dimensions", NOT_GIVEN)
        extra_body = kwargs.get("extra_body", None)
        extra_headers = kwargs.get("extra_headers", None)
        extra_query = kwargs.get("extra_query", None)
        timeout = kwargs.get("timeout", NOT_GIVEN)
        client = get_open_ai_client_async(max_retries=0, credentials=credentials)

        result = await client.embeddings.create(
            input=documents,
            model=model,
            encoding_format="float",
            dimensions=dimensions,
            extra_body=extra_body,
            extra_headers=extra_headers,
            extra_query=extra_query,
            timeout=timeout,
        )

        return [r.embedding for r in result.data]

    async def embed_documents(
        self, model: Deployment, documents: list[str], **kwargs
    ) -> list[list[float]]:
        """Embed the documents.

        Args:
            model (str): The model name.
            documents (list[str]): The documents to embed.
            **kwargs: Additional keyword arguments.

        Returns:
            list[list[float]]: The embeddings of the documents.

        """
        # set dimension value, use model value if available
        # then check settings. If not set, default to 1
        dimensions = 1
        if model.dimensions:
            dimensions = model.dimensions

        if "dimensions" in kwargs:
            dimensions = kwargs["dimensions"]
            if model.dimensions and dimensions > model.dimensions:
                raise ValueError(f"Invalid dimensions for the {model} model")

        default = [0.0] * dimensions

        # check if model accepts dimensions
        if model.accepts_dimensions is False and "dimensions" in kwargs:
            logger.warning(
                "Model %s does not accept dimensions, ignoring dimensions argument",
                model.name,
            )
            del kwargs["dimensions"]

        token_limit = kwargs.get("token_limit")
        if token_limit is None:
            token_limit = 8000

        if token_limit == 0:
            document_chunks = [[doc] for doc in documents]
        else:
            document_chunks = batch_texts(documents, token_limit, model.model)

        logger.debug(
            "Embedding %d documents in %d chunks", len(documents), len(document_chunks)
        )

        results = await process_api_calls(
            self._get_embedding,
            document_chunks,
            "embed",
            model,
            timeout=settings.timeouts.embed,
            **kwargs,
        )
        output = []
        for i, result in enumerate(results):
            if result:
                output.extend(result)
            else:
                output.extend([default] * len(document_chunks[i]))

        return output


SUPPORTED_EMBEDDERS = {
    EmbedderType.OPENAI: OpenAIEmbedder,
}
"""A dictionary of supported embedders, mapping the embedder type to the embedder class."""


def get_embedder(embedder_type: str | EmbedderType) -> OpenAIEmbedder:
    """Get the embedder.

    Args:
        embedder_type (Union[str, EmbedderType]): The embedder type.

    Returns:
        Embedder: The embedder.

    """
    if isinstance(embedder_type, str):
        embedder_type = EmbedderType(embedder_type)

    embedder = SUPPORTED_EMBEDDERS.get(embedder_type)

    if embedder is None:
        raise UnsupportedEmbedderError(f"Unsupported embedder type: {embedder_type}")

    return embedder()
