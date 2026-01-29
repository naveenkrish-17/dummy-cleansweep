"""Module for custom exceptions used in the app."""

__all__ = [
    "PipelineError",
    "EmbeddingError",
    "TranslationError",
    "MetadataGenerationError",
    "UnsupportedEmbedderError",
    "APIRequestError",
    "DataQualityError",
]


class PipelineError(Exception):
    """The PipelineError class is a custom exception for pipeline errors."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)

    def __str__(self):
        """Return the message."""
        return self.message


class EmbeddingError(PipelineError):
    """Embedding error."""


class TranslationError(PipelineError):
    """Translation error."""


class MetadataGenerationError(PipelineError):
    """Metadata generation error."""


class UnsupportedEmbedderError(PipelineError):
    """Unsupported embedder error."""


class APIRequestError(Exception):
    """An error occurred while processing an API request."""


class DataQualityError(PipelineError):
    """Custom exception for data quality errors."""


class DeploymentError(PipelineError):
    """An error occurred while processing a deployment."""


class PromptError(PipelineError):
    """Base exception for prompt errors."""


class ChatError(PipelineError):
    """Base exception for chat errors."""
