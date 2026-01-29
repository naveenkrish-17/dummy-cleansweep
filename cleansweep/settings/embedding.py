"""Settings for embedding."""

from enum import Enum
from typing import Any

from pydantic import BaseModel, ValidationInfo, field_validator

from cleansweep.deployments.deployments import DEPLOYMENTS
from cleansweep.enumerations import EmbedderType
from cleansweep.settings._types import SourceObject
from cleansweep.settings.app import AppSettings, Model


class EmbeddingSettings(AppSettings, extra="allow", arbitrary_types_allowed=True):
    """Settings for embedding."""

    model: Model = DEPLOYMENTS.get_by_model(
        "text-embedding-ada-002"
    )  # pyright: ignore[reportAssignmentType]
    embedder_type: EmbedderType = EmbedderType.OPENAI
    """The provider of the embedding model"""
    dimensions: int = 1536
    """The number of dimensions in the embedding model"""
    token_limit: int = 4000
    """Used to limit the number of tokens in the text to embed when calling the OpenAI API"""
    columns_to_embed: list[str] = []
    """Columns to embed in the text_to_embed field, overwrites base list"""
    extra_columns_to_embed: list[str] = []
    """Additional columns to embed in the text_to_embed field, base list is appended to the end of
    this list"""
    extra_metadata_columns: list[str] = []
    """Additional columns to include in the metadata field"""
    force: bool = False
    """Force embedding of all documents - useful for re-embedding all documents"""
    max_document_length: int = 8000

    source: SourceObject = SourceObject(directory="metadata", extension="avro")

    @field_validator("dimensions", mode="before")
    @classmethod
    def validate_dimensions(
        cls, value: Any, info: ValidationInfo  # pylint: disable=unused-argument
    ) -> Any:
        """Validate the number of dimensions.

        Args:
            value (Any): The value to validate.
            info (ValidationInfo): The validation information.

        Returns:
            int: The number of dimensions.

        """
        if isinstance(value, int) or value is None:
            return value

        if isinstance(value, str) and value.isdigit():
            return int(value)

        return value

    def load(self, **kwargs) -> None:
        """Load the custom settings.

        Args:
            **kwargs: Keyword arguments containing the chunk settings.

        """
        super().load(**kwargs)

        _settings = kwargs.get("steps", kwargs).get("embed", {})

        for key, value in _settings.items():

            if not hasattr(self, key):
                continue

            if key == "model":
                _value = DEPLOYMENTS.get_by_model(value)
                if _value is None:
                    _value = DEPLOYMENTS.get_by_deployment_name(value)
                setattr(self, key, _value)
            elif isinstance(getattr(self, key), BaseModel):
                setattr(self, key, type(getattr(self, key)).model_validate(value))
            elif isinstance(getattr(self, key), Enum):
                setattr(self, key, type(getattr(self, key))(value))
            else:
                setattr(self, key, value)

        # validate the source bucket
        self.validate_source_bucket()
