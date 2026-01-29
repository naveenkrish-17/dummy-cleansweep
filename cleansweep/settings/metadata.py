"""Metadata settings for generating metadata."""

from enum import Enum
from typing import Any

from pydantic import BaseModel, ValidationInfo, field_validator

from cleansweep._types import Prompt
from cleansweep.prompts import PROMPTS
from cleansweep.settings._types import SourceObject
from cleansweep.settings.app import AppSettings


class MetadataGenerationConfig(BaseModel):
    """Configuration for generating metadata."""

    prompt: Prompt
    """The prompt for the configuration"""
    output: str
    """The output column for the generated metadata"""

    @field_validator("prompt")
    @classmethod
    def get_prompt(
        cls, value: Prompt, info: ValidationInfo  # pylint: disable=unused-argument
    ) -> Prompt:
        """Get the prompt from the value provided in the config.

        If the prompt provided is a custom prompt return it, if it is a default prompt then get it
        from the PROMPTS dictionary.

        Args:
            value (Prompt): The prompt value.
            info (ValidationInfo): The validation information.

        Returns:
            Prompt: The prompt.

        """
        if value.name in PROMPTS:
            return PROMPTS[value.name]

        return value

    @field_validator("output")
    @classmethod
    def validate_output(
        cls, value: str, info: ValidationInfo  # pylint: disable=unused-argument
    ):
        """Validate the output column.

        Args:
            value (str): The output column.
            info (ValidationInfo): The validation information.

        Returns:
            str: The output column.

        """
        if value.startswith("metadata_"):
            return value

        return f"metadata_{value}"


class MetadataSettings(AppSettings, validate_assignment=True):
    """Settings for generating metadata following chunking."""

    token_limit: int = 3000
    """Used to limit the number of tokens in the text to embed when calling the OpenAI API"""

    configs: list[MetadataGenerationConfig] = []
    temperature: float = 0
    """The temperature for the Metadata Generation"""

    force: bool = False

    source: SourceObject = SourceObject(directory="chunked", extension="avro")

    @field_validator("configs", mode="before")
    @classmethod
    def convert_configs(
        cls, value: Any, info: ValidationInfo  # pylint: disable=unused-argument
    ):
        """Convert the value to a PathLikeUrl object.

        Args:
            value (Any): The value to convert.
            info (ValidationInfo): The validation information.

        Returns:
            PathLikeUrl: The value as a PathLikeUrl object.

        """
        if not value:
            return []

        output = []
        for config in value:
            if isinstance(config, MetadataGenerationConfig):
                output.append(config)
            else:
                output.append(MetadataGenerationConfig(**config))
        return output

    def load(self, **kwargs) -> None:
        """Load the custom settings.

        Args:
            **kwargs: Keyword arguments containing the chunk settings.

        """
        super().load(**kwargs)

        _settings = kwargs.get("steps", kwargs).get("metadata", {})

        for key, value in _settings.items():

            if not hasattr(self, key):
                continue

            if isinstance(getattr(self, key), BaseModel):
                setattr(self, key, type(getattr(self, key)).model_validate(value))
            elif isinstance(getattr(self, key), Enum):
                setattr(self, key, type(getattr(self, key))(value))
            else:
                setattr(self, key, value)

        # validate the source bucket
        self.validate_source_bucket()
