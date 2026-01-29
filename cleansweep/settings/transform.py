"""Settings for schema transformation."""

from enum import Enum
from typing import Any, Literal, Optional

from pydantic import BaseModel, ValidationInfo, field_validator

from cleansweep.model.network import CloudStorageUrl, FileUrl, PathLikeUrl
from cleansweep.settings._types import SourceObject
from cleansweep.settings.app import AppSettings, override_base_validation_context


class TransformSettings(AppSettings, arbitrary_types_allowed=True):
    """Settings for schema transformation."""

    mapping: Optional[CloudStorageUrl | FileUrl] = None
    """Url for the mapping file"""
    chunk_size: int = 1000
    source_path: Optional[str] = None
    """A JSONPath string to the property within the source document which contains the documents"""

    content_column: Literal["content_full", "content_raw", "html_content"] = (
        "content_full"
    )
    """The column in the source data that contains the content to transform"""

    source: SourceObject = SourceObject(
        directory="landing", extension="ndjson", use_run_id=False
    )

    use_run_id: bool = False

    @field_validator("mapping", mode="before")
    @classmethod
    def convert_uri(
        cls, value: Any, info: ValidationInfo  # pylint: disable=unused-argument
    ):
        """Convert the value to a PathLikeUrl object.

        Args:
            value (Any): The value to convert.
            info (ValidationInfo): The validation information.

        Returns:
            PathLikeUrl: The value as a PathLikeUrl object.

        """
        if isinstance(value, PathLikeUrl) or value is None:
            return value

        if not value.startswith("gs://") and not value.startswith("file://"):
            value = f"gs://skygenai-uk-utl-kosmo-composer-ir-{info.data["env_name"]}/cleansweep/mapping/{value}"

        return PathLikeUrl(str(value))

    def load(self, **kwargs) -> None:
        """Load the transform settings.

        Args:
            kwargs (dict): Keyword arguments containing the transform settings.

        """
        # set this token to stop AppSettings from running custom validation
        token = override_base_validation_context.set(True)
        try:
            super().load(**kwargs)
        finally:
            override_base_validation_context.reset(token)

        _settings = kwargs.get("steps", kwargs).get("transform", {})

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

    def validate_source_bucket(self):
        """Set the source bucket for the application.

        If the `source.bucket` attribute is not already set, this method assigns it
        to the value of `source_bucket` if provided, or defaults to the application's
        `staging_bucket`.

        Returns:
            self: The instance of the class, allowing for method chaining.

        """
        if self.source.bucket is None:
            self.source.bucket = self.landing_bucket
        return self
