"""Simple settings for the input files to be processed by the pipeline."""

import logging
from enum import Enum
from typing import Any, Literal, Optional, TypeAlias

from pydantic import BaseModel, ValidationInfo, field_validator
from pydantic_settings import SettingsConfigDict

from cleansweep.model.network import CloudStorageUrl, FileUrl, PathLikeUrl
from cleansweep.settings._helpers import set_input_file_from_latest_blob
from cleansweep.settings._types import SourceObject
from cleansweep.settings.app import AppSettings

logger = logging.getLogger(__name__)

MergeHow: TypeAlias = Literal["left", "right", "inner", "outer", "cross"]


class MergeSettings(AppSettings, validate_assignment=True):
    """The application settings."""

    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )

    left_source: Optional[SourceObject] = None
    left_columns: Optional[list[str]] = None
    left_on: Optional[list[str] | str] = None

    right_source: Optional[SourceObject] = None
    right_columns: Optional[list[str]] = None
    right_on: Optional[list[str] | str] = None

    left_file_uri: Optional[CloudStorageUrl | FileUrl] = None
    right_file_uri: Optional[CloudStorageUrl | FileUrl] = None

    how: MergeHow = "left"
    on: list[str] | str = "id"

    @field_validator("left_file_uri", "right_file_uri", mode="after")
    @classmethod
    def convert_uri(
        cls, value: Any, info: ValidationInfo  # pylint: disable=unused-argument
    ):
        """Convert the value to a PathLikeUrl object.

        Args:
        ----
            value (Any): The value to convert.
            info (ValidationInfo): The validation information.

        Returns:
        -------
            PathLikeUrl: The value as a PathLikeUrl object.

        """
        if isinstance(value, PathLikeUrl) or value is None:
            return value

        return PathLikeUrl(str(value))

    def initialize_input_files(self):
        """Initialize the input file URIs for the left and right sources.

        This method sets the `left_file_uri` and `right_file_uri` attributes by fetching
        the latest blob from the respective sources if they are not already set.

        Raises:
            AssertionError: If `left_file_uri` or `right_file_uri` is not set after initialization.

        """
        if self.left_file_uri is None and self.left_source is not None:
            self.left_file_uri = set_input_file_from_latest_blob(self, self.left_source)

        if self.right_file_uri is None and self.right_source is not None:
            self.right_file_uri = set_input_file_from_latest_blob(
                self, self.right_source
            )

        assert self.left_file_uri is not None, "left_file_uri should be set"
        assert self.right_file_uri is not None, "right_file_uri should be set"

    def validate_source_bucket(self):
        """Validate and assign bucket values for the source, left_source, and right_source attributes.

        This method ensures that the `bucket` attribute for `source`, `left_source`, and `right_source`
        is properly set. If any of these attributes are `None` or their `bucket` attribute is `None`,
        they are assigned a default value based on the `staging_bucket`.

        Returns:
            self: The instance of the class, with updated bucket values for the source attributes.

        """
        super().validate_source_bucket()

        if self.left_source is None:
            self.left_source = self.source

        if self.left_source.bucket is None:
            self.left_source.bucket = self.source.bucket

        if self.right_source is None:
            self.right_source = self.source

        if self.right_source.bucket is None:
            self.right_source.bucket = self.source.bucket

        return self

    def load(self, **kwargs) -> None:
        """Load the merge settings.

        Args:
            kwargs (dict): Keyword arguments containing the merge settings.

        """
        super().load(**kwargs)

        chunk_settings = kwargs.get("steps", kwargs).get("merge", {})

        for key, value in chunk_settings.items():

            if not hasattr(self, key):
                continue

            if isinstance(getattr(self, key), BaseModel):
                setattr(self, key, type(getattr(self, key)).model_validate(value))
            elif isinstance(getattr(self, key), Enum):
                setattr(self, key, type(getattr(self, key))(value))
            else:
                setattr(self, key, value)

        # validate the source buckets
        self.validate_source_bucket()
