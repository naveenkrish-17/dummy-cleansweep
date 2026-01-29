"""Simple settings for the input files to be processed by the pipeline."""

import logging
from typing import Any, Optional

from pydantic import ValidationInfo, field_validator
from pydantic_settings import SettingsConfigDict

from cleansweep.model.network import CloudStorageUrl, FileUrl, PathLikeUrl
from cleansweep.settings._helpers import set_input_file_from_latest_blob
from cleansweep.settings._types import SourceObject
from cleansweep.settings.base import SettingsBase
from cleansweep.utils.slack import send_notification

logger = logging.getLogger(__name__)


class InputFiles(SettingsBase, arbitrary_types_allowed=True):
    """The application settings."""

    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )

    # app settings which are set by caller and not by the config
    input_file_uri: Optional[CloudStorageUrl | FileUrl] = None
    """The input file Url to be processed by the pipeline"""
    config_file_uri: Optional[CloudStorageUrl | FileUrl] = None
    """The config file Url for the pipeline"""

    source: SourceObject = SourceObject()

    run_id: Optional[str] = None

    use_run_id: bool = True

    @property
    def staging_bucket(self) -> str:
        """The name of the staging bucket."""
        if self.source.bucket is not None:
            return self.source.bucket
        raise ValueError("Staging bucket is not set")

    @field_validator("input_file_uri", "config_file_uri")
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

    @property
    def input_file(self) -> CloudStorageUrl | FileUrl:
        """Retrieve the input file URI.

        Returns:
            CloudStorageUrl | FileUrl: The URI of the input file.

        Raises:
            ValueError: If the input file URI is not set.

        """
        if self.input_file_uri is None:
            raise ValueError("Input file URI is not set")

        return self.input_file_uri

    def initialize_input_file_uri(self, source: Optional[SourceObject] = None):
        """Initialize the input file URI.

        If no input file URI is set and a bucket is specified, the function searches for the latest
        file in the bucket that matches the glob pattern. If no files are found in the specified
        bucket the function:

        * Sends a notification to the default channel.
        * Exits the program with status code 0.

        Args:
            source (SourceObject): The source object containing the bucket and glob pattern.

        Raises:
            FileNotFoundError: If no files matching the glob pattern are found in the bucket.

        """
        if source is None:
            source = self.source

        if self.input_file_uri is None and source.bucket is not None:

            try:
                self.input_file_uri = set_input_file_from_latest_blob(self, source)
            except FileNotFoundError:
                self.input_file_uri = None

        if self.input_file_uri is None:
            logger.warning("No files found in source bucket")
            send_notification(
                self.default_channel,
                f"{self.name}: {self.app} - No files found in source bucket!!",
            )
            exit(0)
