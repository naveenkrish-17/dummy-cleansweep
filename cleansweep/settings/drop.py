"""Simple settings for the input files to be processed by the pipeline."""

import logging
from enum import Enum
from typing import Literal, TypeAlias

from pydantic import BaseModel
from pydantic_settings import SettingsConfigDict

from cleansweep.settings._types import SourceObject
from cleansweep.settings.app import AppSettings

logger = logging.getLogger(__name__)

MergeHow: TypeAlias = Literal["left", "right", "inner", "outer", "cross"]


class DropSettings(AppSettings, validate_assignment=True):
    """The application settings."""

    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )

    source: SourceObject = SourceObject(extension="avro")

    columns: list[str]

    def load(self, **kwargs) -> None:
        """Load the settings.

        Args:
            kwargs (dict): Keyword arguments containing the settings.

        """
        super().load(**kwargs)

        _settings = kwargs.get("steps", kwargs).get("drop", {})

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
