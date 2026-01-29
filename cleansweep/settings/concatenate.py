"""Simple settings for the input files to be processed by the pipeline."""

import logging
from enum import Enum
from typing import List, Literal, TypeAlias

from pydantic import BaseModel, PrivateAttr, computed_field
from pydantic_settings import SettingsConfigDict

from cleansweep.model.network import CloudStorageUrl, FileUrl
from cleansweep.settings._helpers import set_input_file_from_latest_blob
from cleansweep.settings._types import SourceObject
from cleansweep.settings.app import AppSettings

logger = logging.getLogger(__name__)

MergeHow: TypeAlias = Literal["left", "right", "inner", "outer", "cross"]


class ConcatenateSettings(AppSettings, validate_assignment=True):
    """The application settings."""

    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )

    files: List[SourceObject] = []
    _file_uris: List[CloudStorageUrl | FileUrl] = PrivateAttr([])

    @computed_field
    @property
    def file_uris(self) -> List[CloudStorageUrl | FileUrl]:
        """Return the list of file URIs."""
        if not self._file_uris:
            for source in self.files:

                if source is None:
                    source = self.source
                elif source.bucket is None:
                    source.bucket = self.staging_bucket

                self._file_uris.append(set_input_file_from_latest_blob(self, source))
        return self._file_uris

    def load(self, **kwargs) -> None:
        """Load the concatenate settings.

        Args:
            kwargs (dict): Keyword arguments containing the concatenate settings.

        """
        super().load(**kwargs)

        _settings = kwargs.get("steps", kwargs).get("concatenate", {})

        for key, value in _settings.items():

            if not hasattr(self, key):
                continue

            if isinstance(getattr(self, key), BaseModel):
                setattr(self, key, type(getattr(self, key)).model_validate(value))
            elif isinstance(getattr(self, key), Enum):
                setattr(self, key, type(getattr(self, key))(value))
            else:
                setattr(self, key, value)
