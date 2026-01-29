"""Module app settings model."""

import contextvars
import re
from enum import Enum
from pathlib import Path
from typing import Any, Optional

from pydantic import (
    BaseModel,
    BeforeValidator,
    ValidationInfo,
    field_validator,
)
from pydantic_settings import SettingsConfigDict
from typing_extensions import Annotated

from cleansweep.deployments.deployments import DEPLOYMENTS
from cleansweep.enumerations import Classification, DocumentType, LoadType, ServiceLevel
from cleansweep.iso.languages import Language
from cleansweep.iso.regions import Country
from cleansweep.model.network import CloudStorageUrl, FileUrl, HttpUrl
from cleansweep.settings._types import Model, SourceObject
from cleansweep.settings._validators import plugin_validator
from cleansweep.settings.base import SettingsBase
from cleansweep.utils.bucket import BUCKET_PATTERN, get_bucket_description

override_base_validation_context = contextvars.ContextVar(
    "override_base_validation_context", default=False
)
"""Context variable to override custom validation in AppSettings.

Usage:
- Set the context variable to True before calling the load method.

"""


class OwnerSettings(BaseModel):
    """Settings for the owner of the document."""

    name: Optional[str] = None
    email: Optional[str] = None
    link: Optional[HttpUrl] = None


class MetadataSettings(BaseModel):
    """Settings for metadata extraction."""

    data_controller: str = "Sky"
    """Defines the nominated Data Controller"""
    data_territory: Optional[Country] = None
    """The data territory that the data is owned by from a legal and regulatory perspective"""
    data_owner: Optional[str] = None
    """The business data owner accountable within Sky for the data asset. Contains the owner's
    email
    """
    data_owner_name: Optional[str] = None
    """The name of the data owner."""
    includes_protected: bool = True
    """Specifies the object contains one or more data elements classified as 'Protected'"""
    includes_critical: bool = True
    """Specifies the object contains one or more data elements classified as 'Critical'"""
    owners: list[OwnerSettings] = []
    rop_number: Optional[str] = None
    """The RoP number covering the storage and processing of the asset"""


class AppSettings(SettingsBase, arbitrary_types_allowed=True):
    """The application settings."""

    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
        validate_assignment=True,
    )

    name: str = "cleansweep"
    description: str = "A tool for cleaning and transforming articles"
    classification: Classification = Classification.PROTECTED
    """Default classification for documents in the pipeline"""
    document_type: DocumentType = DocumentType.KNOWLEDGE
    language: Language = (
        Language.English  # pyright: ignore[reportAttributeAccessIssue] # pylint: disable=no-member
    )
    """Default language for documents in the pipeline"""
    service_level: ServiceLevel = ServiceLevel.BUSINESS_SUPPORT
    """Service level for documents in the pipeline"""
    load_type: LoadType = LoadType.FULL

    metadata: MetadataSettings = MetadataSettings()
    """Metadata settings for the pipeline"""

    tags: list[str] = []

    channel: str | None = None

    source: SourceObject = SourceObject()

    output: SourceObject = SourceObject(
        extension=".avro",
    )
    """The output source object where the files will be stored."""

    schedule: Optional[str] = None

    model: Model = DEPLOYMENTS.get_by_model(
        "gpt-4o"
    )  # pyright: ignore[reportAssignmentType]
    """The model to use for the pipeline"""

    temperature: float = 0
    """The temperature to be used when calling a model"""

    plugin: Annotated[
        Optional[CloudStorageUrl | FileUrl], BeforeValidator(plugin_validator)
    ] = None

    @field_validator("prompts_template_dir")
    @classmethod
    def validate_path(
        cls, value: Any, info: ValidationInfo  # pylint: disable=unused-argument
    ):
        """Validate the path.

        Args:
            value (Any): The value to validate.
            info (ValidationInfo): The validation information.

        Returns:
            Path: The value as a Path object.

        """
        if isinstance(value, Path):
            return value

        return Path(value)

    @field_validator("schedule")
    @classmethod
    def validate_schedule(
        cls, value: str, info: ValidationInfo  # pylint: disable=unused-argument
    ):
        """Validate the schedule.

        Args:
            value (str): The schedule to validate.
            info (ValidationInfo): The validation information.

        Returns:
            str: The schedule.

        """
        if value is None:
            return value

        m = re.match(
            (
                r"(@(annually|yearly|monthly|weekly|daily|hourly|reboot))|(@every "
                r"(\d+(ns|us|µs|ms|s|m|h))+)|((((\d+,)+\d+|(\d+(\/|-)\d+)|\d+|\*) ?){5,7})"
            ),
            value,
        )
        if m is None:
            raise ValueError(f"Invalid schedule format: {value}")

        return value

    def validate_source_bucket(self):
        """Set the source bucket for the application.

        Returns:
            self: The instance of the class, allowing for method chaining.

        """
        if self.source.bucket is None:
            self.source.bucket = self.staging_bucket
        return self

    @property
    def domain(self) -> str:
        """The domain of the application."""
        domain = (
            str(self.metadata.data_territory.value).lower()
            if self.metadata.data_territory
            else "uk"
        )
        if domain == "gb":
            domain = "uk"
        return domain

    @property
    def landing_bucket(self) -> str:
        """The source bucket for the application."""
        return BUCKET_PATTERN % (
            self.domain,
            get_bucket_description("lan", self.name, self.platform),
            f"{self.env_name}{self.env_id}",
        )

    @property
    def staging_bucket(self) -> str:
        """The staging bucket for the application."""
        return BUCKET_PATTERN % (
            self.domain,
            get_bucket_description("stg", self.name, self.platform),
            f"{self.env_name}{self.env_id}",
        )

    @property
    def publish_bucket(self) -> str:
        """The publish bucket for the application."""
        return BUCKET_PATTERN % (
            self.domain,
            get_bucket_description("pub", self.name, self.platform),
            f"{self.env_name}{self.env_id}",
        )

    @property
    def labels(self) -> dict[str, str]:
        """The labels for the application."""
        _labels = {
            "app": self.app,
            "name": self.name,
            "platform": self.platform,
            "domain": self.domain,
        }

        if self.run_id:
            _labels["run_id"] = self.run_id

        return _labels

    @field_validator("channel")
    @classmethod
    def validate_channel(
        cls, value: str, info: ValidationInfo  # pylint: disable=unused-argument
    ):
        """Validate the channel.

        Args:
            value (str): The channel to validate.
            info (ValidationInfo): The validation information.

        Returns:
            str: The channel.

        """
        if value is None:
            return value

        if value.startswith("#"):
            return value

        return f"#{value}"

    def load(self, **kwargs) -> None:
        """Load the given settings into the application settings.

        Args:
            **kwargs: The settings to load into the application settings.

        """
        for key, value in kwargs.items():

            if not hasattr(self, key):
                continue

            if isinstance(getattr(self, key), BaseModel):
                setattr(self, key, type(getattr(self, key)).model_validate(value))
            elif isinstance(getattr(self, key), Enum):
                setattr(self, key, type(getattr(self, key))(value))
            else:
                setattr(self, key, value)

        if not override_base_validation_context.get():
            self.validate_source_bucket()
