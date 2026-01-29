"""Settings for article cleansing."""

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ValidationInfo, field_validator

from cleansweep.enumerations import RuleType
from cleansweep.model.network import CloudStorageUrl, FileUrl, PathLikeUrl
from cleansweep.settings._types import SourceObject
from cleansweep.settings.app import AppSettings


class RuleSettings(BaseModel, extra="allow"):
    """Settings for article cleansing."""

    rule: str
    """A description of the rule to apply"""
    type: RuleType
    """The type of rule to apply"""


class CleanSettings(AppSettings, arbitrary_types_allowed=True):
    """Settings for article cleansing."""

    rules: list[RuleSettings] = []
    """A list of rules to apply"""

    force: bool = False
    """Force cleansing of all documents - useful for re-cleansing all documents"""

    dq_check: bool = True
    """Run data quality checks"""

    dq_custom_expectations: Optional[CloudStorageUrl | FileUrl] = None

    source: SourceObject = SourceObject(directory="curated", extension="avro")

    @field_validator("dq_custom_expectations")
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
            value = f"gs://skygenai-uk-utl-kosmo-composer-ir-{info.data["env_name"]}/cleansweep/dq/{value}"

        return PathLikeUrl(str(value))

    def load(self, **kwargs) -> None:
        """Load the custom settings.

        Args:
            **kwargs: Keyword arguments containing the chunk settings.

        """
        super().load(**kwargs)

        _settings = kwargs.get("steps", kwargs).get("clean", {})

        for key, value in _settings.items():

            if not hasattr(self, key):
                continue

            if isinstance(getattr(self, key), BaseModel):
                setattr(self, key, type(getattr(self, key)).model_validate(value))
            elif isinstance(getattr(self, key), Enum):
                setattr(self, key, type(getattr(self, key))(value))
            else:
                setattr(self, key, value)

        # process rules
        self.rules = [RuleSettings.model_validate(rule) for rule in self.rules]  # type: ignore

        # validate the source bucket
        self.validate_source_bucket()
