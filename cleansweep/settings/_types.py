"""Settings types."""

from typing import Optional

from pydantic import AfterValidator, BaseModel
from typing_extensions import Annotated

from cleansweep._types import Deployment
from cleansweep.settings._validators import bucket_string_validator, validate_model
from cleansweep.settings.base import EnvironmentName

Bucket = Annotated[Optional[str], AfterValidator(bucket_string_validator)]
"""Bucket type."""


Model = Annotated[str | Deployment, AfterValidator(validate_model)]
"""Model type."""


class SourceObject(BaseModel):
    """The source object settings.

    Used to search for a file when a specific uri is not provided.
    """

    env_name: EnvironmentName = "dev"
    bucket: Optional[Bucket] = None
    directory: Optional[str] = None
    extension: Optional[str] = ".avro"
    prefix: Optional[str] = None
    file_name: Optional[str] = None
    use_run_id: bool = True
