"""Base settings for the application."""

import re
import sys
from pathlib import Path
from typing import Any, Literal, Optional, TypeAlias

from pydantic import BaseModel, PrivateAttr, SecretStr, ValidationInfo, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from cleansweep._types import Platform

EnvironmentName: TypeAlias = Literal["dev", "test", "uat", "prod", "preprod", "local"]


class FeatureFlags(BaseModel):
    """Feature flags for the application."""

    translate: bool = True
    chunk_metadata: bool = True
    root_document: bool = True


class Timeouts(BaseModel):
    """Timeouts for the application."""

    process_api_calls: int = 600
    """Global timeout for processing API calls to OpenAI API."""
    translate: int = 300
    """Timeout for translating text to target language."""
    embed: int = 300
    """Timeout for embedding text."""
    metadata: int = 300
    """Timeout for generating metadata."""


class SettingsBase(BaseSettings, arbitrary_types_allowed=True):
    """The application settings."""

    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )

    feature: FeatureFlags = FeatureFlags()
    """Feature flags for the application"""
    timeouts: Timeouts = Timeouts()
    """Timeouts for the application"""

    log_level: Literal[
        "CRITICAL",
        "ERROR",
        "WARNING",
        "INFO",
        "DEBUG",
    ] = "INFO"

    # app settings set by env
    app: str = "cleansweep"
    env_name: EnvironmentName = "dev"
    env_id: str = ""
    name: str = "cleansweep"
    platform: Platform = "kosmo"
    embedded_file_pattern: str = (
        r"embedded\/(?:20[0-9]{2,}\-?(?:1[0-2]|0[0-9])\-?(?:3[0-1]|[0-2][0-9])"
        r"(?:\-|_)?(?:(?:[0-1][0-9]|2[0-3])\-?[0-5][0-9]\-?[0-5][0-9])?)?"
        r"_embedded_20[0-9]{2,}\-?(?:1[0-2]|0[0-9])\-?(?:3[0-1]|[0-2][0-9])"
        r"(?:\-|_)?(?:(?:[0-1][0-9]|2[0-3])\-?[0-5][0-9]\-?[0-5][0-9])?"
        r"\.parquet"
    )
    dev_mode: bool = False
    prompts_template_dir: Path = Path(__file__).parent.parent.joinpath(
        "prompts", "templates"
    )

    run_id: Optional[str] = None
    """Run id, set by the caller"""

    # Azure Open AI
    openai_api_base: Optional[str] = None
    """The Azure endpoint, including the resource"""
    openai_api_version: str = "2024-02-01"

    azure_tenant_id: Optional[SecretStr] = None
    """ID of the service principal's tenant. Also called its "directory" ID"""
    azure_client_id: Optional[SecretStr] = None
    """The service principal's client ID"""
    azure_client_secret: Optional[SecretStr] = None
    """One of the service principal's client secrets"""
    azure_scope: Optional[str] = None
    """Desired scopes for the access token"""

    max_requests_per_minute: int = 1500
    """The maximum number of requests to OpenAI API per minute"""
    max_tokens_per_minute: int = 125000
    """The maximum number of tokens to send to OpenAI API per minute"""
    max_attempts: int = 5
    """The maximum number of attempts to make to the OpenAI API"""
    rpm_calculation_period_seconds: Literal[1, 10] = 1
    """The period in seconds to calculate the requests per minute. RPM usage can be calculated
    every 1 or 10 seconds, for an RPM of 60 calculated every 1 second the maximum requests per
    second is 1."""

    # Slack Client Settings
    slack_bot_token: Optional[SecretStr] = None

    default_channel: str = "#cleansweep-dev"
    """The default channel to send notifications to"""

    _config_bucket: str = PrivateAttr("skygenai-uk-utl-kosmo-composer-ir")

    @property
    def config_bucket(self) -> str:
        """Return the configuration bucket."""
        return f"{self._config_bucket}-{self.env_name}"

    @field_validator("default_channel")
    @classmethod
    def validate_default_channel(
        cls, value: str, info: ValidationInfo  # pylint: disable=unused-argument
    ):
        """Validate the default channel.

        Args:
            value (str): The default channel to validate.
            info (ValidationInfo): The validation information.

        Returns:
            str: The default channel.

        """
        return f"#cleansweep-{info.data.get('env_name', 'dev')}"

    @field_validator("app")
    @classmethod
    def set_app(
        cls, value: Any, info: ValidationInfo  # pylint: disable=unused-argument
    ):
        """Set the application name.

        Args:
            value (Any): The value to set.
            info (ValidationInfo): The validation information.

        Returns:
            str: The application name.

        """
        root = sys.argv[0]
        m = re.search(r"app\/([\w\/]+)\/__main__\.py", root, re.IGNORECASE)
        if m:
            return m.group(1).replace("/", ".")

        return "cleansweep"

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


settings = SettingsBase()
