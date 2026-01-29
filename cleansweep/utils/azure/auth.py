"""Function for authenticating with Azure."""

import os
from typing import Any, Optional

from azure.identity import ClientSecretCredential
from pydantic import BaseModel, SecretStr, ValidationInfo, field_validator


def _get_env(var: str) -> str | None:
    """Get the value of an environment variable.

    Args:
        var (str): The name of the environment variable.
        is_secret (bool, Optional): Whether the environment variable is a secret.

    Returns:
        value: The value of the environment variable.

    """
    return os.getenv(var.lower()) or os.getenv(var.upper())


def _get_secret_env(var: str) -> SecretStr | None:
    """Get the value of a secret environment variable.

    Args:
        var (str): The name of the environment variable.

    Returns:
        value: The value of the environment variable.

    """
    value = _get_env(var)
    if value is None:
        return None
    return SecretStr(value)


class AzureCredentials(BaseModel, frozen=True):
    """Azure credentials."""

    azure_client_id: Optional[SecretStr] = _get_secret_env("AZURE_CLIENT_ID")
    """The service principal's client ID"""
    azure_client_secret: Optional[SecretStr] = _get_secret_env("AZURE_CLIENT_SECRET")
    """One of the service principal's client secrets"""
    azure_scope: Optional[str] = _get_env("AZURE_SCOPE")
    """Desired scopes for the access token"""
    azure_tenant_id: Optional[SecretStr] = _get_secret_env("AZURE_TENANT_ID")
    """ID of the service principal's tenant. Also called its "directory" ID"""

    openai_api_base: Optional[str] = _get_env("OPENAI_API_BASE")
    """The Azure endpoint, including the resource"""
    openai_api_version: Optional[str] = _get_env("OPENAI_API_VERSION")

    @field_validator(
        "azure_client_id",
        "azure_client_secret",
        "azure_scope",
        "azure_tenant_id",
        "openai_api_base",
        "openai_api_version",
    )
    @classmethod
    def validate_required(cls, value: Any, info: ValidationInfo):
        """Validate that the field is required."""
        if info.field_name:
            message = (
                f"{info.field_name} must be set. Please set the environment variable "
                f"{info.field_name.upper()}"
            )
        else:
            message = (
                "Field is required. Please set the appropriate environment variable."
            )

        assert value is not None, message

    @property
    def api_key(self) -> SecretStr | None:
        """Get the Azure API key.

        Returns
            api_key: The Azure API key.

        """
        if self.azure_scope is None:
            raise ValueError("Azure scope must be set to get the API key")
        return SecretStr(self._get_credentials().get_token(self.azure_scope).token)

    def _get_credentials(self) -> ClientSecretCredential:
        """Get the credentials for the Azure Blob Storage account.

        Returns
            credentials: The credentials for the Azure Blob Storage account.

        """
        if not all(
            [
                self.azure_client_id,
                self.azure_client_secret,
                self.azure_tenant_id,
            ]
        ):
            raise ValueError(
                "Azure client ID, client secret, and tenant ID must be set to get the credentials"
            )

        return ClientSecretCredential(
            **{
                "client_id": (
                    self.azure_client_id.get_secret_value()
                    if self.azure_client_id
                    else ""
                ),
                "client_secret": (
                    self.azure_client_secret.get_secret_value()
                    if self.azure_client_secret
                    else ""
                ),
                "tenant_id": (
                    self.azure_tenant_id.get_secret_value()
                    if self.azure_tenant_id
                    else ""
                ),
            }
        )
