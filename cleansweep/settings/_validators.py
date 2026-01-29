from typing import Any, Optional

from pydantic import ValidationInfo

from cleansweep._types import Deployment
from cleansweep.deployments.deployments import DEPLOYMENTS
from cleansweep.model.network import PathLikeUrl


def bucket_string_validator(value: Optional[str], info: ValidationInfo) -> str | None:
    """Convert a generic bucket string to a specific format.

    Args:
        value (str): The generic bucket string to convert.
        info (ValidationInfo): The validation information.

    Returns:
        str: The converted bucket string.

    """
    if value is None:
        return value

    env_name = info.data.get("env_name")
    assert env_name is not None, "env_name should be set"

    return value.replace("ENV", env_name)


def plugin_validator(
    value: Any, info: ValidationInfo  # pylint: disable=unused-argument
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
        value = f"gs://skygenai-uk-utl-kosmo-composer-ir-{info.data["env_name"]}/cleansweep/plugins/{value}"

    return PathLikeUrl(str(value))


def validate_model(
    value: Any, info: ValidationInfo  # pylint: disable=unused-argument
) -> Deployment | None:
    """Validate the model.

    Args:
        value (Any): The value to validate.
        info (ValidationInfo): The validation information.

    Returns:
        Deployment: The deployment.

    """
    if isinstance(value, Deployment):
        return value

    if isinstance(value, str):
        model = DEPLOYMENTS.get_by_model(value)
        if model is None:
            model = DEPLOYMENTS.get_by_deployment_name(value)
        return model

    return None
