"""The settings for the application."""

from typing import Literal, TypeAlias

from pydantic import PrivateAttr

from cleansweep.settings.files import InputFiles

EnvironmentName: TypeAlias = Literal["dev", "test", "uat", "prod", "preprod", "local"]


class Settings(InputFiles):
    """The application settings."""

    env_name: EnvironmentName = "dev"
    env_id: str = ""

    _utility_bucket: str = PrivateAttr("skygenai-uk-utl-kosmo-composer-ir")

    job_name: str = "cleansweep"
    region: str = "europe-west1"

    @property
    def utility_bucket(self) -> str:
        """Get the utility bucket."""
        return f"{self._utility_bucket}-{self.env_name}"
