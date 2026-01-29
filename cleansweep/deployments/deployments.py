"""Module for prompt configuration and generation."""

from pathlib import Path
from typing import Dict, List, Optional

from pydantic import BaseModel

from cleansweep._types import Deployment
from cleansweep.core.fileio import read_file_to_dict
from cleansweep.exceptions import PipelineError
from cleansweep.model.network import PathLikeUrl, convert_to_url
from cleansweep.settings.base import settings
from cleansweep.utils.google.storage import get_blob


class Deployments(BaseModel):
    """A collection of deployments."""

    deployments: Dict[str, List[Deployment]]

    def get_by_model(self, model_name: str) -> Optional[Deployment]:
        """Return the first deployment for a given model name."""
        return self.deployments.get(model_name, [None])[0]

    def get_by_deployment_name(self, deployment_name: str) -> Optional[Deployment]:
        """Return a deployment by its deployment name."""
        for deployments in self.deployments.values():
            for deployment in deployments:
                if deployment.name == deployment_name:
                    return deployment
        return None

    @staticmethod
    def load_from_file(config_uri: PathLikeUrl) -> "Deployments":
        """Load the deployments from a file."""
        content = read_file_to_dict(config_uri)
        if not content:
            raise PipelineError("No settings found in the configuration file")
        content = content[0]

        # Validate the content - for model in content.keys() ensure each item includes the model
        for model, deployments in content["deployments"].items():
            for deployment in deployments:

                if "model" not in deployment:
                    deployment["model"] = model
                elif deployment["model"] != model:
                    raise PipelineError(
                        f"Model mismatch: {deployment['model']} != {model}"
                    )

        return Deployments(**content)

    def load_and_merge(self, config_uri: PathLikeUrl) -> "Deployments":
        """Load the deployments from a file and merge with the current deployments."""
        new_deployments = Deployments.load_from_file(config_uri=config_uri)
        self.deployments.update(new_deployments.deployments)
        return self

    def __getitem__(self, item: str) -> Optional[Deployment]:
        """Get a deployment by its name."""
        # is it a deployment name?
        deployment = self.get_by_deployment_name(item)
        if deployment:
            return deployment
        # is it a model name?
        return self.get_by_model(item)


def configure() -> Deployments:
    """Configure the deployments."""
    config_uri = convert_to_url(
        f"file://{Path(__file__).parent.joinpath("deployments.yml").as_posix()}"
    )
    deployments = Deployments.load_from_file(config_uri=config_uri)

    config_file = get_blob(settings.config_bucket, "cleansweep/config/deployments.yml")
    if config_file:
        config_uri = convert_to_url(
            f"gs://{config_file.bucket.name}/{config_file.name}"
        )
        deployments.load_and_merge(config_uri)

    return deployments


DEPLOYMENTS: Deployments = configure()
