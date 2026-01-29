import pytest

from cleansweep._types import Deployment
from cleansweep.deployments.deployments import Deployments, configure
from cleansweep.exceptions import PipelineError


class TestDeployments:
    """Test the Deployments class."""

    def test_get_by_model(self):
        """Test getting a deployment by model name."""
        deployments = Deployments(
            deployments={"model1": [Deployment(name="deploy1", tpm=1, model="model1")]}
        )
        assert deployments.get_by_model("model1").name == "deploy1"
        assert deployments.get_by_model("model2") is None

    def test_get_by_deployment_name(self):
        """Test getting a deployment by deployment name."""
        deployments = Deployments(
            deployments={"model1": [Deployment(name="deploy1", tpm=1, model="model1")]}
        )
        assert deployments.get_by_deployment_name("deploy1").name == "deploy1"
        assert deployments.get_by_deployment_name("deploy2") is None

    def test_load_from_file(self, mocker):
        """Test loading deployments from a file."""
        mock_read_file_to_dict = mocker.patch(
            "cleansweep.deployments.deployments.read_file_to_dict"
        )
        mock_read_file_to_dict.return_value = [
            {"deployments": {"model1": [{"name": "deploy1", "tpm": 1}]}}
        ]

        config_uri = "file://path/to/config.yml"
        deployments = Deployments.load_from_file(config_uri)
        assert isinstance(deployments, Deployments)
        assert "model1" in deployments.deployments
        assert deployments.deployments["model1"][0].name == "deploy1"

    def test_load_from_file_empty(self, mocker):
        """Test loading deployments when the file is empty."""
        mock_read_file_to_dict = mocker.patch(
            "cleansweep.deployments.deployments.read_file_to_dict"
        )
        mock_read_file_to_dict.return_value = []

        config_uri = "file://path/to/config.yml"
        with pytest.raises(
            PipelineError, match="No settings found in the configuration file"
        ):
            Deployments.load_from_file(config_uri)

    def test_load_from_file_model_mismatch(self, mocker):
        """Test loading deployments with model mismatch."""
        mock_read_file_to_dict = mocker.patch(
            "cleansweep.deployments.deployments.read_file_to_dict"
        )
        mock_read_file_to_dict.return_value = [
            {
                "deployments": {
                    "model1": [{"name": "deploy1", "tpm": 1, "model": "model2"}]
                }
            }
        ]
        config_uri = "file://path/to/config.yml"
        with pytest.raises(PipelineError, match="Model mismatch: model2 != model1"):
            Deployments.load_from_file(config_uri)

    def test_load_and_merge(self, mocker):
        """Test loading and merging deployments."""
        existing_deployments = Deployments(
            deployments={"model1": [Deployment(name="deploy1", tpm=1, model="model1")]}
        )
        mock_read_file_to_dict = mocker.patch(
            "cleansweep.deployments.deployments.read_file_to_dict"
        )
        mock_read_file_to_dict.return_value = [
            {"deployments": {"model2": [{"name": "deploy2", "tpm": 1}]}}
        ]

        config_uri = "file://path/to/config.yml"
        merged_deployments = existing_deployments.load_and_merge(config_uri)
        assert isinstance(merged_deployments, Deployments)
        assert "model1" in merged_deployments.deployments
        assert "model2" in merged_deployments.deployments


class TestConfigure:
    """Test the configure function."""

    def test_configure(self, mocker):
        """Test configuring deployments."""
        mock_read_file_to_dict = mocker.patch(
            "cleansweep.deployments.deployments.read_file_to_dict"
        )
        mock_read_file_to_dict.return_value = [
            {"deployments": {"model1": [{"name": "deploy1", "tpm": 1}]}}
        ]

        mock_convert_uri = mocker.patch(
            "cleansweep.deployments.deployments.convert_to_url"
        )
        mock_convert_uri.return_value = lambda x: x

        mock_get_blob = mocker.patch("cleansweep.deployments.deployments.get_blob")
        mock_get_blob.return_value = None

        deployments = configure()

        assert isinstance(deployments, Deployments)
        assert "model1" in deployments.deployments
