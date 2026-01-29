"""Module for loading and saving configuration settings for the app."""

from typing import Type, TypeVar

import yaml

import cleansweep.utils.google.storage as gcs
from cleansweep.model.network import (
    CloudStorageUrl,
    FileUrl,
    FtpUrl,
    HttpUrl,
    convert_to_url,
    file_type,
    isurlinstance,
    raw_path,
)
from cleansweep.settings.app import AppSettings
from cleansweep.settings.base import settings

SUPPORTED_FILE_TYPES = ".yml", ".yaml"
"""Supported file types for the config file."""


T = TypeVar("T", bound=AppSettings)


def load_settings(
    settings_object: Type[T],
    config_path: str | CloudStorageUrl | FileUrl | FtpUrl | HttpUrl | None = None,
) -> T:
    """Load settings from a configuration file.

    Args:
        settings_object (Type[T]): The type of the settings object to be loaded.
        config_path (str | CloudStorageUrl | FileUrl | FtpUrl | HttpUrl | None, optional):
            The path or URL of the configuration file. Defaults to None.

    Returns:
        T: The loaded settings object.

    Raises:
        NotImplementedError: If the provided config_path is not a supported URL or file type.

    """
    obj = settings_object()
    if config_path is not None:
        config = load_config_file(config_path)

        if "name" in config:
            # set current name
            settings.name = config["name"]

        obj.load(**config)
    else:
        # ensure sources are initialised
        obj.validate_source_bucket()
    return obj


def load_config_file(config_path: str | CloudStorageUrl | FileUrl | FtpUrl | HttpUrl):
    """Load a configuration file from a specified path or URL.

    This function supports loading configuration files from local file paths
    or cloud storage URLs. The configuration file must be in YAML format.

    Args:
        config_path (str | CloudStorageUrl | FileUrl | FtpUrl | HttpUrl):
            The path or URL to the configuration file. It can be a local file
            path, a CloudStorageUrl, or a FileUrl.

    Returns:
        dict: The parsed configuration data as a dictionary.

    Raises:
        AssertionError: If the `config_path` is None.
        NotImplementedError: If the `config_path` is not a CloudStorageUrl or FileUrl,
            or if the file type is not supported (only YAML files are supported).

    """

    def read(file_path: str) -> str:
        with open(file_path, "r", encoding="utf-8") as file:
            return file.read()

    assert config_path is not None, "Config path cannot be None"

    config_path = convert_to_url(config_path)

    function = None
    if isurlinstance(
        config_path, CloudStorageUrl  # pyright: ignore[reportArgumentType]
    ):
        function = gcs.read
    elif isurlinstance(config_path, FileUrl):  # pyright: ignore[reportArgumentType]
        function = read

    if function is None:
        raise NotImplementedError("Only CloudStorageUrl and FileUrl are supported")

    if file_type(config_path) not in SUPPORTED_FILE_TYPES:
        raise NotImplementedError("Only YAML files are supported")

    content = function(raw_path(config_path))
    config = yaml.safe_load(content)

    return config


def load_pipeline_setting(
    pipeline_step: tuple[str, str],
    settings_object: Type[T],
    config_path: str | CloudStorageUrl | FileUrl | FtpUrl | HttpUrl | None = None,
):
    """Load and configure a settings object based on a specified pipeline step and configuration file.

    Args:
        pipeline_step (tuple[str, str]): A tuple specifying the pipeline name and step name
            (e.g., ("pipeline_name", "step_name")).
        settings_object (Type[T]): The class type of the settings object to be loaded and configured.
        config_path (str | CloudStorageUrl | FileUrl | FtpUrl | HttpUrl | None, optional):
            The path or URL to the configuration file. Can be a local file path, cloud storage URL,
            FTP URL, HTTP URL, or None if no configuration file is provided.

    Returns:
        T: An instance of the settings object, configured based on the pipeline step and configuration file.

    Raises:
        AssertionError: If the configuration file is not found, the specified pipeline is not found
            in the configuration, or the specified step is not found in the pipeline.

    Notes:
        - If a configuration file is provided, it is loaded and used to configure the settings object.
        - The function searches for the specified pipeline and step in the configuration file and
          applies the corresponding settings to the settings object.

    """
    obj = settings_object()

    if config_path is not None:
        config = load_config_file(config_path)
        assert config is not None, "Config file not found"

        if "name" in config:
            # set current name
            settings.name = config["name"]

        obj.load(**config)

        # find step in the pipeline
        pipelines = config.get("pipelines", [])

        # find pipeline
        pipeline = None
        for pipeline in pipelines:
            if pipeline.get("name") == pipeline_step[0]:
                break
        assert pipeline is not None, f"Pipeline {pipeline_step[0]} not found in config"

        # find step
        pipeline_steps = pipeline.get("steps", [])
        step = None
        for step in pipeline_steps:
            if step.get("name") == pipeline_step[1]:
                break
        assert (
            step is not None
        ), f"Step {pipeline_step[1]} not found in pipeline {pipeline_step[0]}"

        # load settings
        obj.load(**step)

    return obj
