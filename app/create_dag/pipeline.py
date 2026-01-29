"""Pipeline module that contains classes for representing a data processing pipeline."""

import json
import re
from copy import deepcopy
from pathlib import Path
from typing import Any, Literal, Optional, Sequence, TypeAlias

from jinja2 import Environment, FileSystemLoader
from pydantic import BaseModel, computed_field

from app.create_dag.overrides import Overrides, create_overrides
from app.create_dag.settings import Settings
from cleansweep.core.fileio import create_glob_pattern
from cleansweep.settings.app import AppSettings
from cleansweep.utils.bucket import BUCKET_PATTERN, get_bucket_description

Target: TypeAlias = Literal[
    "transform",
    "clean",
    "chunk",
    "metadata",
    "embed",
    "semantic.chunk",
    "semantic.cluster",
    "semantic.merge",
    "semantic.validate",
    "translate",
    "merge",
    "drop",
    "run",
    "concatenate",
]

TargetDirectory: TypeAlias = Literal[
    "cleaned", "chunked", "curated", "metadata", "merged"
]

PipelineStepType: TypeAlias = Literal[
    "transform",
    "clean",
    "chunk",
    "metadata",
    "embed",
    "file_check",
    "translate",
    "merge",
    "drop",
    "run",
    "concatenate",
]

env = Environment(loader=FileSystemLoader(Path(__file__).parent.joinpath("templates")))
"""Jinja2 environment for loading templates."""

settings = Settings()


class TaskBase(BaseModel):
    """Base class for tasks in a data processing pipeline."""

    name: str


class ShortCurcuitTask(TaskBase):
    """Short curcuit task class that represents a task in a data processing pipeline."""

    target_dir: TargetDirectory
    prefix: Optional[str] = None
    bucket: Optional[str] = None
    env: dict[str, Any]

    def create_task_string(self, **kwargs) -> str:
        """Generate a task string based on the provided overrides and template.

        Args:
            **kwargs: Arbitrary keyword arguments that may contain overrides.

        Keyword Args:
            overrides (dict): A dictionary containing override values. Expected keys include:
                - "platform" (str): The platform name. Defaults to "kosmo".
                - "name" (str): The name associated with the task.
                - "metadata__data_territory" (str): The country code. Defaults to "GB".

        Returns:
            str: The rendered task string based on the provided template and overrides.

        Raises:
            ValueError: If the "platform" or "name" is not found in the overrides.

        """
        overrides = kwargs.get("overrides")

        if overrides is None:
            overrides = Overrides(env=[])

        overrides = deepcopy(overrides)

        overrides.extend(create_overrides(self.env).env)

        if self.bucket:
            bucket = self.bucket

        else:
            platform = overrides.get("platform", "kosmo")
            if platform is None:
                raise ValueError("Platform not found in overrides")

            name = overrides.get("name")
            if name is None:
                raise ValueError("Name not found in overrides")

            country = overrides.get("metadata__data_territory", "GB")

            bucket = BUCKET_PATTERN % (
                country.replace("GB", "UK").lower(),
                get_bucket_description("stg", name, platform),
                f"{settings.env_name}{settings.env_id}",
            )

        match_glob = create_glob_pattern(
            directory=self.target_dir,
            run_id="{{ get_run_id(dag_run.run_id, dag_run.dag_id) }}",
            prefix=self.prefix,
            extension=".avro",
        )

        template = env.get_template("short_curcuit.jinja2")

        return template.render(name=self.name, bucket=bucket, match_glob=match_glob)


class PipelineTask(TaskBase):
    """Pipeline task class that represents a task in a data processing pipeline."""

    target: Target
    env: dict[str, Any]

    def create_task_string(self, **kwargs) -> str:
        """Generate a task string by rendering a Jinja2 template with the provided task details.

        Args:
            **kwargs: Arbitrary keyword arguments that may contain overrides.

        Keyword Args:
            file_map (dict[Path, str]): A dictionary mapping file paths to their corresponding string
                values.
            overrides (Overrides | None, optional): An optional Overrides object containing environment
                variable overrides. Defaults to None.

        Returns:
            str: The rendered task string.

        """
        file_map = kwargs.get("file_map")
        overrides = kwargs.get("overrides")

        if file_map is None:
            file_map = {}

        # compare the env to the file_map and replace the values
        if file_map:
            for k, v in self.env.items():
                if isinstance(v, str):
                    val_path = Path(v)
                    for file in file_map:
                        if file.name == val_path.name:
                            self.env[k] = file_map[file]
                            break

        template = env.get_template("task.jinja2")

        if overrides is None:
            overrides = Overrides(env=[])

        overrides = deepcopy(overrides)

        overrides.extend(create_overrides(self.env).env)

        stringified_env = ", ".join(
            json.dumps(override) for override in overrides.dump()
        )

        env_string = (
            '[{"name": "run_id", "value": "{{ get_run_id(dag_run.run_id, dag_run.dag_id) }}"}, '
            f"{stringified_env}, ]"
        )

        return template.render(name=self.name, target=self.target, env=env_string)


class PipelineStep(BaseModel, extra="allow"):
    """Pipeline step class that represents a step in a data processing pipeline."""

    type: PipelineStepType
    name: str

    def _task(self, pipeline_id: str, name: str, target: Target) -> PipelineTask:

        return PipelineTask(
            name=f"{pipeline_id}-{name}",
            target=target,
            env=self.model_dump(exclude={"type", "name"}),
        )

    def _short_curcuit(self, pipeline_id: str, name: str) -> ShortCurcuitTask:

        return ShortCurcuitTask(
            name=f"{pipeline_id}-{name}",
            target_dir=self.model_dump().get("target_dir"),  # type: ignore
            prefix=self.model_dump().get("prefix"),
            env=self.model_dump(exclude={"type", "name"}),
        )

    @property
    def id(self) -> str:
        """Generate a sanitized ID string.

        Replaces spaces and underscores with hyphens, and removing all non-word characters from the
        object's name attribute.

        Returns:
            str: A sanitized ID string.

        """
        return re.sub(r"[^a-zA-Z0-9_\-]", "", re.sub(r"[ _]", "-", self.name))

    def tasks(self, platform: str, pipeline_id: str) -> Sequence[TaskBase]:
        """Generate a list of pipeline tasks based on the platform and pipeline type.

        - If the pipeline type is "chunk" and the platform is "em", a specific set of tasks
            ("semantic.chunk", "semantic.cluster", "semantic.merge", "semantic.validate") is
            returned.
        - Otherwise, a single task based on the pipeline type is returned.

        Args:
            platform (str): The platform for which the tasks are being generated.
            pipeline_id (str): The unique identifier for the pipeline.

        Returns:
            list: A list of tasks based on the pipeline type and platform.

        """
        if self.type == "file_check":
            return [self._short_curcuit(pipeline_id, self.id)]
        elif self.type == "chunk" and platform == "em":
            output = [
                self._task(pipeline_id, f"{self.id}-semantic-chunk", "semantic.chunk")
            ]

            cluster_task = self._task(
                pipeline_id, f"{self.id}-semantic-cluster", "semantic.cluster"
            )

            # Get the source dict from the env / config - only once since all the same
            source_env = cluster_task.env.get("source", {})
            if isinstance(source_env, dict):
                source_dict = source_env
            else:
                try:
                    source_dict = json.loads(source_env)
                except (json.JSONDecodeError, TypeError):
                    source_dict = {}
            source_dict["directory"] = "semantic/chunked"
            cluster_task.env["source"] = json.dumps(source_dict)
            output.append(cluster_task)

            merge_task = self._task(
                pipeline_id, f"{self.id}-semantic-merge", "semantic.merge"
            )
            source_dict["directory"] = "semantic/clustered"
            merge_task.env["source"] = json.dumps(source_dict)
            output.append(merge_task)

            validate_task = self._task(
                pipeline_id, f"{self.id}-semantic-validate", "semantic.validate"
            )
            source_dict["directory"] = "semantic/merged"
            validate_task.env["source"] = json.dumps(source_dict)
            output.append(validate_task)

            return output

        else:
            return [self._task(pipeline_id, self.id, self.type)]


class Pipeline(AppSettings):
    """Pipeline class that inherits from AppSettings and represents a data processing pipeline."""

    steps: list[PipelineStep]

    @computed_field
    @property
    def id(self) -> str:
        """Unique identifier for the pipeline.

        The identifier is constructed by combining the platform, data territory,
        and a sanitized version of the pipeline name. The name is converted to
        lowercase and spaces or underscores are replaced with hyphens.

        Returns:
            str: A unique identifier string in the format
                 "{platform}_{data_territory}_{name}".

        """
        name = re.sub(r"[^a-zA-Z0-9_\-]", "", re.sub(r"[ _]", "-", self.name))

        return f"{self.platform}-{self.metadata.data_territory.value}-{name}".lower()  # type: ignore

    def tasks(self):
        """Generate a list of tasks by aggregating tasks from each step.

        This method iterates over the steps and collects tasks from each step
        by calling the `tasks` method of the step with the platform and id
        attributes of the current instance.

        Returns:
            list: A list of tasks aggregated from all steps.

        """
        tasks = []

        for step in self.steps:
            tasks.extend(step.tasks(self.platform, self.id))

        return tasks
