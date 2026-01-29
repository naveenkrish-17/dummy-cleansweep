"""Create a DAG from a config file."""

import re
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from black import FileMode, format_str
from jinja2 import Environment, FileSystemLoader

from app.create_dag.overrides import create_overrides
from app.create_dag.pipeline import Pipeline, PipelineStep, PipelineTask
from app.create_dag.settings import Settings
from app.create_dag.tags import get_tags
from cleansweep import __app_name__
from cleansweep.model.network import (
    CloudStorageUrl,
    FileUrl,
    PathLikeUrl,
    convert_to_url,
    isurlinstance,
    raw_path,
)
from cleansweep.utils.exceptions import error_handler, initialize_except_hook
from cleansweep.utils.google.storage import upload
from cleansweep.utils.google.storage import write as gcs_write
from cleansweep.utils.io import gcs_to_temp
from cleansweep.utils.logging import setup_logging
from cleansweep.utils.tar import extract_tar

logger = setup_logging(__app_name__, dev_mode=False)
"""Logger for the module."""

initialize_except_hook(uncaught_hook=error_handler)

settings = Settings()

env = Environment(loader=FileSystemLoader(Path(__file__).parent.joinpath("templates")))
"""Jinja2 environment for loading templates."""


def upload_supporting_files(source: Path, bucket: str) -> dict[Path, str]:
    """Upload supporting files from a source directory to a specified bucket.

    This function scans the source directory for files matching specific patterns
    and uploads them to corresponding directories in the target bucket.

    Patterns and their corresponding target directories:
        - "*.py": "cleansweep/plugins"
        - "expectation.y*ml": "cleansweep/expectations"
        - "mapping.json": "cleansweep/mapping"

    Args:
        source (Path): The source directory containing the files to be uploaded.
        bucket (str): The target bucket where the files will be uploaded.

    Returns:
        dict: A dictionary mapping the source files to their corresponding target files.

    """
    glob_patterns = {
        "*.py": "cleansweep/plugins",
        "expectation.y*ml": "cleansweep/expectations",
        "mapping.json": "cleansweep/mapping",
    }

    file_map = {}

    for pattern, target in glob_patterns.items():

        files = source.glob(pattern)

        for file in files:
            target_file = f"{bucket}/{target}/{file.name}"
            # upload the file to the control bucket
            upload(file.as_posix(), target_file)
            logger.debug("Uploaded %s to %s/%s", file.name, bucket, target)
            file_map[file] = target_file

    return file_map


def load_config(
    input_file_uri: CloudStorageUrl | FileUrl,
) -> tuple[Any, dict[Path, str]]:
    """Load a configuration file from a given URI.

    Args:
        input_file_uri (CloudStorageUrl | FileUrl): The URI of the input file.

    Returns:
        tuple[Any, dict[Path, str]]: A tuple containing the loaded configuration and a dictionary
            mapping paths to their corresponding file names.

    Raises:
        FileNotFoundError: If no YAML file is found in the tarball.

    """
    file_map = {}

    # download the input file if it's a cloud storage url
    if isurlinstance(input_file_uri, CloudStorageUrl):
        config_file = gcs_to_temp(raw_path(input_file_uri))
    else:
        config_file = Path(raw_path(input_file_uri))

    # check the file type
    if Path(config_file).suffix == ".gz":
        # if it's a tar file, extract it
        extracted = extract_tar(config_file)

        # find the config file
        try:
            config_file = next(extracted.glob("*config.y*ml"))
        except StopIteration as exc:
            logger.error("No YAML file found in tarball")
            raise FileNotFoundError("No YAML file found in tarball") from exc

        # upload the supporting files
        file_map = upload_supporting_files(extracted, settings.utility_bucket)

    # load config file
    with config_file.open(encoding="utf-8") as file:
        config = yaml.safe_load(file)

    return config, file_map


def extract_tasks_and_dependencies(
    config: dict[str, Any],
) -> tuple[list[PipelineTask], dict[str, list[str]]]:
    """Extract tasks and their dependencies from a given configuration.

    Args:
        config (dict[str, Any]): The configuration dictionary containing pipeline definitions.

    Returns:
        tuple[list[PipelineTask], dict[str, list[str]]]: A tuple containing:
            - A list of PipelineTask objects representing the tasks extracted from the pipelines.
            - A dictionary where keys are task names and values are lists of task names that the key
                task depends on.

    """
    tasks = []
    dependencies = {}

    for pipeline in config["pipelines"]:
        if pipeline.get("after") is not None:

            pipeline_dependencies = pipeline.get("after")
            if not isinstance(pipeline_dependencies, list):
                pipeline_dependencies = [pipeline_dependencies]

            dependencies[pipeline.get("name")] = pipeline_dependencies
            # after = [
            #     p for p in config["pipelines"] if p.get("name") in pipeline_dependencies
            # ]
            # platform = pipeline.get("platform", config.get("platform", "kosmo"))
            # country = pipeline.get("metadata", config.get("metadata", {})).get(
            #     "data_territory", "UK"
            # )
            # for p in after:
            #     previous_platform = p.get("platform", config.get("platform", "kosmo"))
            #     previous_country = p.get("metadata", config.get("metadata", {})).get(
            #         "data_territory", "UK"
            #     )

            # if any([platform != previous_platform, country != previous_country]):
            #     pipeline["source_bucket"] = BUCKET_PATTERN % (
            #         previous_country.replace("GB", "UK").lower(),
            #         get_bucket_description(
            #             "stg", config.get("name", "default"), previous_platform
            #         ),
            #         f"{settings.env_name}{settings.env_id}",
            #     )
            #     break

    # a list of keys which exist in pipeline settings, that should not be copied to step settings
    pipeline_keys_to_skip = ["name", "after", "source_bucket"]

    for pipeline in config["pipelines"]:

        # build the pipeline objects

        # for each pipeline, combine the pipeline settings with the base config settings
        # take the base config, and update it with the pipeline settings
        base = deepcopy(config)
        pipeline_steps = pipeline.pop("steps")
        base.update(pipeline)
        _pipeline_steps = []
        # create steps from the pipeline settings
        for step in pipeline_steps:
            # update the base config with the step settings
            step_config = deepcopy(base.get("steps", {}).get(step["type"], {}))
            step_config.update(step)

            # add pipeline settings to each task
            for k, v in pipeline.items():
                if k not in pipeline_keys_to_skip:
                    step_config[k] = v

            step = PipelineStep(**step_config)
            _pipeline_steps.append(step.model_dump())

        base["steps"] = _pipeline_steps
        pipeline_object = Pipeline(**base)

        # get the tasks for the pipeline
        pipeline_tasks = pipeline_object.tasks()

        # update dependencies with pipeline name
        for k, v in [(key, value) for key, value in dependencies.items()]:
            if pipeline_object.name in v:
                dependencies[k][v.index(pipeline_object.name)] = pipeline_tasks[-1].name
            if k == pipeline_object.name:
                dependencies[pipeline_tasks[0].name] = v
                del dependencies[k]

        # add tasks to the dependencies
        for i in range(1, len(pipeline_tasks)):
            if pipeline_tasks[i].name not in dependencies:
                dependencies[pipeline_tasks[i].name] = [pipeline_tasks[i - 1].name]
            else:
                dependencies[pipeline_tasks[i].name].append(pipeline_tasks[i - 1].name)

        # add the source bucket to the first task if it's not a transform task
        # if "source_bucket" in pipeline and pipeline_tasks[0].target != "transform":
        #     pipeline_tasks[0].env["source_bucket"] = pipeline["source_bucket"]

        tasks.extend(pipeline_tasks)

    return tasks, dependencies


def create_dag(
    tasks: list[PipelineTask],
    dependencies: dict[str, list[str]],
    file_map: dict[Path, str],
    config: dict[str, Any],
) -> str:
    """Generate a string representation of the DAG for a given set of tasks and dependencies.

    Args:
        tasks (list[PipelineTask]): A list of PipelineTask objects representing the tasks to be
            included in the DAG.
        dependencies (dict[str, list[str]]): A dictionary where keys are task names and values are
            lists of task names that the key task depends on.
        file_map (dict[Path, str]): A dictionary mapping file paths to their respective string
            representations.
        config (dict[str, Any]): A configuration dictionary containing metadata and other settings
            for the DAG.

    Returns:
        str: A string representation of the DAG in the specified template format.

    """
    # create base env override
    skip_keys = ["steps", "pipelines", "metadata"]
    overrides = create_overrides(
        {k: v for k, v in config.items() if k not in skip_keys}
    )
    overrides.extend(create_overrides(config.get("metadata", {}), "metadata").env)

    # convert the tasks and dependencies to strings
    task_strings = [
        task.create_task_string(file_map=file_map, overrides=overrides)
        for task in tasks
    ]

    dependency_strings = [
        f"{dep.replace('-','_')} >> {k.replace('-','_')}"
        for k, v in dependencies.items()
        for dep in v
    ]

    # add root dependencies
    # 1) collect every parent in one set
    all_parents = {child for children in dependencies.values() for child in children}

    # 2) roots are the parents that never show up as children
    roots = all_parents - set(dependencies.keys())

    for root in roots:
        dependency_strings.append(f"log_run_id >> {root.replace('-','_')}")

    # extract metadata from config
    metadata = config.get("metadata", {})
    data_owner_email = metadata.get("data_owner")
    if data_owner_email is None:
        data_owner_email = ""
    data_owner_name = metadata.get("data_owner_name")
    if data_owner_name is None:
        data_owner_name = ""

    owner_links = {
        owner.get("name"): owner.get("link", f"mailto:{owner.get('email')}")
        for owner in metadata.get("owners", [])
        if owner.get("name") is not None
    }

    name = re.sub(r"[ _]", "-", config.get("name", "default").lower())
    tags = get_tags(config, tasks)
    schedule_interval = config.get("schedule")
    start_date = None
    if schedule_interval:
        schedule_interval = f"'{schedule_interval}'"
        start_date = f"datetime({datetime.now().year}, {datetime.now().month}, {datetime.now().day})"

    # create the DAG string
    template = env.get_template("dag.jinja2")
    dag_content = template.render(
        job_name=f"{settings.job_name}-{settings.env_name}{settings.env_id}",
        region=settings.region,
        tasks="\n".join(task_strings),
        dependencies="\n    ".join(dependency_strings),
        name=name,
        description=config.get("description", "No description provided"),
        owner=data_owner_name,
        email=data_owner_email,
        owner_links=owner_links,
        start_date=start_date,
        schedule_interval=schedule_interval,
        tags=str(tags),
    )
    return dag_content


def write(content: str, file_path: PathLikeUrl):
    """Write the content to the file path.

    Args:
        content: The content to write.
        file_path: The file path to write the content to.

    """

    def local_write(content: str, file_path: str):
        with Path(file_path).open("w", encoding="utf-8") as f:
            f.write(content)

    function = None
    if isurlinstance(file_path, CloudStorageUrl):
        function = gcs_write

    if isurlinstance(file_path, FileUrl):
        function = local_write

    if function is None:
        raise ValueError(f"Unsupported file URI: {file_path}")

    function(content, raw_path(file_path))


def main():
    """Create a DAG from a config file."""
    # Load a config file and create a DAG from it

    if settings.input_file_uri is None:
        logger.error("No input file provided")
        raise ValueError("No input file provided")

    # the config could be a tar with multiple files
    # if uploading a tar file of multiple files the following structure is expected:
    # - *config.yaml: the main config file
    # - *.py: plugin file(s), if needed
    # - *expectations.yaml: expectations file, if needed
    # - *mapping.json: mapping file, if needed
    # plugin, expectations, and mapping files are mapped based on the main config file. All are then
    # uploaded to the utility bucket
    config, file_map = load_config(settings.input_file_uri)

    if "pipelines" not in config:
        base_pipeline = {
            "name": config.get("name", "default"),
            "steps": [
                {"type": "transform", "name": "transform"},
                {"type": "clean", "name": "clean"},
                {"type": "chunk", "name": "chunk"},
                {"type": "embed", "name": "embed"},
            ],
        }
        config["pipelines"] = [base_pipeline]

    logger.info("Creating DAG from config file...")

    # create a list of tasks from each pipeline and the dependencies between them
    tasks, dependencies = extract_tasks_and_dependencies(config)

    # create the DAG
    dag_content = create_dag(
        tasks=tasks, dependencies=dependencies, file_map=file_map, config=config
    )

    # Format the contents
    logger.info("Formatting DAG...")
    dag_content = format_str(dag_content, mode=FileMode())

    dag_name = re.sub(r"[ _]", "-", config.get("name", "default").lower())

    dag_uri = convert_to_url(f"gs://{settings.utility_bucket}/dags/{dag_name}.py")

    logger.info("📝 Writing DAG to %s", dag_uri)
    # Write the DAG file to Google Cloud Storage
    write(dag_content, dag_uri)
