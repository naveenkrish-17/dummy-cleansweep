"""Module that contains the Tag class and the get_tags function."""

from typing import Any, Literal

from pydantic import BaseModel

from app.create_dag.pipeline import PipelineTask


class Tag(BaseModel):
    """Tag class that represents a tag for a DAG."""

    type: Literal["data territory", "load type", "platform", "classification"]
    value: str

    def __repr__(self) -> str:
        return f"'{self.type}: {self.value}'"

    def __str__(self) -> str:
        return f"{self.type}: {self.value}"


def get_tags(config: dict[str, Any], tasks: list[PipelineTask]) -> list[str]:
    """Get the tags from the config.

    Args:
        config (dict[str, Any]): The configuration dictionary.
        tasks (list[PipelineTask]): A list of PipelineTask objects representing the tasks in the
            DAG.

    Returns:
        dict[str, str]: A dictionary containing the tags.

    """
    metadata = config.get("metadata", {})
    tags = metadata.get("tags", [])
    tags.append(Tag(value=metadata.get("data_territory", "UK"), type="data territory"))

    tags.append(Tag(value=config.get("load_type", "FULL"), type="load type"))

    tags.append(Tag(value=config.get("platform", "kosmo"), type="platform"))
    for task in tasks:
        if hasattr(task, "env"):
            task_platform = task.env.get("platform")
            task_country = task.env.get("metadata", {}).get("data_territory")
            if task_platform:
                platform_tag = Tag(value=task_platform, type="platform")
                if platform_tag not in tags:
                    tags.append(platform_tag)
            if task_country:
                country_tag = Tag(value=task_country, type="data territory")
                if country_tag not in tags:
                    tags.append(country_tag)

    tags.append(
        Tag(value=config.get("classification", "PROTECTED"), type="classification")
    )
    return tags
