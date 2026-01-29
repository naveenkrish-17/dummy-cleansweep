"""Module for prompt configuration and generation."""

from pathlib import Path

import cleansweep.utils.google.storage as gcs
from cleansweep._types import Prompt
from cleansweep.config.load import load
from cleansweep.settings.base import settings

PROMPTS: dict[str, Prompt]


def configure():
    """Configure the prompts."""
    global PROMPTS  # pylint: disable=global-statement
    PROMPTS = {}

    config = load(
        f"file://{Path(__file__).parent.joinpath("prompts.yml").as_posix()}",
        "prompts",
        Prompt,
    )
    PROMPTS.update(config)

    config_file = gcs.get_blob(settings.config_bucket, "cleansweep/config/prompts.yml")
    if config_file:
        config = load(
            f"gs://{config_file.bucket.name}/{config_file.name}",
            "prompts",
            Prompt,
        )
        PROMPTS.update(config)


configure()
