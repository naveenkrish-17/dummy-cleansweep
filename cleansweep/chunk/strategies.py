"""Sub-module to load strategies from a configuration file."""

from pathlib import Path

import cleansweep.utils.google.storage as gcs
from cleansweep._types import ChunkingStrategy
from cleansweep.config.load import load
from cleansweep.settings.base import settings

STRATEGIES: dict[str, ChunkingStrategy]


def configure():
    """Configure the strategies."""
    global STRATEGIES  # pylint: disable=global-statement
    STRATEGIES = {}
    config = load(
        f"file://{Path(__file__).parent.joinpath("strategies.yml").as_posix()}",
        "strategies",
        ChunkingStrategy,
    )
    STRATEGIES.update(config)

    config_file = gcs.get_blob(
        settings.config_bucket, "cleansweep/config/strategies.yml"
    )
    if config_file:
        config = load(
            f"gs://{config_file.bucket.name}/{config_file.name}",
            "strategies",
            ChunkingStrategy,
        )
        STRATEGIES.update(config)


configure()
