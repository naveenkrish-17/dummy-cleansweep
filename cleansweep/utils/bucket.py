"""Utility functions for working with buckets."""

import re
from typing import Literal

BUCKET_PATTERN = "skygenai-%s-%s-ir-%s"


def get_bucket_description(
    bucket_type: Literal[
        "arc",
        "lan",
        "pre",
        "poc",
        "pub",
        "rej",
        "san",
        "stg",
        "out",
        "tmp",
        "utl",
    ],
    name: str,
    platform: str | None = None,
):
    """Get the bucket description for the given bucket type.

    Args:
        bucket_type (str): The type of the bucket.
        name (str): The name of the application.
        platform (str, optional): The platform for the bucket.

    """
    description = re.sub(r"[ _]", "-", name)
    return (
        f"{bucket_type}-{platform}-{description}"
        if platform
        else f"{bucket_type}-{description}"
    ).lower()
