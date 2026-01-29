"""Shared validator and helper functions."""

import logging
from typing import Optional, Protocol, runtime_checkable

from cleansweep.core.fileio import create_glob_pattern
from cleansweep.model.network import CloudStorageUrl, convert_to_url
from cleansweep.settings._types import SourceObject
from cleansweep.settings.base import EnvironmentName
from cleansweep.utils.google.storage import select_blob

logger = logging.getLogger(__name__)


@runtime_checkable
class AppSettingsProtocol(Protocol):
    """Protocol defining the structure for application settings.

    Attributes:
        env_name (str): The name of the environment (e.g., development, staging, production).
        run_id (Optional[str]): The unique identifier for a specific run. Can be None if not applicable.
        staging_bucket (str): The name of the staging bucket.

    """

    env_name: EnvironmentName
    run_id: Optional[str]

    @property
    def staging_bucket(self) -> str:
        """The name of the staging bucket."""
        ...  # pylint: disable=unnecessary-ellipsis


def set_input_file_from_latest_blob(
    model: AppSettingsProtocol, source_object: SourceObject
) -> CloudStorageUrl:
    """Set the input file URL from the latest blob in a cloud storage bucket.

    Args:
        model (AppSettingsProtocol): The application settings model containing
            configuration details.
        source_object (SourceObject): The source object containing details such as
            `source_dir`, `extension`, `file_name`, `prefix`, `bucket`, and
            `use_run_id`.

    Returns:
        CloudStorageUrl: The URL of the latest blob in the cloud storage bucket
        that matches the specified pattern.

    Raises:
        AssertionError: If the bucket is not set in either the `source_object`
            or the `model`.
        FileNotFoundError: If no files matching the specified pattern are found
            in the bucket.

    Notes:
        - The bucket name can include the placeholder "ENV", which will be
          replaced with the environment name from the `model`.
        - The function uses a glob pattern to search for files in the bucket
          and retrieves the latest one based on the pattern.

    """
    match_glob = create_glob_pattern(
        directory=source_object.directory,
        extension=source_object.extension,
        run_id=(model.run_id if source_object.use_run_id else None),
        file_name=source_object.file_name,
        prefix=source_object.prefix,
    )

    bucket = source_object.bucket or model.staging_bucket

    assert bucket is not None, "Bucket should be set"

    bucket = bucket.replace("ENV", model.env_name)

    # find the last file in the bucket
    blob = select_blob(bucket, match_glob=match_glob)  # type: ignore
    if blob is None:
        raise FileNotFoundError(f"No files found in {bucket} with {match_glob}")

    uri = convert_to_url(f"gs://{blob.bucket.name}/{blob.name}")
    logger.info(
        "📖 Setting input file to the latest blob in %s/%s: %s",
        source_object.bucket,
        match_glob,
        uri,
    )
    return uri
