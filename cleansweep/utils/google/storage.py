"""Module for interacting with Google Cloud Storage."""

import asyncio
import logging
import re
import warnings
from datetime import datetime
from functools import cache
from pathlib import Path
from re import Pattern
from typing import List, Literal, Sequence, Tuple

from google.cloud.storage import Blob, Client
from pydantic import BaseModel

log = logging.getLogger(__name__)


@cache
def fs(project: str | None = None, token: str | None = None) -> Client:
    """Create and return a Google Cloud Storage client.

    Args:
        project (str, optional): The project ID. Defaults to None.
        token (str, optional): The authentication token. Defaults to None.

    Returns:
        Client: A Google Cloud Storage client.

    """
    return Client(project=project, credentials=token)


class BlobInfo(BaseModel):
    """A representation of a blob in Google Cloud Storage."""

    bucket: str
    contentType: str  # noqa: N815
    crc32c: str | None = None
    ctime: datetime
    etag: str
    eventBasedHold: bool | None = None  # noqa: N815
    generation: str
    id: str
    kind: str
    md5Hash: str | None = None  # noqa: N815
    mediaLink: str  # noqa: N815
    metageneration: str
    mtime: datetime
    name: str
    selfLink: str  # noqa: N815
    size: int
    storageClass: str  # noqa: N815
    temporaryHold: bool | None = None  # noqa: N815
    timeCreated: datetime  # noqa: N815
    timeStorageClassUpdated: datetime  # noqa: N815
    type: str
    updated: datetime

    @property
    def name_and_generation_sorting_key(self) -> Tuple[str, int]:
        """Return a tuple of name and generation for sorting."""
        return self.name, int(self.generation)


def ls(
    path: str,
    project: str | None = None,
    match_glob: str | None = None,
    name_pattern: Pattern | str | None = None,
    token: str | None = None,
) -> List[Blob]:
    """List all blobs in the given bucket that match the provided patterns.

    Args:
        path (str): The path to the bucket.
        project (str | None, optional): The project where the bucket is located.
            If None, the default project is used. Defaults to None.
        match_glob (str | None, optional): A glob pattern to match blob names against.
            If None, all blobs are returned. Defaults to None.
        name_pattern (Pattern | str | None, optional): A regex pattern or string to match blob names
            against. If None, all blobs are returned. Defaults to None.
        token (str | None, optional): The authentication token to use. If None, the default token is
            used.

    Returns:
        List[Blob]: A list of blobs in the bucket that match the provided patterns.

    """
    pattern = (
        re.compile(name_pattern) if isinstance(name_pattern, str) else name_pattern
    )

    def is_name_matching(blob: Blob) -> bool:
        return True if pattern is None else pattern.search(blob.name or "") is not None

    client = fs(project=project, token=token)
    bucket = client.get_bucket(path)
    blobs = bucket.list_blobs(match_glob=match_glob)

    if name_pattern is None:
        return list(blobs)

    return [blob for blob in blobs if is_name_matching(blob)]


async def async_ls(
    path: str,
    project: str | None = None,
    match_glob: str | None = None,
    name_pattern: Pattern | str | None = None,
    token: str | None = None,
) -> List[Blob]:
    """List blobs in a Google Cloud Storage bucket.

    Args:
        path (str): The path to the bucket or directory in Google Cloud Storage.
        project (str | None, optional): The Google Cloud project ID. Defaults to None.
        match_glob (str | None, optional): A glob pattern to filter blob names. Defaults to None.
        name_pattern (Pattern | str | None, optional): A regex pattern or string to match blob names. Defaults to None.
        token (str | None, optional): Authentication token for accessing the storage. Defaults to None.

    Returns:
        List[Blob]: A list of Blob objects matching the specified criteria.

    Raises:
        Any exceptions raised by the underlying `ls` function.

    """
    return await asyncio.to_thread(ls, path, project, match_glob, name_pattern, token)


def read(blob: str | Blob, project: str | None = None, token: str | None = None) -> str:
    """Read the content of a blob from Google Cloud Storage as a string.

    Args:
        blob (str | Blob): The URI of the blob to read or a Blob object. The URI should be in the
            format 'bucket_name/blob_name'.
        project (str, optional): The ID of the Google Cloud project to use. If not specified,
            the default project is used.
        token (str, optional): The authentication token to use. If not specified, no token is used.

    Returns:
        str: The content of the blob as a string.

    """
    if isinstance(blob, str):
        bucket_name, blob_name = _get_blob_path_parts(blob)
        _blob = get_blob(bucket_name, blob_name, project=project, token=token)
        if _blob is None:
            raise FileNotFoundError(f"Blob not found: {blob}")
        blob = _blob

    return blob.download_as_text()


async def async_read(
    blob: str | Blob, project: str | None = None, token: str | None = None
) -> str:
    """Asynchronously reads the content of a blob from Google Cloud Storage as a string.

    This function uses the `google.cloud.storage` library to interact with Google Cloud Storage.
    It creates a `Client` object using the specified project and token, gets the specified blob,
    and reads its content as a string. The read operation is performed in a separate thread.

    Args:
        blob (str | Blob): The URI of the blob to read or a Blob object. The URI should be in the
            format 'bucket_name/blob_name'.
        project (str, optional): The ID of the Google Cloud project to use. If not specified, the
            default project is used.
        token (str, optional): The authentication token to use. If not specified, the default token
            is used.

    Returns:
        str: The content of the blob as a string.

    Raises:
        google.cloud.exceptions.NotFound: If the blob does not exist.
        google.auth.exceptions.DefaultCredentialsError: If the credentials are invalid or not found.

    """
    return await asyncio.to_thread(read, blob, project, token)


def read_lines(
    blob: str | Blob,
    mode: str = "rb",
    project: str | None = None,
    token: str | None = None,
):
    """Read the content of a blob from Google Cloud Storage and splits it into lines.

    This function uses the `google.cloud.storage` library to interact with Google Cloud Storage.
    It creates a `Client` object using the specified project, gets the specified blob,
    and reads its content as a string. Then it splits the string into lines.

    Args:
        blob (str | Blob): The URI of the blob to read or a Blob object. The URI should be in the
            format 'bucket_name/blob_name'.
        mode (str = "rb"): The mode to open the blob in. The default mode is 'rb'. (default: {"rb"}
        project (str, optional): The ID of the Google Cloud project to use. If not specified, the
            default project is used.
        token (str, optional): The authentication token to use. If not specified, the default token
            is used.

    Returns:
        generator: A generator that yields strings, each representing a line in the blob.

    Raises:
        google.cloud.exceptions.NotFound: If the blob does not exist.
        google.auth.exceptions.DefaultCredentialsError: If the credentials are invalid or not found.

    """
    if isinstance(blob, str):
        bucket_name, blob_name = _get_blob_path_parts(blob)
        _blob = get_blob(bucket_name, blob_name, project=project, token=token)
        if _blob is None:
            raise FileNotFoundError(f"Blob not found: {blob}")
        blob = _blob

    with blob.open(mode) as file:
        for line in file:
            yield line


async def async_read_lines(
    blob: Blob,
    mode: str = "rb",
    project: str | None = None,
    token: str | None = None,
):  # pylint: disable=unused-argument
    """Read the content of a blob from Google Cloud Storage and splits it into lines.

    This function uses the `google.cloud.storage` library to interact with Google Cloud Storage.
    It creates a `Client` object using the specified project, gets the specified blob,
    and reads its content as a string. Then it splits the string into lines. The read operation is
        performed in a separate thread.

    Args:
        blob (blob): The URI of the blob to read. The URI should be in the format
            'bucket_name/blob_name'.\
        mode (str, optional): The mode to open the blob in. The default mode is 'rb'. (default:
            {"rb"})
        project (str, optional): The ID of the Google Cloud project to use. If not specified, the
            default project is used.
        token (str, optional): The authentication token to use. If not specified, the default token
            is used.

    Returns:
        generator: A generator that yields strings, each representing a line in the blob.

    """

    async def async_lines(file):
        while True:
            line = await asyncio.to_thread(file.readline)
            if not line:
                break
            yield line

    with blob.open(mode) as file:
        async for line in async_lines(file):
            yield line


def get_blob(
    bucket_name: str,
    blob_name: str,
    project: str | None = None,
    token: str | None = None,
) -> Blob | None:
    """Get Blob object representing the blob in Google Cloud Storage.

    Args:
        bucket_name (str): The name of the bucket where the blob is located.
        blob_name (str): The name of the blob to get information about.
        project (str, optional): The ID of the Google Cloud project to use. If not specified, the
            default project is used.
        token (str, optional): The authentication token to use. If not specified, the default token
            is used.

    Returns:
        blob : Blob object representing the blob in Google Cloud Storage.

    """
    storage_client = fs(project=project, token=token)
    if not isinstance(blob_name, str):
        raise TypeError("blob_name must be a string")

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob(blob_name)

    return blob


def _get_blob_path_parts(uri: str) -> tuple[str, str]:
    """Get the bucket and blob name from a blob URI.

    The blob URI should be in the format 'bucket_name/blob_name'.

    Args:
        uri (str): The URI of the blob.

    Returns:
        tuple: A tuple containing the bucket name and the blob name.

    """
    blob_path = Path(uri)
    bucket = blob_path.parts[0]
    blob = str(blob_path.relative_to(bucket))
    return bucket, blob


def write(
    content: str,
    blob: str | Blob,
    project: str | None = None,
    content_type: str | None = None,
    token: str | None = None,
) -> Blob:
    """Write the content to a blob in Google Cloud Storage.

    This function uses the `google.cloud.storage` library to interact with Google Cloud Storage.
    It creates a `Client` object using the specified project and token, gets the specified blob,
    and writes the content to the blob.

    Args:
        content (str): The content to write to the blob.
        blob (str | Blob): The URI of the blob to write to or a Blob object. The URI should be in
            the format 'bucket_name/blob_name'.
        project (str, optional): The ID of the Google Cloud project to use. If not specified, the
            default project is used.
        content_type (str, optional): The content type of the blob. If not specified, the default
            content type is used.
        token (str, optional): The authentication token to use. If not specified, the default token
            is used.

    Returns:
        Blob: A Blob object representing the written blob.

    Raises:
        google.auth.exceptions.DefaultCredentialsError: If the credentials are invalid or not found.

    """
    if content_type is None:
        content_type = "text/plain"

    if isinstance(blob, str):
        bucket_name, blob_name = _get_blob_path_parts(blob)
        storage_client = fs(project=project, token=token)
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)

    blob.upload_from_string(content, content_type=content_type)
    return blob


async def async_write(
    content: str,
    blob: str,
    project: str | None = None,
    content_type: str | None = None,
    token: str | None = None,
) -> Blob:
    """Asynchronously writes the content to a blob in Google Cloud Storage.

    This function uses the `google.cloud.storage` library to interact with Google Cloud Storage.
    It creates a `Client` object using the specified project and token, gets the specified blob,
    and writes the content to the blob. The write operation is performed in a separate thread.

    Args:
        content (str): The content to write to the blob.
        blob (str | Blob): The URI of the blob to write to or a Blob object. The URI should be in
            the format 'bucket_name/blob_name'.
        project (str, optional): The ID of the Google Cloud project to use. If not specified, the
            default project is used.
        content_type (str, optional): The content type of the blob. If not specified, the default
            content type is used.
        token (str, optional): The authentication token to use. If not specified, the default token
            is used.

    Returns:
        Blob: A Blob object representing the written blob.

    Raises:
        google.auth.exceptions.DefaultCredentialsError: If the credentials are invalid or not found.

    """
    return await asyncio.to_thread(write, content, blob, project, content_type, token)


def delete(
    blob: str | Blob,
    project: str | None = None,
    token: str | None = None,
) -> None:
    """Delete a blob from Google Cloud Storage.

    This function uses the `google.cloud.storage` library to interact with Google Cloud Storage.
    It creates a `Client` object using the specified project and token, gets the specified blob,
    and deletes it.

    Args:
        blob (str | Blob): The URI of the blob to delete or a Blob object. The URI should be in the
            format 'bucket_name/blob_name'.
        project (str, optional): The ID of the Google Cloud project to use. If not specified, the
            default project is used.
        token (str, optional): The authentication token to use. If not specified, the default token
            is used.

    Raises:
        google.cloud.exceptions.NotFound: If the blob does not exist.
        google.auth.exceptions.DefaultCredentialsError: If the credentials are invalid or not found.

    """
    if isinstance(blob, str):
        bucket_name, blob_name = _get_blob_path_parts(blob)
        _blob = get_blob(bucket_name, blob_name, project=project, token=token)
        if _blob is None:
            raise ValueError(f"Blob {blob} not found in bucket {bucket_name}")
        blob = _blob

    blob.delete()


async def async_delete(
    blob: str | Blob,
    project: str | None = None,
    token: str | None = None,
) -> None:
    """Asynchronously deletes a blob from Google Cloud Storage.

    This function uses the `google.cloud.storage` library to interact with Google Cloud Storage.
    It creates a `Client` object using the specified project and token, gets the specified blob,
    and deletes it. The delete operation is performed in a separate thread.

    Args:
        blob (str | Blob): The URI of the blob to delete or a Blob object. The URI should be in the
            format 'bucket_name/blob_name'.
        project (str, optional): The ID of the Google Cloud project to use. If not specified, the
            default project is used.
        token (str, optional): The authentication token to use. If not specified, the default token
            is used.

    Raises:
        google.cloud.exceptions.NotFound: If the blob does not exist.
        google.auth.exceptions.DefaultCredentialsError: If the credentials are invalid or not found.

    """
    return await asyncio.to_thread(delete, blob, project, token)


def copy(
    source_blob: str | Blob,
    destination_blob: str | Blob,
    if_generation_match: int | None = None,
    project: str | None = None,
    token: str | None = None,
) -> Blob:
    """Copy a blob from a source location to a destination location within Google Cloud Storage.

    Args:
        source_blob (str | Blob): The source blob to copy. Can be a blob path string or a Blob object.
        destination_blob (str | Blob): The destination blob path or Blob object where the source
            blob will be copied.
        if_generation_match (int | None, optional): Precondition for the destination blob's generation.
            If set, the copy will only occur if the destination blob's generation matches this value.
        project (str | None, optional): The GCP project ID. If not provided, defaults to the
            configured project.
        token (str | None, optional): Optional authentication token for accessing GCP resources.

    Returns:
        Blob: The copied destination Blob object.

    Raises:
        ValueError: If the source blob does not exist in the specified bucket.

    """
    source_bucket_name = source_blob_name = None
    if isinstance(source_blob, str):
        source_bucket_name, source_blob_name = _get_blob_path_parts(source_blob)
        _blob = get_blob(
            source_bucket_name, source_blob_name, project=project, token=token
        )
        if _blob is None:
            raise ValueError(
                f"Blob {source_blob} not found in bucket {source_bucket_name}"
            )
        source_blob = _blob
    else:
        source_blob_name = source_blob.name
        source_bucket_name = source_blob.bucket.name

    source_bucket = fs(project=project, token=token).get_bucket(source_bucket_name)

    destination_bucket_name = destination_blob_name = None
    if isinstance(destination_blob, str):
        destination_bucket_name, destination_blob_name = _get_blob_path_parts(
            destination_blob
        )
        destination_blob = str(destination_blob_name)
    else:
        destination_blob_name = destination_blob.name
        destination_bucket_name = destination_blob.bucket.name

    destination_generation_match_precondition = if_generation_match
    target_bucket = fs(project=project, token=token).get_bucket(destination_bucket_name)

    dest_blob = source_bucket.copy_blob(
        source_blob,
        target_bucket,
        destination_blob_name,
        if_generation_match=destination_generation_match_precondition,
    )

    return dest_blob


async def async_copy(
    source_blob: str | Blob,
    destination_blob: str | Blob,
    if_generation_match: int | None = None,
    project: str | None = None,
    token: str | None = None,
) -> Blob:
    """Copy a blob from a source location to a destination location within Google Cloud Storage.

    Args:
        source_blob (str | Blob): The source blob to copy. Can be a blob path string or a Blob object.
        destination_blob (str | Blob): The destination blob path or Blob object where the source
            blob will be copied.
        if_generation_match (int | None, optional): Precondition for the destination blob's generation.
            If set, the copy will only occur if the destination blob's generation matches this value.
        project (str | None, optional): The GCP project ID. If not provided, defaults to the
            configured project.
        token (str | None, optional): Optional authentication token for accessing GCP resources.

    Returns:
        Blob: The copied destination Blob object.

    Raises:
        ValueError: If the source blob does not exist in the specified bucket.

    """
    return await asyncio.to_thread(
        copy, source_blob, destination_blob, if_generation_match, project, token
    )


def move(
    source_blob: str | Blob,
    destination_blob: str | Blob,
    if_generation_match: int | None = None,
    project: str | None = None,
    token: str | None = None,
):
    """Move a blob in Google Cloud Storage.

    This function uses the `google.cloud.storage` library to interact with Google Cloud Storage.
    It creates a `Client` object using the specified project and token, gets the specified source
    and destination blobs, and moves the source blob to the destination.

    Args:
        source_blob (str | Blob): The URI of the source blob or a Blob object. The URI should be in
            the format 'bucket_name/blob_name'.
        destination_blob (str | Blob): The URI of the destination blob or a Blob object. The URI
            should be in the format 'bucket_name/blob_name'.
        if_generation_match: int | None, optional: Precondition for the destination blob's generation.
        project (str, optional): The ID of the Google Cloud project to use. If not specified, the
            default project is used.
        token (str, optional): The authentication token to use. If not specified, the default token
            is used.

    Returns:
        Blob: A Blob object representing the moved blob.

    Raises:
        google.cloud.exceptions.NotFound: If the source blob does not exist.
        google.auth.exceptions.DefaultCredentialsError: If the credentials are invalid or not found.

    """
    if isinstance(source_blob, str):
        if isinstance(destination_blob, str):
            copy(source_blob, destination_blob, if_generation_match, project, token)
            delete(blob=source_blob, project=project, token=token)

    return log.info("gs://%s moved to gs://%s", source_blob, destination_blob)


def upload(
    file_path: str,
    blob: str | Blob,
    project: str | None = None,
    content_type: str | None = None,
    token: str | None = None,
) -> Blob:
    """Upload a file to a blob in Google Cloud Storage.

    This function uses the `google.cloud.storage` library to interact with Google Cloud Storage.
    It creates a `Client` object using the specified project and token, gets the specified blob,
    and uploads the file to the blob.

    Args:
        file_path (str): The path to the file to upload.
        blob (str | Blob): The URI of the blob to upload to or a Blob object. The URI should be in
            the format 'bucket_name/blob_name'.
        project (str, optional): The ID of the Google Cloud project to use. If not specified, the
            default project is used.
        content_type (str, optional): The content type of the blob. If not specified, the default
            content type is used.
        token (str, optional): The authentication token to use. If not specified, the default token
            is used.

    Returns:
        Blob: A Blob object representing the uploaded blob.

    Raises:
        google.auth.exceptions.DefaultCredentialsError: If the credentials are invalid or not found.

    """
    if isinstance(blob, str):
        bucket_name, blob_name = _get_blob_path_parts(blob)
        storage_client = fs(project=project, token=token)
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)

    blob.upload_from_filename(file_path, content_type=content_type)
    return blob


def download(
    blob: str | Blob,
    file_path: str,
    project: str | None = None,
    token: str | None = None,
) -> str:
    """Download a blob from Google Cloud Storage to a local file.

    Args:
        blob (str | Blob): The blob to download. Can be a string path or a Blob object.
        file_path (str): The local file path where the blob will be saved.
        project (str | None, optional): The GCP project name. Defaults to None.
        token (str | None, optional): Authentication token for accessing the blob. Defaults to None.

    Returns:
        str: The path to the downloaded file.

    Raises:
        FileNotFoundError: If the specified blob does not exist.

    """
    if isinstance(blob, str):
        bucket_name, blob_name = _get_blob_path_parts(blob)
        _blob = get_blob(bucket_name, blob_name, project=project, token=token)
        if _blob is None:
            raise FileNotFoundError(f"Blob not found: {blob}")
        blob = _blob

    blob.download_to_filename(file_path)

    return file_path


def select_blob(
    bucket: str,
    match_glob: str | None = None,
    name_pattern: re.Pattern | str | None = None,
    project: str | None = None,
    token: str | None = None,
    sort_by: Sequence[
        Literal["name", "generation", "size", "time_created", "updated"]
    ] = ("name", "generation"),
    descending: bool = True,
) -> Blob:
    """Get the latest (or earliest) blob in a Google Cloud Storage bucket.

    This function uses the `google.cloud.storage` library to interact with Google Cloud Storage.

    Args:
        bucket (str): The path to the bucket.
        match_glob (str, optional): A glob pattern to match blob names against. If not specified,
            all blobs are returned.
        name_pattern (Pattern | str, optional): A regex pattern or string to match blob names
            against. If not specified, all blobs are returned.
        project (str, optional): The ID of the Google Cloud project to use. If not specified, the
            default project is used.
        token (str, optional): The authentication token to use. If not specified, the default token
            is used.
        sort_by (Sequence[Literal["name", "generation", "size", "time_created", "updated"]], optional):
            A sequence defining the sorting order of blobs. The default is ("name", "generation").
            - "name" (str): Sorts alphabetically by blob name.
            - "generation" (int): Sorts by the generation of the blob.
            - "size" (int): Sorts by the blob size in bytes.
            - "time_created" (datetime): Sorts by when the blob was created.
            - "updated" (datetime): Sorts by when the blob was last updated.
        descending (bool, optional): Whether to sort in descending order (latest first).
            Default is `True`. If `False`, returns the earliest blob.

    Returns:
        Blob: A Blob object representing the latest or earliest blob in the bucket.

    Raises:
        FileNotFoundError: If there are no matching files in the bucket.

    """
    if not (
        all(
            key in ["name", "generation", "size", "time_created", "updated"]
            for key in sort_by
        )
    ):
        raise ValueError(
            "Invalid sort key provided. Valid keys are 'name', 'generation', 'size', 'time_created', and 'updated'."
        )

    blobs = ls(
        bucket,
        project=project,
        token=token,
        match_glob=match_glob,
        name_pattern=name_pattern,
    )

    if not blobs:
        raise FileNotFoundError(f"There are no matching files in the bucket {bucket}.")

    def sort_key(blob: Blob):
        key_values = []
        for key in sort_by:
            value = getattr(blob, key)
            if value is None:
                # If the attribute is None, use a default value for comparison
                if key in {"generation", "size"}:
                    value = -1
                elif key in {"time_created", "updated"}:
                    value = 0
                else:
                    value = ""
            elif isinstance(value, datetime):
                # Convert datetime attributes to timestamps for proper comparison
                value = value.timestamp()
            key_values.append(value)
        return tuple(key_values)

    # Sort based on ascending or descending order
    return max(blobs, key=sort_key) if descending else min(blobs, key=sort_key)


async def async_select_blob(
    bucket: str,
    match_glob: str | None = None,
    name_pattern: re.Pattern | str | None = None,
    project: str | None = None,
    token: str | None = None,
    sort_by: Sequence[
        Literal["name", "generation", "size", "time_created", "updated"]
    ] = ("name", "generation"),
    descending: bool = True,
) -> Blob:
    """Get the latest (or earliest) blob in a Google Cloud Storage bucket.

    This function uses the `google.cloud.storage` library to interact with Google Cloud Storage.

    Args:
        bucket (str): The path to the bucket.
        match_glob (str, optional): A glob pattern to match blob names against. If not specified,
            all blobs are returned.
        name_pattern (Pattern | str, optional): A regex pattern or string to match blob names
            against. If not specified, all blobs are returned.
        project (str, optional): The ID of the Google Cloud project to use. If not specified, the
            default project is used.
        token (str, optional): The authentication token to use. If not specified, the default token
            is used.
        sort_by (Sequence[Literal["name", "generation", "size", "time_created", "updated"]], optional):
            A sequence defining the sorting order of blobs. The default is ("name", "generation").
            - "name" (str): Sorts alphabetically by blob name.
            - "generation" (int): Sorts by the generation of the blob.
            - "size" (int): Sorts by the blob size in bytes.
            - "time_created" (datetime): Sorts by when the blob was created.
            - "updated" (datetime): Sorts by when the blob was last updated.
        descending (bool, optional): Whether to sort in descending order (latest first).
            Default is `True`. If `False`, returns the earliest blob.

    Returns:
        Blob: A Blob object representing the latest or earliest blob in the bucket.

    Raises:
        FileNotFoundError: If there are no matching files in the bucket.

    """
    return await asyncio.to_thread(
        select_blob,
        bucket,
        match_glob,
        name_pattern,
        project,
        token,
        sort_by,
        descending,
    )


def get_latest_blob(
    path: str,
    match_glob: str | None = None,
    name_pattern: Pattern | str | None = None,
    project: str | None = None,
    token: str | None = None,
):
    """Get the latest blob in a Google Cloud Storage bucket.

    This function uses the `google.cloud.storage` library to interact with Google Cloud Storage.

    Args:
        path (str): The path to the bucket.
        match_glob (str, optional): A glob pattern to match blob names against. If not specified,
            all blobs are returned.
        name_pattern (Pattern | str, optional): A regex pattern or string to match blob names
            against. If not specified, all blobs are returned.
        project (str, optional): The ID of the Google Cloud project to use. If not specified, the
            default project is used.
        token (str, optional): The authentication token to use. If not specified, the default token
            is used.
        sort_by (Sequence[Literal["name", "generation", "size", "time_created", "updated"]], optional):
            A sequence defining the sorting order of blobs. The default is ("name", "generation").
            - "name" (str): Sorts alphabetically by blob name.
            - "generation" (int): Sorts by the generation of the blob.
            - "size" (int): Sorts by the blob size in bytes.
            - "time_created" (datetime): Sorts by when the blob was created.
            - "updated" (datetime): Sorts by when the blob was last updated.
        descending (bool, optional): Whether to sort in descending order (latest first).
            Default is `True`. If `False`, returns the earliest blob.

    Returns:
        Blob: A Blob object representing the latest or earliest blob in the bucket.

    Raises:
        FileNotFoundError: If there are no matching files in the bucket.

    """
    warnings.warn(
        "The `get_latest_blob` function is deprecated. Use `select_blob` instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    return select_blob(
        bucket=path,
        match_glob=match_glob,
        name_pattern=name_pattern,
        project=project,
        token=token,
    )


async def async_get_latest_blob(
    path: str,
    match_glob: str | None = None,
    name_pattern: Pattern | str | None = None,
    project: str | None = None,
    token: str | None = None,
):
    """Asynchronously get the latest blob in a Google Cloud Storage bucket.

    This function uses the `google.cloud.storage` library to interact with Google Cloud Storage.

    Args:
        path (str): The path to the bucket.
        match_glob (str, optional): A glob pattern to match blob names against. If not specified,
            all blobs are returned.
        name_pattern (Pattern | str, optional): A regex pattern or string to match blob names
            against. If not specified, all blobs are returned.
        project (str, optional): The ID of the Google Cloud project to use. If not specified, the
            default project is used.
        token (str, optional): The authentication token to use. If not specified, the default token
            is used.
        sort_by (Sequence[Literal["name", "generation", "size", "time_created", "updated"]], optional):
            A sequence defining the sorting order of blobs. The default is ("name", "generation").
            - "name" (str): Sorts alphabetically by blob name.
            - "generation" (int): Sorts by the generation of the blob.
            - "size" (int): Sorts by the blob size in bytes.
            - "time_created" (datetime): Sorts by when the blob was created.
            - "updated" (datetime): Sorts by when the blob was last updated.
        descending (bool, optional): Whether to sort in descending order (latest first).
            Default is `True`. If `False`, returns the earliest blob.

    Returns:
        Blob: A Blob object representing the latest or earliest blob in the bucket.

    Raises:
        FileNotFoundError: If there are no matching files in the bucket.

    """
    warnings.warn(
        "The `async_get_latest_blob` function is deprecated. Use `async_select_blob` instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    return await async_select_blob(
        path,
        project=project,
        token=token,
        match_glob=match_glob,
        name_pattern=name_pattern,
    )
