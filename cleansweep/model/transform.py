"""Transform unstructured JSON data to target data model."""

from pathlib import Path
from typing import Any

from cleansweep_core.model.transform import (
    Transformer,  # pyright: ignore[reportPrivateImportUsage]
)

from cleansweep.model.network import (
    CloudStorageUrl,
    FileUrl,
    convert_to_url,
    isurlinstance,
    raw_path,
)
from cleansweep.utils.io import gcs_to_temp


def transform_to_model(
    mapping_path: CloudStorageUrl | FileUrl,
    document_path: CloudStorageUrl | FileUrl,
    root: str | None = None,
) -> list[dict[str, Any]]:
    """Transform a document using a given mapping and return the result as a list of JSON objects.

    Args:
        mapping_path (CloudStorageUrl | FileUrl): The path to the mapping file, which can be a
            cloud storage URL or a local file URL.
        document_path (CloudStorageUrl | FileUrl): The path to the document file, which can be a
            cloud storage URL or a local file URL.
        root (str | None, optional): The root element to be used in the transformation. Defaults to
            None.

    Returns:
        list[dict[str, Any]]: A list of JSON objects resulting from the transformation.

    """
    if isinstance(mapping_path, str):
        mapping_path = convert_to_url(mapping_path)

    if isurlinstance(mapping_path, CloudStorageUrl):
        mapping = gcs_to_temp(raw_path(mapping_path))
    else:
        mapping = Path(raw_path(mapping_path))

    if isinstance(document_path, str):
        document_path = convert_to_url(document_path)

    if isurlinstance(document_path, CloudStorageUrl):
        document = gcs_to_temp(raw_path(document_path))
    else:
        document = Path(raw_path(document_path))

    return Transformer().transform_document(
        mapping.as_posix(), document.as_posix(), root
    )
