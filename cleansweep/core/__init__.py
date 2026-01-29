"""Module providing core functionality."""

__all__ = [
    "read_file_to_dict",
    "documents_to_dataframe",
    "read_documents_file_to_dataframe",
    "write_dataframe_to_avro_file",
    "read_curated_file_to_dataframe",
    "write_dataframe_to_parquet_file",
    "write_dataframe_to_newline_delimited_json_file",
    "write_dict_to_newline_delimited_json_file",
    "get_plugin_module",
    "transform_dataframe_for_write",
    "write_to_storage",
]

import logging
import shutil
from importlib import import_module
from pathlib import Path
from tempfile import mkdtemp
from types import ModuleType

from cleansweep.core.fileio import (
    documents_to_dataframe,
    read_curated_file_to_dataframe,
    read_documents_file_to_dataframe,
    read_file_to_dict,
    transform_dataframe_for_write,
    write_dataframe_to_avro_file,
    write_dataframe_to_newline_delimited_json_file,
    write_dataframe_to_parquet_file,
    write_dict_to_newline_delimited_json_file,
    write_to_storage,
)
from cleansweep.model.network import (
    CloudStorageUrl,
    FileUrl,
    convert_to_url,
    isurlinstance,
    raw_path,
)
from cleansweep.utils.io import gcs_to_temp

logger = logging.getLogger(__name__)
"""Logger for the core module."""


def get_plugin_module(file_url: str | CloudStorageUrl | FileUrl) -> ModuleType:
    """Get the plugin for the file URI.

    Args:
        file_url (Union[str, CloudStorageUrl, FileUrl]): The file URI.

    Returns:
        ModuleType: The plugin module.

    """
    plugin_file = None
    file_path = convert_to_url(file_url)
    if isurlinstance(file_path, CloudStorageUrl):  # pyright: ignore[reportArgumentType]
        plugin_file = gcs_to_temp(raw_path(file_path))

    elif isurlinstance(file_path, FileUrl):  # pyright: ignore[reportArgumentType]
        # copy the file to temp
        temp = mkdtemp()
        source_plugin_file = Path(raw_path(file_path))
        plugin_file = Path(temp).joinpath(source_plugin_file.name)
        shutil.copy(source_plugin_file, plugin_file)

    if plugin_file is None:
        raise ValueError(f"Unsupported file URI: {file_path}")

    plugin_file_for_import = Path(
        Path(__file__).parent.parent, "plugins", plugin_file.name
    )

    plugin_file_for_import.parent.mkdir(parents=True, exist_ok=True)

    if plugin_file_for_import.exists():
        plugin_file_for_import.unlink()

    shutil.move(plugin_file, plugin_file_for_import)
    return import_module(f"cleansweep.plugins.{plugin_file_for_import.stem}")
