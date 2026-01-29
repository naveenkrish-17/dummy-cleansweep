"""The io module provides utility functions for reading and writing Avro and Parquet files."""

__all__ = [
    "gcs_to_temp",
    "gcs_avro_read",
    "avro_read",
    "gcs_parquet_read",
    "parquet_read",
    "gcs_avro_read_lines",
    "avro_read_lines",
    "gcs_parquet_read_lines",
    "parquet_read_lines",
    "avro_write",
    "gcs_avro_write",
    "parquet_write",
    "gcs_parquet_write",
    "move_file",
]

import shutil
from pathlib import Path
from tempfile import TemporaryDirectory, mkdtemp
from typing import Any, Generator

import fastavro
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from fastavro.types import AvroMessage

import cleansweep.utils.google.storage as gcs

# region file helper functions


def gcs_to_temp(file_path: str) -> Path:
    """Download the given file from Google Cloud Storage to a temporary directory.

    Args:
        file_path (str): The URL of the file to download.

    Returns:
        Path: The path to the downloaded file.

    """
    temp = (
        mkdtemp()
    )  # TemporaryDirectory is removed after the context manager exits, mkdtemp is not
    pth = Path(file_path)
    target = Path(temp, pth.name)
    gcs.download(pth.as_posix(), target.as_posix())
    return target


def gcs_avro_read(file_path: str) -> list[AvroMessage]:
    """Read the contents of the given file.

    Args:
        file_path (str): The path to the file to read.

    Returns:
        list[dict]: The contents of the file.

    """
    return avro_read(gcs_to_temp(file_path))


def avro_read(file_path: str | Path) -> list[AvroMessage]:
    """Read the contents of the given file.

    Args:
        file_path (Union[str, Path]): The path to the file to read.

    Returns:
        list[dict]: The contents of the file.

    Raises:
        FileNotFoundError: If the file is not found.
        FileNotFoundError: If the file path is invalid.

    """
    if not isinstance(file_path, Path):
        file_path = Path(file_path)

    with file_path.open("rb") as src:
        reader = fastavro.reader(src)
        if not isinstance(reader, list):
            reader = list(reader)

        if not all(isinstance(record, dict) for record in reader):
            raise ValueError(
                "Invalid values returned from reader. Expected a list of dictionaries."
            )
        return [record for record in reader]


def gcs_parquet_read(file_path: str, cols: list[str] | None = None) -> pa.Table:
    """Read the contents of the given file.

    Args:
        file_path (str): The path to the file to read.
        cols (list[str]): The columns to read. Defaults to None.

    Returns:
        Table: The contents of the file.

    """
    return parquet_read(gcs_to_temp(file_path), cols=cols)


def parquet_read(file_path: str | Path, cols: list[str] | None = None) -> pa.Table:
    """Read the contents of the given file.

    Args:
        file_path (Union[str, Path]): The path to the file to read.
        cols (list[str]): The columns to read. Defaults to None.

    Returns:
        Table: The contents of the file.

    Raises:
        FileNotFoundError: If the file is not found.
        FileNotFoundError: If the file path is invalid.

    """
    if not isinstance(file_path, Path):
        file_path = Path(file_path)

    err = None
    try:
        return pq.read_table(file_path, columns=cols)
    except pa.ArrowInvalid as e:
        # capture ArrowInvalid error separately
        err = e

    try:
        # Read the file metadata to get unique column names in case we have duplicate columns.
        # 1. Open the file metadata
        pf = pq.ParquetFile(file_path.as_posix())
        schema = pf.schema_arrow
        all_names = schema.names

        # 2. Build a list of unique names in order
        seen = set()
        unique_cols = []
        for name in all_names:
            if name not in seen:
                seen.add(name)
                unique_cols.append(name)

        # 3. Read only those columns
        return pf.read(columns=unique_cols)
    except Exception as e:  # pylint: disable=broad-except
        if err:
            raise ExceptionGroup(
                "Failed to read Parquet file due to ArrowInvalid and other exceptions.",
                [err, e],
            ) from err
        else:
            raise e


def gcs_avro_read_lines(file_path: str | Path) -> Generator[AvroMessage, Any, Any]:
    """Read the contents of the given file.

    Args:
        file_path (Union[str, Path]): The path to the file to read.

    Returns:
        Generator[dict, None, None]: The contents of the file.

    Raises:
        FileNotFoundError: If the file is not found.
        FileNotFoundError: If the file path is invalid.

    """
    if isinstance(file_path, Path):
        file_path = file_path.as_posix()

    return avro_read_lines(gcs_to_temp(file_path))


def avro_read_lines(file_path: str | Path) -> Generator[AvroMessage, Any, Any]:
    """Read the contents of the given file.

    Args:
        file_path (Union[str, Path]): The path to the file to read.

    Returns:
        Generator[dict, None, None]: The contents of the file.

    Raises:
        FileNotFoundError: If the file is not found.
        FileNotFoundError: If the file path is invalid.

    """
    if not isinstance(file_path, Path):
        file_path = Path(file_path)

    with file_path.open("rb") as src:
        reader = fastavro.reader(src)
        if not isinstance(reader, list):
            reader = list(reader)

        if not all(isinstance(record, dict) for record in reader):
            raise ValueError(
                "Invalid values returned from reader. Expected a list of dictionaries."
            )
        for record in reader:
            yield record


def gcs_parquet_read_lines(file_path: str | Path) -> Generator[dict, None, None]:
    """Read the contents of the given file.

    Args:
        file_path (Union[str, Path]): The path to the file to read.

    Returns:
        Generator[dict, None, None]: The contents of the file.

    """
    if isinstance(file_path, Path):
        file_path = file_path.as_posix()

    return parquet_read_lines(gcs_to_temp(file_path))


def parquet_read_lines(file_path: str | Path) -> Generator[dict, None, None]:
    """Read the contents of the given file.

    Args:
        file_path (Union[str, Path]): The path to the file to read.

    Returns:
        Generator[dict, None, None]: The contents of the file.

    Raises:
        FileNotFoundError: If the file is not found.
        FileNotFoundError: If the file path is invalid.

    """
    if not isinstance(file_path, Path):
        file_path = Path(file_path)

    table = pq.read_table(file_path)
    for record in table.to_pylist():
        yield record


def avro_write(file_path: str, records: list[dict], schema: dict) -> None:
    """Write the given records to the given file.

    Args:
        file_path (str): The path to the file to write.
        records (list[dict]): The records to write.
        schema (dict): The schema of the records.

    Raises:
        FileNotFoundError: If the file path is invalid.

    """
    parsed_schema = fastavro.parse_schema(schema)

    with open(file_path, "wb") as dst:
        fastavro.writer(
            dst, records=records, schema=parsed_schema
        )  # pylint: disable=expression-not-assigned


def gcs_avro_write(file_path: str, records: list[dict], schema: dict) -> None:
    """Write the given records to the given file.

    Args:
        file_path (str): The path to the file to write.
        records (list[dict]): The records to write.
        schema (dict): The schema of the records.

    Raises:
        FileNotFoundError: If the file path is invalid.

    """
    with TemporaryDirectory() as temp:
        temp_file = Path(temp).joinpath(Path(file_path).name).as_posix()
        avro_write(
            temp_file,
            records,
            schema,
        )
        gcs.upload(temp_file, file_path)


def parquet_write(
    file_path: str, table: pa.Table | pd.DataFrame | list[dict] | dict[str, list]
) -> None:
    """Write the given table to the given file.

    Args:
        file_path (str): The path to the file to write.
        table (Union[Table, DataFrame, list[dict], dict[str, list]]): The table to write.

    Raises:
        FileNotFoundError: If the file path is invalid.

    """
    if isinstance(table, pd.DataFrame):
        table = pa.Table.from_pandas(table, preserve_index=False)

    if isinstance(table, list):
        table = pa.Table.from_pylist(table)

    if isinstance(table, dict):
        table = pa.table(table)

    pq.write_table(table, file_path)


def gcs_parquet_write(
    file_path: str, table: pa.Table | pd.DataFrame | list[dict] | dict[str, list]
) -> None:
    """Write the given table to the given file.

    Args:
        file_path (str): The path to the file to write.
        table (Union[Table, DataFrame, list[dict], dict[str, list]]): The table to write.

    Raises:
        FileNotFoundError: If the file path is invalid.

    """
    with TemporaryDirectory() as temp:
        temp_file = Path(temp).joinpath(Path(file_path).name).as_posix()
        parquet_write(
            temp_file,
            table,
        )
        gcs.upload(temp_file, file_path)


def move_file(source_path: Path, destination_path: Path):
    """Move a file from the source path to the destination path.

    Args:
        source_path (Path): The path to the source file that needs to be moved.
        destination_path (Path): The path to the destination where the file should be moved.

    Raises:
        FileNotFoundError: If the source file does not exist.

    """
    # Ensure the source file exists
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    # Create destination directory if it doesn't exist
    destination_dir = destination_path.parent
    if destination_dir and not destination_dir.exists():
        destination_dir.mkdir(parents=True)

    # Move the file
    shutil.move(source_path, destination_path)


# endregion
