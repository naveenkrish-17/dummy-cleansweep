"""File I/O module."""

import json
import logging
import re
import sys
from copy import deepcopy
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Literal, cast

import pandas as pd
import pyarrow as pa
import yaml
from jsonpath_ng.ext import parse
from pandas.io import json as pd_json
from pydantic_core import Url, ValidationError

import cleansweep.utils.google.storage as gcs
from cleansweep.exceptions import PipelineError
from cleansweep.model.core import DocumentModel
from cleansweep.model.network import (
    CloudStorageUrl,
    FileUrl,
    PathLikeUrl,
    convert_to_url,
    file_type,
    isurlinstance,
    raw_path,
)
from cleansweep.utils.io import (
    avro_read,
    avro_write,
    gcs_avro_read,
    gcs_avro_write,
    gcs_parquet_read,
    gcs_parquet_write,
    gcs_to_temp,
    move_file,
    parquet_read,
    parquet_write,
)

logger = logging.getLogger(__name__)
"""Logger for the module."""
SUPPORTED_FILE_TYPES = ["json", "ndjson", "avro", "parquet", "yaml", "csv", "jsonl"]
"""Supported file types."""

# set copy on write mode - will become default in pandas 3.0
pd.options.mode.copy_on_write = True


def read_file_to_dict(
    file_url: str | CloudStorageUrl | FileUrl, path: str | None = None
) -> list[dict[str, Any]]:
    """Read the contents of a file containing semi-structured data into a list of dictionaries.

    The file can be in JSON, NDJSON, AVRO or PARQUET formats.

    Args:
        file_url (Union[str, CloudStorageUrl, FileUrl]): The file containing the data.
        path (str, optional): The JSON path to extract from the contents. Defaults to None.

    Returns:
        list[dict]: The list of dictionaries containing the data.

    """

    def read(path: str) -> str:
        with open(path, "r", encoding="utf-8") as file:
            return file.read()

    def gcs_read_csv(path: str) -> pd.DataFrame:
        tmp = gcs_to_temp(path)
        return pd.read_csv(tmp, encoding="utf-8")

    def recursively_jsonify(
        obj: Any,
    ):
        """Recursively convert all dictionaries in a list to JSON strings."""
        if isinstance(obj, list):
            return [recursively_jsonify(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: recursively_jsonify(value) for key, value in obj.items()}

        try:
            return json.loads(obj)
        except (TypeError, json.JSONDecodeError):
            return obj

    file_url, ext, url_type = extract_file_details(file_url)

    read_function: Callable | None = None

    match (url_type, ext):
        case "gs", "avro":
            read_function = gcs_avro_read
        case "gs", "parquet":
            read_function = gcs_parquet_read
        case ("gs", "json") | ("gs", "ndjson") | ("gs", "yaml"):
            read_function = gcs.read
        case "gs", "csv":
            read_function = gcs_read_csv

        case "file", "avro":
            read_function = avro_read
        case "file", "parquet":
            read_function = parquet_read
        case ("file", "json") | ("file", "ndjson") | ("file", "yaml"):
            read_function = read
        case "file", "csv":
            read_function = pd.read_csv

        case _:
            read_function = None

    if read_function is None:
        raise NotImplementedError(
            f"The file type {ext} for {url_type} is not supported."
        )

    source = read_function(raw_path(file_url))

    content = source
    match (ext):
        case "parquet":
            source = cast(pa.Table, source)
            content = source.to_pylist()

        case "json":
            assert isinstance(source, str), "JSON read function should return a string"
            content = json.loads(source)

        case "ndjson":
            assert isinstance(
                source, str
            ), "NDJSON read function should return a string"
            content = [json.loads(line) for line in source.split("\n") if line.strip()]

        case "yaml":
            assert isinstance(source, str), "YAML read function should return a string"
            content = yaml.safe_load(source)

        case "csv":
            assert isinstance(
                source, pd.DataFrame
            ), "CSV read function should return a DataFrame"
            content = source.to_dict(orient="records")
            content = recursively_jsonify(content)
            content = cast(list[dict[str, Any]], content)  # cast for type checking

        case _:
            pass

    if not isinstance(content, list):
        content = [content]

    if path is not None:
        try:
            content = [parse(path).find(document)[0].value for document in content]
        except IndexError as exc:
            raise ValueError(
                f"The path {path} is not valid for contents of the file {file_url}."
            ) from exc

        # if resulting content is a list of lists, flatten it
        if all(isinstance(item, list) for item in content):
            content = [item for sublist in content for item in sublist]

    assert all(
        isinstance(item, dict) for item in content
    ), "All items in content must be dictionaries"
    return content


def extract_file_details(file_url):
    """Extract and normalize file details from a given file URL or string path.

    Args:
        file_url (str or urllib.parse.ParseResult): The file URL or string path to extract details from.

    Returns:
        tuple: A tuple containing:
            - file_url (urllib.parse.ParseResult): The parsed file URL.
            - ext (str): The normalized file extension (e.g., 'yaml', 'ndjson').
            - url_type (str): The URL scheme (e.g., 'file', 'http').

    Raises:
        NotImplementedError: If the file extension is not in the list of supported file types.

    Notes:
        - Converts '.yml' to 'yaml', '.jsonl' to 'ndjson', and '.json' with '.nd' prefix to 'ndjson'.
        - Requires SUPPORTED_FILE_TYPES and convert_to_url to be defined elsewhere.

    """
    if isinstance(file_url, str):
        file_url = convert_to_url(file_url)

    path_from_url = Path(file_url.path or "")

    ext = path_from_url.suffix.replace(".", "").lower()

    match (ext):
        case "yml":
            ext = "yaml"
        case "jsonl":
            ext = "ndjson"
        case "json":
            if (
                Path(path_from_url.as_posix().replace(path_from_url.suffix, "")).suffix
                == ".nd"
            ):
                ext = "ndjson"
        case _:
            pass

    if ext not in SUPPORTED_FILE_TYPES:
        raise NotImplementedError(f"The file type {ext} is not supported")

    url_type = file_url.scheme
    return file_url, ext, url_type


def documents_to_dataframe(
    documents: list[dict[str, Any]],
    content_column: (
        Literal["content_full", "content_raw", "html_content"] | None
    ) = None,
) -> tuple[pd.DataFrame, list]:
    """Convert a list of document dictionaries into a pandas DataFrame.

    - The function logs errors encountered during the creation of DocumentModel instances.
    - Datetime columns in the DataFrame are converted to pandas datetime format to handle potential
        timezone issues.

    Args:
        documents (list[dict[str, Any]]): A list of dictionaries where each dictionary
            represents a document.
        content_column (Literal["content_full", "content_raw", "html_content"] | None, optional):
            Specifies the content column to be used in the DataFrame. Defaults to None.

    Returns:
        tuple[pd.DataFrame, list]: A tuple containing the resulting DataFrame and a list of
            documents that caused errors.

    Raises:
        ValidationError: If there is an error creating a DocumentModel from a document.

    """
    output_documents = []
    errors = []
    for document in documents:
        try:
            output_documents.append(DocumentModel(**document))  # type: ignore
        except ValidationError as exc:
            errors.append(document)
            doc_id = document.get("id", "unknown")
            doc_name = document.get("name", "unknown")
            logger.error(
                "Error creating DocumentModel from %s:%s - %s",
                doc_id,
                doc_name,
                exc,
            )

    if errors:
        logger.error("Errors creating DocumentModel from %s documents", len(errors))

    df = pd.DataFrame(documents_to_data(output_documents, content_column))

    # convert datetime columns to pandas datetime, this avoids potential issues with timezones
    # when creating the dataframe schema
    for column_name, _ in df.items():
        if (
            df[column_name]
            .astype(str)
            .str.contains(
                (
                    r"^\d{4,}(?:\-\d{2}){2}(?:T| )\d{2}(?:\:\d{2}){2,}(?:\.\d+)(?:Z|[+=]\d{2}"
                    r"\:\d{2})$"
                ),
                regex=True,
            )
            .any()
        ):
            df[column_name] = pd.to_datetime(df[column_name], utc=True)

    return df, errors


def documents_to_data(
    documents: list[DocumentModel],
    content_column: (
        Literal["content_full", "content_raw", "html_content"] | None
    ) = "content_full",
) -> dict[str, list[dict[str, Any]]]:
    """Convert a list of DocumentModel instances into a dictionary of data.

    Args:
        documents (list[DocumentModel]): A list of DocumentModel instances to be converted.
        content_column (Literal["content_full", "content_raw", "html_content"] | None, optional):
            The attribute of the DocumentModel to be used for the content. Defaults to
            "content_full".

    Returns:
        dict[str, list[dict[str, Any]]]: A dictionary where keys are field names and values
        are lists of field values from the documents. Includes metadata and content fields.

    Raises:
        ValueError: If the specified content_column is not a valid attribute of DocumentModel.

    """
    if content_column is None:
        content_column = "content_full"

    if (
        content_column
        not in DocumentModel.model_fields  # pylint: disable=unsupported-membership-test
        and hasattr(DocumentModel, content_column) is False
    ):
        raise ValueError(f"Invalid content column: {content_column}")

    length_column = {
        "content_full": "length_full",
        "content_raw": "length_raw",
        "html_content": "length_html",
    }.get(content_column, "length_full")

    # Fields with custom logic for inclusion in dataframe
    excluded_fields = {
        "metadata",
        "content",
        "content_full",
        "content_raw",
        "html_content",
        "length_full",
        "length_raw",
        "length_html",
    }
    # Fields with custom names in the dataframe
    name_mapping = {"name": "title"}

    data: dict[str, list[dict[str, Any]]] = {}

    for document in documents:
        fields = deepcopy(document.model_fields)
        fields.update(
            **document.model_computed_fields  # pyright: ignore[reportArgumentType]
        )
        for field in fields:

            if field in excluded_fields:
                continue

            f = name_mapping.get(field, field)

            if f not in data:
                data[f] = []

            data[f].append(getattr(document, field))

    metadata_keys = {
        key
        for document in documents
        for key in list(document.metadata.model_dump().keys())
    }

    for key in metadata_keys:
        k = f"metadata_{key}"
        data[k] = [  # pyright: ignore[reportArgumentType]
            document.metadata.model_dump().get(key) for document in documents
        ]

    # add content column
    data["content"] = [getattr(document, content_column) for document in documents]
    data["length"] = [getattr(document, length_column) for document in documents]

    return data


def read_documents_file_to_dataframe(
    file_url: str | PathLikeUrl,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Read the contents of a file containing DocumentModel data to a dataframe.

    The file should be a JSON or NDJSON file containing representations of DocumentModel data. The
    file can be in JSON, NDJSON, AVRO or PARQUET formats.

    Args:
        file_url (Union[str, PathLikeUrl]): The file containing the DocumentModel data.

    Returns:
        DataFrame: The dataframe containing the DocumentModel data.
        list[DocumentJson]: The list of errors that occurred during the conversion.

    """
    file_path = convert_to_url(file_url)
    if file_type(file_path) not in [
        ".json",
        ".ndjson",
    ]:
        raise NotImplementedError(
            f"The file type {file_type(file_path)} is not supported."
        )

    return documents_to_dataframe(read_file_to_dict(file_path))


def write_dataframe_to_avro_file(
    documents: pd.DataFrame, file_url: str | CloudStorageUrl | FileUrl
):
    """Write the contents of a dataframe to an AVRO file.

    Args:
        documents (DataFrame): The dataframe to write.
        file_url (Union[str, CloudStorageUrl, FileUrl]): The file to write to.

    """
    file_path = convert_to_url(file_url)
    if file_path.suffix != "avro":
        raise NotImplementedError(f"The file type {file_path.suffix} is not supported")

    function = None
    if isurlinstance(file_path, CloudStorageUrl):  # pyright: ignore[reportArgumentType]
        function = gcs_avro_write

    if isurlinstance(file_path, FileUrl):  # pyright: ignore[reportArgumentType]
        function = avro_write

    if function is None:
        raise ValueError(f"Unsupported file URI: {file_path}")

    documents = transform_dataframe_for_write(documents)
    records = documents.to_json(orient="records", date_format="iso")
    if records is None:
        raise ValueError("No records to write")
    serialised_records = json.loads(records)
    schema = pd_json.build_table_schema(documents, index=False, version=False)
    if not isinstance(schema["fields"], list):
        raise ValueError("No fields in the schema")

    for field in schema["fields"]:
        if field["type"] == "integer":
            field["type"] = "int"
        if field["type"] == "datetime":
            field["type"] = "string"
        if field["type"] == "number":
            field["type"] = "float"

    # apply adjustments to the schema to handle arrays and nullable fields
    for column_name, _ in documents.items():

        # check for boolean columns
        if documents[column_name].apply(type).eq(bool).any():  # pyright: ignore
            for field in schema["fields"]:
                if field["name"] == column_name:
                    field["type"] = "boolean"
                    break

        if documents[column_name].apply(type).eq(list).any():  # pyright: ignore

            col_type_list = (
                documents[column_name]
                # .apply(lambda x: [type(i) for y in x for i in y])
                .apply(lambda x: [type(y).__name__ for y in x]).to_list()
            )

            # col_type = type(None)
            # for i in col_type_list:
            #     if len(set(i)) == 1:
            #         col_type = i[0]
            #         break

            col_type_map = {
                "str": "string",
                "bool": "boolean",
                "int": "int",
                "float": "float",
                "datetime": "string",
                "NoneType": "null",
                None: "null",
            }
            # col_type = col_type_map.get(col_type.__name__, col_type)

            # arr_schema = {"type": "array", "items": {"type": col_type}}

            flattened_col_type_set = {i for y in col_type_list for i in y}

            arr_schema = {
                "type": "array",
                "items": [
                    {"type": col_type_map.get(i, "null")}
                    for i in flattened_col_type_set
                ],
            }
            for field in schema["fields"]:
                if field["name"] == column_name:
                    field["type"] = arr_schema
                    break

            # if the column has null values, update the schema to allow for null values

            continue

        if (
            documents[column_name]
            .apply(pd.isnull)
            .any()  # pyright: ignore[reportGeneralTypeIssues]
        ):
            for field in schema["fields"]:
                if field["name"] == column_name:
                    field["type"] = ["null", field["type"]]
                    break

    schema["type"] = "record"
    schema["name"] = "DocumentModel"
    function(raw_path(file_path), serialised_records, schema)


def read_curated_file_to_dataframe(
    file_url: str | CloudStorageUrl | FileUrl,
) -> pd.DataFrame:
    """Read the contents of a curated file to a dataframe.

    A curated file is a file that contains an exported dataframe which has been transformed from
    source or following the Clean or Chunk processing.

    Args:
        file_url (Union[str, CloudStorageUrl, FileUrl]): The file containing the curated data.

    Returns:
        DataFrame: The dataframe containing the curated data.

    """
    file_content = read_file_to_dict(file_url)
    return pd.DataFrame(file_content)


def write_dataframe_to_parquet_file(
    documents: pd.DataFrame, file_url: str | CloudStorageUrl | FileUrl
):
    """Write the contents of a dataframe to a Parquet file.

    Args:
        documents (DataFrame): The dataframe to write.
        file_url (Union[str, CloudStorageUrl, FileUrl]): The file to write to.

    """
    file_path = convert_to_url(file_url)
    if file_type(file_path) != ".parquet":
        raise NotImplementedError(
            f"The file type {file_type(file_path)} is not supported."
        )

    function = None
    if isurlinstance(file_path, CloudStorageUrl):  # pyright: ignore[reportArgumentType]
        function = gcs_parquet_write

    if isurlinstance(file_path, FileUrl):  # pyright: ignore[reportArgumentType]
        function = parquet_write

    if function is None:
        raise ValueError(f"Unsupported file URI: {file_path}")

    documents = transform_dataframe_for_write(documents)

    function(raw_path(file_path), documents)


def write_dataframe_to_newline_delimited_json_file(
    documents: pd.DataFrame, file_url: str | CloudStorageUrl | FileUrl
):
    """Write the contents of a dataframe to a newline-delimited JSON file.

    Args:
        documents (DataFrame): The dataframe to write.
        file_url (Union[str, CloudStorageUrl, FileUrl]): The file to write to.

    """
    records = transform_dataframe_for_write(documents).to_json(
        orient="records", date_format="iso"
    )
    if records is None:
        raise ValueError("No records to write")

    write_dict_to_newline_delimited_json_file(json.loads(records), file_url)


def write_dict_to_newline_delimited_json_file(
    content: dict[str, Any] | list[dict[str, Any]],
    file_url: str | CloudStorageUrl | FileUrl,
):
    """Write the contents of a dictionary to a newline-delimited JSON file.

    Args:
        content (Union[Dict[str, Any], List[Dict[str, Any]]): The content to write.
        file_url (Union[str, CloudStorageUrl, FileUrl]): The file to write to.

    """

    def write(content: str, path: str):

        with open(path, "w", encoding="utf-8") as trg:
            trg.write(content)

    if isinstance(file_url, str):
        file_url = convert_to_url(file_url)
    if file_url.suffix not in ["json", "ndjson"]:
        raise NotImplementedError(f"The file type {file_url.suffix} is not supported")

    url_type = file_url.scheme

    write_function = {
        "gs": gcs.write,
        "file": write,
    }.get(url_type, None)

    if write_function is None:
        raise NotImplementedError(f"The file type for {url_type} is not supported")

    if isinstance(content, dict):
        content = [content]

    serialized_content = "\n".join([json.dumps(record) for record in content])

    write_function(serialized_content, raw_path(file_url))


def transform_dataframe_for_write(documents: pd.DataFrame) -> pd.DataFrame:
    """Transform the dataframe prior to writing to a file.

    Transforms non-serializable data types to serializable data types.

    Args:
        documents (DataFrame): The documents to transform.

    Returns:
        DataFrame: The transformed documents.

    """
    # Transform the dataframe prior to creating records
    for column_name, _ in documents.items():

        if documents[column_name].apply(type).eq(list).any():  # pyright: ignore
            # if the column is a list of arrays, convert null rows to empty array
            documents[column_name] = documents[column_name].apply(
                lambda x: [] if not isinstance(x, list) and pd.isnull(x) else x
            )
            # if the row is a list of enums, convert them to string values
            documents[column_name] = documents[column_name].apply(
                lambda x: [y.value if isinstance(y, Enum) else y for y in x]
            )

        # if the row is an enum, convert it to a string value
        if (
            documents[column_name]
            .apply(isinstance, args=(Enum,))
            .any()  # pyright: ignore[reportGeneralTypeIssues]
        ):
            documents[column_name] = documents[column_name].apply(
                lambda x: x if pd.isnull(x) or not isinstance(x, Enum) else x.name
            )

        # if the row is a URL, convert it to a string value
        if (
            documents[column_name]
            .apply(isinstance, args=(Url,))
            .any()  # pyright: ignore[reportGeneralTypeIssues]
        ):
            documents[column_name] = documents[column_name].apply(
                lambda x: x if pd.isnull(x) else str(x)
            )

        # if the row is a datetime, convert it to a string value
        if (
            documents[column_name]
            .apply(isinstance, args=(datetime,))
            .any()  # pyright: ignore[reportGeneralTypeIssues]
        ):
            documents[column_name] = documents[column_name].apply(
                lambda x: (
                    x if pd.isnull(x) or not isinstance(x, datetime) else x.isoformat()
                )
            )

    return documents


def write_to_storage(documents: pd.DataFrame, target_file: str):
    """Write the documents to storage.

    Args:
        documents (DataFrame): The documents to write.
        target_file (str): The target file to write the documents to.

    """
    # check if we have any duplicate columns and raise error if we do
    if documents.columns.duplicated().any():
        raise PipelineError(
            f"Duplicate columns found in the dataframe: {documents.columns[documents.columns.duplicated()]}"
        )

    logger.info("📝 Writing %d records to %s", len(documents.index), target_file)

    file_url, ext, url_type = extract_file_details(target_file)

    match (url_type, ext):
        case ("gs", "avro") | ("file", "avro"):
            write_function = write_dataframe_to_avro_file
        case ("gs", "parquet") | ("file", "parquet"):
            write_function = write_dataframe_to_parquet_file
        case "gs", "ndjson":
            write_function = write_dataframe_to_newline_delimited_json_file

        case _:
            write_function = None

    if write_function is None:
        raise NotImplementedError(
            f"The file type {ext} for {url_type} is not supported."
        )

    write_function(documents, file_url)


def end_process(input_file_uri: CloudStorageUrl | FileUrl | None = None):
    """End the process by moving the input file to a 'processed' directory.

    If no input file URI is provided, the function logs a message and exits the program.
    If the input file URI is a cloud storage URL, it parses the bucket and path, and moves the file
    to a 'processed' directory within the cloud storage.
    If the input file URI is a local file path, it moves the file to a 'processed' directory within
    the local file system.

    Args:
        input_file_uri (CloudStorageUrl | FileUrl | None): The URI of the input file to be
            processed. Can be a cloud storage URL or a local file path. If None, the function exits.

    Raises:
        SystemExit: Exits the program with status code 0 if successful, or 1 if an error occurs.

    """
    if input_file_uri is None:
        logger.info("No input file provided. Exiting.")
        sys.exit(0)

    input_file_path = Path(raw_path(input_file_uri))
    new_path = None
    if isurlinstance(input_file_uri, CloudStorageUrl):
        m = re.match(
            r"(?P<bucket>^[\w_\-]+)(?:\/(?P<path>[\w\-_\/]+))?",
            input_file_path.parent.as_posix(),
        )
        if m:
            bucket = m.group("bucket")
            path = m.group("path")
            new_path = Path(bucket)
            if path:
                new_path = new_path.joinpath(path)
            new_path = new_path.joinpath("processed").joinpath(input_file_path.name)
            gcs.move(input_file_path.as_posix(), new_path.as_posix())

        else:
            logger.error("Invalid input file path: %s", input_file_path.as_posix())
            sys.exit(1)

    else:
        new_path = input_file_path.parent.joinpath("processed").joinpath(
            input_file_path.name
        )
        move_file(input_file_path, new_path)
        logger.info("Moved %s to %s", input_file_path.as_posix(), new_path.as_posix())

    sys.exit(0)


def create_target_file_path(
    bucket: str | None = None,
    target_dir: str | None = None,
    source_file: str | None = None,
    file_name: str | None = None,
    run_id: str | None = None,
    prefix: str | None = None,
    extension: str | None = None,
):
    """Generate a target file path based on the provided parameters.

    Args:
        bucket (str | None): The bucket name for cloud storage (e.g., Google Cloud Storage). If
            provided, the path will be prefixed with "gs://".
        target_dir (str | None): The target directory where the file will be saved. If not provided,
            defaults to the current working directory if bucket is None, or the bucket name if
            bucket is provided.
        source_file (str | None): The source file path. Used to derive the file name if file_name is
            not provided.
        file_name (str | None): The name of the file. If not provided, derived from source_file.
        run_id (str | None): An optional run identifier to append to the file name.
        prefix (str | None): An optional prefix to prepend to the file name.
        extension (str | None): An optional file extension to replace the existing one.

    Returns:
        str: The generated target file path.

    Raises:
        ValueError: If neither source_file nor file_name is provided, or if the target file path
            cannot be constructed.

    """
    if all([source_file is None, file_name is None]):
        raise ValueError("Either source_file or file_name must be provided.")

    root = "file://"
    if bucket:
        root = "gs://"

    if target_dir is None and bucket is None:
        target_dir = Path.cwd().as_posix()
    elif target_dir is None and bucket is not None:
        target_dir = bucket
    elif target_dir is not None and bucket is not None:
        if not target_dir.startswith("/"):
            target_dir = f"/{target_dir}"
        target_dir = f"{bucket}{target_dir}"
    elif target_dir is not None and bucket is None:
        if not target_dir.startswith("/"):
            target_dir = f"/{target_dir}"

    if prefix and not prefix.endswith("_"):
        prefix = f"{prefix}_"

    if extension and not extension.startswith("."):
        extension = f".{extension}"

    if file_name is None and source_file is not None:
        if run_id:
            source_file = source_file.replace(f"_{run_id}", "")
        file = Path(source_file)
    elif file_name is not None:
        file = Path(file_name)
    else:
        raise ValueError("Invalid source file path.")

    if prefix:
        file = file.with_name(f"{prefix}{file.name}")
    if run_id:
        file = file.with_name(f"{file.stem}_{run_id}{file.suffix}")
    if extension:
        file = file.with_suffix(extension)
    file_name = file.name

    if target_dir and file_name:
        target = Path(target_dir).joinpath(file_name)
        return f"{root}{target.as_posix()}"

    raise ValueError("Invalid target file path.")


def create_glob_pattern(
    directory: str | None = None,
    run_id: str | None = None,
    prefix: str | None = None,
    extension: str | None = None,
    file_name: str | None = None,
):
    """Generate a glob pattern string based on the provided parameters.

    Args:
        directory (str | None): The target directory for the glob pattern. Defaults to the root
            directory ("/") if None.
        run_id (str | None): An optional run identifier to include in the pattern. Defaults to an
            empty string if None.
        prefix (str | None): An optional prefix to include in the pattern. Defaults to an empty
            string if None.
        extension (str | None): An optional file extension to include in the pattern. Defaults to an
            empty string if None.
        file_name (str | None): An optional file name to include in the pattern. Defaults to "*"
            if None.

    Notes:
        If both file_name and prefix are provided, the prefix will be prepended to the file name.
        If run_id is provided, it will be appended to the file name.

    Returns:
        str: A glob pattern string constructed from the provided parameters.

    """
    if directory is None:
        directory = ""
    elif not directory.endswith("/"):
        directory = f"{directory}/"

    if file_name is None:
        file_name = "*"

    if prefix is None:
        prefix = ""
    elif prefix and not prefix.endswith("_"):
        prefix = f"{prefix}_"

    if run_id is None:
        run_id = ""
    else:
        run_id = f"_{run_id}"

    if extension is None:
        extension = ""
    elif not extension.startswith("."):
        extension = f".{extension}"

    return f"{directory}{prefix}{file_name}{run_id}{extension}"
