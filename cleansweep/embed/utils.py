"""Utility functions for embedding data into the database."""

import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, TypeVar

import pandas as pd
import pytz
from pydantic_core import ValidationError

from cleansweep._types import Platform, SeriesLike
from cleansweep.embed.model import EmSourceDocument, SourceDocument
from cleansweep.enumerations import ContentType
from cleansweep.flags import flag

T = TypeVar("T", bound=Any)

logger = logging.getLogger(__name__)


def get_document(
    row: SeriesLike,
    max_document_length: int,
    len_func=len,
    **kwargs,  # pylint: disable=unused-argument
):
    """Retrieve the document content from a given ro.

    Args:
        row (SeriesLike): A dictionary-like object containing the document data.
        max_document_length (int): The maximum allowed length for the document content.
        len_func (callable, optional): A function to calculate the length of the document content.
            Defaults to len.
        **kwargs: Additional keyword arguments.

    Returns:
        str or None: The document content if it meets the length requirement or the chunk if it
            exceeds the length. Returns None if the "chunk" key is not present in the row.

    """
    # identify the columns
    chunk_column = "original_chunk" if "original_chunk" in row else "chunk"
    content_column = "original_content" if "original_content" in row else "content"

    if chunk_column in row:
        if len_func(row[content_column]) <= max_document_length:
            return row[content_column]
        else:
            return row[chunk_column]
    return None


def get_id(row: SeriesLike, **kwargs):  # pylint: disable=unused-argument
    """Retrieve the identifier from a given row.

    This function attempts to fetch the "question_id" from the provided row.
    If "question_id" is not present, it will return the "chunk_id" instead.

    Args:
        row (SeriesLike): An object that supports the `get` method, typically a dictionary.
        **kwargs: Additional keyword arguments (not used in this function).

    Returns:
        The value associated with "question_id" if it exists, otherwise the value associated with
            "chunk_id".

    """
    return row.get("question_id", row.get("chunk_id"))


def get_description(
    row: SeriesLike, **kwargs  # pylint: disable=unused-argument
) -> str:
    """Retrieve the description from a given row.

    Args:
        row (SeriesLike): A dictionary-like object containing the description data.
        **kwargs: Additional keyword arguments (not used).

    Returns:
        str: The description content if it exists, otherwise None.

    """
    description = row.get("metadata_description")
    if description is None:
        return ""
    return description


def get_dedup_id(
    row: SeriesLike, platform: Platform, **kwargs
):  # pylint: disable=unused-argument
    """Retrieve a deduplication ID from a row based on the specified platform.

    Args:
        row (SeriesLike): An object that supports the 'get' method to retrieve values.
        platform (Platform): The platform identifier to determine the deduplication logic.
        **kwargs: Additional keyword arguments (not used).

    Returns:
        The deduplication ID if the platform is 'kosmo', otherwise None.

    """
    if platform == "kosmo":
        return row.get("id")
    return None


def is_public(row: SeriesLike, **kwargs):  # pylint: disable=unused-argument
    """Check if the content type of a given row is public.

    Args:
        row (SeriesLike): A dictionary-like object that contains a "content_type" key.
        **kwargs: Additional keyword arguments (not used in this function).

    Returns:
        bool: True if the content type is public, False otherwise.

    """
    return True if row["content_type"] == ContentType.PUBLIC.value else False


@flag("root_document", default=None)
def get_root_document(
    row: SeriesLike, **kwargs  # pylint: disable=unused-argument
) -> SourceDocument | None:
    """Get the root document from a row.

    Args:
        row (SeriesLike): a row from a pandas DataFrame
        **kwargs: Additional keyword arguments

    Returns:
        SourceDocument: a SourceDocument object

    """
    if (
        "metadata_root_document_id" not in row
        or pd.isnull(row["metadata_root_document_id"]) is True
    ):
        return None

    if any(
        pd.isnull(row[col])
        for col in [
            "metadata_root_document_title",
            "metadata_root_document_description",
            "metadata_root_document_url",
        ]
    ):
        return None

    root_source = {
        "title": row["metadata_root_document_title"],
        "description": row["metadata_root_document_description"],
        "url": row["metadata_root_document_url"],
        "is_public": None,
    }

    if pd.isnull(row["metadata_root_document_content_type"]) is False:
        root_source["is_public"] = (
            row["metadata_root_document_content_type"] == ContentType.PUBLIC.value
        )

    return SourceDocument(**root_source)


def create_em_source_document(
    row: SeriesLike, **kwargs  # pylint: disable=unused-argument
) -> EmSourceDocument | list[EmSourceDocument] | None:
    """Create an EmSourceDocument or a list of EmSourceDocuments from a given row of data.

    This function processes a row of data, extracting relevant fields to create one or more
    EmSourceDocument instances. If the 'source_id' field in the row is a list, it creates
    multiple EmSourceDocument instances, one for each item in the list. Otherwise, it creates
    a single EmSourceDocument.

    Args:
        row (SeriesLike): A data row containing fields necessary to create EmSourceDocument(s).
        **kwargs: Additional keyword arguments (currently unused).

    Returns:
        EmSourceDocument or List[EmSourceDocument]: A single EmSourceDocument instance or a list
        of EmSourceDocument instances, depending on the structure of the input row.

    """

    def row_get(row: SeriesLike, key: str, index: int, default: Any) -> Any:
        item = row.get(key)

        if isinstance(item, list):
            if len(item) > index:
                return item[index]
            return default

        if pd.isnull(item):
            return default

        return item

    source_ids = row.get("source_id", "")
    if isinstance(source_ids, list):
        outputs = []
        for i, _ in enumerate(source_ids):
            try:
                outputs.append(
                    EmSourceDocument(
                        **{
                            "article_id": row_get(row, "source_id", i, ""),
                            "title": row_get(row, "title", i, ""),
                            "description": row_get(row, "description", i, ""),
                            "url": row_get(row, "metadata_url", i, ""),
                            "content_type": row_get(row, "content_type", i, ""),
                            "root_source": get_root_document(
                                {
                                    "metadata_root_document_title": row_get(
                                        row, "metadata_root_document_title", i, ""
                                    ),
                                    "metadata_root_document_description": row_get(
                                        row, "metadata_root_document_description", i, ""
                                    ),
                                    "metadata_root_document_url": row_get(
                                        row, "metadata_root_document_url", i, ""
                                    ),
                                    "metadata_root_document_content_type": row_get(
                                        row,
                                        "metadata_root_document_content_type",
                                        i,
                                        "",
                                    ),
                                }
                            ),
                        }
                    )
                )
            except ValidationError as exc:
                logger.error("Error creating EmSourceDocument: %s", exc)
                continue

        return outputs

    try:
        source_document = EmSourceDocument(
            **{
                "article_id": row.get("source_id", ""),
                "title": row.get("title", ""),
                "description": row.get("description", ""),
                "url": row.get("metadata_url", ""),
                "content_type": row.get("content_type", "PRIVATE"),
                "root_source": get_root_document(row),
            }
        )
    except ValidationError as exc:
        logger.error("Error creating EmSourceDocument: %s", exc)
        return None
    return source_document


def get_sufficient_references(
    row: SeriesLike, **kwargs  # pylint: disable=unused-argument
) -> list[int]:
    """Determine the indices of sufficient references in a given row.

    This function checks the 'is_sufficient' and 'source_id' fields in the input row.
    If 'is_sufficient' is not specified or is shorter than 'source_id', it assumes all
    references are sufficient. It then returns the indices of references that are marked
    as sufficient.

    Args:
        row (SeriesLike): A data structure (e.g., pandas Series) containing the fields
                          'is_sufficient' and 'source_id'.
        **kwargs: Additional keyword arguments (not used).

    Returns:
        List[int]: A list of indices where the references are marked as sufficient.

    """
    is_sufficient = row.get("is_sufficient", [])

    if is_sufficient is None:
        is_sufficient = []

    if not isinstance(is_sufficient, list):
        is_sufficient = [is_sufficient]

    # replace is_sufficient with the boolean values only
    is_sufficient = [flag[0] for flag in is_sufficient]

    source_id = row.get("source_id", [])
    if not isinstance(source_id, list):
        source_id = [source_id]
    if len(source_id) > len(is_sufficient):
        # assume all references are sufficient if not specified
        is_sufficient = [True] * len(source_id)

    return [i for i, flag in enumerate(is_sufficient) if flag]


def add_root_document_to_df(df: pd.DataFrame) -> pd.DataFrame:
    """Add root document id, title, description, url, and is_public to each row.

    Args:
        df (DataFrame): a pandas dataframe with clean articles and metadata

    Returns:
        DataFrame: a pandas dataframe with root document id, title, description, url, and is_public

    """
    if df.empty:
        raise ValueError("Input DataFrame is empty")

    # create list of unique root document id which are not null
    root_document_ids = list(df["metadata_root_document_id"].dropna().unique())

    # find title, description, url, and is_public of root document
    root_document = df.loc[
        df["id"].isin(root_document_ids),
        [
            "id",
            "title",
            "metadata_description",
            "metadata_url",
            "content_type",
        ],
    ].drop_duplicates()

    # prefix column names with root_document
    root_document.rename(
        columns=lambda x: (
            f"metadata_root_document_{x}".replace("document_metadata", "document")
            if not x.startswith("metadata_root_document_")
            else x
        ),
        inplace=True,
    )

    # add root document id, title, description, url, and is_public to each row
    df = df.merge(
        root_document,
        how="left",
        left_on="metadata_root_document_id",
        right_on="metadata_root_document_id",
        suffixes=(None, None),
    )

    return df


SOURCE_FILE_DATEPATERN = (
    r"(?P<year>20[0-9]{2,})\-?(?P<month>1[0-2]|0[0-9])\-?(?P<day>3[0-1]|[0-2][0-9])\_?-?"
    r"(?:(?P<hours>[0-1][0-9]|2[0-3])\-?(?P<minutes>[0-5][0-9])\-?(?P<seconds>[0-5][0-9]))?"
)


def get_embedding_file_name(source_file_name: str, extension: str = "ndjson") -> str:
    """Get the embedding file name.

    Args:
        source_file_name (str): The source file name.
        extension (str, optional): The extension of the file.

    Returns:
        str: The embedding file name.

    """
    pth = Path(source_file_name)
    file_name = pth.stem

    processed_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    m = re.search(SOURCE_FILE_DATEPATERN, file_name)
    source_time = (
        datetime(*[int(v) for v in m.groups() if v], tzinfo=pytz.UTC).strftime(
            "%Y-%m-%d_%H-%M-%S"
        )
        if m is not None
        else ""
    )

    return f"{source_time}_embedded_{processed_time}.{extension}"
