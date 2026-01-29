"""Functions to apply delta logic to documents."""

import logging
from abc import abstractmethod
from datetime import datetime
from functools import cache
from typing import Any, Callable, Literal, Optional, Sequence, Tuple, TypeAlias

import pandas as pd
import pytz
from pydantic import BaseModel

from cleansweep.core.fileio import read_curated_file_to_dataframe
from cleansweep.enumerations import LoadType
from cleansweep.utils.google.storage import get_latest_blob

logger = logging.getLogger(__name__)
"""Logger for the delta module."""


ColumnName: TypeAlias = str


class BaseComparison(BaseModel):
    """Base model for comparison."""

    output: ColumnName

    @abstractmethod
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process the comparison."""
        raise NotImplementedError


class DeltaComparison(BaseComparison):
    """Model for comparing record.

    Attributes:
        left (ColumnName): The name of the left column to compare.
        right (Optional[ColumnName]): The name of the right column to compare. If not provided, the
            left column with "_prev" appended is used.
        output (ColumnName): The name of the output column to store the comparison result.

    """

    left: ColumnName
    right: Optional[ColumnName] = None

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process the given DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame containing the columns to be processed.

        Returns:
            pd.DataFrame: The DataFrame with the updated output column.

        The function performs the following operations:
        1. Sets the output column to "U" if the left and right columns are not equal and the right
            column is not NaN.
        2. Sets the output column to "I" if the right column is NaN.
        3. Sets the output column to "D" if the left column is NaN and the right column is not NaN.
        4. Sets the output column to "N" if the left and right columns are equal.

        """
        right = self.right
        if right is None:
            right = f"{self.left}_prev"

        df[self.output] = df.apply(
            lambda x: (
                "U"
                if x[self.left] != x[right] and pd.isna(x[right]) is False
                else x[self.output]
            ),
            axis=1,
        )
        df[self.output] = df.apply(
            lambda x: "I" if pd.isna(x[right]) else x[self.output],
            axis=1,
        )

        df[self.output] = df.apply(
            lambda x: (
                "D"
                if pd.isna(x[self.left]) and not pd.isna(x[self.right])
                else x[self.output]
            ),
            axis=1,
        )

        df[self.output] = df.apply(
            lambda x: ("N" if x[self.left] == x[right] else x[self.output]),
            axis=1,
        )

        return df


class DeltaExpiry(BaseComparison):
    """Model for comparing record expiry.

    Attributes:
        expiry_column (ColumnName): The name of the column containing the expiry date.
        output (ColumnName): The name of the output column to store the comparison result.

    """

    expiry_column: ColumnName

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process the given DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: The DataFrame with an additional column specified by `self.output`,
                containing boolean values indicating if the expiry date in `self.expiry_column` has
                passed (True) or not (False).

        """
        df[self.output] = df.apply(
            lambda x: (
                "D"
                if pd.to_datetime(x[self.expiry_column]) < datetime.now(tz=pytz.utc)
                else x[self.output]
            ),
            axis=1,
        )

        return df


Comparison: TypeAlias = DeltaComparison | DeltaExpiry


def delta_find_inserts(
    df: pd.DataFrame,
    staging_bucket: str,
    match_glob: str,
    id_column: str | None = "id",
) -> pd.Series:
    """Identify new inserts in a DataFrame by comparing it with previously processed data.

    This function loads previously processed data from a specified staging bucket and
    compares the IDs in the given DataFrame with the IDs in the previously processed data.
    It returns a Series containing the IDs that are present in the given DataFrame but
    not in the previously processed data, indicating new inserts.

    Args:
        df (pd.DataFrame): The DataFrame containing the current data to be checked for new inserts.
        staging_bucket (str): The path to the staging bucket where previously processed data is
            stored.
        match_glob (str): The glob pattern to match files in the staging bucket.
        id_column (str | None, optional): The name of the column containing the IDs to be compared.
            Defaults to "id".

    Returns:
        pd.Series: A Series containing the IDs that are new inserts.

    """
    # load previously processed data and compare id, if id is not in previous data, then it is an insert
    # return the "new" id's only
    previously_processed_df = load_delta_file(staging_bucket, match_glob)
    if previously_processed_df is None:
        return df[id_column]

    return df[~df[id_column].isin(previously_processed_df[id_column])][id_column]


def delta_load_and_compare(
    df: pd.DataFrame,
    comparisons: Sequence[Comparison],
    staging_bucket: str,
    match_glob: str,
    id_column: str | None = "id",
    default: Any | None = "N",
    dedupe_columns: Sequence[str] | None = None,
) -> pd.DataFrame:
    """Load previously processed data from a delta file, compare it with the current DataFrame.

    Args:
        df (pd.DataFrame): The current DataFrame to be compared.
        comparisons (Sequence[Comparison]): A sequence of Comparison objects defining the columns to
            compare.
        staging_bucket (str): The staging bucket where the delta file is stored.
        match_glob (str): The glob pattern to match the delta file in the staging bucket.
        id_column (str | None, optional): The column name to use as the identifier for merging.
            Defaults to "id".
        default (Any | None, optional): The default value to use for comparison results. Defaults to
            "N".
        dedupe_columns (Sequence[str] | None, optional): A list of columns to use for deduplication.
            Defaults to None.

    Returns:
        pd.DataFrame: The updated DataFrame with comparison results.

    """
    previously_processed_df = load_delta_file(staging_bucket, match_glob)

    def coalesce(comparison) -> str | None:
        if comparison.right is not None:
            return comparison.right
        return comparison.left

    for comparison in comparisons:
        if comparison.output not in df.columns:
            df[comparison.output] = default

    if previously_processed_df is not None:
        comparison_columns = [
            coalesce(comparison)
            for comparison in comparisons
            if isinstance(comparison, DeltaComparison)
        ]

        comparison_columns.append(id_column)

        # add the comparison columns to the dataframe
        df = df.merge(
            previously_processed_df[comparison_columns],
            on=id_column,
            how="left",
            suffixes=(None, "_prev"),
        )
        if dedupe_columns is not None:
            df.drop_duplicates(subset=dedupe_columns, keep="first", inplace=True)

        # process the comparisons
        df = delta_compare_columns(df, comparisons, default=default)

    return df


def delta_compare_columns(
    df: pd.DataFrame, comparisons: Sequence[Comparison], default: Any | None = "N"
) -> pd.DataFrame:
    """Compare and process a DataFrame based on a sequence of comparisons.

    This function iterates over a list of Comparison objects, applying each
    comparison's process method to the DataFrame. It also logs the count of
    records for different actions such as update, insert, delete, no change,
    and no action.

    Actions are represented by the following codes:
    - U: Update
    - I: Insert
    - D: Delete
    - N: No change

    Args:
        df (pd.DataFrame): The DataFrame to be processed.
        comparisons (Sequence[Comparison]): A sequence of Comparison objects that
            define how to process the DataFrame.
        default (Any | None, optional): The default value to be used for comparison
            outputs that are not present in the DataFrame. Defaults to "N".

    Returns:
        pd.DataFrame: The processed DataFrame with updated comparison results.

    """
    if default is None:
        default = "N"

    for comparison in comparisons:

        if comparison.output not in df.columns:
            df[comparison.output] = default

        df = comparison.process(df)

    return df


def delta_to_process(
    df: pd.DataFrame,
    load_type: LoadType,
    action_column: str | None = "action",
) -> pd.DataFrame:
    """Filter a DataFrame based on the specified load type and action column.

    Args:
        df (pd.DataFrame): The DataFrame to be filtered.
        load_type (LoadType): The type of load operation, which determines the filtering logic.
        action_column (str | None): The name of the column containing action indicators. Defaults
            to "action".

    Returns:
        pd.DataFrame: The filtered DataFrame.

    Raises:
        ValueError: If the input df is not a pandas DataFrame.

    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Dataframe is not a pandas DataFrame")

    if action_column is None:
        action_column = "action"

    if load_type in [LoadType.DELTA, LoadType.INCREMENTAL]:
        to_process = df[df[action_column].isin(["I", "U"])]
    else:
        to_process = df

    return to_process


def delta_processed(  # pylint: disable=keyword-arg-before-vararg
    df: pd.DataFrame,
    staging_bucket: str,
    match_glob: str,
    id_column: str | None = "id",
    action_column: str | None = "action",
    func: Callable[..., pd.DataFrame] | None = None,
    *args: Any,
    **kwargs: Any,
) -> pd.DataFrame | None:
    """Get previously processed records which are not present in the current DataFrame.

    Args:
        df (pd.DataFrame): The current DataFrame containing newly processed data.
        staging_bucket (str): The staging bucket where previously processed data is stored.
        match_glob (str): The glob pattern to match files in the staging bucket.
        id_column (str | None, optional): The column name used to identify records.
            Defaults to "id".
        action_column (str | None, optional): The column name used to identify the action
            to be taken on the records. Defaults to "action".
        func (Callable[..., pd.DataFrame] | None, optional): A function to filter the
            previously processed data. Defaults to None. The function must accept a
            DataFrame as the first argument and return a DataFrame.
        *args (Any): Additional arguments to pass to the filter function.
        **kwargs (Any): Additional keyword arguments to pass to the filter function.

    Returns:
        pd.DataFrame | None: A DataFrame with records that have not been processed,
            or None if no previously processed data is found.

    """
    if id_column is None:
        id_column = "id"

    if action_column is None:
        action_column = "action"

    # load the previously processed data
    previously_processed_df = load_delta_file(staging_bucket, match_glob)
    if previously_processed_df is None:
        return None
    # filter out previously processed records where `id_column` is in the current dataframe
    previously_processed_df = previously_processed_df[
        ~previously_processed_df[id_column].isin(df[id_column])
    ]
    if not isinstance(previously_processed_df, pd.DataFrame):
        previously_processed_df = pd.DataFrame(previously_processed_df)

    # optional additional filters
    if func is not None:
        previously_processed_df = func(previously_processed_df, *args, **kwargs)

    previously_processed_df[action_column] = "N"

    return previously_processed_df


@cache
def load_delta_file(
    bucket: str,
    match_glob: str,
) -> pd.DataFrame | None:
    """Load the latest delta file from a specified Google Cloud Storage bucket.

    This function identifies the latest file in the staging bucket that matches
    the provided glob pattern, reads it, and returns its contents as a pandas
    DataFrame. If no matching file is found, it returns None.

    Args:
        bucket (str): The name of the Google Cloud Storage bucket where
            the delta files are stored.
        match_glob (str): The glob pattern to match the delta files in the bucket.

    Returns:
        pd.DataFrame | None: A pandas DataFrame containing the data from the latest
            delta file if found, otherwise None.

    """
    # prepare the previously chunked data
    logger.info("🔎 Identifying latest file in %s/%s...", bucket, match_glob)
    latest_file = None

    try:
        latest_file = get_latest_blob(bucket, match_glob=match_glob)
    except FileNotFoundError as exc:
        logger.info(exc)

    if latest_file is not None:
        logger.info("📖 Reading curated data file gs://%s/%s", bucket, latest_file.name)
        return read_curated_file_to_dataframe(f"gs://{bucket}/{latest_file.name}")
    return None


def delta_prepare(
    df: pd.DataFrame,
    load_type: LoadType,
    staging_bucket: str,
    match_glob: str | None = None,
    force: bool | None = None,
    filter: (  # pylint: disable=redefined-builtin
        Literal["metadata", "content"] | None
    ) = None,
    action_column: str | None = "action",
    expiry_column: str | None = "metadata_expiry",
    id_column: str | Tuple[str, str] | None = "id",
) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    """Prepare a DataFrame for delta or incremental processing.

    Args:
        df (pd.DataFrame): The input DataFrame containing records to process.
        load_type (LoadType): The type of load operation, either FULL, DELTA, or INCREMENTAL.
        staging_bucket (str): The staging bucket location for delta files.
        match_glob (str | None, optional): A glob pattern to match delta files. Required for DELTA
            and INCREMENTAL load types.
        force (bool | None, optional): If True, forces processing of all records. Defaults to False.
        filter (Literal["metadata", "content"] | None, optional): Specifies a filter for changes,
            either "metadata" or "content".
        action_column (str | None, optional): The name of the column indicating the action type.
            Defaults to "action".
        expiry_column (str | None, optional): The name of the column indicating metadata expiry.
            Defaults to "metadata_expiry".
        id_column (str | Tuple[str, str] | None, optional): The column(s) used to identify records.
            Defaults to "id". Where id columns are different in the current and previous
            DataFrames, a tuple of (current_id_column, previous_id_column) should be provided.

    Returns:
        tuple[pd.DataFrame | None, pd.DataFrame | None]:
            - The first DataFrame contains records to process.
            - The second DataFrame contains previously processed records, if applicable.

    Raises:
        ValueError: If the input DataFrame is not a pandas DataFrame.
        ValueError: If `match_glob` is not provided for DELTA or INCREMENTAL load types.
        ValueError: If previously processed data is not a pandas DataFrame.

    Notes:
        - For FULL load type, all records are processed without delta processing.
        - For DELTA load type, records marked as "N" (not changed) are filtered based on previous data.
        - For INCREMENTAL load type, records marked as "I", "U", or "D" are excluded from previously processed data.
        - Expired records (based on `expiry_column`) are marked as deleted ("D") and excluded from processing.
        - If `force` is True, all records are marked as updated ("U") for processing.
        - If `filter` is specified, records are filtered based on the specified change type.

    """
    if force is None:
        force = False

    if action_column is None:
        action_column = "action"

    if expiry_column is None:
        expiry_column = "metadata_expiry"

    if id_column is None:
        id_column = "id"

    if not isinstance(df, pd.DataFrame):
        raise ValueError("Dataframe is not a pandas DataFrame")

    action_filter = (
        ["I", "N", "D", "U", ""] if load_type == LoadType.FULL else ["I", "U"]
    )

    # If there is no action column, add it and default to "I"
    if action_column not in df.columns:
        logger.warning("No action column found. Adding default action column.")
        df[action_column] = "I"

    to_process = df[df[action_column].isin(action_filter)]

    previously_processed_df = None
    previously_processed_source = None

    if load_type in [LoadType.DELTA, LoadType.INCREMENTAL]:

        if match_glob is None:
            raise ValueError("match_glob is required for delta processing")

        # prepare the previously chunked data
        previously_processed_source = load_delta_file(staging_bucket, match_glob)
        if previously_processed_source is not None:
            # default action column
            if action_column not in previously_processed_source.columns:
                previously_processed_source[action_column] = "N"

            if isinstance(id_column, tuple):
                _id_column, _id_column_prev = id_column
            else:
                _id_column = id_column
                _id_column_prev = id_column

            assert (
                _id_column in df.columns
            ), f"Column '{_id_column}' not found in the current DataFrame."

            if load_type == LoadType.DELTA:

                # filter to documents in the current batch identified as not changed.
                id_series = df[df[action_column] == "N"][_id_column]
                if not isinstance(id_series, pd.Series):
                    id_series = pd.Series(id_series)
                previous_ids = id_series.to_list()
                previously_processed_df = previously_processed_source[
                    previously_processed_source[_id_column_prev].isin(previous_ids)
                ]

            elif load_type == LoadType.INCREMENTAL:

                # remove documents that have been included in the current batch - these are either
                # new, updated, or deleted documents
                id_series = df[df[action_column].isin(["I", "U", "D"])][_id_column]
                if not isinstance(id_series, pd.Series):
                    id_series = pd.Series(id_series)
                previously_processed_df = previously_processed_source[
                    ~previously_processed_source[_id_column_prev].isin(
                        id_series.to_list()
                    )
                ]

            assert (
                previously_processed_df is not None
            ), "Previously processed data is None"

            # check previous documents for any expirations
            if expiry_column in previously_processed_df.columns:
                previously_processed_df[action_column] = previously_processed_df.apply(
                    lambda x: (
                        "D"
                        if pd.to_datetime(x[expiry_column]) < datetime.now(tz=pytz.utc)
                        else x[action_column]
                    ),
                    axis=1,
                )

            previously_processed_df = previously_processed_df[
                previously_processed_df[action_column] != "D"
            ]

            if not isinstance(previously_processed_df, pd.DataFrame):
                raise ValueError("Previous records is not a pandas DataFrame")

            # mark the previously processed documents as no action
            kwargs = {action_column: "N"}
            previously_processed_df = previously_processed_df.assign(**kwargs)

            logger.info(
                "🔎 Identified %s existing records from delta.",
                previously_processed_df.shape[0],
            )
        else:
            # we have no previous file so set the entire input as updated
            # if df[action_column] == "N" then set it to U
            # exclude any records that are marked as "D"
            df[action_column] = df.apply(
                lambda x: "U" if x[action_column] == "N" else x[action_column], axis=1
            )
            df = df[df[action_column] != "D"]
            logger.info("No previous records found. All records are new.")
            return df, None
    else:
        # no delta processing required
        return df, None

    if force is True and previously_processed_df is not None:
        logger.warning("Forcing processing of all records")

        previously_processed_df = previously_processed_df.assign(action="U")

        if not isinstance(previously_processed_df, pd.DataFrame):
            raise ValueError("Previous records is not a pandas DataFrame")

        to_process = delta_merge(to_process, previously_processed_df)

        previously_processed_df = None
    elif filter:
        logger.info("🔽 Filtering records for %s changes...", filter)

        filter_column = f"{filter}_is_modified"

        if filter_column in to_process.columns:

            groups = to_process.groupby(filter_column)

            isnt = []
            _is = []
            for group in groups:
                if group[0] not in action_filter:
                    isnt.append(group[1])
                elif group[0] in action_filter:
                    _is.append(group[1])

            if not _is:
                to_process = None
                logger.info("No records for %s changes.", filter)
            else:
                to_process = pd.concat(_is)
                logger.info("Records for %s changes: %s", filter, len(to_process.index))

            if (
                isnt
                and previously_processed_source is not None
                and previously_processed_source.empty is False
            ):

                # concat dataframes not to process
                not_to_process = pd.concat(isnt)

                # find the previously processed data that is in the not_to_process
                not_to_process_prev = previously_processed_source[
                    previously_processed_source[id_column].isin(
                        not_to_process[id_column]
                    )
                ]
                previously_processed_df = pd.concat(
                    [not_to_process_prev, previously_processed_df]
                )

                previously_processed_df.drop_duplicates(
                    subset=id_column, inplace=True, keep="first"
                )
                previously_processed_df.reset_index(drop=True, inplace=True)

    return to_process, previously_processed_df


def delta_merge(
    processed_documents: pd.DataFrame, previous_documents: pd.DataFrame | None
) -> pd.DataFrame:
    """Merge the previously processed data with the new processed data.

    Args:
        processed_documents (DataFrame): The new processed documents.
        previous_documents (DataFrame): The previously processed documents.

    Returns:
        DataFrame: The merged documents.

    """
    if previous_documents is not None:

        for column in processed_documents.columns:
            if column not in previous_documents.columns:
                previous_documents[column] = None

        filtered_df = previous_documents[processed_documents.columns]
        if not isinstance(filtered_df, pd.DataFrame):
            raise ValueError("Filtered DataFrame is not a pandas DataFrame")

        logger.info("Merging previously processed data with new processed data...")

        return pd.concat([processed_documents, filtered_df])

    return processed_documents
