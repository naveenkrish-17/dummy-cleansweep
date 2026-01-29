"""The filter module contains classes and functions for filtering content dataframe."""

from enum import Enum
from typing import Literal

import pandas as pd

from cleansweep._types import DataframeTypes


class FilterOperators(Enum):
    """The FilterOperators class contains operators for filtering content dataframe."""

    EQUAL = 0
    NOT_EQUAL = 1
    GREATER_THAN = 2
    GREATER_THAN_OR_EQUAL = 3
    LESS_THAN = 4
    LESS_THAN_OR_EQUAL = 5
    REGEX = 6
    IS_IN = 7
    IS_NOT_IN = 8


def get_filter_operator(operator: str | None = None) -> FilterOperators:
    """Get the filter operator from string.

    Args:
        operator (str, Optional): The operator to be used for filtering.

    Returns:
        FilterOperators: The filter operator.

    """
    if operator is None:
        return FilterOperators.EQUAL

    operators_map = {
        "==": FilterOperators.EQUAL,
        "!=": FilterOperators.NOT_EQUAL,
        ">": FilterOperators.GREATER_THAN,
        ">=": FilterOperators.GREATER_THAN_OR_EQUAL,
        "<": FilterOperators.LESS_THAN,
        "<=": FilterOperators.LESS_THAN_OR_EQUAL,
        "eq": FilterOperators.EQUAL,
        "ne": FilterOperators.NOT_EQUAL,
        "gt": FilterOperators.GREATER_THAN,
        "ge": FilterOperators.GREATER_THAN_OR_EQUAL,
        "lt": FilterOperators.LESS_THAN,
        "le": FilterOperators.LESS_THAN_OR_EQUAL,
        "=": FilterOperators.EQUAL,
        "=<": FilterOperators.LESS_THAN_OR_EQUAL,
        "=>": FilterOperators.GREATER_THAN_OR_EQUAL,
        "<>": FilterOperators.NOT_EQUAL,
        "regex": FilterOperators.REGEX,
        "in": FilterOperators.IS_IN,
        "not in": FilterOperators.IS_NOT_IN,
    }
    return operators_map.get(operator, FilterOperators.EQUAL)


class Filter:
    """The Filter class contains methods for filtering content dataframe."""

    @staticmethod
    def filter_by_match(
        content_df: pd.DataFrame, column: str, value: str
    ) -> pd.DataFrame:
        """Filter content dataframe by column and regex pattern.

        Args:
            content_df (DataFrame): The content dataframe to be filtered.
            column (str): The column to be filtered.
            value (str): The value to be filtered, which can be a regex pattern.

        Returns:
            DataFrame: The filtered dataframe.

        """
        if column not in content_df.columns:
            raise ValueError(f"{column} is not a column in the dataframe")

        result = content_df[
            content_df[column].str.contains(value, case=False, na=False, regex=True)
        ].reset_index(drop=True)

        if not isinstance(result, pd.DataFrame):
            raise ValueError("Result is not a pandas DataFrame")

        return result

    @staticmethod
    def remove_by_match(
        content_df: pd.DataFrame, column: str, value: str
    ) -> pd.DataFrame:
        """Remove content dataframe by column and regex pattern.

        Args:
            content_df (DataFrame): The content dataframe to be filtered.
            column (str): The column to be filtered.
            value (str): The value to be filtered, which can be a regex pattern.

        Returns:
            DataFrame: The filtered dataframe.

        """
        if column not in content_df.columns:
            raise ValueError(f"{column} is not a column in the dataframe")

        result = content_df[
            ~content_df[column].str.contains(value, case=False, na=False, regex=True)
        ].reset_index(drop=True)

        if not isinstance(result, pd.DataFrame):
            raise ValueError("Result is not a pandas DataFrame")

        return result

    @staticmethod
    def filter_by_column(  # pylint: disable=too-many-return-statements
        content_df: pd.DataFrame,
        column: str,
        value: DataframeTypes,
        operator: FilterOperators | str | None = None,
    ) -> pd.DataFrame:
        """Filter content dataframe by column and value.

        Args:
            content_df (DataFrame): The content dataframe to be filtered.
            column (str): The column to be filtered.
            value (Union[str, int, float, bool, list[Union[str, int, float, bool]]]): The value to
                be filtered.
            operator (Union[FilterOperators, str], Optional): The operator to be used for filtering.

        Returns:
            DataFrame: The filtered dataframe.

        """
        result = None

        if not isinstance(operator, FilterOperators):
            operator = get_filter_operator(operator)

        if operator == FilterOperators.EQUAL:

            if isinstance(value, list):
                result = content_df[content_df[column].isin(value)].reset_index(
                    drop=True
                )
            else:
                result = content_df[content_df[column] == value].reset_index(drop=True)

        elif operator == FilterOperators.NOT_EQUAL:

            if isinstance(value, list):
                result = content_df[~content_df[column].isin(value)].reset_index(
                    drop=True
                )
            else:
                result = content_df[content_df[column] != value].reset_index(drop=True)

        elif operator == FilterOperators.GREATER_THAN:
            result = content_df[content_df[column] > value].reset_index(drop=True)

        elif operator == FilterOperators.GREATER_THAN_OR_EQUAL:
            result = content_df[content_df[column] >= value].reset_index(drop=True)

        elif operator == FilterOperators.LESS_THAN:
            result = content_df[content_df[column] < value].reset_index(drop=True)

        elif operator == FilterOperators.LESS_THAN_OR_EQUAL:
            result = content_df[content_df[column] <= value].reset_index(drop=True)

        elif operator == FilterOperators.IS_IN:

            if isinstance(value, list):
                result = content_df[content_df[column].isin(value)].reset_index(
                    drop=True
                )
            else:
                result = content_df[
                    content_df[column].apply(lambda array: value in array)
                ].reset_index(drop=True)

        elif operator == FilterOperators.IS_NOT_IN:

            if isinstance(value, list):
                result = content_df[~content_df[column].isin(value)].reset_index(
                    drop=True
                )
            else:
                result = content_df[
                    content_df[column].apply(lambda array: value not in array)
                ].reset_index(drop=True)
        else:
            raise ValueError(f"Unsupported operator {operator}")

        if not isinstance(result, pd.DataFrame):
            raise ValueError("Result is not a pandas DataFrame")

        return result

    @staticmethod
    def filter_by_columns(
        content_df: pd.DataFrame,
        filters: dict[str, tuple[DataframeTypes, FilterOperators | str | None]],
    ) -> pd.DataFrame:
        """Filter content dataframe by multiple columns and values.

        The filters argument is a
        dictionary with column as key and tuple of value and operator as value.

        The values can include RegEx patterns; where the value is a RegEx pattern, the operator
        should be "regex". This is important to avoid a string pattern being interpreted as a
        regular expression.

        Args:
            content_df (DataFrame): The content dataframe to be filtered.
            filters (dict[str, tuple[DataframeTypes, Union[FilterOperators, str, None]]]): The
                columns and values to be filtered.

        Returns:
            DataFrame: The filtered dataframe.

        """
        for column, value in filters.items():
            if column not in content_df.columns:
                raise ValueError(f"{column} is not a column in the dataframe")

            if (
                not value
                or value is None
                or (isinstance(value[0], str) and value[0].strip() == "")
            ):
                raise ValueError(f"value is required for {column} filter")

            operator = (
                (
                    get_filter_operator(value[1])
                    if not isinstance(value[1], FilterOperators)
                    else value[1]
                )
                if len(value) > 1
                else None
            )

            if operator == FilterOperators.REGEX:
                if not isinstance(value[0], str):
                    raise ValueError("Value must be a string")
                content_df = Filter.filter_by_match(content_df, column, value[0])
            else:
                content_df = Filter.filter_by_column(
                    content_df, column, value[0], operator
                )
        return content_df.reset_index(drop=True)

    @staticmethod
    def filter_by_date_range(
        content_df: pd.DataFrame,
        date_column: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """Filter content dataframe by date.

        Args:
            content_df (DataFrame): The content dataframe to be filtered.
            date_column (str): The date column to be filtered.
            start_date (str): The start date of the date range.
            end_date (str): The end date of the date range.

        Returns:
            DataFrame: The filtered dataframe.

        """
        if date_column not in content_df.columns:
            raise ValueError(f"{date_column} is not a column in the dataframe")

        return content_df.loc[
            (content_df[date_column] >= start_date)
            & (content_df[date_column] <= end_date)
        ].reset_index(drop=True)

    @staticmethod
    def exclude_by_date_range(
        content_df: pd.DataFrame,
        date_column: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """Exclude content dataframe by date.

        Args:
            content_df (DataFrame): The content dataframe to be filtered.
            date_column (str): The date column to be filtered.
            start_date (str): The start date of the date range.
            end_date (str): The end date of the date range.

        Returns:
            DataFrame: The filtered dataframe.

        """
        if date_column not in content_df.columns:
            raise ValueError(f"{date_column} is not a column in the dataframe")

        result = content_df[
            (content_df[date_column] < start_date)
            | (content_df[date_column] > end_date)
        ].reset_index(drop=True)

        if not isinstance(result, pd.DataFrame):
            raise ValueError("Result is not a pandas DataFrame")

        return result

    @staticmethod
    def remove_null_or_empty(
        content_df: pd.DataFrame, columns: list[str]
    ) -> pd.DataFrame:
        """Remove rows with null or empty values in the specified columns.

        Args:
            content_df (DataFrame): The content dataframe to be filtered.
            columns (list[str]): The columns to be checked for null or empty values.

        Returns:
            DataFrame: The filtered dataframe.

        """
        if not all(column in content_df.columns for column in columns):
            raise ValueError(
                f"columns {', '.join(columns)} are not columns in the dataframe"
            )

        return (
            content_df.replace("", pd.NA).dropna(subset=columns).reset_index(drop=True)
        )

    @staticmethod
    def remove_duplicates(
        content_df: pd.DataFrame,
        columns: list[str],
        keep: Literal["first", "last", False] | None = None,
        order_by: str | None = None,
        order: Literal["asc", "desc"] | None = None,
    ) -> pd.DataFrame:
        """Remove duplicate rows based on the specified columns.

        Args:
            content_df (DataFrame): The content dataframe to be filtered.
            columns (list[str]): The columns to be checked for duplicates.
            keep (str, optional): The strategy to keep duplicates.
                Defaults to None.
            order_by (str, optional): The column to order by. Defaults to None.
            order (str, optional): The order to sort the dataframe. Defaults to
                None.

        Returns:
            DataFrame: The filtered dataframe.

        """
        if not all(column in content_df.columns for column in columns):
            raise ValueError("columns provided are not columns in the dataframe")

        if keep is None:
            keep = "first"

        if order is None:
            order = "asc"

        if order_by is not None:
            if order_by not in content_df.columns:
                raise ValueError(f"{order_by} is not a column in the dataframe")
            ascending = order == "asc"
            content_df = content_df.sort_values(by=order_by, ascending=ascending)

        return content_df.drop_duplicates(
            subset=columns, keep=keep, ignore_index=True
        ).reset_index(drop=True)
