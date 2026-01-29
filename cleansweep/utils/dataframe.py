"""Utility functions for working with pandas DataFrames."""

from typing import Callable, Mapping, Sequence

import pandas as pd


def aggregate_dataframe_by_columns(
    df: pd.DataFrame, group_cols: Sequence[str]
) -> pd.DataFrame:
    """Aggregate a DataFrame by specified columns, ensuring unique values in each group.

    Args:
        df (pd.DataFrame): The DataFrame to be aggregated.
        group_cols (Sequence[str]): A sequence of column names to group by.

    Returns:
        pd.DataFrame: A new DataFrame with the specified columns grouped and aggregated.
            If a column has multiple unique values within a group, a list of unique values is
            returned. Otherwise, the single unique value is returned.

    """

    # Group by the specified columns
    def aggregate_column(x):
        # Manually check for uniqueness by comparing values
        unique_values = []
        for item in x:
            if item not in unique_values:
                unique_values.append(item)
        # Return the unique values if there are multiple, else return the single value
        return unique_values if len(unique_values) > 1 else unique_values[0]

    group_cols = list(group_cols)
    # Group by the group_cols and aggregate
    aggregated_df = df.groupby(group_cols).agg(  # pyright: ignore[reportCallIssue]
        aggregate_column
    )

    # Reset index to bring group_cols back as columns
    return aggregated_df.reset_index()


def refactor_dataframe(
    df: pd.DataFrame, mapping: Mapping[str, str | Callable], **kwargs
) -> pd.DataFrame:
    """Refactor a DataFrame using a mapping of column names to new column names or functions.

    Args:
        df (pd.DataFrame): The DataFrame to refactor.
        mapping (Mapping[str, str | Callable]): A mapping of column names to new column names or
            functions to apply to the column.
        **kwargs: Additional keyword arguments.

    Returns:
        pd.DataFrame: The refactored DataFrame.

    """
    data = {}
    for key, value in mapping.items():
        if value is not None:

            if callable(value):
                data[key] = df.apply(value, axis=1, **kwargs)
            elif value in df.columns:
                data[key] = df[value]
            else:
                data[key] = [None for _ in range(len(df))]

    return pd.DataFrame(data)
