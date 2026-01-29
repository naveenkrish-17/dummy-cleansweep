"""Rules for cleaning.

Rules are applied to documents to clean them. The rules are applied in the order they are defined.

Each rule is a class that inherits from the `Rule` class. The `Rule` class is an abstract class
that defines the interface for a rule.
"""

# pylint: disable=too-few-public-methods

import logging
import re
from abc import ABC, abstractmethod

import pandas as pd

from cleansweep.clean.filter import Filter
from cleansweep.clean.substrings import remove_substrings, replace_substrings
from cleansweep.enumerations import RuleType

logger = logging.getLogger(__name__)


def get_rule(rule_type: RuleType) -> "Rule":
    """Get the rule class from the rule type.

    Args:
        rule_type (RuleType): The rule type.

    Returns:
        Rule: The rule class.

    """
    mapping = {
        RuleType.REPLACE_SUBSTRINGS: ReplaceSubstrings,
        RuleType.REMOVE_SUBSTRINGS: RemoveSubstrings,
        RuleType.REMOVE_NULL_OR_EMPTY: RemoveNullOrEmpty,
        RuleType.FILTER_BY_DATE_RANGE: FilterByDateRange,
        RuleType.EXCLUDE_BY_DATE_RANGE: ExcludeByDateRange,
        RuleType.FILTER_BY_COLUMNS: FilterByColumns,
        RuleType.FILTER_BY_COLUMN: FilterByColumn,
        RuleType.FILTER_BY_MATCH: FilterByMatch,
        RuleType.REMOVE_BY_MATCH: RemoveByMatch,
        RuleType.REMOVE_DUPLICATES: RemoveDuplicates,
        RuleType.REFERENCE_TO_INLINE: ReferenceToInLine,
    }
    return mapping[rule_type]


class Rule(ABC):
    """The Rule class is an abstract class that defines the interface for a rule.

    Methods
        apply: Apply the rule to a document.

    """

    @staticmethod
    @abstractmethod
    def apply(documents: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Apply the rule to a document.

        Args:
            documents (DataFrame): The documents to apply the rule to.
            **kwargs: Additional arguments for the rule.

        Returns:
            str: The document after the rule is applied.

        """
        raise NotImplementedError


# region rules


class ReplaceSubstrings(Rule):
    """The ReplaceSubstrings class is a rule that replaces substrings in a document."""

    @staticmethod
    def apply(documents: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Apply the rule to a document."""
        columns = kwargs.get("columns")
        if not columns:
            raise ValueError("columns is required for replace_substrings rule")

        substrings = kwargs.get("substrings")
        if not substrings:
            raise ValueError("substrings is required for replace_substrings rule")

        replacement = kwargs.get("replacement")
        if not replacement:
            raise ValueError("replacement is required for replace_substrings rule")

        curated_df = documents.copy()
        for column in columns:
            if column not in documents.columns:
                raise ValueError(f"Attribute '{column}' not found in documents")

            curated_df[column] = curated_df[column].apply(
                replace_substrings, old=substrings, new=replacement
            )

        return curated_df


class RemoveSubstrings(Rule):
    """The RemoveSubstrings class is a rule that removes substrings in a document."""

    @staticmethod
    def apply(documents: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Apply the rule to a document."""
        columns = kwargs.get("columns")
        if not columns:
            raise ValueError("columns is required for replace_substrings rule")

        substrings = kwargs.get("substrings")
        if not substrings:
            raise ValueError("substrings is required for replace_substrings rule")

        curated_df = documents.copy()
        for column in columns:
            if column not in documents.columns:
                raise ValueError(f"Attribute '{column}' not found in documents")

            curated_df[column] = curated_df[column].apply(
                remove_substrings, substrings=substrings
            )

        return curated_df


class FilterByDateRange(Rule):
    """The FilterByDateRange class is a rule that filters documents by date range."""

    @staticmethod
    def apply(documents: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Apply the rule to a document."""
        date_column = kwargs.get("date_column")
        if not date_column:
            raise ValueError("date_column is required for filter_by_date_range rule")

        start_date = kwargs.get("start_date")
        if not start_date:
            raise ValueError("start_date is required for filter_by_date_range rule")

        end_date = kwargs.get("end_date")
        if not end_date:
            raise ValueError("end_date is required for filter_by_date_range rule")

        return Filter.filter_by_date_range(documents, date_column, start_date, end_date)


class ExcludeByDateRange(Rule):
    """The ExcludeByDateRange class is a rule that excludes documents by date range."""

    @staticmethod
    def apply(documents: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Apply the rule to a document."""
        date_column = kwargs.get("date_column")
        if not date_column:
            raise ValueError("date_column is required for exclude_by_date_range rule")

        start_date = kwargs.get("start_date")
        if not start_date:
            raise ValueError("start_date is required for exclude_by_date_range rule")

        end_date = kwargs.get("end_date")
        if not end_date:
            raise ValueError("end_date is required for exclude_by_date_range rule")

        return Filter.exclude_by_date_range(
            documents, date_column, start_date, end_date
        )


class FilterByColumns(Rule):
    """The FilterByColumns class is a rule that filters documents by columns."""

    @staticmethod
    def apply(documents: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Apply the rule to a document."""
        filters = kwargs.get("filters")
        if not filters:
            raise ValueError("filters is required for filter_by_columns rule")

        return Filter.filter_by_columns(documents, filters)


class FilterByColumn(Rule):
    """The FilterByColumn class is a rule that filters documents by a column."""

    @staticmethod
    def apply(documents: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Apply the rule to a document."""
        column = kwargs.get("column")
        if not column:
            raise ValueError("column is required for filter_by_column rule")

        value = kwargs.get("value")
        if value is None:
            raise ValueError("value is required for filter_by_column rule")

        operator = kwargs.get("operator")
        if not operator:
            raise ValueError("operator is required for filter_by_column rule")

        return Filter.filter_by_column(documents, column, value, operator)


class FilterByMatch(Rule):
    """The FilterByMatch class is a rule that filters documents by a match."""

    @staticmethod
    def apply(documents: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Apply the rule to a document."""
        column = kwargs.get("column")
        if not column:
            raise ValueError("column is required for filter_by_column rule")

        value = kwargs.get("value")
        if not value:
            raise ValueError("value is required for filter_by_column rule")

        return Filter.filter_by_match(documents, column, value)


class RemoveNullOrEmpty(Rule):
    """The RemoveNullOrEmpty class is a rule that removes null or empty values from a document."""

    @staticmethod
    def apply(documents: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Apply the rule to a document."""
        columns = kwargs.get("columns")
        if not columns:
            raise ValueError("columns is required for remove_null_or_empty rule")

        return Filter.remove_null_or_empty(documents, columns)


class RemoveByMatch(Rule):
    """The RemoveByMatch class is a rule that removes documents by a regex match."""

    @staticmethod
    def apply(documents: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Apply the rule to a document."""
        column = kwargs.get("column")
        if not column:
            raise ValueError("column is required for remove_by_match rule")

        value = kwargs.get("value")
        if not value:
            raise ValueError("value is required for remove_by_match rule")

        return Filter.remove_by_match(documents, column, value)


class RemoveDuplicates(Rule):
    """The RemoveDuplicates class is a rule that removes duplicates from a document."""

    @staticmethod
    def apply(documents: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Apply the rule to a document."""
        columns = kwargs.get("columns")
        if not columns:
            raise ValueError("columns is required for remove_duplicates rule")

        keep = kwargs.get("keep")
        order_by = kwargs.get("order_by")
        order = kwargs.get("order")

        return Filter.remove_duplicates(
            documents, columns, keep=keep, order_by=order_by, order=order
        )


class ReferenceToInLine(Rule):
    """Rule that converts references to inline links in a document."""

    match_links = re.compile(r"(\[.*?\])\s?(\[.*?\]|\(.*?\))")
    match_refs = re.compile(r"(?<=\n)[ \t]*\[.*?\]:\s?.*")

    @classmethod
    def _ref_dict(cls, text):
        """Extract and organizes references from the given text.

        This method searches for references in the provided text using a regular
        expression pattern defined by `cls.match_refs`. It then sorts these references
        based on the part before the first colon. The references are stored in a
        dictionary where the keys are the unique reference identifiers and the values
        are lists of corresponding reference details.

        Args:
            text (str): The input text containing references.

        Returns:
            dict: A dictionary where keys are reference identifiers and values are lists
                  of reference details.

        """
        refs = re.findall(cls.match_refs, text)
        refs.sort(key=lambda x: x.split(":", 1)[0])
        ref_dict = {}
        for k, v in (i.split(":", 1) for i in refs):
            _k = k.strip()
            if _k not in ref_dict:
                ref_dict[_k] = []
            ref_dict[_k].append(v.strip())

        return ref_dict

    @classmethod
    def _map_links(cls, text, ref_dict):
        """Map links in the given text to their references from the reference dictionary.

        Args:
            text (str): The text containing links to be mapped.
            ref_dict (dict): A dictionary where keys are reference identifiers and values are lists
                of reference links.

        Returns:
            list: A list of strings where each string is a link followed by its mapped reference
                link.

        Raises:
            None

        Logs:
            Warning: If a reference is not found in the reference dictionary.

        """
        mapped_links = []

        for link, ref in cls.match_links.findall(text):
            if ref in ref_dict and ref_dict[ref]:
                ref_link = f"({ref_dict[ref].pop(0)})"
            else:
                logger.debug("Reference not found for %s", ref)
                ref_link = ref

            mapped_links.append(f"{link}{ref_link}")

        return mapped_links

    @staticmethod
    def apply(documents: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """Apply reference to inline transformation on the content of the given DataFrame.

        This function processes the 'content' column of the provided DataFrame by converting
        references to inline links using the ReferenceToInLine class methods.

        Args:
            documents (pd.DataFrame): A pandas DataFrame containing a 'content' column with text
                data.
            **kwargs: Additional keyword arguments (not used in this function).

        Returns:
            pd.DataFrame: A new DataFrame with the 'content' column transformed by applying
                          reference to inline link conversion.

        """
        column = kwargs.get("column")
        if not column:
            raise ValueError("column is required for remove_by_match rule")

        target_column = kwargs.get("target_column", "content")
        if not target_column:
            target_column = "content"

        def _apply(text):
            # create a dictionary of all references
            ref_dict = ReferenceToInLine._ref_dict(text)
            # map the links
            mapped_links = ReferenceToInLine._map_links(text, ref_dict)

            to_replace = iter(mapped_links)
            working_text = ReferenceToInLine.match_links.sub(
                lambda _: next(to_replace), text
            )
            return ReferenceToInLine.match_refs.sub("", working_text)

        df = documents.copy()
        df[target_column] = df[column].apply(_apply)
        return df


# endregion
