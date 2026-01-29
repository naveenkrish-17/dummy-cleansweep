"""Test suite for the rules module."""

import pandas as pd
import pytest

from cleansweep.clean.filter import FilterOperators
from cleansweep.clean.rules import (
    ExcludeByDateRange,
    FilterByColumn,
    FilterByColumns,
    FilterByDateRange,
    FilterByMatch,
    ReferenceToInLine,
    RemoveByMatch,
    RemoveDuplicates,
    RemoveNullOrEmpty,
    RemoveSubstrings,
    ReplaceSubstrings,
)
from cleansweep.iso.languages import Language
from cleansweep.settings.clean import CleanSettings
from cleansweep.settings.load import load_settings


class TestReplaceSubstrings:
    """Test suite for the ReplaceSubstrings rule."""

    def test_apply(self):
        """Test the apply method of the ReplaceSubstrings rule."""
        documents = pd.DataFrame({"text": ["hello world", "goodbye world"]})
        result = ReplaceSubstrings.apply(
            documents, columns=["text"], substrings=["world"], replacement="universe"
        )
        assert result.equals(
            pd.DataFrame({"text": ["hello universe", "goodbye universe"]})
        )

    def test_apply_no_columns(self):
        """Test the apply method of the ReplaceSubstrings rule with no columns."""
        documents = pd.DataFrame({"text": ["hello world", "goodbye world"]})
        with pytest.raises(ValueError):
            ReplaceSubstrings.apply(
                documents, substrings=["world"], replacement="universe"
            )

    def test_apply_no_substrings(self):
        """Test the apply method of the ReplaceSubstrings rule with no substrings."""
        documents = pd.DataFrame({"text": ["hello world", "goodbye world"]})
        with pytest.raises(ValueError):
            ReplaceSubstrings.apply(documents, columns=["text"], replacement="universe")

    def test_apply_no_replacement(self):
        """Test the apply method of the ReplaceSubstrings rule with no replacement."""
        documents = pd.DataFrame({"text": ["hello world", "goodbye world"]})
        with pytest.raises(ValueError):
            ReplaceSubstrings.apply(documents, columns=["text"], substrings=["world"])

    def test_apply_attribute_not_found(self):
        """Test the apply method of the ReplaceSubstrings rule with an attribute not found."""
        documents = pd.DataFrame({"text": ["hello world", "goodbye world"]})
        with pytest.raises(ValueError):
            ReplaceSubstrings.apply(
                documents,
                columns=["not_found"],
                substrings=["world"],
                replacement="universe",
            )


class TestRemoveSubstrings:
    """Test suite for the RemoveSubstrings rule."""

    def test_apply(self):
        """Test the apply method of the RemoveSubstrings rule."""
        documents = pd.DataFrame({"text": ["hello world", "goodbye world"]})
        result = RemoveSubstrings.apply(
            documents, columns=["text"], substrings=["world"]
        )
        assert result.equals(pd.DataFrame({"text": ["hello ", "goodbye "]}))

    def test_apply_no_columns(self):
        """Test the apply method of the RemoveSubstrings rule with no columns."""
        documents = pd.DataFrame({"text": ["hello world", "goodbye world"]})
        with pytest.raises(ValueError):
            RemoveSubstrings.apply(documents, substrings=["world"])

    def test_apply_no_substrings(self):
        """Test the apply method of the RemoveSubstrings rule with no substrings."""
        documents = pd.DataFrame({"text": ["hello world", "goodbye world"]})
        with pytest.raises(ValueError):
            RemoveSubstrings.apply(documents, columns=["text"])

    def test_apply_attribute_not_found(self):
        """Test the apply method of the RemoveSubstrings rule with an attribute not found."""
        documents = pd.DataFrame({"text": ["hello world", "goodbye world"]})
        with pytest.raises(ValueError):
            RemoveSubstrings.apply(
                documents,
                columns=["not_found"],
                substrings=["world"],
            )


class TestExcludeByDateRange:
    """Test suite for the ExcludeByDateRange rule."""

    def test_apply(self):
        """Test the apply method of the ExcludeByDateRange rule."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": ["2022-01-01", "2022-01-02"],
            }
        )
        documents["date"] = pd.to_datetime(documents["date"], format="%Y-%m-%d")

        result = ExcludeByDateRange.apply(
            documents,
            date_column="date",
            start_date="2022-01-01",
            end_date="2022-01-31",
        )

        assert result.empty

    def test_apply_attribute_not_found(self):
        """Test the apply method of the ExcludeByDateRange rule with an attribute not found."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": ["2022-01-01", "2022-01-02"],
            }
        )
        documents["date"] = pd.to_datetime(documents["date"], format="%Y-%m-%d")

        with pytest.raises(ValueError):
            ExcludeByDateRange.apply(
                documents,
                date_column="not_found",
                start_date="2022-01-01",
                end_date="2022-01-31",
            )

    def test_apply_no_date_column(self):
        """Test the apply method of the ExcludeByDateRange rule with no date_column."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": [pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-02")],
            }
        )
        with pytest.raises(ValueError):
            ExcludeByDateRange.apply(
                documents,
                start_date=pd.Timestamp("2022-01-01"),
                end_date=pd.Timestamp("2022-01-01"),
            )

    def test_apply_no_start_date(self):
        """Test the apply method of the ExcludeByDateRange rule with no start_date."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": [pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-02")],
            }
        )
        with pytest.raises(ValueError):
            ExcludeByDateRange.apply(
                documents,
                date_column="date",
                end_date=pd.Timestamp("2022-01-01"),
            )

    def test_apply_no_end_date(self):
        """Test the apply method of the ExcludeByDateRange rule with no end_date."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": [pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-02")],
            }
        )
        with pytest.raises(ValueError):
            ExcludeByDateRange.apply(
                documents,
                date_column="date",
                start_date=pd.Timestamp("2022-01-01"),
            )


class TestFilterByDateRange:
    """Test suite for the FilterByDateRange rule."""

    def test_apply(self):
        """Test the apply method of the FilterByDateRange rule."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": ["2022-01-01", "2022-02-02"],
            }
        )
        documents["date"] = pd.to_datetime(documents["date"], format="%Y-%m-%d")

        result = FilterByDateRange.apply(
            documents,
            date_column="date",
            start_date="2022-01-01",
            end_date="2022-01-31",
        )

        assert result.equals(
            pd.DataFrame(
                {
                    "text": ["hello world"],
                    "date": [pd.Timestamp("2022-01-01")],
                }
            )
        )

    def test_apply_attribute_not_found(self):
        """Test the apply method of the FilterByDateRange rule with an attribute not found."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": ["2022-01-01", "2022-01-02"],
            }
        )
        documents["date"] = pd.to_datetime(documents["date"], format="%Y-%m-%d")

        with pytest.raises(ValueError):
            FilterByDateRange.apply(
                documents,
                date_column="not_found",
                start_date="2022-01-01",
                end_date="2022-01-31",
            )

    def test_apply_no_date_column(self):
        """Test the apply method of the FilterByDateRange rule with no date_column."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": [pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-02")],
            }
        )
        with pytest.raises(ValueError):
            FilterByDateRange.apply(
                documents,
                start_date=pd.Timestamp("2022-01-01"),
                end_date=pd.Timestamp("2022-01-01"),
            )

    def test_apply_no_start_date(self):
        """Test the apply method of the FilterByDateRange rule with no start_date."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": [pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-02")],
            }
        )
        with pytest.raises(ValueError):
            FilterByDateRange.apply(
                documents,
                date_column="date",
                end_date=pd.Timestamp("2022-01-01"),
            )

    def test_apply_no_end_date(self):
        """Test the apply method of the FilterByDateRange rule with no end_date."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": [pd.Timestamp("2022-01-01"), pd.Timestamp("2022-01-02")],
            }
        )
        with pytest.raises(ValueError):
            FilterByDateRange.apply(
                documents,
                date_column="date",
                start_date=pd.Timestamp("2022-01-01"),
            )


class TestFilterByColumn:
    """Test suite for the FilterByColumn rule."""

    def test_apply(self):
        """Test the apply method of the FilterByColumn rule."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": ["2022-01-01", "2022-01-02"],
            }
        )
        result = FilterByColumn.apply(
            documents,
            column="text",
            value="hello world",
            operator=FilterOperators.EQUAL,
        )
        assert result.equals(
            pd.DataFrame(
                {
                    "text": ["hello world"],
                    "date": ["2022-01-01"],
                }
            )
        )

    def test_apply_attribute_not_found(self):
        """Test the apply method of the FilterByColumn rule with an attribute not found."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": ["2022-01-01", "2022-01-02"],
            }
        )
        with pytest.raises(ValueError):
            FilterByColumn.apply(documents, column="not_found", value="hello world")

    def test_apply_no_column(self):
        """Test the apply method of the FilterByColumn rule with no column."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": ["2022-01-01", "2022-01-02"],
            }
        )
        with pytest.raises(ValueError):
            FilterByColumn.apply(documents, value="hello world")

    def test_apply_no_value(self):
        """Test the apply method of the FilterByColumn rule with no value."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": ["2022-01-01", "2022-01-02"],
            }
        )
        with pytest.raises(ValueError):
            FilterByColumn.apply(documents, column="text")


class TestFilterByColumns:
    """Test suite for the FilterByColumns rule."""

    def test_apply(self):
        """Test the apply method of the FilterByColumns rule."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": ["2022-01-01", "2022-01-02"],
            }
        )
        result = FilterByColumns.apply(
            documents,
            filters={"text": ("hello world",)},
        )
        assert result.equals(
            pd.DataFrame(
                {
                    "text": ["hello world"],
                    "date": ["2022-01-01"],
                }
            )
        )

    def test_apply_no_filters(self):
        """Test the apply method of the FilterByColumns rule with no filters."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": ["2022-01-01", "2022-01-02"],
            }
        )
        with pytest.raises(ValueError):
            FilterByColumns.apply(documents)

    def test_apply_no_column(self):
        """Test the apply method of the FilterByColumns rule with no column."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": ["2022-01-01", "2022-01-02"],
            }
        )
        with pytest.raises(ValueError):
            FilterByColumns.apply(documents, filters={"not_found": ("hello world",)})

    def test_apply_no_value(self):
        """Test the apply method of the FilterByColumns rule with no value."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": ["2022-01-01", "2022-01-02"],
            }
        )
        with pytest.raises(ValueError):
            FilterByColumns.apply(documents, filters={"text": ("",)})


class TestFilterByMatch:
    """Test suite for the FilterByMatch rule."""

    def test_apply(self):
        """Test the apply method of the FilterByMatch rule."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": ["2022-01-01", "2022-01-02"],
            }
        )
        result = FilterByMatch.apply(
            documents,
            column="text",
            value="hello",
            operator=FilterOperators.REGEX,
        )
        assert result.equals(
            pd.DataFrame(
                {
                    "text": ["hello world"],
                    "date": ["2022-01-01"],
                }
            )
        )

    def test_apply_attribute_not_found(self):
        """Test the apply method of the FilterByMatch rule with an attribute not found."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": ["2022-01-01", "2022-01-02"],
            }
        )
        with pytest.raises(ValueError):
            FilterByMatch.apply(documents, column="not_found", value="hello")

    def test_apply_no_column(self):
        """Test the apply method of the FilterByMatch rule with no column."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": ["2022-01-01", "2022-01-02"],
            }
        )
        with pytest.raises(ValueError):
            FilterByMatch.apply(documents, value="hello")

    def test_apply_no_value(self):
        """Test the apply method of the FilterByMatch rule with no value."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": ["2022-01-01", "2022-01-02"],
            }
        )
        with pytest.raises(ValueError):
            FilterByMatch.apply(documents, column="text")


class TestRemoveNullOrEmpty:
    """Test suite for the RemoveNullOrEmpty rule."""

    def test_apply(self):
        """Test the apply method of the RemoveNullOrEmpty rule."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world", ""],
                "date": ["2022-01-01", "2022-01-02", ""],
            }
        )
        result = RemoveNullOrEmpty.apply(documents, columns=["text", "date"])
        assert result.equals(
            pd.DataFrame(
                {
                    "text": ["hello world", "goodbye world"],
                    "date": ["2022-01-01", "2022-01-02"],
                }
            )
        )

    def test_apply_no_columns(self):
        """Test the apply method of the RemoveNullOrEmpty rule with no columns."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world", ""],
                "date": ["2022-01-01", "2022-01-02", ""],
            }
        )
        with pytest.raises(ValueError):
            RemoveNullOrEmpty.apply(documents)

    def test_apply_attribute_not_found(self):
        """Test the apply method of the RemoveNullOrEmpty rule with an attribute not found."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world", ""],
                "date": ["2022-01-01", "2022-01-02", ""],
            }
        )
        with pytest.raises(ValueError):
            RemoveNullOrEmpty.apply(documents, columns=["not_found"])


class TestRemoveByMatch:
    """Test suite for the RemoveByMatch rule."""

    def test_apply(self):
        """Test the apply method of the RemoveByMatch rule."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": ["2022-01-01", "2022-01-02"],
            }
        )
        result = RemoveByMatch.apply(
            documents,
            column="text",
            value="hello",
            operator=FilterOperators.REGEX,
        )
        assert result.equals(
            pd.DataFrame(
                {
                    "text": ["goodbye world"],
                    "date": ["2022-01-02"],
                }
            )
        )

    def test_apply_attribute_not_found(self):
        """Test the apply method of the RemoveByMatch rule with an attribute not found."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": ["2022-01-01", "2022-01-02"],
            }
        )
        with pytest.raises(ValueError):
            RemoveByMatch.apply(documents, column="not_found", value="hello")

    def test_apply_no_column(self):
        """Test the apply method of the RemoveByMatch rule with no column."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": ["2022-01-01", "2022-01-02"],
            }
        )
        with pytest.raises(ValueError):
            RemoveByMatch.apply(documents, value="hello")

    def test_apply_no_value(self):
        """Test the apply method of the RemoveByMatch rule with no value."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "goodbye world"],
                "date": ["2022-01-01", "2022-01-02"],
            }
        )
        with pytest.raises(ValueError):
            RemoveByMatch.apply(documents, column="text")


class TestRemoveDuplicates:
    """Test suite for the RemoveDuplicates rule."""

    def test_apply(self):
        """Test the apply method of the RemoveDuplicates rule."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "hello world"],
                "date": ["2022-01-01", "2022-01-01"],
            }
        )
        result = RemoveDuplicates.apply(documents, columns=["text"])
        assert result.equals(
            pd.DataFrame(
                {
                    "text": ["hello world"],
                    "date": ["2022-01-01"],
                }
            )
        )

    def test_apply_no_columns(self):
        """Test the apply method of the RemoveDuplicates rule with no columns."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "hello world"],
                "date": ["2022-01-01", "2022-01-01"],
            }
        )
        with pytest.raises(ValueError):
            RemoveDuplicates.apply(documents)

    def test_apply_attribute_not_found(self):
        """Test the apply method of the RemoveDuplicates rule with an attribute not found."""
        documents = pd.DataFrame(
            {
                "text": ["hello world", "hello world"],
                "date": ["2022-01-01", "2022-01-01"],
            }
        )
        with pytest.raises(ValueError):
            RemoveDuplicates.apply(documents, columns=["not_found"])


class TestReferenceToInLine:
    """Test suite for the ReferenceToInLine rule."""

    def test_ref_dict(self):
        """Test the _ref_dict method of the ReferenceToInLine rule."""
        text = """
        [1]: http://example.com
        [2]: http://example.org
        """
        expected = {
            "[1]": ["http://example.com"],
            "[2]": ["http://example.org"],
        }
        result = ReferenceToInLine._ref_dict(text)
        assert result == expected

    def test_map_links(self):
        """Test the _map_links method of the ReferenceToInLine rule."""
        text = """
        This is a [link][1] and another [link][2].
        """
        ref_dict = {
            "[1]": ["http://example.com"],
            "[2]": ["http://example.org"],
        }
        expected = [
            "[link](http://example.com)",
            "[link](http://example.org)",
        ]
        result = ReferenceToInLine._map_links(text, ref_dict)
        assert result == expected

    def test_apply(self):
        """Test the apply method of the ReferenceToInLine rule."""
        documents = pd.DataFrame(
            {
                "content": [
                    "This is a [link][1] and another [link][2].\n\n[1]: http://example.com\n[2]: http://example.org"
                ]
            }
        )
        expected = pd.DataFrame(
            {
                "content": [
                    "This is a [link](http://example.com) and another [link](http://example.org).\n\n\n"
                ]
            }
        )
        result = ReferenceToInLine.apply(documents, column="content")
        assert result.equals(expected)

    def test_apply_no_references(self):
        """Test the apply method of the ReferenceToInLine rule with no references."""
        documents = pd.DataFrame(
            {
                "content": [
                    "This is a [link](http://example.com) and another [link](http://example.org)."
                ]
            }
        )
        expected = pd.DataFrame(
            {
                "content": [
                    "This is a [link](http://example.com) and another [link](http://example.org)."
                ]
            }
        )
        result = ReferenceToInLine.apply(documents, column="content")
        assert result.equals(expected)

    def test_apply_missing_reference(self):
        """Test the apply method of the ReferenceToInLine rule with a missing reference."""
        documents = pd.DataFrame(
            {
                "content": [
                    "This is a [link][1] and another [link][2].\n\n[1]: http://example.com"
                ]
            }
        )
        expected = pd.DataFrame(
            {
                "content": [
                    "This is a [link](http://example.com) and another [link][2].\n\n"
                ]
            }
        )
        result = ReferenceToInLine.apply(documents, column="content")
        assert result.equals(expected)
