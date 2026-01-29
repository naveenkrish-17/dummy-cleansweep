"""Test suite for the Filter class."""

import datetime

import pandas as pd
import pytest

from cleansweep.clean.filter import Filter, FilterOperators, get_filter_operator


class TestGetFilterOperator:

    def test_get_filter_operator(self):
        """Test the `get_filter_operator` method."""
        assert get_filter_operator("==") == FilterOperators.EQUAL
        assert get_filter_operator("!=") == FilterOperators.NOT_EQUAL
        assert get_filter_operator(">") == FilterOperators.GREATER_THAN
        assert get_filter_operator(">=") == FilterOperators.GREATER_THAN_OR_EQUAL
        assert get_filter_operator("<") == FilterOperators.LESS_THAN
        assert get_filter_operator("<=") == FilterOperators.LESS_THAN_OR_EQUAL
        assert get_filter_operator("eq") == FilterOperators.EQUAL
        assert get_filter_operator("ne") == FilterOperators.NOT_EQUAL
        assert get_filter_operator("gt") == FilterOperators.GREATER_THAN
        assert get_filter_operator("ge") == FilterOperators.GREATER_THAN_OR_EQUAL
        assert get_filter_operator("lt") == FilterOperators.LESS_THAN
        assert get_filter_operator("le") == FilterOperators.LESS_THAN_OR_EQUAL
        assert get_filter_operator("=") == FilterOperators.EQUAL
        assert get_filter_operator("=<") == FilterOperators.LESS_THAN_OR_EQUAL
        assert get_filter_operator("=>") == FilterOperators.GREATER_THAN_OR_EQUAL
        assert get_filter_operator("<>") == FilterOperators.NOT_EQUAL
        assert get_filter_operator("invalid") == FilterOperators.EQUAL


def get_scenario(key: str) -> dict:
    """Return the scenario for the given key."""
    return SCENARIOS[key]


FILTER_BY_COLUMN_SCENARIOS = [
    pytest.param(
        pd.DataFrame(
            {"column1": [1, 2, 3, 4, 5], "column2": ["a", "b", "c", "d", "e"]}
        ),
        {
            "column": "column2",
            "value": ["a", "b"],
            "operator": None,
        },
        pd.DataFrame({"column1": [1, 2], "column2": ["a", "b"]}),
        id="filter by list",
    ),
    pytest.param(
        pd.DataFrame(
            {"column1": [1, 2, 3, 4, 5], "column2": ["a", "b", "c", "d", "e"]}
        ),
        {
            "column": "column2",
            "value": ["a", "b"],
            "operator": "not in",
        },
        pd.DataFrame({"column1": [3, 4, 5], "column2": ["c", "d", "e"]}),
        id="filter out by list",
    ),
    pytest.param(
        pd.DataFrame(
            {"column1": [1, 2, 3, 4, 5], "column2": ["a", "b", "c", "d", "e"]}
        ),
        {
            "column": "column1",
            "value": 3,
            "operator": FilterOperators.EQUAL,
        },
        pd.DataFrame({"column1": [3], "column2": ["c"]}),
        id="equal to value",
    ),
    pytest.param(
        pd.DataFrame(
            {"column1": [1, 2, 3, 4, 5], "column2": ["a", "b", "c", "d", "e"]}
        ),
        {
            "column": "column1",
            "value": 3,
            "operator": FilterOperators.NOT_EQUAL,
        },
        pd.DataFrame({"column1": [1, 2, 4, 5], "column2": ["a", "b", "d", "e"]}),
        id="not equal to value",
    ),
    pytest.param(
        pd.DataFrame(
            {"column1": [1, 2, 3, 4, 5], "column2": ["a", "b", "c", "d", "e"]}
        ),
        {
            "column": "column1",
            "value": 3,
            "operator": FilterOperators.LESS_THAN,
        },
        pd.DataFrame({"column1": [1, 2], "column2": ["a", "b"]}),
        id="less than value",
    ),
    pytest.param(
        pd.DataFrame(
            {"column1": [1, 2, 3, 4, 5], "column2": ["a", "b", "c", "d", "e"]}
        ),
        {
            "column": "column1",
            "value": 3,
            "operator": FilterOperators.LESS_THAN_OR_EQUAL,
        },
        pd.DataFrame({"column1": [1, 2, 3], "column2": ["a", "b", "c"]}),
        id="less than or equal to value",
    ),
    pytest.param(
        pd.DataFrame(
            {"column1": [1, 2, 3, 4, 5], "column2": ["a", "b", "c", "d", "e"]}
        ),
        {
            "column": "column1",
            "value": 3,
            "operator": FilterOperators.GREATER_THAN,
        },
        pd.DataFrame({"column1": [4, 5], "column2": ["d", "e"]}),
        id="greater than value",
    ),
    pytest.param(
        pd.DataFrame(
            {"column1": [1, 2, 3, 4, 5], "column2": ["a", "b", "c", "d", "e"]}
        ),
        {
            "column": "column1",
            "value": 3,
            "operator": FilterOperators.GREATER_THAN_OR_EQUAL,
        },
        pd.DataFrame({"column1": [3, 4, 5], "column2": ["c", "d", "e"]}),
        id="greater than or equal to value",
    ),
    pytest.param(
        pd.DataFrame({"column1": [1, 2], "column2": [["a", "b", "c"], ["d", "e"]]}),
        {
            "column": "column2",
            "value": "a",
            "operator": FilterOperators.IS_IN,
        },
        pd.DataFrame({"column1": [1], "column2": [["a", "b", "c"]]}),
        id="value is in - dataframe series is an array",
    ),
    pytest.param(
        pd.DataFrame({"column1": [1, 2], "column2": [["a", "b", "c"], ["d", "e"]]}),
        {
            "column": "column2",
            "value": "a",
            "operator": FilterOperators.IS_NOT_IN,
        },
        pd.DataFrame({"column1": [2], "column2": [["d", "e"]]}),
        id="value is not in - dataframe series is an array",
    ),
]

SCENARIOS = {
    "filter_by_columns": [
        {
            "df": pd.DataFrame(
                {"column1": [1, 2, 3, 4, 5], "column2": ["a", "b", "c", "d", "e"]}
            ),
            "kwargs": {
                "filters": {
                    "column1": (3, FilterOperators.GREATER_THAN),
                    "column2": (["d"], None),
                }
            },
            "expected_df": pd.DataFrame({"column1": [4], "column2": ["d"]}),
        },
        {
            "df": pd.DataFrame(
                {"column1": [1, 2, 3, 4, 5], "column2": ["a", "b ", "c", " d", "e"]}
            ),
            "kwargs": {
                "filters": {
                    "column2": ("\\s", "regex"),
                }
            },
            "expected_df": pd.DataFrame({"column1": [2, 4], "column2": ["b ", " d"]}),
        },
    ],
}


class TestFilter:
    """Test suite for the Filter class."""

    def test_filter_by_match(self):
        """Test the `filter_by_match` method."""
        df = pd.DataFrame({"column1": ["alpha", "bravo", "cappa", "delta", "epsilon"]})
        filtered_df = Filter.filter_by_match(df, "column1", "a$")
        assert filtered_df.equals(
            pd.DataFrame({"column1": ["alpha", "cappa", "delta"]})
        )

    @pytest.mark.parametrize("df, kwargs, expected_df", FILTER_BY_COLUMN_SCENARIOS)
    def test_filter_by_column(self, df, kwargs, expected_df):
        """Test the `filter_by_column` method."""
        filtered_df = Filter.filter_by_column(df, **kwargs)
        assert filtered_df.equals(expected_df)

    @pytest.mark.parametrize("scenario", get_scenario("filter_by_columns"))
    def test_filter_by_columns(self, scenario):
        """Test the `filter_by_columns` method."""
        df = scenario["df"]
        expected_df = scenario["expected_df"]
        filtered_df = Filter.filter_by_columns(df, **scenario["kwargs"])
        assert filtered_df.equals(expected_df)

    def test_filter_by_date_range(self):
        """Test the `filter_by_date_range` method."""
        df = pd.DataFrame({"date": pd.date_range(start="1/1/2022", end="1/31/2022")})
        start_date = datetime.datetime(2022, 1, 10)
        end_date = datetime.datetime(2022, 1, 20)
        filtered_df = Filter.filter_by_date_range(df, "date", start_date, end_date)

        expected_df = pd.DataFrame(
            {"date": pd.date_range(start="1/10/2022", end="1/20/2022")}
        )

        assert filtered_df.equals(expected_df)

    def test_exclude_by_date_range(self):
        """Test the `exclude_by_date_range` method."""
        df = pd.DataFrame({"date": pd.date_range(start="1/1/2022", end="1/31/2022")})
        start_date = datetime.datetime(2022, 1, 10)
        end_date = datetime.datetime(2022, 1, 20)
        filtered_df = Filter.exclude_by_date_range(df, "date", start_date, end_date)

        expected_df = pd.DataFrame(
            {
                "date": pd.date_range(start="1/1/2022", end="1/9/2022").append(
                    pd.date_range(start="1/21/2022", end="1/31/2022")
                )
            }
        )

        assert filtered_df.equals(expected_df)

    def test_remove_null_or_empty(self):
        """Test the `remove_null_or_empty` method."""
        df = pd.DataFrame(
            {
                "column1": [1, 2, 3, 4, None, 6],
                "column2": ["a", "b", "c", "d", "e", ""],
            }
        )
        filtered_df = Filter.remove_null_or_empty(df, ["column1", "column2"])
        assert filtered_df.equals(df.replace("", pd.NA).dropna())

    def test_remove_by_match(self):
        """Test the `remove_by_match` method."""
        df = pd.DataFrame({"column1": ["alpha", "bravo", "cappa", "delta", "epsilon"]})
        filtered_df = Filter.remove_by_match(df, "column1", "a$")
        assert filtered_df.equals(pd.DataFrame({"column1": ["bravo", "epsilon"]}))

    def test_remove_duplicates(self):
        """Test the `remove_duplicates` method."""
        df = pd.DataFrame(
            {
                "column1": [1, 2, 3, 4, 5, 1, 2, 3, 4, 5],
                "column2": ["a", "b", "c", "d", "e", "a", "b", "c", "d", "e"],
            }
        )

        expected_df = pd.DataFrame(
            {"column1": [1, 2, 3, 4, 5], "column2": ["a", "b", "c", "d", "e"]}
        )
        filtered_df = Filter.remove_duplicates(df, ["column1", "column2"])
        assert filtered_df.equals(expected_df)

    def test_remove_duplicates_errors(self):
        """Test the `remove_duplicates` method with errors."""
        df = pd.DataFrame(
            {"column1": [1, 2, 3, 4, 5], "column2": ["a", "b", "c", "d", "e"]}
        )
        with pytest.raises(ValueError):
            Filter.remove_duplicates(df, ["column1", "column2", "column3"])

        with pytest.raises(ValueError):
            Filter.remove_duplicates(df, ["column1"], order_by="column3")
