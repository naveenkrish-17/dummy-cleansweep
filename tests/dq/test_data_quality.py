import pandas as pd
import pytest

from cleansweep.dq.data_quality import run_data_quality_checks
from cleansweep.dq.dq_expectations import get_expectations
from cleansweep.settings.clean import CleanSettings
from cleansweep.settings.load import load_settings

# Prepare
df = pd.DataFrame({"title": ["hello"], "content": ["world"]})
settings = load_settings(CleanSettings)
settings.dq_check = True


class TestDataQuality:
    """Test suite for the data quality module."""

    def test_run_data_quality_checks(self):
        """Test the run_data_quality_checks function."""
        config = {
            "expectations": [
                {
                    "expectation": "expect_column_values_to_be_of_type",
                    "include": [("title", "string"), ("content", "string")],
                }
            ],
            "schema": {"title": "string", "content": "string"},
        }
        # Execute
        run_data_quality_checks(df, "test_suite", get_expectations(config))
        # Assert
        assert True

    def test_run_data_quality_checks_no_expectations(self):
        """Test the run_data_quality_checks function with no expectations."""

        # Prepare
        df = pd.DataFrame(
            {"title": ["Sample Title"], "content": ["Sample content"]}
        )  # Example DataFrame

        # Execute and Assert
        with pytest.raises(TypeError, match="'NoneType' object is not iterable"):
            run_data_quality_checks(df, "test_suite", None)
