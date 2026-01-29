import json

import pandas as pd
import pytest

from cleansweep.dq.dq_expectations import create_expectations, get_expectations
from cleansweep.settings.clean import CleanSettings
from cleansweep.settings.load import load_settings

# Prepare
df = pd.DataFrame({"title": ["hello"], "content": ["world"]})
settings = load_settings(CleanSettings)
settings.dq_check = True
settings.dq_custom_expectations = None


class TestDQExpectations:
    """Test suite for the data quality expectations module."""

    def test_get_expectations(self):
        """Test the create_expectations function."""
        # Prepare
        config = {
            "expectations": [
                {
                    "expectation": "expect_column_values_to_be_of_type",
                }
            ],
            "schema": {"title": "string", "content": "string"},
        }
        # Execute

        expectations = get_expectations(config)
        # Assert
        # Assert
        expected_expectations = [
            {
                "expectation_type": "expect_column_values_to_be_of_type",
                "kwargs": {"column": "title", "type_": "string"},
                "meta": {},
            },
            {
                "expectation_type": "expect_column_values_to_be_of_type",
                "kwargs": {"column": "content", "type_": "string"},
                "meta": {},
            },
        ]

        assert str(expectations) == json.dumps(expected_expectations)
        assert len(expected_expectations) == 2
        assert expectations[0].expectation_type == "expect_column_values_to_be_of_type"
        assert expectations[1].expectation_type == "expect_column_values_to_be_of_type"
        assert expectations[0].kwargs == {"column": "title", "type_": "string"}
        assert expectations[1].kwargs == {"column": "content", "type_": "string"}

    def test_get_expectations_no_schema(self):
        """Test the create_expectations function with no schema."""
        # Prepare
        config = {
            "expectations": [
                {
                    "expectation": "expect_column_values_to_be_of_type",
                    "kwargs": {"column": "title", "type_": "string"},
                    "meta": {},
                },
                {
                    "expectation": "expect_column_values_to_be_of_type",
                    "kwargs": {"column": "content", "type_": "string"},
                    "meta": {},
                },
            ]
        }

        # Execute and Assert
        with pytest.raises(KeyError, match="'schema'"):
            create_expectations(config, settings.dq_custom_expectations)

    def test_get_expectations_no_expectations(self):
        """Test the create_expectations function with no expectations."""
        # Prepare
        config = {
            "schema": {"title": "string", "content": "string"},
        }
        # Execute and Assert
        with pytest.raises(KeyError, match="'expectations'"):
            create_expectations(config, settings.dq_custom_expectations)
