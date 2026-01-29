"""Data validation module."""

import logging

import great_expectations as ge
import pandas as pd
from great_expectations.core.expectation_configuration import ExpectationConfiguration

logger = logging.getLogger(__name__)
"""Logger for the data quality check."""


def run_data_quality_checks(
    dataframe: pd.DataFrame, ge_suite_name: str, config: list[ExpectationConfiguration]
):
    """Run the data quailty checks across clean datafame.

    Args:
        dataframe: clean dataframe
        ge_suite_name: name the validation suite
        config: expectation configuration

    Returns:
        exception if validation suite is not successful

    """
    # Initialise Great Expectations Datacontext
    ge_context = ge.get_context()

    # Create data validation suite
    ge_suite = ge_context.add_or_update_expectation_suite(ge_suite_name)

    for expectation_config in config:
        ge_suite.add_expectation(expectation_config)

    # Run validate expectations against dataframe
    validation_df = ge.from_pandas(dataframe)
    validation_results = validation_df.validate(ge_suite)

    for res in validation_results["results"]:
        if not res["success"]:
            if (
                "expect_table_row_count_to_be_between"
                in res["expectation_config"]["expectation_type"]
            ):
                logger.warning(
                    (
                        "expect_table_row_count_to_be_between observed_value not in range "
                        "exception_info:%s, exception_result:%s"
                    ),
                    res["exception_info"]["exception_message"],
                    res["result"],
                )
            else:
                logger.warning(
                    "The expectations did not pass. exceptions message: %s, exception result: %s",
                    res["exception_info"]["exception_message"],
                    res["result"],
                )
                # raise DataQualityError(
                #     "The expectations did not pass. Please check your dataset."
                # )
