"""DQ expectations module."""

import logging
from typing import Any, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration

from cleansweep.core import read_file_to_dict
from cleansweep.model.network import CloudStorageUrl, FileUrl

logger = logging.getLogger(__name__)
"""Logger for the data quality expectations check."""


def get_expectations(config: dict[str, Any]) -> list[ExpectationConfiguration]:
    """Get the expectations from the configuration file.

    Args:
    ----
        config: The configuration file

    Returns:
    -------
        A list of ExpectationConfiguration objects

    """
    # Convert the configurations to ExpectationConfiguration objects
    expectations = []

    for expectation in config["expectations"]:
        # Get the include and exclude keys
        exclude = expectation.get("exclude", [])
        include = expectation.get("include", [])

        # Get the keys and key values
        all_keys = list(config["schema"].keys())
        all_key_values = list(config["schema"].items())

        # Get the included keys and key values
        include_only_kv = [key for key in all_key_values if key[0] in include]
        included_keys = [key for key in all_keys if key not in exclude]
        included_key_values = [key for key in all_key_values if key[0] not in exclude]

        # Check if the schema is empty
        if not config["schema"]:
            logger.debug("Schema is empty")
            return []

        if expectation["expectation"] == "expect_table_columns_to_match_ordered_list":
            # kwargs = {'column_list': list(config['schema'].keys())}
            if include:
                # kwargs = {'column_list': list(config['schema'].keys())}
                kwargs = {"column_list": list(include)}
            elif exclude:
                kwargs = {"column_list": list(included_keys)}
            else:
                kwargs = {"column_list": list(all_keys)}

            expectations.append(
                ExpectationConfiguration(expectation["expectation"], kwargs=kwargs)
            )
        elif expectation["expectation"] == "expect_column_values_to_be_of_type":
            if include:
                for column, typ in include_only_kv:
                    kwargs = {"column": column, "type_": typ}
                    expectations.append(
                        ExpectationConfiguration(
                            expectation["expectation"], kwargs=kwargs
                        )
                    )
            elif exclude:
                for column, typ in included_key_values:
                    kwargs = {"column": column, "type_": typ}
                    expectations.append(
                        ExpectationConfiguration(
                            expectation["expectation"], kwargs=kwargs
                        )
                    )
            else:
                for column, typ in config["schema"].items():
                    kwargs = {"column": column, "type_": typ}
                    expectations.append(
                        ExpectationConfiguration(
                            expectation["expectation"], kwargs=kwargs
                        )
                    )
        elif expectation["expectation"] == "expect_column_values_to_not_be_null":
            if include:
                for column in include:
                    kwargs = {"column": column}
                    expectations.append(
                        ExpectationConfiguration(
                            expectation["expectation"], kwargs=kwargs
                        )
                    )
            elif exclude:
                for column in included_keys:
                    kwargs = {"column": column}
                    expectations.append(
                        ExpectationConfiguration(
                            expectation["expectation"], kwargs=kwargs
                        )
                    )
            else:
                for column, typ in all_key_values:
                    if not column.startswith("metadata_") and column != "md5_prev":
                        kwargs = {"column": column}
                        expectations.append(
                            ExpectationConfiguration(
                                expectation["expectation"], kwargs=kwargs
                            )
                        )
        else:
            kwargs = expectation.get("kwargs", {})
            expectations.append(
                ExpectationConfiguration(expectation["expectation"], kwargs=kwargs)
            )
    return expectations


def create_expectations(
    config: dict[str, Any],
    custom_config_uri: Optional[CloudStorageUrl | FileUrl] = None,
) -> list[ExpectationConfiguration]:
    """Get generic and custom expectations for expectation suite.

    Args:
        config (dict): The configuration file
        custom_config_uri (CloudStorageUrl | FileUrl, optional): The custom configuration file.
            Defaults to None.

    Returns:
        A list of ExpectationConfiguration objects

    """
    if custom_config_uri:
        # updating or adding custom schema and expectations
        logger.info("Updating standard clean expectations with custom expectations")
        custom_config = read_file_to_dict(custom_config_uri)[0]

        config["schema"].update(custom_config["schema"])
        config["expectations"].extend(custom_config["expectations"])

    # Get the expectations
    logger.info(
        "Generate expectations from the combination of standard and custom expectations"
    )
    e_suite = get_expectations(config)

    return e_suite
