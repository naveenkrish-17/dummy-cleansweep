"""Main module for the clean module."""

from pathlib import Path

from cleansweep import __app_name__
from cleansweep.clean import clean_documents
from cleansweep.core import (
    get_plugin_module,
    read_curated_file_to_dataframe,
    read_file_to_dict,
    write_to_storage,
)
from cleansweep.core.delta import delta_merge, delta_prepare
from cleansweep.core.fileio import create_glob_pattern, create_target_file_path
from cleansweep.dq.data_quality import run_data_quality_checks
from cleansweep.dq.dq_expectations import create_expectations
from cleansweep.enumerations import LoadType
from cleansweep.model.network import raw_path
from cleansweep.settings.base import settings
from cleansweep.settings.clean import CleanSettings
from cleansweep.settings.files import InputFiles
from cleansweep.settings.load import load_settings
from cleansweep.utils.exceptions import error_handler, initialize_except_hook
from cleansweep.utils.logging import set_app_labels, setup_logging

logger = setup_logging(__app_name__, settings.log_level, settings.dev_mode)
"""Logger for the module."""

initialize_except_hook(uncaught_hook=error_handler)


def main():
    """Process cleaning steps."""
    input_files = InputFiles()

    if input_files.config_file_uri is not None:
        logger.info("Config file: %s", input_files.config_file_uri)

    logger.info("Loading settings...")
    app = load_settings(CleanSettings, input_files.config_file_uri)
    set_app_labels(app.labels)

    input_files.initialize_input_file_uri(app.source)

    target_file = create_target_file_path(
        bucket=app.output.bucket or app.staging_bucket,
        target_dir=app.output.directory or "cleaned",
        source_file=raw_path(input_files.input_file),
        file_name=app.output.file_name,
        extension=app.output.extension or ".avro",
        run_id=app.run_id if app.output.use_run_id else None,
        prefix=app.output.prefix,
    )

    logger.info("📖 Reading curated data file %s.", input_files.input_file)
    source_df = read_curated_file_to_dataframe(input_files.input_file)

    logger.info("📚 Read %d documents.", len(source_df.index))

    to_process, previous_df = delta_prepare(
        source_df,
        app.load_type,
        app.staging_bucket,
        match_glob=create_glob_pattern(
            directory=app.output.directory or "cleaned",
            extension=".avro",
            prefix=app.source.prefix,
        ),
        force=app.force,
    )

    cleaned_df = None

    if to_process is not None and not to_process.empty:
        plugin = None
        if app.plugin is not None:
            logger.info("🔌 Applying plugins...")
            plugin = [get_plugin_module(app.plugin)]

        logger.info("🧹 Cleaning records...")
        pre_clean_count = to_process.shape[0]
        cleaned_df = clean_documents(to_process, app.rules, plugins=plugin)
        post_clean_count = cleaned_df.shape[0]

        logger.info(
            "Data cleaning complete. %s records removed from %s records.",
            (pre_clean_count - post_clean_count),
            pre_clean_count,
        )
    elif app.load_type in [LoadType.DELTA, LoadType.INCREMENTAL]:
        # we could have only deletions so there are no records to process
        # but we still need to load the previous_df
        logger.info("No records to process. Loading previous_df.")
        cleaned_df = previous_df
        previous_df = None
    else:
        logger.info("No records to process. Exiting...")
        exit(0)

    assert cleaned_df is not None, "Cleaned dataframe is None."
    final_df = delta_merge(cleaned_df, previous_df)
    # Load standard clean expectations.
    if app.dq_check:
        logger.info("Getting standard clean expectations...")
        config = read_file_to_dict(
            f'file:/{Path(__file__).parent.joinpath("clean_expectations.yml").as_posix()}'
        )[0]
        logger.info("Getting custom clean expectations...")
        e_suite = create_expectations(config, app.dq_custom_expectations)

        # set parameters
        suite_name = f"{app.platform}_{app.domain}_{app.name}_{app.app}"
        logger.info("Running data validation checks...")
        run_data_quality_checks(final_df, suite_name, e_suite)
        logger.info("Data validation complete.")

    if not final_df.empty:
        write_to_storage(final_df, target_file)
        logger.info("Data cleaning complete")
    else:
        logger.warning("No records to write to storage. The DataFrame is empty.")

    exit(0)
