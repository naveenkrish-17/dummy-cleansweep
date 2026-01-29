"""Module for merging data."""

import pandas as pd

from cleansweep import __app_name__
from cleansweep.core import read_curated_file_to_dataframe
from cleansweep.core.fileio import create_target_file_path, write_to_storage
from cleansweep.model.network import raw_path
from cleansweep.settings.base import settings
from cleansweep.settings.files import InputFiles
from cleansweep.settings.load import load_settings
from cleansweep.settings.merge import MergeSettings
from cleansweep.utils.exceptions import error_handler, initialize_except_hook
from cleansweep.utils.logging import set_app_labels, setup_logging

logger = setup_logging(__app_name__, settings.log_level, settings.dev_mode)
"""Logger for the module."""

initialize_except_hook(uncaught_hook=error_handler)


def main():
    """Process merge."""
    input_files = InputFiles()

    if input_files.config_file_uri is not None:
        logger.info("Config file: %s", input_files.config_file_uri)

    logger.info("Loading settings...")
    app = load_settings(MergeSettings, input_files.config_file_uri)
    set_app_labels(app.labels)

    app.initialize_input_files()

    assert app.left_file_uri is not None, "Left file URI is required."
    assert app.right_file_uri is not None, "Right file URI is required."

    logger.info("Merging data from %s with %s.", app.left_file_uri, app.right_file_uri)

    target_file = create_target_file_path(
        bucket=app.output.bucket or app.staging_bucket,
        target_dir=app.output.directory or "merged",
        source_file=raw_path(app.left_file_uri),
        file_name=app.output.file_name,
        extension=app.output.extension or ".avro",
        run_id=app.run_id if app.output.use_run_id else None,
        prefix=app.output.prefix,
    )

    logger.info("📖 Reading curated data files...")
    left = read_curated_file_to_dataframe(app.left_file_uri)

    logger.info("📚 Read %d records from %s.", len(left.index), app.left_file_uri)

    if app.left_columns:
        left = left[app.left_columns]

    right = read_curated_file_to_dataframe(app.right_file_uri)

    if app.right_columns:
        right = right[app.right_columns]

    logger.info("📚 Read %d records from %s.", len(right.index), app.right_file_uri)

    on = app.on
    left_on = app.left_on
    right_on = app.right_on

    if left_on and right_on:
        on = None
    elif left_on:
        right_on = on
        on = None
    elif right_on:
        left_on = on
        on = None

    merged_df = pd.merge(
        left, right, how=app.how, on=on, left_on=left_on, right_on=right_on
    )
    logger.info("🔗 Merged %d records.", len(merged_df.index))

    write_to_storage(merged_df, target_file)
    exit(0)
