"""Module for dropping data."""

from cleansweep import __app_name__
from cleansweep.core import read_curated_file_to_dataframe
from cleansweep.core.fileio import create_target_file_path, write_to_storage
from cleansweep.model.network import raw_path
from cleansweep.settings.base import settings
from cleansweep.settings.drop import DropSettings
from cleansweep.settings.files import InputFiles
from cleansweep.settings.load import load_settings
from cleansweep.utils.exceptions import error_handler, initialize_except_hook
from cleansweep.utils.logging import set_app_labels, setup_logging

logger = setup_logging(__app_name__, settings.log_level, settings.dev_mode)
"""Logger for the module."""

initialize_except_hook(uncaught_hook=error_handler)


def main():
    """Process drop."""
    input_files = InputFiles()

    if input_files.config_file_uri is not None:
        logger.info("Config file: %s", input_files.config_file_uri)

    logger.info("Loading settings...")
    app = load_settings(DropSettings, input_files.config_file_uri)
    set_app_labels(app.labels)

    input_files.initialize_input_file_uri(app.source)

    target_file = create_target_file_path(
        target_dir=app.output.directory or app.source.directory,
        source_file=raw_path(input_files.input_file),
        bucket=app.output.bucket or app.staging_bucket,
        file_name=app.output.file_name,
        extension=app.output.extension or ".avro",
        run_id=app.run_id if app.output.use_run_id else None,
        prefix=app.output.prefix,
    )

    logger.info("📖 Reading curated data file %s.", input_files.input_file)
    source_df = read_curated_file_to_dataframe(input_files.input_file)

    logger.info("📚 Read %d documents.", len(source_df.index))

    columns_to_drop = [column for column in app.columns if column in source_df.columns]

    target_df = source_df.drop(columns=columns_to_drop)

    write_to_storage(target_df, target_file)
    exit(0)
