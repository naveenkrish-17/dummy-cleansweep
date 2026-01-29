"""Module for concatenating data."""

from pathlib import Path

import pandas as pd

from cleansweep import __app_name__
from cleansweep.core import read_curated_file_to_dataframe
from cleansweep.core.fileio import create_target_file_path, write_to_storage
from cleansweep.settings.base import settings
from cleansweep.settings.concatenate import ConcatenateSettings
from cleansweep.settings.files import InputFiles
from cleansweep.settings.load import load_settings
from cleansweep.utils.exceptions import error_handler, initialize_except_hook
from cleansweep.utils.logging import set_app_labels, setup_logging

logger = setup_logging(__app_name__, settings.log_level, settings.dev_mode)
"""Logger for the module."""

initialize_except_hook(uncaught_hook=error_handler)


def main():
    """Process concatenate."""
    input_files = InputFiles()

    if input_files.config_file_uri is not None:
        logger.info("Config file: %s", input_files.config_file_uri)

    logger.info("Loading settings...")
    app = load_settings(ConcatenateSettings, input_files.config_file_uri)
    set_app_labels(app.labels)

    logger.info("Concatenating %s files", len(app.file_uris))

    target_file_name = (
        app.output.file_name
        or (Path(app.file_uris[0].unicode_string()).name if app.file_uris else None)
        or "concatenated_data"
    )

    # clean up funny extensions 😱
    for ext in [".nd.json"]:
        if target_file_name.endswith(ext):
            target_file_name = target_file_name.replace(ext, "")

    if not app.output.use_run_id:
        target_file_name.replace(f"_{app.run_id or ""}", "")

    target_file = create_target_file_path(
        target_dir=app.output.directory or "concatenated",
        source_file=target_file_name,
        extension=app.output.extension or ".avro",
        run_id=app.run_id if app.output.use_run_id else None,
        prefix=app.output.prefix,
        bucket=app.output.bucket or app.staging_bucket,
        file_name=app.output.file_name,
    )

    logger.info("📖 Reading curated data files...")
    files = []

    for file_uri in app.file_uris:
        logger.info("Reading file %s", file_uri)
        df = read_curated_file_to_dataframe(file_uri)
        files.append(df)

    columns = {col for file in files for col in file.columns}
    for file in files:
        for col in columns:
            if col not in file.columns:
                file[col] = None

    logger.info("📖 Concatenating data files...")

    merged_df = pd.concat(files, ignore_index=True)

    write_to_storage(merged_df, target_file)
    exit(0)
