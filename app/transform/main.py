"""Main module for the transform module."""

from pathlib import Path

import pandas as pd

from cleansweep import __app_name__
from cleansweep.chunk.utils import get_paragraph_delimiter
from cleansweep.core import (
    documents_to_dataframe,
    get_plugin_module,
    read_file_to_dict,
    write_dict_to_newline_delimited_json_file,
    write_to_storage,
)
from cleansweep.core.delta import (
    DeltaComparison,
    DeltaExpiry,
    delta_compare_columns,
    load_delta_file,
)
from cleansweep.core.fileio import create_glob_pattern, create_target_file_path
from cleansweep.enumerations import LoadType
from cleansweep.exceptions import PipelineError
from cleansweep.hooks.hookimpl import get_plugin_manager
from cleansweep.model.core import Defaults
from cleansweep.model.network import raw_path
from cleansweep.model.transform import transform_to_model
from cleansweep.settings.base import settings
from cleansweep.settings.chunk import ChunkSettings
from cleansweep.settings.files import InputFiles
from cleansweep.settings.load import load_settings
from cleansweep.settings.transform import TransformSettings
from cleansweep.utils.exceptions import error_handler, initialize_except_hook
from cleansweep.utils.logging import set_app_labels, setup_logging
from cleansweep.utils.slack import send_notification

logger = setup_logging(__app_name__, settings.log_level, settings.dev_mode)
"""Logger for the module."""

initialize_except_hook(uncaught_hook=error_handler)


def main():
    """Process transform steps."""
    input_files = InputFiles()

    if input_files.config_file_uri is not None:
        logger.info("Config file: %s", input_files.config_file_uri)

    logger.info("Loading settings...")
    app = load_settings(TransformSettings, input_files.config_file_uri)
    set_app_labels(app.labels)
    # load chunk settings to get the delimiter
    chunk_settings = load_settings(ChunkSettings, input_files.config_file_uri)

    input_files.initialize_input_file_uri(app.source)

    logger.info("🔄 Processing %s...", input_files.input_file)

    # set model defaults
    Defaults.language = app.language
    Defaults.classification = app.classification
    Defaults.document_type = app.document_type
    Defaults.delimiter = get_paragraph_delimiter(*chunk_settings.strategy_settings)

    df: pd.DataFrame | None = None

    if app.mapping is None:
        logger.info("📖 Reading source...")
        try:
            input_content = read_file_to_dict(input_files.input_file, app.source_path)
        except ValueError as exc:
            logger.error("Error reading source: %s", exc)
            raise PipelineError("Error reading source") from exc
    else:
        # load and transform source file
        logger.info("🔀 Transforming source...")
        input_content = transform_to_model(
            app.mapping, input_files.input_file, app.source_path
        )

    if app.plugin is not None:
        logger.info("🔌 Applying plugins...")
        plugin_manager = get_plugin_manager()
        try:
            plugin_manager.register(get_plugin_module(app.plugin))
        except ValueError as exc:
            logger.error("Error registering plugin: %s", exc)
        else:
            input_content = plugin_manager.hook.post_transform(documents=input_content)[
                0
            ]

    df, errors = documents_to_dataframe(input_content, app.content_column)

    if errors:
        error_file = create_target_file_path(
            app.staging_bucket,
            "errors",
            raw_path(input_files.input_file),
            extension=".ndjson",
            run_id=app.run_id,
        )

        logger.info("📝 Writing errors to %s", error_file)
        write_dict_to_newline_delimited_json_file(
            errors,
            error_file,
        )

    if df is None or len(df.index) == 0:
        logger.error("No records found")
        exit(1)

    logger.info("📚 Records loaded: %s", len(df.index))

    if app.load_type in [LoadType.DELTA, LoadType.INCREMENTAL]:
        previous_docs_df = load_delta_file(
            app.staging_bucket,
            create_glob_pattern(
                directory=app.output.directory or "curated",
                extension="avro",
            ),
        )

        if previous_docs_df is not None:
            previous_docs_df = previous_docs_df[previous_docs_df["action"] != "D"]

            df = df.merge(
                previous_docs_df[["id", "md5", "metadata_md5", "content_md5"]],
                on="id",
                how="outer",
                suffixes=(None, "_prev"),
            )
        else:
            df["md5_prev"] = [None] * df.shape[0]
            df["metadata_md5_prev"] = [None] * df.shape[0]
            df["content_md5_prev"] = [None] * df.shape[0]

        df = delta_compare_columns(
            df,
            [
                DeltaComparison(left="md5", right="md5_prev", output="action"),
                DeltaExpiry(expiry_column="metadata_expiry", output="action"),
                DeltaComparison(
                    left="metadata_md5",
                    right="metadata_md5_prev",
                    output="metadata_is_modified",
                ),
                DeltaComparison(
                    left="content_md5",
                    right="content_md5_prev",
                    output="content_is_modified",
                ),
            ],
        )
        logger.info("📥 Records for update: %s", len(df[df["action"] == "U"].index))
        logger.info("📥 Records for insert: %s", len(df[df["action"] == "I"].index))
        logger.info("🗑️ Records for delete: %s", len(df[df["action"] == "D"].index))
        logger.info(
            "❌ Records with no change: %s",
            len(df[df["action"] == "N"].index),
        )
        logger.info(
            "❌ Records with no action: %s",
            len(df[df["action"] == ""].index),
        )
    else:
        df["md5_prev"] = [None] * df.shape[0]
        df["action"] = ["I"] * df.shape[0]
        df["metadata_md5_prev"] = [None] * df.shape[0]
        df["metadata_is_modified"] = [True] * df.shape[0]
        df["content_md5_prev"] = [None] * df.shape[0]
        df["content_is_modified"] = [True] * df.shape[0]

    df["action"] = df["action"].apply(lambda x: "N" if x == "" else x)

    if app.load_type in [LoadType.DELTA, LoadType.INCREMENTAL]:
        # check if all records are marked for no action
        if len(df[df["action"] == "N"].index) == len(df.index):
            logger.info("No records to process. Exiting")
            send_notification(
                app.default_channel,
                f"File {input_files.input_file} has no records to process.",
            )
            exit(0)

    input_file_path = Path(raw_path(input_files.input_file))
    df["source_file"] = input_file_path.name

    target_file = create_target_file_path(
        bucket=app.output.bucket or app.staging_bucket,
        target_dir=app.output.directory or "curated",
        source_file=raw_path(input_files.input_file),
        file_name=app.output.file_name,
        extension=app.output.extension or ".avro",
        run_id=app.run_id if app.output.use_run_id else None,
        prefix=app.output.prefix,
    )

    write_to_storage(df, target_file)

    exit(0)
