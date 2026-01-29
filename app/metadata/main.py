"""Module for adding metadata to documents before embedding."""

from cleansweep import __app_name__
from cleansweep.core import (
    get_plugin_module,
    read_curated_file_to_dataframe,
    write_to_storage,
)
from cleansweep.core.delta import delta_merge, delta_prepare
from cleansweep.core.fileio import create_glob_pattern, create_target_file_path
from cleansweep.enumerations import LoadType
from cleansweep.hooks.hookimpl import get_plugin_manager
from cleansweep.metadata import add_metadata_to_df
from cleansweep.model.network import raw_path
from cleansweep.settings.base import settings
from cleansweep.settings.files import InputFiles
from cleansweep.settings.load import load_settings
from cleansweep.settings.metadata import MetadataSettings
from cleansweep.utils.exceptions import error_handler, initialize_except_hook
from cleansweep.utils.logging import set_app_labels, setup_logging

logger = setup_logging(__app_name__, settings.log_level, settings.dev_mode)
"""Logger for the module."""

initialize_except_hook(uncaught_hook=error_handler)


def main():
    """Process metadata steps."""
    input_files = InputFiles()

    if input_files.config_file_uri is not None:
        logger.info("Config file: %s", input_files.config_file_uri)

    logger.info("Loading settings...")
    app = load_settings(MetadataSettings, input_files.config_file_uri)
    assert not isinstance(app.model, str), "Model must be a Deployment object."
    set_app_labels(app.labels)

    input_files.initialize_input_file_uri(app.source)

    target_file = create_target_file_path(
        source_file=raw_path(input_files.input_file),
        bucket=app.output.bucket or app.staging_bucket,
        target_dir=app.output.directory or "metadata",
        file_name=app.output.file_name,
        extension=app.output.extension or ".avro",
        run_id=app.run_id if app.output.use_run_id else None,
        prefix=app.output.prefix,
    )

    logger.info("📖 Reading curated data file %s.", input_files.input_file)
    source = read_curated_file_to_dataframe(input_files.input_file)

    logger.info("📚 Read %d documents.", len(source.index))

    to_process, previous_df = delta_prepare(
        source,
        app.load_type,
        app.staging_bucket,
        match_glob=create_glob_pattern(
            directory=app.output.directory or "metadata",
            extension=".avro",
        ),
        force=app.force,
    )

    metadata_df = None

    if to_process is not None and not to_process.empty:

        metadata_df = add_metadata_to_df(
            to_process,
            app.configs,
            app.prompts_template_dir,
            app.model,
            temperature=app.temperature,
            timeout=app.timeouts.metadata,
        )

        # execute the hooks
        if app.plugin is not None:
            logger.info("🔌 Applying plugins...")
            plugin = get_plugin_module(app.plugin)
            plugin_manager = get_plugin_manager()
            plugin_manager.register(plugin)

            results = plugin_manager.hook.metadata_transform(documents=metadata_df)
            if results:
                metadata_df = results[0]
    elif app.load_type in [LoadType.DELTA, LoadType.INCREMENTAL]:
        # we could have only deletions so there are no records to process
        # but we still need to load the previous_df
        logger.info("No records to process. Loading previous_df.")
        metadata_df = previous_df
        previous_df = None
    else:
        logger.info("No records to process. Exiting...")
        exit(0)

    assert metadata_df is not None, "Metadata dataframe is None."

    final_documents = delta_merge(metadata_df, previous_df)

    write_to_storage(final_documents, target_file)
    exit(0)
