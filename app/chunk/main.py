"""Module for chunking and translating text data."""

from cleansweep import __app_name__
from cleansweep.chunk.chunk import create_chunked_df
from cleansweep.core import get_plugin_module, read_curated_file_to_dataframe
from cleansweep.core.delta import delta_merge, delta_prepare
from cleansweep.core.fileio import (
    create_glob_pattern,
    create_target_file_path,
    write_to_storage,
)
from cleansweep.enumerations import EmbedderType, LoadType
from cleansweep.hooks.hookimpl import get_plugin_manager
from cleansweep.model.network import raw_path
from cleansweep.settings.base import settings
from cleansweep.settings.chunk import ChunkSettings
from cleansweep.settings.embedding import EmbeddingSettings
from cleansweep.settings.files import InputFiles
from cleansweep.settings.load import load_settings
from cleansweep.settings.translation import TranslationSettings
from cleansweep.utils.azure.utils import num_tokens_from_strings
from cleansweep.utils.exceptions import error_handler, initialize_except_hook
from cleansweep.utils.logging import set_app_labels, setup_logging

logger = setup_logging(__app_name__, settings.log_level, settings.dev_mode)
"""Logger for the module."""

initialize_except_hook(uncaught_hook=error_handler)


def main():
    """Process chunking steps."""
    input_files = InputFiles()

    if input_files.config_file_uri is not None:
        logger.info("Config file: %s", input_files.config_file_uri)

    logger.info("Loading settings...")
    app = load_settings(ChunkSettings, input_files.config_file_uri)
    embedding = load_settings(EmbeddingSettings, input_files.config_file_uri)
    assert not isinstance(embedding.model, str), "Model must be a Deployment object."
    translation_settings = load_settings(
        TranslationSettings, input_files.config_file_uri
    )
    assert not isinstance(
        translation_settings.model, str
    ), "Model must be a Deployment object."

    set_app_labels(app.labels)

    input_files.initialize_input_file_uri(app.source)

    target_file = create_target_file_path(
        bucket=app.output.bucket or app.staging_bucket,
        target_dir=app.output.directory or "chunked",
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
            directory="chunked",
            extension=".avro",
            prefix=app.source.prefix,
        ),
        force=app.force,
    )

    chunked_df = None

    if to_process is not None and not to_process.empty:
        plugin_manager = None
        # execute the pre chunk hooks
        if app.plugin is not None:
            logger.info("🔌 Applying plugins...")
            plugin = get_plugin_module(app.plugin)
            plugin_manager = get_plugin_manager()
            plugin_manager.register(plugin)

            results = plugin_manager.hook.pre_chunk(documents=to_process)
            if results:
                to_process = results[0]

        strategy, strategy_repository = app.strategy_settings

        logger.info("Chunking strategy set to %s", strategy)

        logger.info("Chunking documents...")
        chunked_df = create_chunked_df(
            to_process, "content", strategy, strategy_repository
        )
        logger.info("Chunking complete. %s chunks created.", len(chunked_df.index))

        length_function = (
            num_tokens_from_strings
            if embedding.embedder_type == EmbedderType.OPENAI
            else len
        )

        chunked_df["length"] = chunked_df["chunk"].apply(length_function)

        # execute the post chunk hooks
        if app.plugin is not None:

            logger.info("🔌 Applying plugins...")
            if plugin_manager is None:
                plugin = get_plugin_module(app.plugin)
                plugin_manager = get_plugin_manager()
                plugin_manager.register(plugin)

            logger.info("Executing post_chunk hook...")
            results = plugin_manager.hook.post_chunk(documents=chunked_df)
            if results:
                to_process = results[0]
    elif app.load_type in [LoadType.DELTA, LoadType.INCREMENTAL]:
        # we could have only deletions so there are no records to process
        # but we still need to load the previous_df
        logger.info("No records to process. Loading previous_df.")
        chunked_df = previous_df
        previous_df = None
    else:
        logger.info("No records to process. Exiting...")
        exit(0)

    assert chunked_df is not None, "Chunked dataframe is None."
    final_df = delta_merge(chunked_df, previous_df)

    write_to_storage(final_df, target_file)
    exit(0)
