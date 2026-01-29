"""Module for chunking and translating text data."""

import pandas as pd

from cleansweep import __app_name__
from cleansweep.chunk.semantic import (
    create_question_answer_dataframe,
    create_question_answer_pairs,
)
from cleansweep.clean.filter import Filter
from cleansweep.core import (
    get_plugin_module,
    read_curated_file_to_dataframe,
    write_to_storage,
)
from cleansweep.core.delta import (
    delta_merge,
    delta_prepare,
    delta_processed,
    load_delta_file,
)
from cleansweep.core.fileio import create_glob_pattern, create_target_file_path
from cleansweep.enumerations import LoadType
from cleansweep.hooks.hookimpl import get_plugin_manager
from cleansweep.model.network import raw_path
from cleansweep.settings.base import settings
from cleansweep.settings.chunk import ChunkSettings
from cleansweep.settings.files import InputFiles
from cleansweep.settings.load import load_settings
from cleansweep.settings.translation import TranslationSettings
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
    translation_settings = load_settings(
        TranslationSettings, input_files.config_file_uri
    )

    assert not isinstance(app.semantic.model, str), "Model must be a Deployment object."
    set_app_labels(app.labels)

    input_files.initialize_input_file_uri(app.source)

    target_file = create_target_file_path(
        bucket=app.staging_bucket,
        target_dir="semantic/chunked",
        source_file=raw_path(input_files.input_file),
        extension=".avro",
        run_id=app.run_id,
    )

    logger.info("📖 Reading curated data file %s.", input_files.input_file)
    source_df = read_curated_file_to_dataframe(input_files.input_file)

    logger.info("📚 Read %d documents.", len(source_df.index))

    to_process, previous_df = delta_prepare(
        source_df,
        app.load_type,
        app.staging_bucket,
        force=app.force,
        match_glob=create_glob_pattern(
            directory=app.output.directory or "semantic/chunked",
            extension="avro",
        ),
        id_column=("id", "source_id"),
    )

    chunk_df = None

    if to_process is not None and not to_process.empty:
        logger.info("Processing %d documents.", len(to_process.index))
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

        to_process["output_language"] = translation_settings.target_language.name

        logger.info(
            "Creating QA pairs from articles with %s model...",
            app.semantic.model.model,
        )
        chunk_df, failed_question_df = create_question_answer_dataframe(
            to_process,
            create_question_answer_pairs(
                to_process,
                settings.prompts_template_dir,
                app.semantic.qa_prompt,
                app.semantic.model,
                temperature=app.temperature,
            ),
        )

        if failed_question_df is not None:
            logger.info("Attempting to retry failed questions...")
            retry_results, _ = create_question_answer_dataframe(
                failed_question_df,
                create_question_answer_pairs(
                    to_process,
                    settings.prompts_template_dir,
                    app.semantic.qa_prompt,
                    app.semantic.model,
                    temperature=app.temperature,
                ),
            )

            if not retry_results.empty:
                logger.info("Adding successful retries to the chunked data.")
                chunk_df = pd.concat([chunk_df, retry_results], ignore_index=True)

        if chunk_df.empty:
            logger.warning("No question answer pairs were created.")
            chunk_df = None
        else:
            chunk_df["metadata_language"] = pd.merge(
                chunk_df,
                to_process,
                left_on="source_id",
                right_on="id",
                how="left",
                suffixes=("", "_y"),
            )["metadata_language"]

            # assign an action to each record
            chunk_df["action"] = "I"

        if app.load_type == LoadType.DELTA and chunk_df is not None:
            previous_df = None
            # get the source articles - if the previous article is not in this list then it should be
            # dropped.
            cleaned_articles_df = load_delta_file(
                app.staging_bucket,
                create_glob_pattern(
                    directory="cleaned", extension="avro", run_id=app.run_id
                ),
            )
            if cleaned_articles_df is None:
                raise FileNotFoundError("No source articles found.")
            previous_df = delta_processed(
                chunk_df,
                app.staging_bucket,
                match_glob=create_glob_pattern(
                    directory="semantic/chunked", extension="avro"
                ),
                id_column="source_id",
                func=Filter.filter_by_column,
                column="source_id",
                value=cleaned_articles_df["id"].to_list(),
                operator="in",
            )
        elif app.load_type == LoadType.DELTA and chunk_df is None:
            chunk_df = previous_df
            previous_df = None
        else:
            previous_df = None

    elif app.load_type in [LoadType.DELTA, LoadType.INCREMENTAL]:
        # we could have only deletions so there are no records to process
        # but we still need to load the previous_df
        logger.info("No records to process. Loading previous_df.")
        chunk_df = previous_df
        previous_df = None
    else:
        logger.info("No records to process. Exiting...")
        exit(0)

    assert chunk_df is not None, "Chunked dataframe is None."
    final_df = delta_merge(chunk_df, previous_df)

    write_to_storage(final_df, target_file)
    exit(0)
