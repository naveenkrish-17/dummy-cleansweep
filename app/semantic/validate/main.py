"""Module for chunking and translating text data."""

from cleansweep import __app_name__
from cleansweep.chunk.semantic import validate_questions
from cleansweep.core import (
    get_plugin_module,
    read_curated_file_to_dataframe,
    write_dataframe_to_avro_file,
    write_to_storage,
)
from cleansweep.core.delta import delta_merge, delta_prepare
from cleansweep.core.fileio import create_glob_pattern, create_target_file_path
from cleansweep.enumerations import LoadType
from cleansweep.hooks.hookimpl import get_plugin_manager
from cleansweep.model.network import raw_path
from cleansweep.settings._types import SourceObject
from cleansweep.settings.base import settings
from cleansweep.settings.chunk import ChunkSettings
from cleansweep.settings.files import InputFiles
from cleansweep.settings.load import load_settings
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
    assert not isinstance(app.semantic.model, str), "Model must be a Deployment object."
    assert not isinstance(
        app.semantic.embedding_model, str
    ), "Model must be a Deployment object."
    set_app_labels(app.labels)

    input_files.initialize_input_file_uri(
        SourceObject(
            bucket=app.staging_bucket,
            directory="semantic/merged",
            extension=".avro",
        )
    )

    target_file = create_target_file_path(
        source_file=raw_path(input_files.input_file),
        bucket=app.output.bucket or app.staging_bucket,
        target_dir=app.output.directory or "chunked",
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
        force=app.force,
        match_glob=create_glob_pattern(
            directory=app.output.directory or "semantic/validated",
            extension="avro",
        ),
        action_column="action",
        id_column="question_id",
    )

    validated_df = None

    if to_process is not None and not to_process.empty:

        logger.info("Validating QA pairs...")

        validated_df = validate_questions(
            to_process[to_process["source_id"].notnull()],
            settings.prompts_template_dir,
            app.semantic.validation_prompt,
            app.semantic.model,
            temperature=app.temperature,
        )

        # summarise the validation results
        grp_val = validated_df.copy().groupby("rating").size().reset_index(name="count")  # type: ignore
        total_count = grp_val["count"].sum()
        grp_val["percent"] = (grp_val["count"] / total_count) * 100
        summary = grp_val.sort_values(by="count", ascending=False).reset_index(
            drop=True
        )
        for _, row in summary.iterrows():
            logger.info(
                "%s %s - %s (%.2f%%)",
                "✅" if row["rating"] == "consistent" else "❌",
                row["rating"],
                row["count"],
                row["percent"],
            )

    elif app.load_type in [LoadType.DELTA, LoadType.INCREMENTAL]:
        # we could have only deletions so there are no records to process
        # but we still need to load the previous_df
        logger.info("No records to process. Loading previous_df.")
        validated_df = previous_df
        previous_df = None
    else:
        logger.info("No records to process. Exiting...")
        exit(0)

    assert validated_df is not None, "There are no records to process."

    output_df = delta_merge(validated_df, previous_df)

    validated_file = create_target_file_path(
        target_dir="semantic/validated",
        bucket=app.staging_bucket,
        source_file=raw_path(input_files.input_file),
        extension=".avro",
        run_id=app.run_id,
    )

    logger.info("📝 Writing validated QA dataframe to %s", validated_file)
    write_dataframe_to_avro_file(output_df, validated_file)

    logger.info("🗑️ Removing inconsistent questions...")

    # refactor dataframe
    output_df = output_df[output_df["rating"] == "consistent"]
    output_df.reset_index(drop=True, inplace=True)
    if "embedding" in output_df.columns:
        output_df.drop("embedding", axis=1, inplace=True)

    # execute the post chunk hooks
    if app.plugin is not None:

        logger.info("🔌 Applying plugins...")
        plugin = get_plugin_module(app.plugin)
        plugin_manager = get_plugin_manager()
        plugin_manager.register(plugin)

        results = plugin_manager.hook.post_chunk(documents=output_df)
        if results:
            to_process = results[0]

    write_to_storage(output_df, target_file)
    exit(0)
