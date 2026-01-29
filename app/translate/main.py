"""Module for translating text data."""

from pydantic import create_model

from cleansweep import __app_name__
from cleansweep.core import read_curated_file_to_dataframe, write_to_storage
from cleansweep.core.delta import delta_merge, delta_prepare
from cleansweep.core.fileio import create_glob_pattern, create_target_file_path
from cleansweep.enumerations import LoadType
from cleansweep.model.network import raw_path
from cleansweep.settings.base import settings
from cleansweep.settings.files import InputFiles
from cleansweep.settings.load import load_settings
from cleansweep.settings.translation import TranslationSettings
from cleansweep.translate import create_translated_df
from cleansweep.utils.exceptions import error_handler, initialize_except_hook
from cleansweep.utils.logging import set_app_labels, setup_logging
from cleansweep.utils.pydantic import create_simple_model

logger = setup_logging(__app_name__, settings.log_level, settings.dev_mode)
"""Logger for the module."""

initialize_except_hook(uncaught_hook=error_handler)


def main():
    """Process translation."""
    input_files = InputFiles()

    if input_files.config_file_uri is not None:
        logger.info("Config file: %s", input_files.config_file_uri)

    logger.info("Loading settings...")
    app = load_settings(TranslationSettings, input_files.config_file_uri)
    assert not isinstance(app.model, str), "Model must be a Deployment object."
    set_app_labels(app.labels)

    input_files.initialize_input_file_uri(app.source)

    target_file = create_target_file_path(
        bucket=app.output.bucket or app.staging_bucket,
        target_dir=app.output.directory or "translated",
        source_file=raw_path(input_files.input_file),
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
            directory=app.output.directory or "translated",
            extension=".avro",
            prefix=app.source.prefix,
        ),
        force=app.force,
    )
    translated_df = None

    if to_process is not None and not to_process.empty:
        # create response schema and type
        response_type = create_simple_model("ResponseType", app.fields_to_translate)
        response_schema = create_model("ResponseSchema", **{"items": (list[response_type | None], ...)})  # type: ignore

        logger.info("🗣️ Translating...")

        translated_df = create_translated_df(
            to_process,
            app.fields_to_translate,
            "metadata_language",
            app.target_language,
            settings.prompts_template_dir,
            app.prompt,
            app.model,
            app.token_limit,
            app.temperature,
            settings.timeouts.translate,
            response_type=response_type,
            response_schema=response_schema,
        )
    elif app.load_type in [LoadType.DELTA, LoadType.INCREMENTAL]:
        # we could have only deletions so there are no records to process
        # but we still need to load the previous_df
        logger.info("No records to process. Loading previous_df.")
        translated_df = previous_df
        previous_df = None
    else:
        logger.info("No records to process. Exiting...")
        exit(0)

    assert translated_df is not None, "Translated dataframe is None."

    final_df = delta_merge(translated_df, previous_df)

    write_to_storage(final_df, target_file)
    exit(0)
