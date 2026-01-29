"""Module for clustering generated question and answer pairs."""

from cleansweep import __app_name__
from cleansweep.chunk.semantic import create_clustered_dataframe
from cleansweep.core import read_curated_file_to_dataframe, write_to_storage
from cleansweep.core.delta import (
    DeltaComparison,
    delta_load_and_compare,
    load_delta_file,
)
from cleansweep.core.fileio import create_glob_pattern, create_target_file_path
from cleansweep.enumerations import LoadType
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
            directory="semantic/chunked",
            extension=".avro",
        )
    )

    target_file = create_target_file_path(
        bucket=app.staging_bucket,
        target_dir="semantic/clustered",
        source_file=raw_path(input_files.input_file),
        extension=".avro",
        run_id=app.run_id,
    )

    logger.info("📖 Reading curated data file %s.", input_files.input_file)
    qa_pairs_df = read_curated_file_to_dataframe(input_files.input_file)

    logger.info("📚 Read %d documents.", len(qa_pairs_df.index))

    if app.load_type in [LoadType.DELTA, LoadType.INCREMENTAL] and app.force is False:
        # add embeddings for questions which already exist in the previous clusters
        previous_clusters_df = load_delta_file(
            app.staging_bucket,
            create_glob_pattern(directory="semantic/clustered", extension="avro"),
        )
        if previous_clusters_df is not None:

            merge_columns = [
                cdef.cluster_name
                for cdef in app.semantic.cluster_definitions
                if cdef.cluster_name in previous_clusters_df.columns
            ]

            if merge_columns:

                merge_columns.append("question_id")

                qa_pairs_df = qa_pairs_df.merge(
                    previous_clusters_df[merge_columns],
                    how="left",
                    on="question_id",
                    suffixes=(None, "_delta"),
                )
                # delta merges can cause duplicate rows along question_uuid, remove them
                qa_pairs_df.drop_duplicates(
                    subset=["question_uuid"], keep="first", inplace=True
                )

    clustered_df = create_clustered_dataframe(
        qa_pairs_df,
        app.semantic.embedder_type,
        app.semantic.embedding_model,
        app.semantic.cluster_definitions,
        token_limit=app.semantic.token_limit,
        store=app.staging_bucket,
        cluster_config=app.semantic.cluster_config,
    )

    # delta compare - have we created this cluster before?
    if app.load_type in [LoadType.DELTA, LoadType.INCREMENTAL]:
        clustered_df = delta_load_and_compare(
            clustered_df,
            [DeltaComparison(left="cluster_uuid", output="cluster_action")],
            app.staging_bucket,
            match_glob=create_glob_pattern(
                directory="semantic/clustered", extension="avro"
            ),
            id_column="question_id",
            dedupe_columns=["question_uuid"],
        )
    else:
        clustered_df["cluster_action"] = "I"

    write_to_storage(clustered_df, target_file)
    exit(0)
