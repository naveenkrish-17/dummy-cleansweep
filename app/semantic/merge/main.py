"""Module for chunking and translating text data."""

import pandas as pd

from cleansweep import __app_name__
from cleansweep._types import MergeConfig, RecursiveMergeConfig, TranslationConfig
from cleansweep.chunk.semantic import merge_question_answer_pairs
from cleansweep.clean.filter import Filter
from cleansweep.core import read_curated_file_to_dataframe, write_to_storage
from cleansweep.core.delta import (
    delta_merge,
    delta_prepare,
    delta_processed,
    delta_to_process,
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
from cleansweep.settings.translation import TranslationSettings
from cleansweep.utils.exceptions import error_handler, initialize_except_hook
from cleansweep.utils.google.storage import fs
from cleansweep.utils.logging import set_app_labels, setup_logging

logger = setup_logging(__app_name__, settings.log_level, settings.dev_mode)
"""Logger for the Semantic Merge module."""

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
            directory="semantic/clustered",
            extension=".avro",
        )
    )

    target_file = create_target_file_path(
        bucket=app.staging_bucket,
        target_dir="semantic/merged",
        source_file=raw_path(input_files.input_file),
        extension=".avro",
        run_id=app.run_id,
    )

    logger.info("📖 Reading curated data file %s.", input_files.input_file)
    source_df = read_curated_file_to_dataframe(input_files.input_file)

    logger.info("📚 Read %d documents.", len(source_df.index))

    to_process = delta_to_process(
        source_df, app.load_type, action_column="cluster_action"
    )

    merged_df = None
    source_articles_suffix = "_source_article"

    if to_process is not None and not to_process.empty:
        store = fs().get_bucket(app.staging_bucket)

        recursive_config = RecursiveMergeConfig(
            cluster_definitions=app.semantic.cluster_definitions,
            store=store,
            embedder_type=app.semantic.embedder_type,
            embedding_model=app.semantic.embedding_model,
            token_limit=app.semantic.token_limit,
            cluster_model_config=app.semantic.cluster_config,
            max_cluster_distance=app.semantic.recursive_merge.max_cluster_distance,
            min_cluster_distance=app.semantic.recursive_merge.min_cluster_distance,
            max_cluster_size=app.semantic.recursive_merge.max_cluster_size,
            step_size=app.semantic.recursive_merge.step_size,
        )

        translation_settings = load_settings(
            TranslationSettings, input_files.config_file_uri
        )
        assert not isinstance(
            translation_settings.model, str
        ), "Model must be a Deployment object."
        set_app_labels(app.labels)

        translation_config = TranslationConfig(
            prompt=translation_settings.prompt,
            model=translation_settings.model,
            target_language=translation_settings.target_language,
            token_limit=translation_settings.token_limit,
            temperature=translation_settings.temperature,
        )

        config = MergeConfig(
            prompt_dir=settings.prompts_template_dir,
            merge_prompt=app.semantic.merge_prompt,
            model=app.semantic.model,
            recursive_merge=recursive_config,
            translation_config=translation_config,
        )

        logger.info("Merging QA pairs...")

        merged_df = merge_question_answer_pairs(
            df=to_process,
            config=config,
            temperature=app.temperature,
        )

        logger.info(
            "Merged QA pairs to %s unique questions",
            len(merged_df["question_id"].unique()),
        )
        logger.info(
            "%s questions have been dropped.",
            len(to_process) - len(merged_df),
        )

        # assign an action to each record
        merged_df["question_action"] = "I"
        previous_df = None

        if app.load_type == LoadType.DELTA:
            # get data for clusters identified that have been processed before.
            old_clusters = source_df[
                ~source_df["cluster_uuid"].isin(to_process["cluster_uuid"])
            ]

            previous_df = delta_processed(
                merged_df,
                app.staging_bucket,
                match_glob=create_glob_pattern(
                    directory=app.output.directory or "semantic/merged",
                    extension="avro",
                ),
                id_column="cluster_uuid",
                action_column="question_action",
                func=Filter.filter_by_column,
                column="cluster_uuid",
                value=old_clusters["cluster_uuid"].tolist(),
                operator="in",
            )
        elif app.load_type == LoadType.INCREMENTAL:
            _, previous_df = delta_prepare(
                source_df,
                app.load_type,
                app.staging_bucket,
                force=app.force,
                action_column="question_action",
                match_glob=create_glob_pattern(
                    directory=app.output.directory or "semantic/merged",
                    extension="avro",
                ),
                id_column="source_id",
            )

    elif app.load_type in [LoadType.DELTA, LoadType.INCREMENTAL]:
        # we could have only deletions so there are no records to process
        # but we still need to load the previous_df
        logger.info("No records to process. Loading previous_df.")
        merged_df = delta_processed(
            to_process,
            app.staging_bucket,
            match_glob=create_glob_pattern(
                directory="semantic/merged", extension="avro"
            ),
            id_column="question_id",
            action_column="question_action",
        )
        previous_df = None

        # remove source article columns
        assert merged_df is not None, "Merged dataframe is None."
        columns_to_drop = merged_df.columns[
            merged_df.columns.str.endswith(source_articles_suffix)
        ].tolist()
        merged_df.drop(columns=columns_to_drop, inplace=True, errors="ignore")

    else:
        logger.info("No records to process. Exiting...")
        exit(0)

    assert merged_df is not None, "Merged dataframe is None."

    if previous_df is not None:
        # ensure we aren't inserting duplicate question_uuids
        previous_df = previous_df[
            ~previous_df["question_uuid"].isin(merged_df["question_uuid"])
        ]

    final_df = delta_merge(merged_df, previous_df)
    # delta merges can cause duplicate rows along question_uuid, remove them
    final_df.drop_duplicates(subset=["question_uuid"], keep="first", inplace=True)

    logger.info("Searching for source articles...")
    cleaned_articles_df = load_delta_file(
        app.staging_bucket,
        create_glob_pattern(
            directory=app.semantic.merge_source.source_dir,
            extension=app.semantic.merge_source.source_extension,
            run_id=app.run_id,
            prefix=app.semantic.merge_source.source_prefix,
        ),
    )
    if cleaned_articles_df is None:
        raise FileNotFoundError("No source articles found.")

    logger.info("📚 Read %d source articles", len(cleaned_articles_df.index))
    logger.info("Merge consolidated QA pairs with source articles...")

    article_columns = [
        "id",
        "title",
        "action",
        "content",
        "content_type",
        "source_file",
    ]
    drop_columns = [
        col
        for col in cleaned_articles_df.columns
        if col not in article_columns and not col.startswith("metadata_")
    ]

    # list of columns that we want to drop
    static_drop_columns = ["metadata_md5_prev"]
    for col in static_drop_columns:
        if col in cleaned_articles_df.columns:
            drop_columns.append(col)

    # drop action, we'll add the original value from the source docs
    final_df.drop(["action"], axis=1, inplace=True, errors="ignore")

    # drop any columns that are not needed
    cleaned_articles_df.drop(drop_columns, axis=1, inplace=True, errors="ignore")

    with_article_df = pd.merge(
        final_df,
        cleaned_articles_df,
        how="inner",
        left_on="source_id",
        right_on="id",
        suffixes=("", source_articles_suffix),
    )

    # compare questions with previous run and set action
    if app.load_type in [LoadType.DELTA, LoadType.INCREMENTAL]:
        previous_merge_df = load_delta_file(
            app.staging_bucket,
            create_glob_pattern(directory="semantic/merged", extension="avro"),
        )

        if previous_merge_df is not None:
            # add a column to with_article_df to indicate if the question_id is in the previous_merge_df
            with_article_df["question_id_prev"] = with_article_df["question_id"].isin(
                previous_merge_df["question_id"]
            )
            deleted_questions = (
                previous_merge_df["question_id"]
                .isin(with_article_df["question_id"])
                .tolist()
            )
            deleted_questions = [r for r in deleted_questions if r is False]
        else:
            with_article_df["question_id_prev"] = [None] * with_article_df.shape[0]
            deleted_questions = []

        # set action where action == 'N' and question_id_prev is not null
        with_article_df["action"] = with_article_df.apply(
            lambda x: ("N" if x["question_id_prev"] is True else x["question_action"]),
            axis=1,
        )
        logger.info(
            "New or changed questions: %s",
            len(with_article_df[with_article_df["action"].isin(["U", "I"])].index),
        )

        logger.info(
            "Unchanged questions: %s",
            len(with_article_df[with_article_df["action"].isin(["N", "D"])].index),
        )

        logger.info(
            "Deleted questions: %s",
            len(deleted_questions),
        )

        # drop question_id_prev
        with_article_df.drop(
            ["question_id_prev"], axis=1, inplace=True, errors="ignore"
        )

    else:
        with_article_df["action"] = "I"

    logger.info(
        "Merged QA pairs with source articles to %d unique questions",
        len(with_article_df["question_id"].unique()),
    )
    logger.info(
        "%s questions have been dropped.",
        len(final_df) - len(with_article_df),
    )

    write_to_storage(with_article_df, target_file)
    exit(0)
